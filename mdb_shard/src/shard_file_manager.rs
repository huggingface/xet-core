use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use async_trait::async_trait;
use merklehash::{HMACKey, MerkleHash};
use tokio::sync::RwLock;
use tracing::{debug, info, instrument, trace};

use crate::cas_structs::*;
use crate::constants::{
    CHUNK_INDEX_TABLE_MAX_SIZE, MDB_SHARD_EXPIRATION_BUFFER_SECS, MDB_SHARD_MIN_TARGET_SIZE, SHARD_CACHE_SIZE_LIMIT,
};
use crate::error::{MDBShardError, Result};
use crate::file_structs::*;
use crate::shard_file_handle::MDBShardFile;
use crate::shard_file_reconstructor::FileReconstructor;
use crate::shard_in_memory::MDBInMemoryShard;
use crate::utils::truncate_hash;

// The shard manager cache
lazy_static::lazy_static! {
    static ref MDB_SHARD_FILE_MANAGER_CACHE: RwLock<HashMap<PathBuf, Arc<ShardFileManager>>> = RwLock::new(HashMap::default());
}

// The structure used as the target for the dedup lookup
#[repr(Rust, packed)]
struct ChunkCacheElement {
    cas_start_index: u32, // the index of the first chunk
    cas_chunk_offset: u16,
    shard_index: u16, // This one is last so that the u16 bits can be packed in.
}

#[derive(Default)]
struct KeyedShardCollection {
    hmac_key: HMACKey,
    shard_list: Vec<Arc<MDBShardFile>>,
    chunk_lookup: HashMap<u64, ChunkCacheElement>,
}

impl KeyedShardCollection {
    fn new(hmac_key: HMACKey) -> Self {
        Self {
            hmac_key,
            ..Default::default()
        }
    }
}

/// We have a couple levels of arc redirection here to handle the different queries needed.  
/// This just holds the two things needed for lookup.
#[derive(Default)]
struct ShardBookkeeper {
    shard_collections: Vec<KeyedShardCollection>,
    collection_by_key: HashMap<HMACKey, usize>,
    shard_lookup_by_shard_hash: HashMap<MerkleHash, (usize, usize)>,

    // We cap the number of chunks indexed for dedup; beyond those, we simply drop the search.
    total_indexed_chunks: usize,
}

impl ShardBookkeeper {
    fn new() -> Self {
        // In creating the bookkeeping class, put the one without the hmac key in first so
        // we always try to dedup locally first.
        Self {
            shard_collections: vec![KeyedShardCollection::new(HMACKey::default())],
            collection_by_key: HashMap::from([(HMACKey::default(), 0)]),
            ..Default::default()
        }
    }
}

pub struct ShardFileManager {
    shard_bookkeeper: RwLock<ShardBookkeeper>,
    current_state: RwLock<MDBInMemoryShard>,
    shard_directory: PathBuf,
    target_shard_min_size: u64,
    shard_directory_cleaned: AtomicBool,
}

/// Shard file manager to manage all the shards.  It is fully thread-safe and async enabled.
///
/// Usage:
///
/// // Session directory is where it stores shard and shard state.
/// let mut mng = ShardFileManager::new("<session_directory>")
///
/// // Add other known shards with register_shards.
/// mng.register_shards(&[other shard, directories, etc.])?;
///
/// // Run queries, add data, etc. with get_file_reconstruction_info, chunk_hash_dedup_query,
/// add_cas_block, add_file_reconstruction_info.
///
/// // Finalize by calling process_session_directory
/// let new_shards = mdb.process_session_directory()?;
///
/// // new_shards is the list of new shards for this session.
impl ShardFileManager {
    // Construct in a session directory.
    pub async fn new_in_session_directory(
        session_directory: impl AsRef<Path>,
        scan_directory: bool,
    ) -> Result<Arc<Self>> {
        Self::new_impl(session_directory, false, *MDB_SHARD_MIN_TARGET_SIZE, scan_directory, 0).await
    }

    // Construction functions
    pub async fn new_in_cache_directory(cache_directory: impl AsRef<Path>) -> Result<Arc<Self>> {
        Self::new_impl(cache_directory, true, *MDB_SHARD_MIN_TARGET_SIZE, true, *SHARD_CACHE_SIZE_LIMIT).await
    }

    async fn new_impl(
        directory: impl AsRef<Path>,
        is_cachable: bool,
        target_shard_min_size: u64,
        scan_directory: bool,
        prune_cache_to_size: u64, // Set to 0 to disable pruning
    ) -> Result<Arc<Self>> {
        let shard_directory = std::path::absolute(directory)?;

        // Make sure the shard directory exists; create it if not.
        if !shard_directory.exists() {
            std::fs::create_dir_all(&shard_directory)?;
        }

        let create_new_sfm = || {
            Arc::new(Self {
                shard_bookkeeper: RwLock::new(ShardBookkeeper::new()),
                current_state: RwLock::new(MDBInMemoryShard::default()),
                shard_directory: shard_directory.clone(),
                target_shard_min_size,
                shard_directory_cleaned: AtomicBool::new(false),
            })
        };

        let sfm = 'load_sfm: {
            if !is_cachable {
                break 'load_sfm create_new_sfm();
            }

            {
                let ro_lg = MDB_SHARD_FILE_MANAGER_CACHE.read().await;

                if let Some(sfm) = ro_lg.get(&shard_directory) {
                    sfm.refresh_shard_dir(false, 0).await?;
                    break 'load_sfm sfm.clone();
                }
            }

            // Now, create and insert it.
            let mut rw_lg = MDB_SHARD_FILE_MANAGER_CACHE.write().await;
            let sfm_entry = rw_lg.entry(shard_directory.clone());

            // See if it's in there; insert otherwise
            match sfm_entry {
                std::collections::hash_map::Entry::Vacant(sfm_slot) => {
                    let sfm = create_new_sfm();
                    sfm_slot.insert(sfm.clone());
                    sfm
                },
                std::collections::hash_map::Entry::Occupied(sfm) => sfm.get().clone(),
            }
        };

        if scan_directory {
            sfm.refresh_shard_dir(true, prune_cache_to_size).await?;
        }

        Ok(sfm)
    }

    pub async fn refresh_shard_dir(&self, prune_expired: bool, prune_cache_to_size: u64) -> Result<()> {
        let mut shard_files = MDBShardFile::load_managed_directory(
            &self.shard_directory,
            true,
            false,
            prune_expired,
            prune_cache_to_size,
        )?;

        {
            let shard_read_guard = self.shard_bookkeeper.read().await;
            shard_files.retain(|s| !shard_read_guard.shard_lookup_by_shard_hash.contains_key(&s.shard_hash));
        }

        self.register_shards(&shard_files).await?;

        Ok(())
    }

    pub fn shard_directory(&self) -> &Path {
        &self.shard_directory
    }

    // Clean out expired shards, with an expiration deletion window.
    pub fn clean_expired_shards_if_needed(&self) -> Result<()> {
        let needs_clean = self.shard_directory_cleaned.swap(true, std::sync::atomic::Ordering::Relaxed);

        if needs_clean {
            MDBShardFile::clean_shard_cache(&self.shard_directory, *MDB_SHARD_EXPIRATION_BUFFER_SECS)?;
        }

        Ok(())
    }

    pub async fn register_shards_by_path<P: AsRef<Path>>(&self, new_shards: &[P]) -> Result<()> {
        let new_shards: Vec<Arc<_>> = new_shards.iter().try_fold(Vec::new(), |mut acc, p| {
            acc.extend(MDBShardFile::load_all_valid(p)?);

            Result::Ok(acc)
        })?;

        self.register_shards(&new_shards).await
    }

    pub async fn register_shards(&self, new_shards: &[Arc<MDBShardFile>]) -> Result<()> {
        let mut sbkp_lg = self.shard_bookkeeper.write().await;

        // Go through and register the shards in order of newest to oldest
        let mut new_shards = Vec::from(new_shards);

        // Compare in reverse order to sort from newest to oldest
        new_shards.sort_by(|s1, s2| s2.last_modified_time.cmp(&s1.last_modified_time));
        let num_shards = new_shards.len();

        for s in new_shards {
            s.verify_shard_integrity_debug_only();

            // Make sure the shard is in the shard directory
            debug_assert!(
                s.path.starts_with(&self.shard_directory),
                "{:?} not in {:?}",
                &s.path,
                &self.shard_directory
            );

            if sbkp_lg.shard_lookup_by_shard_hash.contains_key(&s.shard_hash) {
                continue;
            }

            debug!("register_shards: Registering shard {:?} at {:?}.", s.shard_hash, s.path);

            let shard_hmac_key = s.shard.metadata.chunk_hash_hmac_key;

            let n_current_collections = sbkp_lg.shard_collections.len();
            let shard_col_index: usize =
                *sbkp_lg.collection_by_key.entry(shard_hmac_key).or_insert(n_current_collections);

            // do we actually need to insert it?
            if shard_col_index == n_current_collections {
                sbkp_lg.shard_collections.push(KeyedShardCollection::new(shard_hmac_key));
            }

            let update_chunk_lookup = sbkp_lg.total_indexed_chunks < *CHUNK_INDEX_TABLE_MAX_SIZE;

            // Now add in the chunk indices.
            let shard_index;
            let num_inserted_chunks;
            {
                let shard_col = &mut sbkp_lg.shard_collections[shard_col_index];
                shard_index = shard_col.shard_list.len();
                shard_col.shard_list.push(s.clone());

                let old_chunk_lookup_size = shard_col.chunk_lookup.len();

                if update_chunk_lookup {
                    let insert_hashes = s.read_all_truncated_hashes()?;

                    shard_col.chunk_lookup.reserve(insert_hashes.len());

                    for (h, (cas_start_index, cas_chunk_offset)) in insert_hashes {
                        if cas_chunk_offset > u16::MAX as u32 {
                            continue;
                        }

                        let cas_chunk_offset = cas_chunk_offset as u16;

                        shard_col.chunk_lookup.insert(
                            h,
                            ChunkCacheElement {
                                cas_start_index,
                                cas_chunk_offset,
                                shard_index: shard_index as u16,
                            },
                        );
                    }
                }

                num_inserted_chunks = shard_col.chunk_lookup.len() - old_chunk_lookup_size;
            }

            sbkp_lg
                .shard_lookup_by_shard_hash
                .insert(s.shard_hash, (shard_col_index, shard_index));

            sbkp_lg.total_indexed_chunks += num_inserted_chunks;
        }

        if num_shards != 0 {
            debug!("Registered {num_shards} new shards.");
        }

        Ok(())
    }

    pub async fn shard_is_registered(&self, shard_hash: &MerkleHash) -> bool {
        self.shard_bookkeeper
            .read()
            .await
            .shard_lookup_by_shard_hash
            .contains_key(shard_hash)
    }

    pub async fn all_file_info(&self) -> Result<Vec<MDBFileInfo>> {
        // Start with everything that is now in-memory
        let mut all_file_info: Vec<MDBFileInfo> =
            self.current_state.read().await.file_content.values().cloned().collect();

        let shard_files = MDBShardFile::load_all_valid(&self.shard_directory)?;

        for shard in shard_files {
            all_file_info.append(&mut shard.read_all_file_info_sections()?);
        }

        Ok(all_file_info)
    }

    pub async fn registered_shard_list(&self) -> Result<Vec<Arc<MDBShardFile>>> {
        let shards = self.shard_bookkeeper.read().await;

        Ok(shards
            .shard_lookup_by_shard_hash
            .values()
            .map(|&(i, j)| shards.shard_collections[i].shard_list[j].clone())
            .collect())
    }
}

#[async_trait]
impl FileReconstructor<MDBShardError> for ShardFileManager {
    // Given a file pointer, returns the information needed to reconstruct the file.
    // The information is stored in the destination vector dest_results.  The function
    // returns true if the file hash was found, and false otherwise.
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        if *file_hash == MerkleHash::default() {
            return Ok(Some((MDBFileInfo::default(), None)));
        }

        // First attempt the in-memory version of this.
        {
            let lg = self.current_state.read().await;
            let file_info = lg.get_file_reconstruction_info(file_hash);
            if let Some(fi) = file_info {
                return Ok(Some((fi, None)));
            }
        }

        let current_shards = self.shard_bookkeeper.read().await;

        for sc in current_shards.shard_collections.iter() {
            for si in sc.shard_list.iter() {
                trace!("Querying for hash {file_hash:?} in {:?}.", si.path);
                if let Some(fi) = si.get_file_reconstruction_info(file_hash)? {
                    return Ok(Some((fi, Some(si.shard_hash))));
                }
            }
        }

        Ok(None)
    }
}

impl ShardFileManager {
    // Performs a query of chunk hashes against known chunk hashes, matching
    // as many of the values in query_hashes as possible.  It returns the number
    // of entries matched from the input hashes, the CAS block hash of the match,
    // and the range matched from that block.
    pub async fn chunk_hash_dedup_query(
        &self,
        query_hashes: &[MerkleHash],
    ) -> Result<Option<(usize, FileDataSequenceEntry)>> {
        // First attempt the in-memory version of this.
        {
            let lg = self.current_state.read().await;
            let ret = lg.chunk_hash_dedup_query(query_hashes);
            if ret.is_some() {
                return Ok(ret);
            }
        }

        let shard_lg = self.shard_bookkeeper.read().await;

        for shard_col in shard_lg.shard_collections.iter() {
            let query_hash = {
                if shard_col.hmac_key == HMACKey::default() {
                    truncate_hash(&query_hashes[0])
                } else {
                    truncate_hash(&query_hashes[0].hmac(shard_col.hmac_key))
                }
            };

            if let Some(cce) = shard_col.chunk_lookup.get(&query_hash) {
                let si = &shard_col.shard_list[cce.shard_index as usize];

                if let Some((count, fdse)) =
                    si.chunk_hash_dedup_query_direct(query_hashes, cce.cas_start_index, cce.cas_chunk_offset as u32)?
                {
                    return Ok(Some((count, fdse)));
                }
            }
        }

        Ok(None)
    }

    /// Add CAS info to the in-memory state.
    pub async fn add_cas_block(&self, cas_block_contents: impl Into<Arc<MDBCASInfo>>) -> Result<()> {
        let mut lg = self.current_state.write().await;

        lg.add_cas_block(cas_block_contents)?;

        // See if this put it over the target minimum size, allowing us to cut a new shard
        if lg.shard_file_size() >= self.target_shard_min_size {
            // Drop the lock guard before doing the flush.
            drop(lg);
            self.flush().await?;
        }

        Ok(())
    }

    /// Add file reconstruction info to the in-memory state.
    pub async fn add_file_reconstruction_info(&self, file_info: MDBFileInfo) -> Result<()> {
        let mut lg = self.current_state.write().await;

        lg.add_file_reconstruction_info(file_info)?;

        // See if this put it over the target minimum size, allowing us to cut a new shard
        if lg.shard_file_size() >= self.target_shard_min_size {
            // Drop the lock guard before doing the flush.
            drop(lg);
            self.flush().await?;
        }

        Ok(())
    }

    /// Flush the current state of the in-memory lookups to a shard in the session directory,
    /// returning the hash of the shard and the file written, or None if no file was written.
    #[instrument(skip_all, name = "ShardFileManager::flush")]
    pub async fn flush(&self) -> Result<Option<PathBuf>> {
        let new_shard_path;

        // The locked section here.
        {
            let mut lg = self.current_state.write().await;

            if lg.is_empty() {
                return Ok(None);
            }

            new_shard_path = lg.write_to_directory(&self.shard_directory, None)?;
            *lg = MDBInMemoryShard::default();

            info!("Shard manager flushed new shard to {new_shard_path:?}.");
        }

        // Load this one into our local shard catalog
        self.register_shards(&[MDBShardFile::load_from_file(&new_shard_path)?]).await?;

        Ok(Some(new_shard_path))
    }
}

impl ShardFileManager {
    /// Calculate the total materialized bytes (before deduplication) tracked by the manager,
    /// including in-memory state and on-disk shards.
    pub async fn calculate_total_materialized_bytes(&self) -> Result<u64> {
        let mut bytes = 0;
        {
            let lg = self.current_state.read().await;
            bytes += lg.materialized_bytes();
        }

        for ksc in self.shard_bookkeeper.read().await.shard_collections.iter() {
            for si in ksc.shard_list.iter() {
                bytes += si.shard.materialized_bytes();
            }
        }
        Ok(bytes)
    }

    /// Calculate the total stored bytes tracked (after deduplication) tracked by the manager,
    /// including in-memory state and on-disk shards.
    pub async fn calculate_total_stored_bytes(&self) -> Result<u64> {
        let mut bytes = 0;
        {
            let lg = self.current_state.read().await;
            bytes += lg.stored_bytes();
        }

        for ksc in self.shard_bookkeeper.read().await.shard_collections.iter() {
            for si in ksc.shard_list.iter() {
                bytes += si.shard.stored_bytes();
            }
        }

        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::min;
    use std::collections::HashSet;
    use std::time::{Duration, SystemTime};

    use rand::prelude::*;
    use tempfile::TempDir;

    use super::*;
    use crate::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader};
    use crate::error::Result;
    use crate::file_structs::FileDataSequenceHeader;
    use crate::session_directory::{consolidate_shards_in_directory, merge_shards};
    use crate::shard_format::test_routines::{gen_random_file_info, rng_hash, simple_hash};
    use crate::utils::parse_shard_filename;

    #[allow(clippy::type_complexity)]
    pub async fn fill_with_specific_shard(
        shard: &ShardFileManager,
        in_mem_shard: &mut MDBInMemoryShard,
        cas_nodes: &[(u64, &[(u64, u32)])],
        file_nodes: &[(u64, &[(u64, (u32, u32))])],
    ) -> Result<()> {
        for (hash, chunks) in cas_nodes {
            let mut cas_block = Vec::<_>::new();
            let mut pos = 0;

            for (h, s) in chunks.iter() {
                cas_block.push(CASChunkSequenceEntry::new(simple_hash(*h), *s, pos));
                pos += *s;
            }
            let cas_info = MDBCASInfo {
                metadata: CASChunkSequenceHeader::new(simple_hash(*hash), chunks.len(), pos),
                chunks: cas_block,
            };

            shard.add_cas_block(cas_info.clone()).await?;

            in_mem_shard.add_cas_block(cas_info)?;
        }

        for (file_hash, segments) in file_nodes {
            let file_contents: Vec<_> = segments
                .iter()
                .map(|(h, (lb, ub))| FileDataSequenceEntry::new(simple_hash(*h), *ub - *lb, *lb, *ub))
                .collect();
            let file_info = MDBFileInfo {
                metadata: FileDataSequenceHeader::new(simple_hash(*file_hash), segments.len(), false, false),
                segments: file_contents,
                verification: vec![],
                metadata_ext: None,
            };

            shard.add_file_reconstruction_info(file_info.clone()).await?;

            in_mem_shard.add_file_reconstruction_info(file_info)?;
        }

        Ok(())
    }

    // Create n_shards new random shards in the directory pointed
    pub async fn create_random_shard_collection(
        seed: u64,
        shard_dir: impl AsRef<Path>,
        n_shards: usize,
        cas_block_sizes: &[usize],
        file_chunk_range_sizes: &[usize],
    ) -> Result<MDBInMemoryShard> {
        // generate the cas content stuff.
        let mut rng = StdRng::seed_from_u64(seed);

        let shard_dir = shard_dir.as_ref();
        let sfm = ShardFileManager::new_in_session_directory(shard_dir, false).await?;
        let mut reference_shard = MDBInMemoryShard::default();

        for _ in 0..n_shards {
            fill_with_random_shard(&sfm, &mut reference_shard, rng.random(), cas_block_sizes, file_chunk_range_sizes)
                .await?;

            sfm.flush().await?;
        }

        Ok(reference_shard)
    }

    async fn fill_with_random_shard(
        shard: &Arc<ShardFileManager>,
        in_mem_shard: &mut MDBInMemoryShard,
        seed: u64,
        cas_block_sizes: &[usize],
        file_chunk_range_sizes: &[usize],
    ) -> Result<()> {
        // generate the cas content stuff.
        let mut rng = StdRng::seed_from_u64(seed);

        for cas_block_size in cas_block_sizes {
            let mut chunks = Vec::<_>::new();
            let mut pos = 0u32;

            for _ in 0..*cas_block_size {
                chunks.push(CASChunkSequenceEntry::new(rng_hash(rng.random()), rng.random_range(10000..20000), pos));
                pos += rng.random_range(10000..20000);
            }
            let metadata = CASChunkSequenceHeader::new(rng_hash(rng.random()), *cas_block_size, pos);
            let mdb_cas_info = MDBCASInfo { metadata, chunks };

            shard.add_cas_block(mdb_cas_info.clone()).await?;
            in_mem_shard.add_cas_block(mdb_cas_info)?;
        }

        for file_block_size in file_chunk_range_sizes {
            let file_info = gen_random_file_info(&mut rng, file_block_size, false, false);
            shard.add_file_reconstruction_info(file_info.clone()).await?;

            in_mem_shard.add_file_reconstruction_info(file_info)?;
        }
        Ok(())
    }

    pub async fn verify_mdb_shards_match(
        mdb: &ShardFileManager,
        mem_shard: &MDBInMemoryShard,
        test_file_reconstruction: bool,
    ) -> Result<()> {
        // Now, test that the results of queries from the in-memory shard match those
        // of the other shard.
        for (k, cas_block) in mem_shard.cas_content.iter() {
            // Go through and test queries on both the in-memory shard and the
            // serialized shard, making sure that they match completely.

            for i in 0..cas_block.chunks.len() {
                // Test the dedup query over a few hashes in which all the
                // hashes queried are part of the cas_block.
                let query_hashes_1: Vec<MerkleHash> = cas_block.chunks[i..(i + 3).min(cas_block.chunks.len())]
                    .iter()
                    .map(|c| c.chunk_hash)
                    .collect();
                let n_items_to_read = query_hashes_1.len();

                // Also test the dedup query over a few hashes in which some of the
                // hashes are part of the query, and the last is not.
                let mut query_hashes_2 = query_hashes_1.clone();
                query_hashes_2.push(rng_hash(1000000 + i as u64));

                let lb = i as u32;
                let ub = min(i + 3, cas_block.chunks.len()) as u32;

                for query_hashes in [&query_hashes_1, &query_hashes_2] {
                    let result_m = mem_shard.chunk_hash_dedup_query(query_hashes).unwrap();

                    let result_f = mdb.chunk_hash_dedup_query(query_hashes).await?.unwrap();

                    // Returns a tuple of (num chunks matched, FileDataSequenceEntry)
                    assert_eq!(result_m.0, n_items_to_read);
                    assert_eq!(result_f.0, n_items_to_read);

                    // Make sure it gives the correct CAS block hash as the second part of the
                    assert_eq!(result_m.1.cas_hash, *k);
                    assert_eq!(result_f.1.cas_hash, *k);

                    // Make sure the bounds are correct
                    assert_eq!((result_m.1.chunk_index_start, result_m.1.chunk_index_end), (lb, ub));
                    assert_eq!((result_f.1.chunk_index_start, result_f.1.chunk_index_end), (lb, ub));

                    // Make sure everything else equal.
                    assert_eq!(result_m, result_f);
                }
            }
        }

        // Test get file reconstruction info.
        if test_file_reconstruction {
            // Against some valid hashes,
            let mut query_hashes: Vec<MerkleHash> = mem_shard.file_content.iter().map(|file| *file.0).collect();
            // and a few random invalid ones.
            for i in 0..3 {
                query_hashes.push(rng_hash(1000000 + i as u64));
            }

            for k in query_hashes.iter() {
                let result_m = mem_shard.get_file_reconstruction_info(k);
                let result_f = mdb.get_file_reconstruction_info(k).await?;

                // Make sure two queries return same results.
                assert_eq!(result_m.is_some(), result_f.is_some());

                // Make sure retriving the expected file.
                if result_m.is_some() {
                    assert_eq!(result_m.unwrap().metadata.file_hash, *k);
                    assert_eq!(result_f.unwrap().0.metadata.file_hash, *k);
                }
            }

            // Make sure manager correctly tracking repo size.
            assert_eq!(mdb.calculate_total_materialized_bytes().await?, mem_shard.materialized_bytes());
        }

        assert_eq!(mdb.calculate_total_stored_bytes().await?, mem_shard.stored_bytes());

        Ok(())
    }

    async fn sfm_with_target_shard_size(path: impl AsRef<Path>, target_size: u64) -> Result<Arc<ShardFileManager>> {
        ShardFileManager::new_impl(path, false, target_size, true, 0).await
    }

    #[tokio::test]
    async fn test_basic_retrieval() -> Result<()> {
        let tmp_dir = TempDir::with_prefix("gitxet_shard_test_1")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();

        {
            let mdb = ShardFileManager::new_in_session_directory(tmp_dir.path(), true).await?;

            fill_with_specific_shard(&mdb, &mut mdb_in_mem, &[(0, &[(11, 5)])], &[(100, &[(200, (0, 5))])]).await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;

            let out_file = mdb.flush().await?.unwrap();

            // Make sure it still stays consistent after a flush
            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;

            // Verify that the file is correct
            MDBShardFile::load_from_file(&out_file)?.verify_shard_integrity();
        }
        {
            // Now, make sure that this happens if this directory is opened up
            let mdb2 = ShardFileManager::new_in_session_directory(tmp_dir.path(), true).await?;

            // Make sure it's all in there this round.
            verify_mdb_shards_match(&mdb2, &mdb_in_mem, true).await?;

            // Now add some more, based on this directory
            fill_with_random_shard(&mdb2, &mut mdb_in_mem, 0, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6]).await?;

            verify_mdb_shards_match(&mdb2, &mdb_in_mem, true).await?;

            // Now, merge shards in the background.
            let merged_shards = consolidate_shards_in_directory(tmp_dir.path(), *MDB_SHARD_MIN_TARGET_SIZE, false)?;

            assert_eq!(merged_shards.len(), 1);
            for si in merged_shards {
                assert!(si.path.exists());
                assert!(si.path.to_str().unwrap().contains(&si.shard_hash.hex()))
            }

            verify_mdb_shards_match(&mdb2, &mdb_in_mem, true).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_larger_simulated() -> Result<()> {
        let tmp_dir = TempDir::with_prefix("gitxet_shard_test_2")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();
        let mdb = ShardFileManager::new_in_session_directory(tmp_dir.path(), true).await?;

        for i in 0..10 {
            fill_with_random_shard(&mdb, &mut mdb_in_mem, i, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6]).await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;

            let out_file = mdb.flush().await?.unwrap();

            // Make sure it still stays consistent
            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;

            // Verify that the file is correct
            MDBShardFile::load_from_file(&out_file)?.verify_shard_integrity();

            // Make sure an empty flush doesn't bother anything.
            mdb.flush().await?;

            // Now, make sure that this happens if this directory is opened up
            let mdb2 = ShardFileManager::new_in_session_directory(tmp_dir.path(), true).await?;

            // Make sure it's all in there this round.
            verify_mdb_shards_match(&mdb2, &mdb_in_mem, true).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_process_session_management() -> Result<()> {
        let tmp_dir = TempDir::with_prefix("gitxet_shard_test_3").unwrap();
        let mut mdb_in_mem = MDBInMemoryShard::default();

        for sesh in 0..3 {
            for i in 0..10 {
                {
                    let mdb = ShardFileManager::new_in_session_directory(tmp_dir.path(), true).await?;
                    fill_with_random_shard(&mdb, &mut mdb_in_mem, 100 * sesh + i, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6])
                        .await
                        .unwrap();

                    verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await.unwrap();

                    let out_file = mdb.flush().await.unwrap().unwrap();

                    // Make sure it still stays together
                    verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await.unwrap();

                    // Verify that the file is correct
                    MDBShardFile::load_from_file(&out_file)?.verify_shard_integrity();

                    mdb.flush().await.unwrap();

                    verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await.unwrap();
                }
            }

            {
                let merged_shards =
                    consolidate_shards_in_directory(tmp_dir.path(), *MDB_SHARD_MIN_TARGET_SIZE, false).unwrap();

                assert_eq!(merged_shards.len(), 1);

                for si in merged_shards {
                    assert!(si.path.exists());
                    assert!(si.path.to_str().unwrap().contains(&si.shard_hash.hex()))
                }
            }

            {
                // Now, make sure that this happens if this directory is opened up
                let mdb2 = ShardFileManager::new_in_session_directory(tmp_dir.path(), true).await?;

                verify_mdb_shards_match(&mdb2, &mdb_in_mem, true).await.unwrap();
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_flush_and_consolidation() -> Result<()> {
        let tmp_dir = TempDir::with_prefix("gitxet_shard_test_4b")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();

        const T: u64 = 10000;

        {
            let mdb = sfm_with_target_shard_size(tmp_dir.path(), T).await?;
            fill_with_random_shard(&mdb, &mut mdb_in_mem, 0, &[16; 16], &[16; 16]).await?;
            mdb.flush().await?;
        }
        {
            let mdb = sfm_with_target_shard_size(tmp_dir.path(), 2 * T).await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;

            fill_with_random_shard(&mdb, &mut mdb_in_mem, 1, &[25; 25], &[25; 25]).await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;

            mdb.flush().await?;
        }

        // Reload and verify
        {
            let mdb = ShardFileManager::new_in_session_directory(tmp_dir.path(), true).await?;
            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;
        }

        // Merge through the session directory.
        {
            let tmp_merge_dir = TempDir::new()?;

            let shard_merge_result = merge_shards(tmp_dir.path(), tmp_merge_dir.path(), 8 * T, false)?;
            let mut merged_shards = shard_merge_result.merged_shards;
            let m_del_shards = shard_merge_result.obsolete_shards;

            for sfi in merged_shards.iter() {
                sfi.verify_shard_integrity();
            }

            let paths = std::fs::read_dir(tmp_dir.path()).unwrap();
            assert_eq!(paths.count(), m_del_shards.len());

            // This call should be the same, but
            let mut rv = consolidate_shards_in_directory(tmp_dir.path(), 8 * T, false)?;

            let paths = std::fs::read_dir(tmp_dir.path()).unwrap();
            let n_paths = paths.count();
            assert_eq!(n_paths, rv.len());
            assert_eq!(n_paths, merged_shards.len());

            for sfi in rv.iter() {
                sfi.verify_shard_integrity();
            }

            // Now, make sure they have the same hashes in the two calls
            merged_shards.sort_by_key(|v| v.shard_hash);
            rv.sort_by_key(|v| v.shard_hash);

            for (ms, rs) in merged_shards.iter().zip(rv.iter()) {
                assert_eq!(ms.shard_hash, rs.shard_hash);
            }
        }

        // Reload and verify
        {
            let mdb = ShardFileManager::new_in_session_directory(tmp_dir.path(), true).await?;
            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_size_threshholds() -> Result<()> {
        let tmp_dir = TempDir::with_prefix("gitxet_shard_test_4")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();

        const T: u64 = 4096;

        for i in 0..5 {
            let mdb = sfm_with_target_shard_size(tmp_dir.path(), T).await?;
            fill_with_random_shard(&mdb, &mut mdb_in_mem, i, &[5; 25], &[5; 25]).await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;

            let out_file = mdb.flush().await?.unwrap();

            // Verify that the file is correct
            MDBShardFile::load_from_file(&out_file).unwrap().verify_shard_integrity();

            // Make sure it still stays together
            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;

            assert!(mdb.flush().await?.is_none());
        }

        // Now, do a new shard that has less
        let mut last_num_files = None;
        let mut target_size = T;

        loop {
            let mdb2 = sfm_with_target_shard_size(tmp_dir.path(), 2 * T).await?;

            // Make sure it's all in there this round.
            verify_mdb_shards_match(&mdb2, &mdb_in_mem, true).await?;

            let merged_shards = consolidate_shards_in_directory(tmp_dir.path(), target_size, false)?;

            for si in merged_shards.iter() {
                assert!(si.path.exists());
                assert!(si.path.to_str().unwrap().contains(&si.shard_hash.hex()))
            }

            let n_merged_shards = merged_shards.len();

            if n_merged_shards == 1 {
                break;
            }

            if let Some(n) = last_num_files {
                assert!(n_merged_shards < n, "n_merged_shards({n_merged_shards}) < n({n})");
            }

            last_num_files = Some(n_merged_shards);

            // So the shards will all be consolidated in the next round.
            target_size *= 2;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_keyed_shard_tooling() -> Result<()> {
        let tmp_dir = TempDir::with_prefix("shard_test_unkeyed")?;
        let tmp_dir_path = tmp_dir.path();

        let ref_shard = create_random_shard_collection(0, tmp_dir_path, 2, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6]).await?;

        // First, load all of these with a shard file manager and check them.
        {
            let shard_file_manager = ShardFileManager::new_in_session_directory(tmp_dir_path, true).await?;
            verify_mdb_shards_match(&shard_file_manager, &ref_shard, true).await?;
        }

        // Now convert them them into keyed shards.
        for include_info in [false, true] {
            let _tmp_dir_keyed = TempDir::with_prefix("shard_test_keyed")?;
            let tmp_dir_path_keyed = _tmp_dir_keyed.path();

            // Enumerate all the .mdbshard files in the tmp_dir_path directory
            let paths = std::fs::read_dir(tmp_dir_path)?.map(|p| p.unwrap().path()).collect::<Vec<_>>();

            // Convert all but one of the given shards.
            for (i, p) in paths.iter().enumerate() {
                if i == 0 {
                    std::fs::copy(p, tmp_dir_path_keyed.join(p.file_name().unwrap()))?;
                    continue;
                }

                let key: HMACKey = {
                    if i == 1 {
                        // This tests that the default route with no hmac translation is solid too
                        HMACKey::default()
                    } else {
                        // Do some repeat keys to make sure that path is tested as well.
                        rng_hash((i % 6) as u64)
                    }
                };

                let shard = MDBShardFile::load_from_file(p)?;

                // Reexport all these shards as keyed shards.
                let out = shard
                    .export_as_keyed_shard(
                        tmp_dir_path_keyed,
                        key,
                        Duration::new(100, 0),
                        include_info,
                        include_info,
                        include_info,
                    )
                    .unwrap();
                if key != HMACKey::default() {
                    assert_eq!(out.chunk_hmac_key(), Some(key));
                } else {
                    assert_eq!(out.chunk_hmac_key(), None);
                }
            }

            // Now, verify that everything still works great.
            let shard_file_manager = ShardFileManager::new_in_session_directory(tmp_dir_path_keyed, true).await?;

            verify_mdb_shards_match(&shard_file_manager, &ref_shard, include_info).await?;
        }

        Ok(())
    }

    async fn shard_list_with_timestamp_filtering(path: &Path) -> Result<Vec<Arc<MDBShardFile>>> {
        Ok(ShardFileManager::new_impl(path, false, *MDB_SHARD_MIN_TARGET_SIZE, true, 0)
            .await?
            .registered_shard_list()
            .await?)
    }

    #[tokio::test]
    async fn test_timestamp_filtering() -> Result<()> {
        let tmp_dir = TempDir::with_prefix("shard_test_timestamp")?;
        let tmp_dir_path = tmp_dir.path();

        // Just create a single shard; we'll key it with other keys and timestamps and then test loading.
        create_random_shard_collection(0, tmp_dir_path, 1, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6]).await?;

        let path = std::fs::read_dir(tmp_dir_path)?.map(|p| p.unwrap().path()).next().unwrap();

        // Create another that has an expiration date of one second from now.
        let key: HMACKey = rng_hash(0);

        let shard = MDBShardFile::load_from_file(&path)?;

        let _tmp_dir_keyed = TempDir::with_prefix("shard_test_keyed_1")?;
        let tmp_dir_path_keyed = _tmp_dir_keyed.path();

        // Reexport this shard as a keyed shards.
        let out = shard
            .export_as_keyed_shard(tmp_dir_path_keyed, key, Duration::new(1, 0), false, false, false)
            .unwrap();

        {
            let loaded_shards = shard_list_with_timestamp_filtering(tmp_dir_path_keyed).await?;

            assert_eq!(loaded_shards.len(), 1);
            assert_eq!(loaded_shards[0].shard_hash, out.shard_hash)
        }

        // Sleep for 2.01 seconds to make sure at least a second has passed for the +1 and a second to handle the <=
        // part of the equality,
        std::thread::sleep(Duration::new(2, 10_000_000));

        {
            let loaded_shards = shard_list_with_timestamp_filtering(tmp_dir_path_keyed).await?;

            // No shards loaded
            assert!(loaded_shards.is_empty());

            // shard file still there.
            let n_files = std::fs::read_dir(tmp_dir_path_keyed)?.map(|p| p.unwrap().path()).count();
            assert_eq!(n_files, 1);

            // Now try deletion with a large window; shouldn't touch the shard.
            MDBShardFile::clean_shard_cache(tmp_dir_path_keyed, 100)?;

            // shard file still there.
            let n_files = std::fs::read_dir(tmp_dir_path_keyed)?.map(|p| p.unwrap().path()).count();
            assert_eq!(n_files, 1);

            // Now try deletion with 0 expiration
            MDBShardFile::clean_shard_cache(tmp_dir_path_keyed, 0)?;

            // File should be gone.
            let n_files = std::fs::read_dir(tmp_dir_path_keyed)?.map(|p| p.unwrap().path()).count();
            assert_eq!(n_files, 0);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_export_expiration() -> Result<()> {
        let tmp_dir = TempDir::with_prefix("shard_test_timestamp_2")?;
        let tmp_dir_path = tmp_dir.path();

        // Just create a single shard; we'll key it with other keys and timestamps and then test loading.
        create_random_shard_collection(0, tmp_dir_path, 1, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6]).await?;

        let path = std::fs::read_dir(tmp_dir_path)?.map(|p| p.unwrap().path()).next().unwrap();

        let shard = MDBShardFile::load_from_file(&path)?;

        let _tmp_dir_expiry = TempDir::with_prefix("shard_test_expiry_2")?;
        let tmp_dir_path_expiry = _tmp_dir_expiry.path();

        // Create another that has an expiration date of one second from now.
        let out = shard.export_with_expiration(tmp_dir_path_expiry, Duration::new(1, 0))?;

        {
            let loaded_shards = shard_list_with_timestamp_filtering(tmp_dir_path_expiry).await?;

            assert_eq!(loaded_shards.len(), 1);
            assert_eq!(loaded_shards[0].shard_hash, out.shard_hash)
        }

        // Sleep for 2.01 seconds to make sure at least a second has passed for the +1 and a second to handle the <=
        // part of the equality,
        std::thread::sleep(Duration::new(2, 10_000_000));

        {
            let loaded_shards = shard_list_with_timestamp_filtering(tmp_dir_path_expiry).await?;

            assert!(loaded_shards.is_empty());

            // Make sure it leaves the shard there.
            let n_files = std::fs::read_dir(tmp_dir_path_expiry)?.map(|p| p.unwrap().path()).count();
            assert_eq!(n_files, 1);

            // Now try deletion with a large window; shouldn't touch the shard.
            MDBShardFile::clean_shard_cache(tmp_dir_path_expiry, 100)?;

            // shard file still there.
            let n_files = std::fs::read_dir(tmp_dir_path_expiry)?.map(|p| p.unwrap().path()).count();
            assert_eq!(n_files, 1);

            // Now try deletion with 0 expiration
            MDBShardFile::clean_shard_cache(tmp_dir_path_expiry, 0)?;

            // File should be gone.
            let n_files = std::fs::read_dir(tmp_dir_path_expiry)?.map(|p| p.unwrap().path()).count();
            assert_eq!(n_files, 0);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_size_pruning() {
        let tmp_dir = TempDir::with_prefix("shard_test_cache_size_pruning").unwrap();

        let tmp_dir_1 = tmp_dir.path().join("src");

        let n_shards = 4;

        create_random_shard_collection(0, &tmp_dir_1, n_shards, &[16; 16], &[16; 16])
            .await
            .unwrap();

        // Export each of the shards in tmp_dir_1
        let tmp_dir_2_ = tmp_dir.path().join("timestamped");
        let tmp_dir_2 = &tmp_dir_2_;
        std::fs::create_dir_all(tmp_dir_2).unwrap();

        let mut shard_list = Vec::new();

        let base_time = SystemTime::now() - Duration::from_secs(256);
        let expiration = SystemTime::now() + Duration::from_secs(256);

        for (i, p) in std::fs::read_dir(&tmp_dir_1).unwrap().enumerate() {
            let p = p.unwrap();
            let shard = MDBShardFile::load_from_file(&p.path()).unwrap();

            let s = shard
                .export_with_specific_expiration(tmp_dir_2, expiration, base_time + Duration::from_secs(i as u64))
                .unwrap();

            shard_list.push(s);
        }

        let get_shards = |cache_size: u64| async move {
            let sfm = ShardFileManager::new_impl(tmp_dir_2, false, 64 * 1024, true, cache_size)
                .await
                .unwrap();
            sfm.registered_shard_list().await.unwrap()
        };

        for i in 0..n_shards {
            let current_size_limit = shard_list.iter().skip(i).map(|s| s.shard.num_bytes()).sum();

            let loaded_shards = get_shards(current_size_limit).await;

            // Make sure this is the same set of shard hashes as in the shard list by comparing sets of the shard hashes
            let loaded_hashes: HashSet<_> = loaded_shards.iter().map(|s| s.shard_hash).collect();
            let reference_hashes: HashSet<_> = shard_list.iter().skip(i).map(|s| s.shard_hash).collect();

            assert_eq!(loaded_hashes, reference_hashes);

            let existing_files: HashSet<_> = std::fs::read_dir(tmp_dir_2)
                .unwrap()
                .map(|p| MDBShardFile::load_from_file(&p.unwrap().path()).unwrap().shard_hash)
                .collect();

            assert_eq!(existing_files, reference_hashes);

            let directory_size: u64 = std::fs::read_dir(tmp_dir_2)
                .unwrap()
                .map(|p| p.unwrap().metadata().unwrap().len())
                .sum();

            // We set this one to be exact, so we can compare these as equal
            assert_eq!(directory_size, current_size_limit);
        }
    }

    #[tokio::test]
    async fn test_shard_deletion_ok() {
        let tmp_dir = TempDir::with_prefix("shard_test_deletion").unwrap();

        let tmp_dir_1 = tmp_dir.path().join("src");

        create_random_shard_collection(0, &tmp_dir_1, 4, &[4; 4], &[4; 4])
            .await
            .unwrap();

        // Get the size of one of these shards to use as
        let base_size = 1 + tmp_dir_1.read_dir().unwrap().next().unwrap().unwrap().metadata().unwrap().len();

        for (merge_size, n_merged) in [(1, 3), (2, 2), (4, 1)] {
            for corrupt_file_index in 0..4 {
                let work_dir = tmp_dir.path().join(format!("tmp_{merge_size}_{corrupt_file_index}"));
                let tmp_src_dir = work_dir.join("src");
                std::fs::create_dir_all(&tmp_src_dir).unwrap();

                let mut bad_shard_hash = Default::default();

                // copy all the shard files in tmp_dir_1 to dir, but delet
                std::fs::create_dir_all(&tmp_src_dir).unwrap();
                for (i, p) in std::fs::read_dir(&tmp_dir_1).unwrap().enumerate() {
                    let p = p.unwrap();
                    let dest_file_name = tmp_src_dir.join(p.file_name());
                    std::fs::copy(p.path(), &dest_file_name).unwrap();

                    if i == corrupt_file_index {
                        // Load this so the metadata gets cached; that will get loaded from cache.
                        let sfi = MDBShardFile::load_from_file(&dest_file_name).unwrap();

                        // Turn off the typical verification checks on this shard, so it isn't verified
                        // on loading from the cache.
                        sfi.disable_verifications.store(true, std::sync::atomic::Ordering::Relaxed);

                        // Replace the file with an empty file.  On read, this will error, and should skip.
                        std::fs::File::create(&dest_file_name).unwrap();

                        bad_shard_hash = sfi.shard_hash;
                    }
                }

                // Now attempt a merge; this should cause an error.
                let out_dir_1 = work_dir.join("out_err");
                std::fs::create_dir_all(&out_dir_1).unwrap();
                let res = merge_shards(&tmp_src_dir, &out_dir_1, base_size * merge_size, false);
                assert!(res.is_err());

                // Now attempt a merge with error skipping; which should not cause an error.
                let out_dir_2 = work_dir.join("out_skips");
                std::fs::create_dir_all(&out_dir_2).unwrap();
                let res = merge_shards(&tmp_src_dir, &out_dir_2, base_size * merge_size, true).unwrap();

                assert_eq!(res.merged_shards.len(), n_merged);

                assert_eq!(res.obsolete_shards.len(), 3);

                assert_eq!(res.skipped_shards.len(), 1);
                assert_eq!(
                    res.skipped_shards
                        .first()
                        .map(|s| &s.path)
                        .and_then(parse_shard_filename)
                        .unwrap(),
                    bad_shard_hash
                );
            }
        }
    }
}
