use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use lazy_static::lazy_static;
use merklehash::{HMACKey, MerkleHash};
use tokio::sync::RwLock;
use tracing::{debug, error, info, trace};

use crate::cas_structs::*;
use crate::constants::{MDB_SHARD_EXPIRATION_BUFFER, MDB_SHARD_MIN_TARGET_SIZE};
use crate::error::{MDBShardError, Result};
use crate::file_structs::*;
use crate::shard_file::current_timestamp;
use crate::shard_file_handle::MDBShardFile;
use crate::shard_file_reconstructor::FileReconstructor;
use crate::shard_in_memory::MDBInMemoryShard;
use crate::utils::truncate_hash;

/// A wrapper struct for the in-memory shard to make sure that it gets flushed on teardown.
struct MDBShardFlushGuard {
    shard: MDBInMemoryShard,
    session_directory: Option<PathBuf>,
}

impl Drop for MDBShardFlushGuard {
    fn drop(&mut self) {
        if self.shard.is_empty() {
            return;
        }

        if let Some(sd) = &self.session_directory {
            // Check if the flushing directory exists.
            if !sd.is_dir() {
                error!("Error flushing reconstruction data on shutdown: {sd:?} is not a directory or doesn't exist");
                return;
            }

            self.flush().unwrap_or_else(|e| {
                error!("Error flushing reconstruction data on shutdown: {e:?}");
                None
            });
        }
    }
}

// Store a maximum of this many indices in memory
const CHUNK_INDEX_TABLE_DEFAULT_MAX_SIZE: usize = 64 * 1024 * 1024;
lazy_static! {
    static ref CHUNK_INDEX_TABLE_MAX_SIZE: usize = std::env::var("XET_CHUNK_INDEX_TABLE_MAX_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(CHUNK_INDEX_TABLE_DEFAULT_MAX_SIZE);
}

// The structure used as the target for the dedup lookup
#[repr(packed)]
struct ChunkCacheElement {
    cas_start_index: u32, // the index of the first chunk
    cas_chunk_offset: u16,
    shard_index: u16, // This one is last so that the u16 bits can be packed in.
}

#[derive(Default)]
struct KeyedShardCollection {
    hmac_key: HMACKey,
    shard_list: Vec<MDBShardFile>,
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
    shard_bookkeeper: Arc<RwLock<ShardBookkeeper>>,
    current_state: Arc<RwLock<MDBShardFlushGuard>>,
    target_shard_min_size: u64,
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
    /// Creates a new shard file manager at the
    pub async fn new(session_directory: &Path, load_and_clean_directory: bool) -> Result<Self> {
        let session_directory = {
            if session_directory == PathBuf::default() {
                None
            } else {
                Some(std::fs::canonicalize(session_directory).map_err(|e| {
                    error!("Error accessing session directory {session_directory:?}: {e:?}");
                    e
                })?)
            }
        };

        let s = Self {
            shard_bookkeeper: Arc::new(RwLock::new(ShardBookkeeper::new())),
            current_state: Arc::new(RwLock::new(MDBShardFlushGuard {
                shard: MDBInMemoryShard::default(),
                session_directory: session_directory.clone(),
            })),
            target_shard_min_size: MDB_SHARD_MIN_TARGET_SIZE,
        };

        if load_and_clean_directory {
            if let Some(sd) = &session_directory {
                s.load_and_cleanup_shards_by_path(&[sd]).await?;
            }
        }

        Ok(s)
    }

    /// Construct a new shard file manager that uses session_directory as the temporary dumping  
    pub async fn load_dir(session_directory: &Path) -> Result<Self> {
        Self::new(session_directory, true).await
    }

    /// Sets the target value of a shard file size.  By default, it is given by MDB_SHARD_MIN_TARGET_SIZE
    pub fn set_target_shard_min_size(&mut self, s: u64) {
        self.target_shard_min_size = s;
    }

    /// Registers all the files in a directory with filenames matching the names
    /// of an MerkleDB shard.
    pub async fn register_shards_by_path<P: AsRef<Path>>(
        &self,
        paths: &[P],
        allow_expired_deletion: bool,
        expiration_deletion_buffer: Duration,
    ) -> Result<Vec<MDBShardFile>> {
        let mut new_shards = Vec::new();

        let current_time = current_timestamp();

        let expiration_deletion_buffer_secs = expiration_deletion_buffer.as_secs();
        let mut deletion_candidates = Vec::new();

        for p in paths {
            let shard_files = MDBShardFile::load_all(p.as_ref())?;

            // Now, go through and filter out the ones that can't be used any more, and also filter out the ones that
            // can't be
            new_shards.extend(shard_files.into_iter().filter_map(|s| {
                let expiry_time = s.shard.metadata.shard_key_expiry;
                if current_time < expiry_time {
                    Some(s)
                } else {
                    if allow_expired_deletion && expiry_time + expiration_deletion_buffer_secs <= current_time {
                        deletion_candidates.push(s.path);
                    }

                    None
                }
            }));
        }

        for p in deletion_candidates {
            // silently delete expired shard files, swallowing errors if need be.
            let _ = std::fs::remove_file(p);
        }

        self.register_shards(&new_shards).await?;
        Ok(new_shards)
    }

    /// This is a wrapper function to register_shards_by_path that uses the defaults above.
    pub async fn load_and_cleanup_shards_by_path<P: AsRef<Path>>(&self, paths: &[P]) -> Result<Vec<MDBShardFile>> {
        self.register_shards_by_path(paths, true, Duration::new(MDB_SHARD_EXPIRATION_BUFFER, 0))
            .await
    }

    pub async fn register_shards(&self, new_shards: &[MDBShardFile]) -> Result<()> {
        let mut sbkp_lg = self.shard_bookkeeper.write().await;

        // Go through and register the shards in order of newest to oldest
        let mut new_shards: Vec<(&MDBShardFile, SystemTime)> = new_shards
            .iter()
            .map(|s| {
                let modified = std::fs::metadata(&s.path)?.modified()?;
                Ok((s, modified))
            })
            .collect::<Result<Vec<_>>>()?;

        // Compare in reverse order to sort from newest to oldest
        new_shards.sort_by(|(_, t1), (_, t2)| t2.cmp(t1));
        let num_shards = new_shards.len();

        for (s, _) in new_shards {
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
            info!("Registered {num_shards} new shards.");
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
            let file_info = lg.shard.get_file_reconstruction_info(file_hash);
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
            let ret = lg.shard.chunk_hash_dedup_query(query_hashes);
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
    pub async fn add_cas_block(&self, cas_block_contents: MDBCASInfo) -> Result<()> {
        let mut lg = self.current_state.write().await;

        if lg.shard.shard_file_size() + cas_block_contents.num_bytes() >= self.target_shard_min_size {
            self.flush_internal(&mut lg).await?;
        }

        lg.shard.add_cas_block(cas_block_contents)?;

        Ok(())
    }

    /// Add file reconstruction info to the in-memory state.
    pub async fn add_file_reconstruction_info(&self, file_info: MDBFileInfo) -> Result<()> {
        let mut lg = self.current_state.write().await;

        if lg.shard.shard_file_size() + file_info.num_bytes() >= self.target_shard_min_size {
            self.flush_internal(&mut lg).await?;
        }

        lg.shard.add_file_reconstruction_info(file_info)?;

        Ok(())
    }

    async fn flush_internal<'a>(
        &self,
        mem_shard: &mut tokio::sync::RwLockWriteGuard<'a, MDBShardFlushGuard>,
    ) -> Result<Option<PathBuf>> {
        Ok(if let Some(path) = mem_shard.flush()? {
            self.load_and_cleanup_shards_by_path(&[&path]).await?;
            Some(path)
        } else {
            None
        })
    }

    /// Flush the current state of the in-memory lookups to a shard in the session directory,
    /// returning the hash of the shard and the file written, or None if no file was written.
    pub async fn flush(&self) -> Result<Option<PathBuf>> {
        let mut lg = self.current_state.write().await;
        self.flush_internal(&mut lg).await
    }
}

impl MDBShardFlushGuard {
    // The flush logic is put here so that this can be done both manually and by drop
    pub fn flush(&mut self) -> Result<Option<PathBuf>> {
        if self.shard.is_empty() {
            return Ok(None);
        }

        if let Some(sd) = &self.session_directory {
            let path = self.shard.write_to_directory(sd)?;
            self.shard = MDBInMemoryShard::default();

            info!("Shard manager flushed new shard to {path:?}.");

            Ok(Some(path))
        } else {
            debug!("Shard manager in ephemeral mode; skipping flush to disk.");
            Ok(None)
        }
    }
}

impl ShardFileManager {
    /// Calculate the total materialized bytes (before deduplication) tracked by the manager,
    /// including in-memory state and on-disk shards.
    pub async fn calculate_total_materialized_bytes(&self) -> Result<u64> {
        let mut bytes = 0;
        {
            let lg = self.current_state.read().await;
            bytes += lg.shard.materialized_bytes();
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
            bytes += lg.shard.stored_bytes();
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
    use std::time::Duration;

    use more_asserts::assert_lt;
    use rand::prelude::*;
    use tempdir::TempDir;

    use super::*;
    use crate::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader};
    use crate::error::Result;
    use crate::file_structs::FileDataSequenceHeader;
    use crate::session_directory::consolidate_shards_in_directory;
    use crate::shard_format::test_routines::{rng_hash, simple_hash};

    #[allow(clippy::type_complexity)]
    pub async fn fill_with_specific_shard(
        shard: &mut ShardFileManager,
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
                metadata: FileDataSequenceHeader::new(simple_hash(*file_hash), segments.len(), false),
                segments: file_contents,
                verification: vec![],
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
        let mut sfm = ShardFileManager::load_dir(shard_dir).await?;
        let mut reference_shard = MDBInMemoryShard::default();

        for _ in 0..n_shards {
            fill_with_random_shard(&mut sfm, &mut reference_shard, rng.gen(), cas_block_sizes, file_chunk_range_sizes)
                .await?;

            sfm.flush().await?;
        }

        Ok(reference_shard)
    }

    async fn fill_with_random_shard(
        shard: &mut ShardFileManager,
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
                chunks.push(CASChunkSequenceEntry::new(rng_hash(rng.gen()), rng.gen_range(10000..20000), pos));
                pos += rng.gen_range(10000..20000);
            }
            let metadata = CASChunkSequenceHeader::new(rng_hash(rng.gen()), *cas_block_size, pos);
            let mdb_cas_info = MDBCASInfo { metadata, chunks };

            shard.add_cas_block(mdb_cas_info.clone()).await?;
            in_mem_shard.add_cas_block(mdb_cas_info)?;
        }

        for file_block_size in file_chunk_range_sizes {
            let file_hash = rng_hash(rng.gen());

            let segments: Vec<_> = (0..*file_block_size)
                .map(|_| {
                    let lb = rng.gen_range(0..10000);
                    let ub = lb + rng.gen_range(0..10000);
                    FileDataSequenceEntry::new(rng_hash(rng.gen()), ub - lb, lb, ub)
                })
                .collect();

            let metadata = FileDataSequenceHeader::new(file_hash, *file_block_size, false);

            let file_info = MDBFileInfo {
                metadata,
                segments,
                verification: vec![],
            };

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
    #[tokio::test]
    async fn test_basic_retrieval() -> Result<()> {
        let tmp_dir = TempDir::new("gitxet_shard_test_1")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();

        {
            let mut mdb = ShardFileManager::load_dir(tmp_dir.path()).await?;

            fill_with_specific_shard(&mut mdb, &mut mdb_in_mem, &[(0, &[(11, 5)])], &[(100, &[(200, (0, 5))])]).await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;

            let out_file = mdb.flush().await?.unwrap();

            // Make sure it still stays consistent after a flush
            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;

            // Verify that the file is correct
            MDBShardFile::load_from_file(&out_file)?.verify_shard_integrity();
        }
        {
            // Now, make sure that this happens if this directory is opened up
            let mut mdb2 = ShardFileManager::load_dir(tmp_dir.path()).await?;

            // Make sure it's all in there this round.
            verify_mdb_shards_match(&mdb2, &mdb_in_mem, true).await?;

            // Now add some more, based on this directory
            fill_with_random_shard(&mut mdb2, &mut mdb_in_mem, 0, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6]).await?;

            verify_mdb_shards_match(&mdb2, &mdb_in_mem, true).await?;

            // Now, merge shards in the background.
            let merged_shards = consolidate_shards_in_directory(tmp_dir.path(), MDB_SHARD_MIN_TARGET_SIZE)?;

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
        let tmp_dir = TempDir::new("gitxet_shard_test_2")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();
        let mut mdb = ShardFileManager::load_dir(tmp_dir.path()).await?;

        for i in 0..10 {
            fill_with_random_shard(&mut mdb, &mut mdb_in_mem, i, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6]).await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;

            let out_file = mdb.flush().await?.unwrap();

            // Make sure it still stays consistent
            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;

            // Verify that the file is correct
            MDBShardFile::load_from_file(&out_file)?.verify_shard_integrity();

            // Make sure an empty flush doesn't bother anything.
            mdb.flush().await?;

            // Now, make sure that this happens if this directory is opened up
            let mdb2 = ShardFileManager::load_dir(tmp_dir.path()).await?;

            // Make sure it's all in there this round.
            verify_mdb_shards_match(&mdb2, &mdb_in_mem, true).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_process_session_management() -> Result<()> {
        let tmp_dir = TempDir::new("gitxet_shard_test_3").unwrap();
        let mut mdb_in_mem = MDBInMemoryShard::default();

        for sesh in 0..3 {
            for i in 0..10 {
                {
                    let mut mdb = ShardFileManager::load_dir(tmp_dir.path()).await.unwrap();
                    fill_with_random_shard(
                        &mut mdb,
                        &mut mdb_in_mem,
                        100 * sesh + i,
                        &[1, 5, 10, 8],
                        &[4, 3, 5, 9, 4, 6],
                    )
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
                let merged_shards = consolidate_shards_in_directory(tmp_dir.path(), MDB_SHARD_MIN_TARGET_SIZE).unwrap();

                assert_eq!(merged_shards.len(), 1);

                for si in merged_shards {
                    assert!(si.path.exists());
                    assert!(si.path.to_str().unwrap().contains(&si.shard_hash.hex()))
                }
            }

            {
                // Now, make sure that this happens if this directory is opened up
                let mdb2 = ShardFileManager::load_dir(tmp_dir.path()).await.unwrap();

                verify_mdb_shards_match(&mdb2, &mdb_in_mem, true).await.unwrap();
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_flush_and_consolidation() -> Result<()> {
        let tmp_dir = TempDir::new("gitxet_shard_test_4b")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();

        const T: u64 = 10000;

        {
            let mut mdb = ShardFileManager::load_dir(tmp_dir.path()).await?;
            mdb.set_target_shard_min_size(T); // Set the targe shard size really low
            fill_with_random_shard(&mut mdb, &mut mdb_in_mem, 0, &[16; 16], &[16; 16]).await?;
        }
        {
            let mut mdb = ShardFileManager::load_dir(tmp_dir.path()).await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;

            mdb.set_target_shard_min_size(2 * T);
            fill_with_random_shard(&mut mdb, &mut mdb_in_mem, 1, &[25; 25], &[25; 25]).await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;
        }

        // Reload and verify
        {
            let mdb = ShardFileManager::load_dir(tmp_dir.path()).await?;
            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;
        }

        // Merge through the session directory.
        {
            let rv = consolidate_shards_in_directory(tmp_dir.path(), 8 * T)?;

            let paths = std::fs::read_dir(tmp_dir.path()).unwrap();
            assert_eq!(paths.count(), rv.len());

            for sfi in rv {
                sfi.verify_shard_integrity();
            }
        }

        // Reload and verify
        {
            let mdb = ShardFileManager::load_dir(tmp_dir.path()).await?;
            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_size_threshholds() -> Result<()> {
        let tmp_dir = TempDir::new("gitxet_shard_test_4")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();

        const T: u64 = 4096;

        for i in 0..5 {
            let mut mdb = ShardFileManager::load_dir(tmp_dir.path()).await?;
            mdb.set_target_shard_min_size(T); // Set the targe shard size really low
            fill_with_random_shard(&mut mdb, &mut mdb_in_mem, i, &[5; 25], &[5; 25]).await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;

            let out_file = mdb.flush().await?.unwrap();

            // Verify that the file is correct
            MDBShardFile::load_from_file(&out_file)?.verify_shard_integrity();

            // Make sure it still stays together
            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;

            assert!(mdb.flush().await?.is_none());
        }

        // Now, do a new shard that has less
        let mut last_num_files = None;
        let mut target_size = T;
        loop {
            // Now, make sure that this happens if this directory is opened up
            let mut mdb2 = ShardFileManager::load_dir(tmp_dir.path()).await?;
            mdb2.set_target_shard_min_size(target_size);

            // Make sure it's all in there this round.
            verify_mdb_shards_match(&mdb2, &mdb_in_mem, true).await?;

            let merged_shards = consolidate_shards_in_directory(tmp_dir.path(), target_size)?;

            for si in merged_shards.iter() {
                assert!(si.path.exists());
                assert!(si.path.to_str().unwrap().contains(&si.shard_hash.hex()))
            }

            let n_merged_shards = merged_shards.len();

            if n_merged_shards == 1 {
                break;
            }

            if let Some(n) = last_num_files {
                assert_lt!(n_merged_shards, n);
            }

            last_num_files = Some(n_merged_shards);

            // So the shards will all be consolidated in the next round.
            target_size *= 2;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_keyed_shard_tooling() -> Result<()> {
        let tmp_dir = TempDir::new("shard_test_unkeyed")?;
        let tmp_dir_path = tmp_dir.path();

        let ref_shard = create_random_shard_collection(0, tmp_dir_path, 8, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6]).await?;

        // First, load all of these with a shard file manager and check them.
        {
            let shard_file_manager = ShardFileManager::load_dir(tmp_dir_path).await?;
            shard_file_manager.load_and_cleanup_shards_by_path(&[tmp_dir_path]).await?;
            verify_mdb_shards_match(&shard_file_manager, &ref_shard, true).await?;
        }

        // Now convert them them into keyed shards.
        for include_info in [false, true] {
            let _tmp_dir_keyed = TempDir::new("shard_test_keyed")?;
            let tmp_dir_path_keyed = _tmp_dir_keyed.path();

            // Enumerate all the .mdbshard files in the tmp_dir_path directory
            let paths = std::fs::read_dir(tmp_dir_path)?.map(|p| p.unwrap().path()).collect::<Vec<_>>();

            // Convert all but one of the given shards.
            for (i, p) in paths.iter().enumerate() {
                if i == 0 {
                    std::fs::copy(p, tmp_dir_path_keyed.join(p.file_name().unwrap()))?;
                    continue;
                }

                // Do some repeat keys to make sure that path is tested as well.
                let key: HMACKey = rng_hash((i % 6) as u64);

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
                assert_eq!(out.chunk_hmac_key(), Some(key));
            }

            // Now, verify that everything still works great.
            let shard_file_manager = ShardFileManager::load_dir(tmp_dir_path_keyed).await?;
            shard_file_manager
                .load_and_cleanup_shards_by_path(&[tmp_dir_path_keyed])
                .await?;

            verify_mdb_shards_match(&shard_file_manager, &ref_shard, include_info).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_timestamp_filtering() -> Result<()> {
        let tmp_dir = TempDir::new("shard_test_timestamp")?;
        let tmp_dir_path = tmp_dir.path();

        // Just create a single shard; we'll key it with other keys and timestamps and then test loading.
        create_random_shard_collection(0, tmp_dir_path, 1, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6]).await?;

        let path = std::fs::read_dir(tmp_dir_path)?.map(|p| p.unwrap().path()).next().unwrap();

        // Create another that has an expiration date of one second from now.
        let key: HMACKey = rng_hash(0);

        let shard = MDBShardFile::load_from_file(&path)?;

        let _tmp_dir_keyed = TempDir::new("shard_test_keyed_1")?;
        let tmp_dir_path_keyed = _tmp_dir_keyed.path();

        // Reexport this shard as a keyed shards.
        let out = shard
            .export_as_keyed_shard(tmp_dir_path_keyed, key, Duration::new(1, 0), false, false, false)
            .unwrap();

        {
            // Make sure this one gets loaded properly.
            let shard_file_manager = ShardFileManager::new(tmp_dir_path_keyed, false).await?;
            let loaded_shards = shard_file_manager
                .register_shards_by_path(&[tmp_dir_path_keyed], false, Duration::new(100, 0))
                .await?;

            assert_eq!(loaded_shards.len(), 1);
            assert_eq!(loaded_shards[0].shard_hash, out.shard_hash)
        }

        // Sleep for 1.25 seconds to make sure at least a second has passed
        std::thread::sleep(Duration::new(1, 250000000));

        {
            // Now, it shouldn't load any of them.
            let shard_file_manager = ShardFileManager::new(tmp_dir_path_keyed, false).await?;
            let loaded_shards = shard_file_manager
                .register_shards_by_path(&[tmp_dir_path_keyed], false, Duration::new(100, 0))
                .await?;

            assert!(loaded_shards.is_empty());

            // Make sure it leaves the shard there.
            let n_files = std::fs::read_dir(tmp_dir_path_keyed)?.map(|p| p.unwrap().path()).count();

            assert_eq!(n_files, 1);
        }

        // Try again, but allow deletion.  Make sure it gets cleaned up.
        {
            // Now, it shouldn't load any of them.
            let shard_file_manager = ShardFileManager::new(tmp_dir_path_keyed, false).await?;
            let loaded_shards = shard_file_manager
                .register_shards_by_path(&[tmp_dir_path_keyed], true, Duration::new(0, 0))
                .await?;

            assert!(loaded_shards.is_empty());
            let n_files = std::fs::read_dir(tmp_dir_path_keyed)?.map(|p| p.unwrap().path()).count();

            assert_eq!(n_files, 0);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_teardown() -> Result<()> {
        let tmp_dir = TempDir::new("gitxet_shard_test_1")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();

        {
            let mut mdb = ShardFileManager::load_dir(tmp_dir.path()).await?;

            fill_with_specific_shard(&mut mdb, &mut mdb_in_mem, &[(0, &[(11, 5)])], &[(100, &[(200, (0, 5))])]).await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem, true).await?;
            // Note, no flush
        }

        {
            // Now, make sure that this happens if this directory is opened up
            let mdb2 = ShardFileManager::load_dir(tmp_dir.path()).await?;

            // Make sure it's all in there this round.
            verify_mdb_shards_match(&mdb2, &mdb_in_mem, true).await?;
        }

        Ok(())
    }
}
