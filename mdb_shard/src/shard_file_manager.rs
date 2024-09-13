use crate::error::{MDBShardError, Result};
use crate::shard_file_handle::MDBShardFile;
use crate::shard_file_reconstructor::FileReconstructor;
use crate::utils::truncate_hash;
use async_trait::async_trait;
use merklehash::MerkleHash;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tracing::{debug, error, info, trace};

use crate::constants::MDB_SHARD_MIN_TARGET_SIZE;
use crate::{cas_structs::*, file_structs::*, shard_in_memory::MDBInMemoryShard};

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
                error!(
                "Error flushing reconstruction data on shutdown: {sd:?} is not a directory or doesn't exist"
            );
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
const CHUNK_INDEX_TABLE_MAX_DEFAULT_SIZE: usize = 64 * 1024 * 1024;

#[derive(Default)]
struct ShardFileCollection {
    shard_list: Vec<(MDBShardFile, bool)>,
    shards: HashMap<MerkleHash, usize>,
    chunk_lookup: HashMap<u64, ChunkCacheElement>,
    num_indexed_shards: usize,
    chunk_index_max_size: usize,
}

impl ShardFileCollection {
    fn new() -> Self {
        Self {
            chunk_index_max_size: std::env::var("XET_MAX_CHUNK_CACHE_SIZE")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(CHUNK_INDEX_TABLE_MAX_DEFAULT_SIZE),
            ..Default::default()
        }
    }
}

pub struct ShardFileManager {
    shard_file_lookup: Arc<RwLock<ShardFileCollection>>,
    current_state: Arc<RwLock<MDBShardFlushGuard>>,
    target_shard_min_size: u64,
}

struct ChunkCacheElement {
    cas_start_index: u32,
    cas_chunk_offset: u16,
    shard_index: u16, // This one is last so that the u16 bits can be packed in.
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
///
impl ShardFileManager {
    /// Construct a new shard file manager that uses session_directory as the temporary dumping  
    pub async fn new(session_directory: &Path) -> Result<Self> {
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
            shard_file_lookup: Arc::new(RwLock::new(ShardFileCollection::new())),
            current_state: Arc::new(RwLock::new(MDBShardFlushGuard {
                shard: MDBInMemoryShard::default(),
                session_directory: session_directory.clone(),
            })),
            target_shard_min_size: MDB_SHARD_MIN_TARGET_SIZE,
        };

        if let Some(sd) = &session_directory {
            s.register_shards_by_path(&[sd], false).await?;
        }
        Ok(s)
    }

    // Clear out everything; used mainly for debugging.
    pub async fn clear(&self) {
        {
            let mut sfc_rw = self.shard_file_lookup.write().await;
            sfc_rw.shards.clear();
            sfc_rw.chunk_lookup.clear();
        }

        self.current_state.write().await.shard = <_>::default();
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
        shards_are_permanent: bool,
    ) -> Result<Vec<MDBShardFile>> {
        let mut new_shards = Vec::new();
        for p in paths {
            new_shards.append(&mut MDBShardFile::load_all(p.as_ref())?);
        }

        self.register_shards(&new_shards, shards_are_permanent)
            .await?;
        Ok(new_shards)
    }

    pub async fn register_shards(
        &self,
        new_shards: &[MDBShardFile],
        shards_are_permanent: bool,
    ) -> Result<()> {
        let mut shards_lg = self.shard_file_lookup.write().await;

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
            debug!(
                "register_shards: Registering shard {:?} at {:?}.",
                s.shard_hash, s.path
            );
            let cur_index = shards_lg.shard_list.len();
            let mut inserted = false;

            shards_lg.shards.entry(s.shard_hash).or_insert_with(|| {
                inserted = true;
                cur_index
            });

            if inserted {
                shards_lg.shard_list.push((s.clone(), shards_are_permanent));
                if cur_index < u16::MAX as usize
                    && shards_lg.chunk_lookup.len() + s.shard.total_num_chunks()
                        < shards_lg.chunk_index_max_size
                {
                    for (h, (cas_start_index, cas_chunk_offset)) in s.read_all_truncated_hashes()? {
                        if cas_chunk_offset > u16::MAX as u32 {
                            break;
                        }
                        let cas_chunk_offset = cas_chunk_offset as u16;

                        shards_lg.chunk_lookup.insert(
                            h,
                            ChunkCacheElement {
                                cas_start_index,
                                cas_chunk_offset,
                                shard_index: cur_index as u16,
                            },
                        );
                        shards_lg.num_indexed_shards = cur_index + 1;
                    }
                }
            }
        }

        if num_shards != 0 {
            info!(
                "Registered {} new shards, for {} shards total. Chunk pre-lookup now has {} chunks.",
                num_shards,
                shards_lg.shard_list.len(),
                shards_lg.chunk_lookup.len()
            );
        }

        Ok(())
    }

    pub async fn shard_is_registered(&self, shard_hash: &MerkleHash) -> bool {
        self.shard_file_lookup
            .read()
            .await
            .shards
            .contains_key(shard_hash)
    }

    // If the shard with the given hash is present, then it a handle to it is returned.  If not,
    // returns None.
    pub async fn get_shard_handle(
        &self,
        shard_hash: &MerkleHash,
        allow_temporary: bool,
    ) -> Option<MDBShardFile> {
        let shards_lg = self.shard_file_lookup.read().await;
        if let Some(index) = shards_lg.shards.get(shard_hash) {
            let (sfi, is_permanent) = &shards_lg.shard_list[*index];
            if !*is_permanent && !allow_temporary {
                None
            } else {
                Some(sfi.clone())
            }
        } else {
            None
        }
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
            return Ok(Some((
                MDBFileInfo {
                    metadata: FileDataSequenceHeader::new(MerkleHash::default(), 0),
                    segments: Vec::default(),
                },
                None,
            )));
        }

        // First attempt the in-memory version of this.
        {
            let lg = self.current_state.read().await;
            let file_info = lg.shard.get_file_reconstruction_info(file_hash);
            if let Some(fi) = file_info {
                return Ok(Some((fi, None)));
            }
        }

        let current_shards = self.shard_file_lookup.read().await;

        for (si, _) in current_shards.shard_list.iter() {
            trace!("Querying for hash {file_hash:?} in {:?}.", si.path);
            if let Some(fi) = si.get_file_reconstruction_info(file_hash)? {
                return Ok(Some((fi, Some(si.shard_hash))));
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
        origin_tracking: Option<&mut HashMap<MerkleHash, usize>>,
    ) -> Result<Option<(usize, FileDataSequenceEntry)>> {
        // First attempt the in-memory version of this.
        {
            let lg = self.current_state.read().await;
            let ret = lg.shard.chunk_hash_dedup_query(query_hashes);
            if ret.is_some() {
                return Ok(ret);
            }
        }

        let shard_lg = self.shard_file_lookup.read().await;

        if let Some(cce) = shard_lg.chunk_lookup.get(&truncate_hash(&query_hashes[0])) {
            let (si, is_permanent) = &shard_lg.shard_list[cce.shard_index as usize];

            if let Some((count, fdse)) = si.chunk_hash_dedup_query_direct(
                query_hashes,
                cce.cas_start_index,
                cce.cas_chunk_offset as u32,
            )? {
                if *is_permanent {
                    if let Some(tracker) = origin_tracking {
                        *tracker.entry(si.shard_hash).or_default() += count;
                    }
                }
                return Ok(Some((count, fdse)));
            }
        }

        // Now we skip the indices for the upcoming aspects of stuff.
        for (si, is_permanent) in shard_lg.shard_list[shard_lg.num_indexed_shards..].iter() {
            trace!("Querying for hash {:?} in {:?}.", &query_hashes[0], si.path);
            if let Some((count, fdse)) = si.chunk_hash_dedup_query(query_hashes)? {
                if *is_permanent {
                    if let Some(tracker) = origin_tracking {
                        *tracker.entry(si.shard_hash).or_default() += count;
                    }
                }
                return Ok(Some((count, fdse)));
            }
        }

        Ok(None)
    }

    /// Add CAS info to the in-memory state.
    pub async fn add_cas_block(&self, cas_block_contents: MDBCASInfo) -> Result<()> {
        let mut lg = self.current_state.write().await;

        if lg.shard.shard_file_size() + cas_block_contents.num_bytes() >= self.target_shard_min_size
        {
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
            self.register_shards_by_path(&[&path], false).await?;
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

        for (si, _) in self.shard_file_lookup.read().await.shard_list.iter() {
            bytes += si.shard.materialized_bytes();
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

        for (si, _) in self.shard_file_lookup.read().await.shard_list.iter() {
            bytes += si.shard.stored_bytes();
        }

        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader},
        file_structs::FileDataSequenceHeader,
        session_directory::consolidate_shards_in_directory,
        shard_format::test_routines::{rng_hash, simple_hash},
    };

    use super::*;
    use crate::error::Result;
    use more_asserts::assert_lt;
    use rand::prelude::*;
    use tempdir::TempDir;

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
                .map(|(h, (lb, ub))| {
                    FileDataSequenceEntry::new(simple_hash(*h), *ub - *lb, *lb, *ub)
                })
                .collect();
            let file_info = MDBFileInfo {
                metadata: FileDataSequenceHeader::new(simple_hash(*file_hash), segments.len()),
                segments: file_contents,
            };

            shard
                .add_file_reconstruction_info(file_info.clone())
                .await?;

            in_mem_shard.add_file_reconstruction_info(file_info)?;
        }

        Ok(())
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
                chunks.push(CASChunkSequenceEntry::new(
                    rng_hash(rng.gen()),
                    rng.gen_range(10000..20000),
                    pos,
                ));
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

            let metadata = FileDataSequenceHeader::new(file_hash, *file_block_size);

            let file_info = MDBFileInfo { metadata, segments };

            shard
                .add_file_reconstruction_info(file_info.clone())
                .await?;

            in_mem_shard.add_file_reconstruction_info(file_info)?;
        }
        Ok(())
    }

    pub async fn verify_mdb_shards_match(
        mdb: &ShardFileManager,
        mem_shard: &MDBInMemoryShard,
    ) -> Result<()> {
        // Now, test that the results on queries from the
        for (k, cas_block) in mem_shard.cas_content.iter() {
            // Go through and test queries on both the in-memory shard and the
            // serialized shard, making sure that they match completely.

            for i in 0..cas_block.chunks.len() {
                // Test the dedup query over a few hashes in which all the
                // hashes queried are part of the cas_block.
                let query_hashes_1: Vec<MerkleHash> = cas_block.chunks
                    [i..(i + 3).min(cas_block.chunks.len())]
                    .iter()
                    .map(|c| c.chunk_hash)
                    .collect();
                let n_items_to_read = query_hashes_1.len();

                // Also test the dedup query over a few hashes in which some of the
                // hashes are part of the query, and the last is not.
                let mut query_hashes_2 = query_hashes_1.clone();
                query_hashes_2.push(rng_hash(1000000 + i as u64));

                let lb = cas_block.chunks[i].chunk_byte_range_start;
                let ub = if i + 3 >= cas_block.chunks.len() {
                    cas_block.metadata.num_bytes_in_cas
                } else {
                    cas_block.chunks[i + 3].chunk_byte_range_start
                };

                for query_hashes in [&query_hashes_1, &query_hashes_2] {
                    let result_m = mem_shard.chunk_hash_dedup_query(query_hashes).unwrap();

                    let result_f = mdb
                        .chunk_hash_dedup_query(query_hashes, None)
                        .await?
                        .unwrap();

                    // Returns a tuple of (num chunks matched, FileDataSequenceEntry)
                    assert_eq!(result_m.0, n_items_to_read);
                    assert_eq!(result_f.0, n_items_to_read);

                    // Make sure it gives the correct CAS block hash as the second part of the
                    assert_eq!(result_m.1.cas_hash, *k);
                    assert_eq!(result_f.1.cas_hash, *k);

                    // Make sure the bounds are correct
                    assert_eq!(
                        (
                            result_m.1.chunk_byte_range_start,
                            result_m.1.chunk_byte_range_end
                        ),
                        (lb, ub)
                    );
                    assert_eq!(
                        (
                            result_f.1.chunk_byte_range_start,
                            result_f.1.chunk_byte_range_end
                        ),
                        (lb, ub)
                    );

                    // Make sure everything else equal.
                    assert_eq!(result_m, result_f);
                }
            }
        }

        // Test get file reconstruction info.
        // Against some valid hashes,
        let mut query_hashes: Vec<MerkleHash> =
            mem_shard.file_content.iter().map(|file| *file.0).collect();
        // and a few (very likely) invalid somes.
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
        assert_eq!(
            mdb.calculate_total_materialized_bytes().await?,
            mem_shard.materialized_bytes()
        );
        assert_eq!(
            mdb.calculate_total_stored_bytes().await?,
            mem_shard.stored_bytes()
        );

        Ok(())
    }
    #[tokio::test]
    async fn test_basic_retrieval() -> Result<()> {
        let tmp_dir = TempDir::new("gitxet_shard_test_1")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();

        {
            let mut mdb = ShardFileManager::new(tmp_dir.path()).await?;

            fill_with_specific_shard(
                &mut mdb,
                &mut mdb_in_mem,
                &[(0, &[(11, 5)])],
                &[(100, &[(200, (0, 5))])],
            )
            .await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;

            let out_file = mdb.flush().await?.unwrap();

            // Make sure it still stays consistent after a flush
            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;

            // Verify that the file is correct
            MDBShardFile::load_from_file(&out_file)?.verify_shard_integrity();
        }
        {
            // Now, make sure that this happens if this directory is opened up
            let mut mdb2 = ShardFileManager::new(tmp_dir.path()).await?;

            // Make sure it's all in there this round.
            verify_mdb_shards_match(&mdb2, &mdb_in_mem).await?;

            // Now add some more, based on this directory
            fill_with_random_shard(
                &mut mdb2,
                &mut mdb_in_mem,
                0,
                &[1, 5, 10, 8],
                &[4, 3, 5, 9, 4, 6],
            )
            .await?;

            verify_mdb_shards_match(&mdb2, &mdb_in_mem).await?;

            // Now, merge shards in the background.
            let merged_shards =
                consolidate_shards_in_directory(tmp_dir.path(), MDB_SHARD_MIN_TARGET_SIZE)?;

            assert_eq!(merged_shards.len(), 1);
            for si in merged_shards {
                assert!(si.path.exists());
                assert!(si.path.to_str().unwrap().contains(&si.shard_hash.hex()))
            }

            verify_mdb_shards_match(&mdb2, &mdb_in_mem).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_larger_simulated() -> Result<()> {
        let tmp_dir = TempDir::new("gitxet_shard_test_2")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();
        let mut mdb = ShardFileManager::new(tmp_dir.path()).await?;

        for i in 0..10 {
            fill_with_random_shard(
                &mut mdb,
                &mut mdb_in_mem,
                i,
                &[1, 5, 10, 8],
                &[4, 3, 5, 9, 4, 6],
            )
            .await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;

            let out_file = mdb.flush().await?.unwrap();

            // Make sure it still stays consistent
            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;

            // Verify that the file is correct
            MDBShardFile::load_from_file(&out_file)?.verify_shard_integrity();

            // Make sure an empty flush doesn't bother anything.
            mdb.flush().await?;

            // Now, make sure that this happens if this directory is opened up
            let mdb2 = ShardFileManager::new(tmp_dir.path()).await?;

            // Make sure it's all in there this round.
            verify_mdb_shards_match(&mdb2, &mdb_in_mem).await?;
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
                    let mut mdb = ShardFileManager::new(tmp_dir.path()).await.unwrap();
                    fill_with_random_shard(
                        &mut mdb,
                        &mut mdb_in_mem,
                        100 * sesh + i,
                        &[1, 5, 10, 8],
                        &[4, 3, 5, 9, 4, 6],
                    )
                    .await
                    .unwrap();

                    verify_mdb_shards_match(&mdb, &mdb_in_mem).await.unwrap();

                    let out_file = mdb.flush().await.unwrap().unwrap();

                    // Make sure it still stays together
                    verify_mdb_shards_match(&mdb, &mdb_in_mem).await.unwrap();

                    // Verify that the file is correct
                    MDBShardFile::load_from_file(&out_file)?.verify_shard_integrity();

                    mdb.flush().await.unwrap();

                    verify_mdb_shards_match(&mdb, &mdb_in_mem).await.unwrap();
                }
            }

            {
                let merged_shards =
                    consolidate_shards_in_directory(tmp_dir.path(), MDB_SHARD_MIN_TARGET_SIZE)
                        .unwrap();

                assert_eq!(merged_shards.len(), 1);

                for si in merged_shards {
                    assert!(si.path.exists());
                    assert!(si.path.to_str().unwrap().contains(&si.shard_hash.hex()))
                }
            }

            {
                // Now, make sure that this happens if this directory is opened up
                let mdb2 = ShardFileManager::new(tmp_dir.path()).await.unwrap();

                verify_mdb_shards_match(&mdb2, &mdb_in_mem).await.unwrap();
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
            let mut mdb = ShardFileManager::new(tmp_dir.path()).await?;
            mdb.set_target_shard_min_size(T); // Set the targe shard size really low
            fill_with_random_shard(&mut mdb, &mut mdb_in_mem, 0, &[16; 16], &[16; 16]).await?;
        }
        {
            let mut mdb = ShardFileManager::new(tmp_dir.path()).await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;

            mdb.set_target_shard_min_size(2 * T);
            fill_with_random_shard(&mut mdb, &mut mdb_in_mem, 1, &[25; 25], &[25; 25]).await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;
        }

        // Reload and verify
        {
            let mdb = ShardFileManager::new(tmp_dir.path()).await?;
            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;
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
            let mdb = ShardFileManager::new(tmp_dir.path()).await?;
            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_size_threshholds() -> Result<()> {
        let tmp_dir = TempDir::new("gitxet_shard_test_4")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();

        const T: u64 = 4096;

        for i in 0..5 {
            let mut mdb = ShardFileManager::new(tmp_dir.path()).await?;
            mdb.set_target_shard_min_size(T); // Set the targe shard size really low
            fill_with_random_shard(&mut mdb, &mut mdb_in_mem, i, &[5; 25], &[5; 25]).await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;

            let out_file = mdb.flush().await?.unwrap();

            // Verify that the file is correct
            MDBShardFile::load_from_file(&out_file)?.verify_shard_integrity();

            // Make sure it still stays together
            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;

            assert!(mdb.flush().await?.is_none());
        }

        // Now, do a new shard that has less
        let mut last_num_files = None;
        let mut target_size = T;
        loop {
            // Now, make sure that this happens if this directory is opened up
            let mut mdb2 = ShardFileManager::new(tmp_dir.path()).await?;
            mdb2.set_target_shard_min_size(target_size);

            // Make sure it's all in there this round.
            verify_mdb_shards_match(&mdb2, &mdb_in_mem).await?;

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
    async fn test_teardown() -> Result<()> {
        let tmp_dir = TempDir::new("gitxet_shard_test_1")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();

        {
            let mut mdb = ShardFileManager::new(tmp_dir.path()).await?;

            fill_with_specific_shard(
                &mut mdb,
                &mut mdb_in_mem,
                &[(0, &[(11, 5)])],
                &[(100, &[(200, (0, 5))])],
            )
            .await?;

            verify_mdb_shards_match(&mdb, &mdb_in_mem).await?;
            // Note, no flush
        }

        {
            // Now, make sure that this happens if this directory is opened up
            let mdb2 = ShardFileManager::new(tmp_dir.path()).await?;

            // Make sure it's all in there this round.
            verify_mdb_shards_match(&mdb2, &mdb_in_mem).await?;
        }

        Ok(())
    }
}
