use std::collections::{BTreeSet, HashMap};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use tokio::sync::RwLock;
use tracing::{debug, info, instrument, trace, warn};
use xet_runtime::core::{XetCommon, XetContext};
use xet_runtime::utils::RwTaskLock;

use super::constants::{MDB_SHARD_EXPIRATION_BUFFER, MDB_SHARD_TARGET_SIZE};
use super::file_structs::*;
use super::shard_file_handle::{MDBShardFile, ShardFileCache};
use super::shard_file_reconstructor::FileReconstructor;
use super::shard_in_memory::MDBInMemoryShard;
use super::utils::truncate_hash;
use super::xorb_structs::*;
use crate::error::{CoreError, Result};
use crate::merklehash::{HMACKey, MerkleHash};
use crate::{MerkleHashMap, TruncatedMerkleHashMap};

type ShardFileManagerCache = Arc<RwLock<HashMap<PathBuf, Arc<ShardFileManager>>>>;

fn get_sfm_cache(common: &XetCommon) -> ShardFileManagerCache {
    common.cache_get_or_create("mdb_shard_file_manager_cache", || Arc::new(RwLock::new(HashMap::new())))
}

pub fn get_shard_file_cache(common: &XetCommon) -> ShardFileCache {
    common.cache_get_or_create("mdb_shard_file_cache", super::shard_file_handle::new_shard_file_cache)
}

// The structure used as the target for the dedup lookup
#[repr(Rust, packed)]
struct ChunkCacheElement {
    xorb_start_index: u32, // the index of the first chunk
    xorb_chunk_offset: u16,
    shard_index: u16, // This one is last so that the u16 bits can be packed in.
}

#[derive(Default)]
struct KeyedShardCollection {
    hmac_key: HMACKey,
    shard_list: Vec<Arc<MDBShardFile>>,
    chunk_lookup: TruncatedMerkleHashMap<ChunkCacheElement>,

    // The number of shards in this collection with entries in chunk_lookup.
    num_indexed_shards: usize,
}

impl KeyedShardCollection {
    fn new(hmac_key: HMACKey) -> Self {
        Self {
            hmac_key,
            ..Default::default()
        }
    }
}

/// A shard with entries in the chunk index.  Ordered oldest first, so the first one is evicted first.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct IndexedShard {
    // The footer's shard_creation_timestamp, in seconds since the epoch.  As the first field, it decides the
    // eviction order through the derived Ord.  Shards written locally from an in-memory shard leave it at 0;
    // ties fall back to the collection and shard indices.
    creation_timestamp: u64,
    shard_collection_index: usize,
    shard_index_in_collection: usize,
}

/// What registering a shard does when its chunks don't fit in the chunk index, which holds at most
/// `chunk_index_table_max_size` entries.  Either way the shard is registered; the policy only decides whether
/// its chunks are indexed for dedup.
#[derive(Clone, Copy)]
pub enum ChunkIndexSizeEnforcement {
    /// Leave the index as it is: a shard that doesn't fit is registered without indexing its chunks.
    StopAtCap,

    /// Evict the oldest indexed shards.
    Eviction,
}

/// We have a couple levels of arc redirection here to handle the different queries needed.
/// This holds the registered shards, their chunk index, and the state for keeping that index under its cap.
struct ShardBookkeeper {
    // The collections of shards that share an HMAC key; chunk lookups query them in order.  Index 0 is the
    // unkeyed collection, so local shards are tried first.  A key gets another collection each time its current
    // one holds max_shards_per_collection shards.
    shard_collections: Vec<KeyedShardCollection>,

    // Maps an HMAC key to the index in shard_collections of its newest collection, which new shards go into.
    collection_by_key: MerkleHashMap<usize>,

    // Maps the hash of every registered shard to its (collection index, index in that
    // collection's shard_list).  A shard already in here is not registered again.
    shard_lookup_by_shard_hash: MerkleHashMap<(usize, usize)>,

    // We cap the number of chunks indexed for dedup.
    total_indexed_chunks: usize,

    // Shards with entries in their collection's chunk_lookup, oldest first.  Evicted shards stay registered.
    indexed_shards: BTreeSet<IndexedShard>,

    // The most shards a collection holds.  The chunk index stores a shard's index in its collection as a u16, so
    // this is u16::MAX + 1; it's a field only so tests can shrink it.
    max_shards_per_collection: usize,
}

impl ShardBookkeeper {
    fn new() -> Self {
        // In creating the bookkeeping class, put the one without the hmac key in first so
        // we always try to dedup locally first.
        Self {
            shard_collections: vec![KeyedShardCollection::new(HMACKey::default())],
            collection_by_key: MerkleHashMap::from([(HMACKey::default(), 0)]),
            shard_lookup_by_shard_hash: MerkleHashMap::new(),
            total_indexed_chunks: 0,
            indexed_shards: BTreeSet::new(),
            max_shards_per_collection: u16::MAX as usize + 1,
        }
    }

    /// Adds `shard` to the newest collection for its HMAC key, opening a new one if there is none or it is full,
    /// and returns (collection index, index in that collection's shard_list).
    fn add_shard_to_collection(&mut self, shard: Arc<MDBShardFile>) -> (usize, usize) {
        let hmac_key = shard.shard.metadata.chunk_hash_hmac_key;

        let open_col_index = self
            .collection_by_key
            .get(&hmac_key)
            .copied()
            .filter(|&i| self.shard_collections[i].shard_list.len() < self.max_shards_per_collection);

        let shard_col_index = match open_col_index {
            Some(i) => i,
            None => {
                self.shard_collections.push(KeyedShardCollection::new(hmac_key));
                let new_col_index = self.shard_collections.len() - 1;
                self.collection_by_key.insert(hmac_key, new_col_index);
                new_col_index
            },
        };

        let shard_list = &mut self.shard_collections[shard_col_index].shard_list;
        shard_list.push(shard);
        (shard_col_index, shard_list.len() - 1)
    }

    /// Removes the index entries of the oldest indexed shards until `num_new_chunks` more entries fit under
    /// `chunk_index_table_max_size`.
    fn check_and_evict_indexed_shards(&mut self, num_new_chunks: usize, chunk_index_table_max_size: usize) {
        while self.total_indexed_chunks + num_new_chunks > chunk_index_table_max_size
            && let Some(oldest) = self.indexed_shards.pop_first()
        {
            info!(
                "Chunk index reached its limit of {chunk_index_table_max_size} entries; evicting the oldest \
                    shards from it. Set HF_XET_SHARD_CHUNK_INDEX_TABLE_MAX_SIZE to raise the limit."
            );

            self.remove_indexed_chunks(oldest);
        }
    }

    /// Removes an indexed shard's entries from its collection's chunk_lookup.  The caller has already taken it out
    /// of indexed_shards.
    fn remove_indexed_chunks(&mut self, indexed: IndexedShard) {
        let shard_col = &mut self.shard_collections[indexed.shard_collection_index];
        let old_chunk_lookup_size = shard_col.chunk_lookup.len();

        if shard_col.num_indexed_shards == 1 {
            // Release the memory rather than keeping the capacity around.
            shard_col.chunk_lookup = TruncatedMerkleHashMap::default();
        } else {
            shard_col
                .chunk_lookup
                .retain(|_, cce| cce.shard_index as usize != indexed.shard_index_in_collection);
        }
        shard_col.num_indexed_shards -= 1;

        self.total_indexed_chunks -= old_chunk_lookup_size - shard_col.chunk_lookup.len();
    }
}

pub struct ShardFileManager {
    chunk_index_table_max_size: usize,
    shard_file_cache: ShardFileCache,
    shard_bookkeeper: RwTaskLock<ShardBookkeeper, CoreError>,
    current_state: RwLock<MDBInMemoryShard>,
    shard_directory: PathBuf,
    target_shard_max_size: u64,
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
/// mng.register_shards(&[other shard, directories, etc.], ChunkIndexSizeEnforcement::StopAtCap)?;
///
/// // Run queries, add data, etc. with get_file_reconstruction_info, chunk_hash_dedup_query,
/// add_xorb_block, add_file_reconstruction_info.
impl ShardFileManager {
    // Construct in a session directory.
    pub async fn new_in_session_directory(
        ctx: &XetContext,
        session_directory: impl AsRef<Path>,
        scan_directory: bool,
    ) -> Result<Arc<Self>> {
        Self::new_impl(ctx, session_directory, false, *MDB_SHARD_TARGET_SIZE, scan_directory, 0).await
    }

    // Construction functions
    pub async fn new_in_cache_directory(ctx: &XetContext, cache_directory: impl AsRef<Path>) -> Result<Arc<Self>> {
        Self::new_impl(
            ctx,
            cache_directory,
            true,
            *MDB_SHARD_TARGET_SIZE,
            true,
            ctx.config.shard.cache_size_limit.as_u64(),
        )
        .await
    }

    async fn new_impl(
        ctx: &XetContext,
        directory: impl AsRef<Path>,
        is_cachable: bool,
        target_shard_max_size: u64,
        scan_directory: bool,
        prune_cache_to_size: u64, // Set to 0 to disable pruning
    ) -> Result<Arc<Self>> {
        let shard_directory = std::path::absolute(directory)?;

        // Make sure the shard directory exists; create it if not.
        if !shard_directory.exists() {
            std::fs::create_dir_all(&shard_directory)?;
        }

        let chunk_index_table_max_size = ctx.config.shard.chunk_index_table_max_size;
        let shard_file_cache = get_shard_file_cache(&ctx.common);
        let sfm_cache = is_cachable.then(|| get_sfm_cache(&ctx.common));

        // Reuse the manager cached for this directory, if there is one.
        let cached_sfm = match &sfm_cache {
            Some(sfm_cache) => sfm_cache.read().await.get(&shard_directory).cloned(),
            None => None,
        };

        if let Some(sfm) = cached_sfm {
            if scan_directory {
                // The manager has indexed the directory before, so the shards registered now were added since then
                // and are newer than the ones it holds; make room for them.
                sfm.refresh_shard_dir(true, prune_cache_to_size, ChunkIndexSizeEnforcement::Eviction)
                    .await?;
            }
            return Ok(sfm);
        }

        let sfm = Arc::new(Self {
            chunk_index_table_max_size,
            shard_file_cache,
            shard_bookkeeper: RwTaskLock::from_value(ShardBookkeeper::new()),
            current_state: RwLock::new(MDBInMemoryShard::default()),
            shard_directory: shard_directory.clone(),
            target_shard_max_size,
            shard_directory_cleaned: AtomicBool::new(false),
        });

        if scan_directory {
            // Shards are registered newest first, so once the index is full, stop rather than evict the shards we
            // just indexed.
            sfm.refresh_shard_dir(true, prune_cache_to_size, ChunkIndexSizeEnforcement::StopAtCap)
                .await?;
        }

        let Some(sfm_cache) = sfm_cache else {
            return Ok(sfm);
        };

        // Scanning happens outside the cache lock, so another caller may have cached a manager for this directory
        // in the meantime; use theirs so every caller shares one manager.
        Ok(sfm_cache.write().await.entry(shard_directory).or_insert(sfm).clone())
    }

    /// Registers the shards in the shard directory that aren't registered yet, first deleting expired shards if
    /// `prune_expired` is set and the oldest shards beyond `prune_cache_to_size` bytes if it's nonzero.
    /// `index_size_enforcement` applies once the chunk index is full.
    pub async fn refresh_shard_dir(
        &self,
        prune_expired: bool,
        prune_cache_to_size: u64,
        index_size_enforcement: ChunkIndexSizeEnforcement,
    ) -> Result<()> {
        let mut shard_files = MDBShardFile::load_managed_directory(
            &self.shard_directory,
            true,
            false,
            prune_expired,
            prune_cache_to_size,
            &self.shard_file_cache,
        )?;

        {
            let shard_read_guard = self.shard_bookkeeper.read().await?;
            shard_files.retain(|s| !shard_read_guard.shard_lookup_by_shard_hash.contains_key(&s.shard_hash));
        }

        self.register_shards(&shard_files, index_size_enforcement).await?;

        Ok(())
    }

    pub async fn import_shard_from_bytes(
        &self,
        shard: &[u8],
        index_size_enforcement: ChunkIndexSizeEnforcement,
    ) -> Result<()> {
        let new_shard_file = MDBShardFile::write_out_from_reader(
            &self.shard_directory,
            &mut Cursor::new(shard),
            &self.shard_file_cache,
        )?;

        self.register_shards(&[new_shard_file], index_size_enforcement).await
    }

    pub fn shard_directory(&self) -> &Path {
        &self.shard_directory
    }

    // Clean out expired shards, with an expiration deletion window.
    pub fn clean_expired_shards_if_needed(&self) -> Result<()> {
        let needs_clean = self.shard_directory_cleaned.swap(true, std::sync::atomic::Ordering::Relaxed);

        if needs_clean {
            MDBShardFile::clean_shard_cache(
                &self.shard_directory,
                MDB_SHARD_EXPIRATION_BUFFER.as_secs(),
                &self.shard_file_cache,
            )?;
        }

        Ok(())
    }

    pub async fn register_shards_by_path<P: AsRef<Path>>(
        &self,
        new_shards: &[P],
        index_size_enforcement: ChunkIndexSizeEnforcement,
    ) -> Result<()> {
        let new_shards: Vec<Arc<_>> = new_shards.iter().try_fold(Vec::new(), |mut acc, p| {
            acc.extend(MDBShardFile::load_all_valid(p, &self.shard_file_cache)?);

            Result::Ok(acc)
        })?;

        self.register_shards(&new_shards, index_size_enforcement).await
    }

    /// Registers shards that live in the shard directory, newest first by modification time, and indexes their
    /// chunks for dedup.  Shards already registered are skipped.  Once the chunk index is full,
    /// `index_size_enforcement` decides whether a shard's chunks are left out or replace the oldest indexed shards.
    pub async fn register_shards(
        &self,
        new_shards: &[Arc<MDBShardFile>],
        index_size_enforcement: ChunkIndexSizeEnforcement,
    ) -> Result<()> {
        // Go through and register the shards in order of newest to oldest
        let mut new_shards = Vec::from(new_shards);

        // Compare in reverse order to sort from newest to oldest
        new_shards.sort_by_key(|shard| std::cmp::Reverse(shard.last_modified_time));
        let num_shards = new_shards.len();

        for s in new_shards {
            s.verify_shard_integrity_debug_only();

            // Make sure the shard is in the shard directory
            debug_assert!(s.path.starts_with(&self.shard_directory), "{:?} not in {:?}", s.path, self.shard_directory);

            // Is this shard already registered?
            if self
                .shard_bookkeeper
                .read()
                .await?
                .shard_lookup_by_shard_hash
                .contains_key(&s.shard_hash)
            {
                continue;
            }

            // Begin loading the truncated hashes in the background for this shard so they're ready
            // when we have to insert them.
            let s_rth = s.clone();
            let s_truncated_hashes_jh = tokio::task::spawn_blocking(move || s_rth.read_all_truncated_hashes());

            let chunk_index_table_max_size = self.chunk_index_table_max_size;
            // Update the bookkeeper with the task of
            self.shard_bookkeeper
                .update(move |mut sbkp_lg| async move {
                    debug!("register_shards: Registering shard {:?} at {:?}.", s.shard_hash, s.path);

                    // A shard whose hashes can't be read is left unregistered, so a later rescan tries it again.
                    let Ok(insert_hashes_task_result) = s_truncated_hashes_jh.await else {
                        warn!("Join error on task reading hashes from {:?}; skipping registration.", &s.path);
                        return Ok(sbkp_lg);
                    };

                    let Ok(insert_hashes) = insert_hashes_task_result.map_err(|e| {
                        warn!("Error reading hashes from {:?}: {e:?}. Skipping registration.", &s.path);
                    }) else {
                        return Ok(sbkp_lg);
                    };

                    let creation_timestamp = s.shard.metadata.shard_creation_timestamp;
                    let shard_hash = s.shard_hash;

                    let (shard_col_index, shard_index) = sbkp_lg.add_shard_to_collection(s.clone());

                    sbkp_lg
                        .shard_lookup_by_shard_hash
                        .insert(shard_hash, (shard_col_index, shard_index));

                    // If chunk index is full and we don't want eviction
                    if sbkp_lg.total_indexed_chunks + insert_hashes.len() > chunk_index_table_max_size
                        && matches!(index_size_enforcement, ChunkIndexSizeEnforcement::StopAtCap)
                    {
                        return Ok(sbkp_lg);
                    }

                    sbkp_lg.check_and_evict_indexed_shards(insert_hashes.len(), chunk_index_table_max_size);

                    // Now add in the chunk indices.
                    let shard_col = &mut sbkp_lg.shard_collections[shard_col_index];
                    let old_chunk_lookup_size = shard_col.chunk_lookup.len();

                    shard_col.chunk_lookup.reserve(insert_hashes.len());

                    for (h, (xorb_start_index, xorb_chunk_offset)) in insert_hashes {
                        if xorb_chunk_offset > u16::MAX as u32 {
                            continue;
                        }

                        let xorb_chunk_offset = xorb_chunk_offset as u16;

                        shard_col.chunk_lookup.insert(
                            h,
                            ChunkCacheElement {
                                xorb_start_index,
                                xorb_chunk_offset,
                                shard_index: shard_index as u16,
                            },
                        );
                    }

                    shard_col.num_indexed_shards += 1;

                    sbkp_lg.total_indexed_chunks += shard_col.chunk_lookup.len() - old_chunk_lookup_size;

                    sbkp_lg.indexed_shards.insert(IndexedShard {
                        creation_timestamp,
                        shard_collection_index: shard_col_index,
                        shard_index_in_collection: shard_index,
                    });

                    Ok(sbkp_lg)
                })
                .await?;
        }

        if num_shards != 0 {
            // Resolve any pending bookkeeper update so its JoinHandle is dropped.
            // A lingering JoinHandle keeps the Tokio I/O driver alive, leaking FDs
            // when the ShardFileManager outlives the runtime (e.g. in the global cache).
            let _ = self.shard_bookkeeper.read().await?;
            debug!("Registered {num_shards} new shards.");
        }

        Ok(())
    }

    pub async fn all_file_info(&self) -> Result<Vec<MDBFileInfo>> {
        // Start with everything that is now in-memory
        let mut all_file_info: Vec<MDBFileInfo> =
            self.current_state.read().await.file_content.values().cloned().collect();

        let shard_files = MDBShardFile::load_all_valid(&self.shard_directory, &self.shard_file_cache)?;

        for shard in shard_files {
            all_file_info.append(&mut shard.read_all_file_info_sections()?);
        }

        Ok(all_file_info)
    }

    pub async fn registered_shard_list(&self) -> Result<Vec<Arc<MDBShardFile>>> {
        let shards = self.shard_bookkeeper.read().await?;

        Ok(shards
            .shard_lookup_by_shard_hash
            .values()
            .map(|&(i, j)| shards.shard_collections[i].shard_list[j].clone())
            .collect())
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl FileReconstructor<CoreError> for ShardFileManager {
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

        let current_shards = self.shard_bookkeeper.read().await?;

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
    // of entries matched from the input hashes, the XORB block hash of the match,
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

        let shard_lg = self.shard_bookkeeper.read().await?;

        for shard_col in shard_lg.shard_collections.iter() {
            // Skip collections with nothing indexed, such as fully evicted ones, without computing the HMAC.
            if shard_col.chunk_lookup.is_empty() {
                continue;
            }

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
                    si.chunk_hash_dedup_query_direct(query_hashes, cce.xorb_start_index, cce.xorb_chunk_offset as u32)?
                {
                    return Ok(Some((count, fdse)));
                }
            }
        }

        Ok(None)
    }

    /// Add XORB info to the in-memory state.
    pub async fn add_xorb_block(&self, xorb_block_contents: impl Into<Arc<MDBXorbInfo>>) -> Result<()> {
        let xorb_block_contents = xorb_block_contents.into();
        let mut lg = self.current_state.write().await;

        // cut a new shard if adding this xorb block will take us over the max shard file size
        let new_shard_path = if lg.shard_file_size() + xorb_block_contents.num_bytes() > self.target_shard_max_size {
            let path = Self::cut_shard(&mut lg, &self.shard_directory)?;
            Some(path)
        } else {
            None
        };

        lg.add_xorb_block(xorb_block_contents)?;
        drop(lg);

        if let Some(new_shard_path) = new_shard_path {
            self.register_shards(
                &[MDBShardFile::load_from_file(&new_shard_path, &self.shard_file_cache)?],
                // Just cut from this session's data, so it's the most likely to match what comes next.
                ChunkIndexSizeEnforcement::Eviction,
            )
            .await?;
        }

        Ok(())
    }

    /// Add file reconstruction info to the in-memory state.
    pub async fn add_file_reconstruction_info(&self, file_info: MDBFileInfo) -> Result<()> {
        let mut lg = self.current_state.write().await;

        let new_shard_path = if lg.shard_file_size() + file_info.num_bytes()? > self.target_shard_max_size {
            let path = Self::cut_shard(&mut lg, &self.shard_directory)?;
            Some(path)
        } else {
            None
        };

        lg.add_file_reconstruction_info(file_info)?;
        drop(lg);

        if let Some(new_shard_path) = new_shard_path {
            self.register_shards(
                &[MDBShardFile::load_from_file(&new_shard_path, &self.shard_file_cache)?],
                // Just cut from this session's data, so it's the most likely to match what comes next.
                ChunkIndexSizeEnforcement::Eviction,
            )
            .await?;
        }

        Ok(())
    }

    fn cut_shard(lg: &mut MDBInMemoryShard, shard_directory: &Path) -> Result<PathBuf> {
        let new_shard_path = lg.write_to_directory(shard_directory, None)?;
        *lg = MDBInMemoryShard::default();
        Ok(new_shard_path)
    }

    #[instrument(skip_all, name = "ShardFileManager::flush")]
    pub async fn flush(&self) -> Result<Option<PathBuf>> {
        let new_shard_path;

        {
            let mut lg = self.current_state.write().await;

            if lg.is_empty() {
                return Ok(None);
            }

            new_shard_path = lg.write_to_directory(&self.shard_directory, None)?;
            *lg = MDBInMemoryShard::default();

            info!("Shard manager flushed new shard to {new_shard_path:?}.");
        }

        self.register_shards(
            &[MDBShardFile::load_from_file(&new_shard_path, &self.shard_file_cache)?],
            // Just cut from this session's data, so it's the most likely to match what comes next.
            ChunkIndexSizeEnforcement::Eviction,
        )
        .await?;

        Ok(Some(new_shard_path))
    }

    pub fn shard_file_cache(&self) -> &ShardFileCache {
        &self.shard_file_cache
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

        for ksc in self.shard_bookkeeper.read().await?.shard_collections.iter() {
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

        for ksc in self.shard_bookkeeper.read().await?.shard_collections.iter() {
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
    use tokio::runtime::Handle;
    use xet_runtime::config::XetConfig;
    use xet_runtime::core::XetContext;

    use super::super::file_structs::FileDataSequenceHeader;
    use super::super::session_directory::{consolidate_shards_in_directory, merge_shards};
    use super::super::shard_format::test_routines::{gen_random_file_info, rng_hash, simple_hash};
    use super::super::utils::parse_shard_filename;
    use super::super::xorb_structs::{XorbChunkSequenceEntry, XorbChunkSequenceHeader};
    use super::{get_shard_file_cache, *};
    use crate::error::Result;

    fn test_context() -> XetContext {
        XetContext::from_external(Handle::current(), XetConfig::new())
    }

    #[allow(clippy::type_complexity)]
    pub async fn fill_with_specific_shard(
        shard: &ShardFileManager,
        in_mem_shard: &mut MDBInMemoryShard,
        xorb_nodes: &[(u64, &[(u64, u32)])],
        file_nodes: &[(u64, &[(u64, (u32, u32))])],
    ) -> Result<()> {
        for (hash, chunks) in xorb_nodes {
            let mut xorb_block = Vec::<_>::new();
            let mut pos = 0;

            for (h, s) in chunks.iter() {
                xorb_block.push(XorbChunkSequenceEntry::new(simple_hash(*h), *s, pos));
                pos += *s;
            }
            let xorb_info = MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(simple_hash(*hash), chunks.len(), pos),
                chunks: xorb_block,
            };

            shard.add_xorb_block(xorb_info.clone()).await?;

            in_mem_shard.add_xorb_block(xorb_info)?;
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
        ctx: &XetContext,
        seed: u64,
        shard_dir: impl AsRef<Path>,
        n_shards: usize,
        xorb_block_sizes: &[usize],
        file_chunk_range_sizes: &[usize],
    ) -> Result<MDBInMemoryShard> {
        // generate the xorb content stuff.
        let mut rng = StdRng::seed_from_u64(seed);

        let shard_dir = shard_dir.as_ref();
        let sfm = ShardFileManager::new_in_session_directory(ctx, shard_dir, false).await?;
        let mut reference_shard = MDBInMemoryShard::default();

        for _ in 0..n_shards {
            fill_with_random_shard(&sfm, &mut reference_shard, rng.random(), xorb_block_sizes, file_chunk_range_sizes)
                .await?;

            sfm.flush().await?;
        }

        Ok(reference_shard)
    }

    async fn fill_with_random_shard(
        shard: &Arc<ShardFileManager>,
        in_mem_shard: &mut MDBInMemoryShard,
        seed: u64,
        xorb_block_sizes: &[usize],
        file_chunk_range_sizes: &[usize],
    ) -> Result<()> {
        // generate the xorb content stuff.
        let mut rng = StdRng::seed_from_u64(seed);

        for xorb_block_size in xorb_block_sizes {
            let mut chunks = Vec::<_>::new();
            let mut pos = 0u32;

            for _ in 0..*xorb_block_size {
                chunks.push(XorbChunkSequenceEntry::new(rng_hash(rng.random()), rng.random_range(10000..20000), pos));
                pos += rng.random_range(10000..20000);
            }
            let metadata = XorbChunkSequenceHeader::new(rng_hash(rng.random()), *xorb_block_size, pos);
            let mdb_xorb_info = MDBXorbInfo { metadata, chunks };

            shard.add_xorb_block(mdb_xorb_info.clone()).await?;
            in_mem_shard.add_xorb_block(mdb_xorb_info)?;
        }

        for file_block_size in file_chunk_range_sizes {
            let file_info = gen_random_file_info(&mut rng, file_block_size, false, false);
            shard.add_file_reconstruction_info(file_info.clone()).await?;

            in_mem_shard.add_file_reconstruction_info(file_info)?;
        }
        Ok(())
    }

    pub async fn verify_metadata_shards_match(
        mdb: &ShardFileManager,
        mem_shard: &MDBInMemoryShard,
        test_file_reconstruction: bool,
    ) -> Result<()> {
        // Now, test that the results of queries from the in-memory shard match those
        // of the other shard.
        for (k, xorb_block) in mem_shard.xorb_content.iter() {
            // Go through and test queries on both the in-memory shard and the
            // serialized shard, making sure that they match completely.

            for i in 0..xorb_block.chunks.len() {
                // Test the dedup query over a few hashes in which all the
                // hashes queried are part of the xorb_block.
                let query_hashes_1: Vec<MerkleHash> = xorb_block.chunks[i..(i + 3).min(xorb_block.chunks.len())]
                    .iter()
                    .map(|c| c.chunk_hash)
                    .collect();
                let n_items_to_read = query_hashes_1.len();

                // Also test the dedup query over a few hashes in which some of the
                // hashes are part of the query, and the last is not.
                let mut query_hashes_2 = query_hashes_1.clone();
                query_hashes_2.push(rng_hash(1000000 + i as u64));

                let lb = i as u32;
                let ub = min(i + 3, xorb_block.chunks.len()) as u32;

                for query_hashes in [&query_hashes_1, &query_hashes_2] {
                    let result_m = mem_shard.chunk_hash_dedup_query(query_hashes).unwrap();

                    let result_f = mdb.chunk_hash_dedup_query(query_hashes).await?.unwrap();

                    // Returns a tuple of (num chunks matched, FileDataSequenceEntry)
                    assert_eq!(result_m.0, n_items_to_read);
                    assert_eq!(result_f.0, n_items_to_read);

                    // Make sure it gives the correct XORB block hash as the second part of the
                    assert_eq!(result_m.1.xorb_hash, *k);
                    assert_eq!(result_f.1.xorb_hash, *k);

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

                // Make sure retrieving the expected file.
                if let (Some(result_m), Some(result_f)) = (result_m, result_f) {
                    assert_eq!(result_m.metadata.file_hash, *k);
                    assert_eq!(result_f.0.metadata.file_hash, *k);
                }
            }

            // Make sure manager correctly tracking repo size.
            assert_eq!(mdb.calculate_total_materialized_bytes().await?, mem_shard.materialized_bytes());
        }

        assert_eq!(mdb.calculate_total_stored_bytes().await?, mem_shard.stored_bytes());

        Ok(())
    }

    async fn sfm_with_target_shard_size(
        ctx: &XetContext,
        path: impl AsRef<Path>,
        target_size: u64,
    ) -> Result<Arc<ShardFileManager>> {
        ShardFileManager::new_impl(ctx, path, false, target_size, true, 0).await
    }

    #[tokio::test]
    async fn test_basic_retrieval() -> Result<()> {
        let ctx = test_context();
        let sfc = get_shard_file_cache(&ctx.common);
        let tmp_dir = TempDir::with_prefix("gitxet_shard_test_1")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();

        {
            let mdb = ShardFileManager::new_in_session_directory(&ctx, tmp_dir.path(), true).await?;

            fill_with_specific_shard(&mdb, &mut mdb_in_mem, &[(0, &[(11, 5)])], &[(100, &[(200, (0, 5))])]).await?;

            verify_metadata_shards_match(&mdb, &mdb_in_mem, true).await?;

            let out_file = mdb.flush().await?.unwrap();

            // Make sure it still stays consistent after a flush
            verify_metadata_shards_match(&mdb, &mdb_in_mem, true).await?;

            // Verify that the file is correct
            MDBShardFile::load_from_file(&out_file, &sfc)?.verify_shard_integrity();
        }
        {
            // Now, make sure that this happens if this directory is opened up
            let mdb2 = ShardFileManager::new_in_session_directory(&ctx, tmp_dir.path(), true).await?;

            // Make sure it's all in there this round.
            verify_metadata_shards_match(&mdb2, &mdb_in_mem, true).await?;

            // Now add some more, based on this directory
            fill_with_random_shard(&mdb2, &mut mdb_in_mem, 0, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6]).await?;

            verify_metadata_shards_match(&mdb2, &mdb_in_mem, true).await?;

            // Now, merge shards in the background.
            let merged_shards =
                consolidate_shards_in_directory(&ctx.runtime, tmp_dir.path(), *MDB_SHARD_TARGET_SIZE, false, &sfc)?;

            assert_eq!(merged_shards.len(), 1);
            for si in merged_shards {
                assert!(si.path.exists());
                assert!(si.path.to_str().unwrap().contains(&si.shard_hash.hex()))
            }

            verify_metadata_shards_match(&mdb2, &mdb_in_mem, true).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_larger_simulated() -> Result<()> {
        let ctx = test_context();
        let sfc = get_shard_file_cache(&ctx.common);
        let tmp_dir = TempDir::with_prefix("gitxet_shard_test_2")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();
        let mdb = ShardFileManager::new_in_session_directory(&ctx, tmp_dir.path(), true).await?;

        for i in 0..10 {
            fill_with_random_shard(&mdb, &mut mdb_in_mem, i, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6]).await?;

            verify_metadata_shards_match(&mdb, &mdb_in_mem, true).await?;

            let out_file = mdb.flush().await?.unwrap();

            // Make sure it still stays consistent
            verify_metadata_shards_match(&mdb, &mdb_in_mem, true).await?;

            // Verify that the file is correct
            MDBShardFile::load_from_file(&out_file, &sfc)?.verify_shard_integrity();

            // Make sure an empty flush doesn't bother anything.
            mdb.flush().await?;

            // Now, make sure that this happens if this directory is opened up
            let mdb2 = ShardFileManager::new_in_session_directory(&ctx, tmp_dir.path(), true).await?;

            // Make sure it's all in there this round.
            verify_metadata_shards_match(&mdb2, &mdb_in_mem, true).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_process_session_management() -> Result<()> {
        let ctx = test_context();
        let sfc = get_shard_file_cache(&ctx.common);
        let tmp_dir = TempDir::with_prefix("gitxet_shard_test_3").unwrap();
        let mut mdb_in_mem = MDBInMemoryShard::default();

        for sesh in 0..3 {
            for i in 0..10 {
                {
                    let mdb = ShardFileManager::new_in_session_directory(&ctx, tmp_dir.path(), true).await?;
                    fill_with_random_shard(&mdb, &mut mdb_in_mem, 100 * sesh + i, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6])
                        .await
                        .unwrap();

                    verify_metadata_shards_match(&mdb, &mdb_in_mem, true).await.unwrap();

                    let out_file = mdb.flush().await.unwrap().unwrap();

                    // Make sure it still stays together
                    verify_metadata_shards_match(&mdb, &mdb_in_mem, true).await.unwrap();

                    // Verify that the file is correct
                    MDBShardFile::load_from_file(&out_file, &sfc)?.verify_shard_integrity();

                    mdb.flush().await.unwrap();

                    verify_metadata_shards_match(&mdb, &mdb_in_mem, true).await.unwrap();
                }
            }

            {
                let merged_shards =
                    consolidate_shards_in_directory(&ctx.runtime, tmp_dir.path(), *MDB_SHARD_TARGET_SIZE, false, &sfc)
                        .unwrap();

                assert_eq!(merged_shards.len(), 1);

                for si in merged_shards {
                    assert!(si.path.exists());
                    assert!(si.path.to_str().unwrap().contains(&si.shard_hash.hex()))
                }
            }

            {
                // Now, make sure that this happens if this directory is opened up
                let mdb2 = ShardFileManager::new_in_session_directory(&ctx, tmp_dir.path(), true).await?;

                verify_metadata_shards_match(&mdb2, &mdb_in_mem, true).await.unwrap();
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_flush_and_consolidation() -> Result<()> {
        let ctx = test_context();
        let sfc = get_shard_file_cache(&ctx.common);
        let tmp_dir = TempDir::with_prefix("gitxet_shard_test_4b")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();

        const T: u64 = 10000;

        {
            let mdb = sfm_with_target_shard_size(&ctx, tmp_dir.path(), T).await?;
            fill_with_random_shard(&mdb, &mut mdb_in_mem, 0, &[16; 16], &[16; 16]).await?;
            mdb.flush().await?;
        }
        {
            let mdb = sfm_with_target_shard_size(&ctx, tmp_dir.path(), 2 * T).await?;

            verify_metadata_shards_match(&mdb, &mdb_in_mem, true).await?;

            fill_with_random_shard(&mdb, &mut mdb_in_mem, 1, &[25; 25], &[25; 25]).await?;

            verify_metadata_shards_match(&mdb, &mdb_in_mem, true).await?;

            mdb.flush().await?;
        }

        // Reload and verify
        {
            let mdb = ShardFileManager::new_in_session_directory(&ctx, tmp_dir.path(), true).await?;
            verify_metadata_shards_match(&mdb, &mdb_in_mem, true).await?;
        }

        // Merge through the session directory.
        {
            let tmp_merge_dir = TempDir::new()?;

            let shard_merge_result =
                merge_shards(&ctx.runtime, tmp_dir.path(), tmp_merge_dir.path(), 8 * T, false, &sfc)?;
            let mut merged_shards = shard_merge_result.merged_shards;
            let m_del_shards = shard_merge_result.obsolete_shards;

            for sfi in merged_shards.iter() {
                sfi.verify_shard_integrity();
            }

            let paths = std::fs::read_dir(tmp_dir.path()).unwrap();
            assert_eq!(paths.count(), m_del_shards.len());

            // This call should be the same, but
            let mut rv = consolidate_shards_in_directory(&ctx.runtime, tmp_dir.path(), 8 * T, false, &sfc)?;

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
            let mdb = ShardFileManager::new_in_session_directory(&ctx, tmp_dir.path(), true).await?;
            verify_metadata_shards_match(&mdb, &mdb_in_mem, true).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_size_threshholds() -> Result<()> {
        let ctx = test_context();
        let sfc = get_shard_file_cache(&ctx.common);
        let tmp_dir = TempDir::with_prefix("gitxet_shard_test_4")?;
        let mut mdb_in_mem = MDBInMemoryShard::default();

        const T: u64 = 4096;

        for i in 0..5 {
            let mdb = sfm_with_target_shard_size(&ctx, tmp_dir.path(), T).await?;
            fill_with_random_shard(&mdb, &mut mdb_in_mem, i, &[5; 25], &[5; 25]).await?;

            verify_metadata_shards_match(&mdb, &mdb_in_mem, true).await?;

            let out_file = mdb.flush().await?.unwrap();

            // Verify that the file is correct
            MDBShardFile::load_from_file(&out_file, &sfc).unwrap().verify_shard_integrity();

            // Make sure it still stays together
            verify_metadata_shards_match(&mdb, &mdb_in_mem, true).await?;

            assert!(mdb.flush().await?.is_none());
        }

        // Now, do a new shard that has less
        let mut last_num_files = None;
        let mut target_size = T;

        loop {
            let mdb2 = sfm_with_target_shard_size(&ctx, tmp_dir.path(), 2 * T).await?;

            // Make sure it's all in there this round.
            verify_metadata_shards_match(&mdb2, &mdb_in_mem, true).await?;

            let merged_shards =
                consolidate_shards_in_directory(&ctx.runtime, tmp_dir.path(), target_size, false, &sfc)?;

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
        let ctx = test_context();
        let sfc = get_shard_file_cache(&ctx.common);
        let tmp_dir = TempDir::with_prefix("shard_test_unkeyed")?;
        let tmp_dir_path = tmp_dir.path();

        let ref_shard =
            create_random_shard_collection(&ctx, 0, tmp_dir_path, 2, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6]).await?;

        // First, load all of these with a shard file manager and check them.
        {
            let shard_file_manager = ShardFileManager::new_in_session_directory(&ctx, tmp_dir_path, true).await?;
            verify_metadata_shards_match(&shard_file_manager, &ref_shard, true).await?;
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

                let shard = MDBShardFile::load_from_file(p, &sfc)?;

                // Reexport all these shards as keyed shards.
                let out = shard
                    .export_as_keyed_shard(
                        tmp_dir_path_keyed,
                        key,
                        Duration::new(100, 0),
                        include_info,
                        include_info,
                        include_info,
                        &sfc,
                    )
                    .unwrap();
                if key != HMACKey::default() {
                    assert_eq!(out.chunk_hmac_key(), Some(key));
                } else {
                    assert_eq!(out.chunk_hmac_key(), None);
                }
            }

            // Now, verify that everything still works great.
            let shard_file_manager = ShardFileManager::new_in_session_directory(&ctx, tmp_dir_path_keyed, true).await?;

            verify_metadata_shards_match(&shard_file_manager, &ref_shard, include_info).await?;
        }

        Ok(())
    }

    async fn shard_list_with_timestamp_filtering(ctx: &XetContext, path: &Path) -> Result<Vec<Arc<MDBShardFile>>> {
        ShardFileManager::new_impl(ctx, path, false, *MDB_SHARD_TARGET_SIZE, true, 0)
            .await?
            .registered_shard_list()
            .await
    }

    #[tokio::test]
    #[cfg_attr(feature = "smoke-test", ignore)]
    async fn test_timestamp_filtering() -> Result<()> {
        let ctx = test_context();
        let sfc = get_shard_file_cache(&ctx.common);
        let tmp_dir = TempDir::with_prefix("shard_test_timestamp")?;
        let tmp_dir_path = tmp_dir.path();

        // Just create a single shard; we'll key it with other keys and timestamps and then test loading.
        create_random_shard_collection(&ctx, 0, tmp_dir_path, 1, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6]).await?;

        let path = std::fs::read_dir(tmp_dir_path)?.map(|p| p.unwrap().path()).next().unwrap();

        // Create another that has an expiration date of one second from now.
        let key: HMACKey = rng_hash(0);

        let shard = MDBShardFile::load_from_file(&path, &sfc)?;

        let _tmp_dir_keyed = TempDir::with_prefix("shard_test_keyed_1")?;
        let tmp_dir_path_keyed = _tmp_dir_keyed.path();

        // Reexport this shard as a keyed shards.
        let out = shard
            .export_as_keyed_shard(tmp_dir_path_keyed, key, Duration::new(1, 0), false, false, false, &sfc)
            .unwrap();

        {
            let loaded_shards = shard_list_with_timestamp_filtering(&ctx, tmp_dir_path_keyed).await?;

            assert_eq!(loaded_shards.len(), 1);
            assert_eq!(loaded_shards[0].shard_hash, out.shard_hash)
        }

        // Sleep for 2.01 seconds to make sure at least a second has passed for the +1 and a second to handle the <=
        // part of the equality,
        std::thread::sleep(Duration::new(2, 10_000_000));

        {
            let loaded_shards = shard_list_with_timestamp_filtering(&ctx, tmp_dir_path_keyed).await?;

            // No shards loaded
            assert!(loaded_shards.is_empty());

            // shard file still there.
            let n_files = std::fs::read_dir(tmp_dir_path_keyed)?.count();
            assert_eq!(n_files, 1);

            // Now try deletion with a large window; shouldn't touch the shard.
            MDBShardFile::clean_shard_cache(tmp_dir_path_keyed, 100, &sfc)?;

            // shard file still there.
            let n_files = std::fs::read_dir(tmp_dir_path_keyed)?.count();
            assert_eq!(n_files, 1);

            // Now try deletion with 0 expiration
            MDBShardFile::clean_shard_cache(tmp_dir_path_keyed, 0, &sfc)?;

            // File should be gone.
            let n_files = std::fs::read_dir(tmp_dir_path_keyed)?.count();
            assert_eq!(n_files, 0);
        }

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(feature = "smoke-test", ignore)]
    async fn test_export_expiration() -> Result<()> {
        let ctx = test_context();
        let sfc = get_shard_file_cache(&ctx.common);
        let tmp_dir = TempDir::with_prefix("shard_test_timestamp_2")?;
        let tmp_dir_path = tmp_dir.path();

        // Just create a single shard; we'll key it with other keys and timestamps and then test loading.
        create_random_shard_collection(&ctx, 0, tmp_dir_path, 1, &[1, 5, 10, 8], &[4, 3, 5, 9, 4, 6]).await?;

        let path = std::fs::read_dir(tmp_dir_path)?.map(|p| p.unwrap().path()).next().unwrap();

        let shard = MDBShardFile::load_from_file(&path, &sfc)?;

        let _tmp_dir_expiry = TempDir::with_prefix("shard_test_expiry_2")?;
        let tmp_dir_path_expiry = _tmp_dir_expiry.path();

        // Create another that has an expiration date of one second from now.
        let out = shard.export_with_expiration(tmp_dir_path_expiry, Duration::new(1, 0), &sfc)?;

        {
            let loaded_shards = shard_list_with_timestamp_filtering(&ctx, tmp_dir_path_expiry).await?;

            assert_eq!(loaded_shards.len(), 1);
            assert_eq!(loaded_shards[0].shard_hash, out.shard_hash)
        }

        // Sleep for 2.01 seconds to make sure at least a second has passed for the +1 and a second to handle the <=
        // part of the equality,
        std::thread::sleep(Duration::new(2, 10_000_000));

        {
            let loaded_shards = shard_list_with_timestamp_filtering(&ctx, tmp_dir_path_expiry).await?;

            assert!(loaded_shards.is_empty());

            // Make sure it leaves the shard there.
            let n_files = std::fs::read_dir(tmp_dir_path_expiry)?.count();
            assert_eq!(n_files, 1);

            // Now try deletion with a large window; shouldn't touch the shard.
            MDBShardFile::clean_shard_cache(tmp_dir_path_expiry, 100, &sfc)?;

            // shard file still there.
            let n_files = std::fs::read_dir(tmp_dir_path_expiry)?.count();
            assert_eq!(n_files, 1);

            // Now try deletion with 0 expiration
            MDBShardFile::clean_shard_cache(tmp_dir_path_expiry, 0, &sfc)?;

            // File should be gone.
            let n_files = std::fs::read_dir(tmp_dir_path_expiry)?.count();
            assert_eq!(n_files, 0);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_cache_size_pruning() {
        let ctx = test_context();
        let sfc = get_shard_file_cache(&ctx.common);
        let tmp_dir = TempDir::with_prefix("shard_test_cache_size_pruning").unwrap();

        let tmp_dir_1 = tmp_dir.path().join("src");

        let n_shards = 4;

        create_random_shard_collection(&ctx, 0, &tmp_dir_1, n_shards, &[16; 16], &[16; 16])
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
            let shard = MDBShardFile::load_from_file(&p.path(), &sfc).unwrap();

            let s = shard
                .export_with_specific_expiration(tmp_dir_2, expiration, base_time + Duration::from_secs(i as u64), &sfc)
                .unwrap();

            shard_list.push(s);
        }

        let get_shards = |cache_size: u64| {
            let ctx = ctx.clone();
            async move {
                let sfm = ShardFileManager::new_impl(&ctx, tmp_dir_2, false, 64 * 1024, true, cache_size)
                    .await
                    .unwrap();
                sfm.registered_shard_list().await.unwrap()
            }
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
                .map(|p| MDBShardFile::load_from_file(&p.unwrap().path(), &sfc).unwrap().shard_hash)
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
        let ctx = test_context();
        let sfc = get_shard_file_cache(&ctx.common);
        let tmp_dir = TempDir::with_prefix("shard_test_deletion").unwrap();

        let tmp_dir_1 = tmp_dir.path().join("src");

        create_random_shard_collection(&ctx, 0, &tmp_dir_1, 4, &[4; 4], &[4; 4])
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
                        let sfi = MDBShardFile::load_from_file(&dest_file_name, &sfc).unwrap();

                        // Turn off the typical verification checks on this shard, so it isn't verified
                        // on loading from the cache.
                        #[cfg(debug_assertions)]
                        sfi.disable_verifications.store(true, std::sync::atomic::Ordering::Relaxed);

                        // Replace the file with an empty file.  On read, this will error, and should skip.
                        std::fs::File::create(&dest_file_name).unwrap();

                        bad_shard_hash = sfi.shard_hash;
                    }
                }

                // Now attempt a merge; this should cause an error.
                let out_dir_1 = work_dir.join("out_err");
                std::fs::create_dir_all(&out_dir_1).unwrap();
                let res = merge_shards(&ctx.runtime, &tmp_src_dir, &out_dir_1, base_size * merge_size, false, &sfc);
                assert!(res.is_err());

                // Now attempt a merge with error skipping; which should not cause an error.
                let out_dir_2 = work_dir.join("out_skips");
                std::fs::create_dir_all(&out_dir_2).unwrap();
                let res =
                    merge_shards(&ctx.runtime, &tmp_src_dir, &out_dir_2, base_size * merge_size, true, &sfc).unwrap();

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_new_in_cache_directory_cache_identity_under_concurrency() -> Result<()> {
        let ctx = test_context();
        let tmp_dir = TempDir::with_prefix("shard_test_cache_identity")?;
        let shard_dir = tmp_dir.path().to_path_buf();

        let mut join_handles = Vec::new();
        for _ in 0..32 {
            let ctx = ctx.clone();
            let shard_dir = shard_dir.clone();
            join_handles
                .push(tokio::spawn(async move { ShardFileManager::new_in_cache_directory(&ctx, &shard_dir).await }));
        }

        let mut managers = Vec::new();
        for jh in join_handles {
            managers.push(jh.await.unwrap()?);
        }

        let first = managers.first().unwrap().clone();
        for manager in managers.iter().skip(1) {
            assert!(Arc::ptr_eq(&first, manager));
        }

        Ok(())
    }

    // Returns how many of the reference shard's chunks the shard file manager can dedup against.
    async fn num_dedupable_chunks(sfm: &ShardFileManager, ref_shard: &MDBInMemoryShard) -> Result<usize> {
        let mut n = 0;
        for xorb_info in ref_shard.xorb_content.values() {
            for chunk in xorb_info.chunks.iter() {
                if sfm.chunk_hash_dedup_query(&[chunk.chunk_hash]).await?.is_some() {
                    n += 1;
                }
            }
        }
        Ok(n)
    }

    // Checks the bookkeeper's invariants: every registered shard's recorded location holds that shard, and the
    // chunk index totals agree with the collections and with the set of indexed shards.
    async fn assert_bookkeeping_consistent(sfm: &ShardFileManager) -> Result<()> {
        let bookkeeper = sfm.shard_bookkeeper.read().await?;

        for (shard_hash, &(col_index, index)) in bookkeeper.shard_lookup_by_shard_hash.iter() {
            assert_eq!(bookkeeper.shard_collections[col_index].shard_list[index].shard_hash, *shard_hash);
        }

        let num_entries: usize = bookkeeper.shard_collections.iter().map(|c| c.chunk_lookup.len()).sum();
        assert_eq!(bookkeeper.total_indexed_chunks, num_entries);

        let num_indexed_shards: usize = bookkeeper.shard_collections.iter().map(|c| c.num_indexed_shards).sum();
        assert_eq!(bookkeeper.indexed_shards.len(), num_indexed_shards);

        for collection in bookkeeper.shard_collections.iter() {
            assert!(collection.shard_list.len() <= bookkeeper.max_shards_per_collection);
        }

        for indexed in bookkeeper.indexed_shards.iter() {
            let shard = &bookkeeper.shard_collections[indexed.shard_collection_index].shard_list
                [indexed.shard_index_in_collection];
            assert_eq!(indexed.creation_timestamp, shard.shard.metadata.shard_creation_timestamp);
        }

        Ok(())
    }

    // Creates `n` unkeyed shards of 20 chunks each, one per directory under `base_dir`; returns their
    // reference contents and paths.
    async fn create_shards_for_eviction(
        ctx: &XetContext,
        base_dir: &Path,
        n: usize,
    ) -> Result<(Vec<MDBInMemoryShard>, Vec<PathBuf>)> {
        let mut ref_shards = Vec::new();
        let mut paths = Vec::new();
        for i in 0..n {
            let src_dir = base_dir.join(format!("src_{i}"));
            ref_shards.push(create_random_shard_collection(ctx, i as u64, &src_dir, 1, &[10, 10], &[]).await?);

            let src_paths = std::fs::read_dir(&src_dir)?.map(|p| p.unwrap().path()).collect::<Vec<_>>();
            assert_eq!(src_paths.len(), 1);
            paths.push(src_paths[0].clone());
        }
        Ok((ref_shards, paths))
    }

    // Re-keys the shard at `path` the way the server protects a global dedup response, and returns the bytes the
    // server would send.
    fn protect_shard(path: &Path, key: HMACKey, keyed_dir: &Path, sfc: &ShardFileCache) -> Result<Vec<u8>> {
        std::fs::create_dir_all(keyed_dir)?;
        let keyed = MDBShardFile::load_from_file(path, sfc)?.export_as_keyed_shard(
            keyed_dir,
            key,
            Duration::new(100, 0),
            false,
            false,
            false,
            sfc,
        )?;
        Ok(std::fs::read(&keyed.path)?)
    }

    // Each test shard has 20 chunks, so this index holds two of them.
    fn context_with_40_entry_chunk_index() -> XetContext {
        let config = XetConfig::new().with_config("shard.chunk_index_table_max_size", "40").unwrap();
        XetContext::from_external(Handle::current(), config)
    }

    #[tokio::test]
    async fn test_chunk_index_evicts_oldest_global_dedup_shards() -> Result<()> {
        let ctx = context_with_40_entry_chunk_index();
        let sfc = get_shard_file_cache(&ctx.common);
        let tmp_dir = TempDir::with_prefix("shard_test_eviction_keyed")?;
        let (ref_shards, paths) = create_shards_for_eviction(&ctx, tmp_dir.path(), 3).await?;
        let (a, b, c) = (0, 1, 2);

        // The server's key rotates weekly, so shards from the same week share a collection, and evicting one must
        // leave the others' entries in place.
        let keyed_dir = tmp_dir.path().join("keyed");
        let protect = |i: usize, key: HMACKey| protect_shard(&paths[i], key, &keyed_dir, &sfc);

        let cache_dir = TempDir::with_prefix("shard_test_eviction_cache")?;
        let sfm = ShardFileManager::new_in_session_directory(&ctx, cache_dir.path(), false).await?;

        sfm.import_shard_from_bytes(&protect(a, rng_hash(1))?, ChunkIndexSizeEnforcement::Eviction)
            .await?;
        sfm.import_shard_from_bytes(&protect(b, rng_hash(1))?, ChunkIndexSizeEnforcement::Eviction)
            .await?;
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[a]).await?, 20);
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[b]).await?, 20);
        assert_bookkeeping_consistent(&sfm).await?;

        // The index is full; C evicts A, the oldest.
        sfm.import_shard_from_bytes(&protect(c, rng_hash(1))?, ChunkIndexSizeEnforcement::Eviction)
            .await?;
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[a]).await?, 0);
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[b]).await?, 20);
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[c]).await?, 20);
        assert_bookkeeping_consistent(&sfm).await?;

        // The server returns A again after its key rotates; it evicts B.
        sfm.import_shard_from_bytes(&protect(a, rng_hash(2))?, ChunkIndexSizeEnforcement::Eviction)
            .await?;
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[a]).await?, 20);
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[b]).await?, 0);
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[c]).await?, 20);
        assert_bookkeeping_consistent(&sfm).await?;

        // Evicted shards stay registered, and rescanning the directory neither registers nor indexes them again.
        assert_eq!(sfm.registered_shard_list().await?.len(), 4);
        sfm.refresh_shard_dir(false, 0, ChunkIndexSizeEnforcement::Eviction).await?;
        assert_eq!(sfm.registered_shard_list().await?.len(), 4);
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[b]).await?, 0);
        assert_bookkeeping_consistent(&sfm).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_chunk_index_evicts_oldest_session_shards() -> Result<()> {
        let ctx = context_with_40_entry_chunk_index();
        let session_dir = TempDir::with_prefix("shard_test_eviction_session")?;
        let sfm = ShardFileManager::new_in_session_directory(&ctx, session_dir.path(), false).await?;

        // The manager cuts and registers each shard itself when flushing, as an upload session does.  Shards cut
        // from an in-memory shard have no creation timestamp, so they're evicted in the order they were cut.
        let mut ref_shards = Vec::new();
        for seed in 0..3 {
            let mut ref_shard = MDBInMemoryShard::default();
            fill_with_random_shard(&sfm, &mut ref_shard, seed, &[10, 10], &[]).await?;
            sfm.flush().await?;
            ref_shards.push(ref_shard);
        }
        let (a, b, c) = (0, 1, 2);

        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[a]).await?, 0);
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[b]).await?, 20);
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[c]).await?, 20);
        assert_eq!(sfm.shard_bookkeeper.read().await?.total_indexed_chunks, 40);
        assert_bookkeeping_consistent(&sfm).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_chunk_index_keeps_newest_shards_on_load() -> Result<()> {
        let ctx = context_with_40_entry_chunk_index();
        let sfc = get_shard_file_cache(&ctx.common);
        let tmp_dir = TempDir::with_prefix("shard_test_eviction_load")?;
        let (ref_shards, paths) = create_shards_for_eviction(&ctx, tmp_dir.path(), 3).await?;
        let (a, b, c) = (0, 1, 2);

        // Give the shards creation timestamps and modification times A < B < C.  The directory scan registers
        // shards newest first by modification time, so A comes last, when the index is already full.
        let cache_dir = tmp_dir.path().join("cache");
        std::fs::create_dir_all(&cache_dir)?;
        let t0 = SystemTime::now() - Duration::from_secs(1000);
        for (i, p) in paths.iter().enumerate() {
            let created = t0 + Duration::from_secs(100 * i as u64);
            let exported = MDBShardFile::load_from_file(p, &sfc)?.export_with_specific_expiration(
                &cache_dir,
                t0 + Duration::from_secs(2000),
                created,
                &sfc,
            )?;
            std::fs::File::options()
                .write(true)
                .open(&exported.path)?
                .set_modified(created)?;
        }

        // Load with a fresh context, whose shard file cache reads the modification times set above.  A new manager
        // scans with StopAtCap, so A is left out; Eviction would instead evict B, the older of the indexed shards.
        let load_ctx = context_with_40_entry_chunk_index();
        let sfm = ShardFileManager::new_in_session_directory(&load_ctx, &cache_dir, true).await?;
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[a]).await?, 0);
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[b]).await?, 20);
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[c]).await?, 20);
        assert_bookkeeping_consistent(&sfm).await?;

        Ok(())
    }

    // Limits each collection of the manager to `max_shards_per_collection` shards.
    async fn set_max_shards_per_collection(sfm: &ShardFileManager, max_shards_per_collection: usize) -> Result<()> {
        sfm.shard_bookkeeper
            .update(move |mut bookkeeper| async move {
                bookkeeper.max_shards_per_collection = max_shards_per_collection;
                Ok(bookkeeper)
            })
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_full_collection_opens_another_with_same_key() -> Result<()> {
        let ctx = test_context();
        let sfc = get_shard_file_cache(&ctx.common);
        let tmp_dir = TempDir::with_prefix("shard_test_split")?;
        let (ref_shards, paths) = create_shards_for_eviction(&ctx, tmp_dir.path(), 4).await?;

        // All four share one key.
        let key = rng_hash(1);
        let keyed_dir = tmp_dir.path().join("keyed");
        let keyed_shard_bytes = paths
            .iter()
            .map(|p| protect_shard(p, key, &keyed_dir, &sfc))
            .collect::<Result<Vec<_>>>()?;

        let cache_dir = TempDir::with_prefix("shard_test_split_cache")?;
        let sfm = ShardFileManager::new_in_session_directory(&ctx, cache_dir.path(), false).await?;
        set_max_shards_per_collection(&sfm, 2).await?;

        for bytes in &keyed_shard_bytes {
            sfm.import_shard_from_bytes(bytes, ChunkIndexSizeEnforcement::Eviction).await?;
        }

        // The chunk index has room for every shard, so all of them are indexed, split over two collections
        // with the same key after the unkeyed one.
        for ref_shard in &ref_shards {
            assert_eq!(num_dedupable_chunks(&sfm, ref_shard).await?, 20);
        }
        {
            let bookkeeper = sfm.shard_bookkeeper.read().await?;
            assert_eq!(bookkeeper.shard_collections.len(), 3);
            for collection in &bookkeeper.shard_collections[1..] {
                assert_eq!(collection.hmac_key, key);
                assert_eq!(collection.shard_list.len(), 2);
            }
            assert_eq!(bookkeeper.collection_by_key[&key], 2);
        }
        assert_bookkeeping_consistent(&sfm).await?;

        // Rescanning the directory doesn't register them again.
        sfm.refresh_shard_dir(false, 0, ChunkIndexSizeEnforcement::Eviction).await?;
        assert_eq!(sfm.registered_shard_list().await?.len(), 4);
        assert_bookkeeping_consistent(&sfm).await?;

        Ok(())
    }

    // Registers the shards at `paths` one at a time under Eviction, after exporting them into the manager's
    // directory with the given creation timestamps, in seconds after `t0`.
    async fn register_with_creation_times(
        sfm: &ShardFileManager,
        sfc: &ShardFileCache,
        paths: &[PathBuf],
        created_after_t0_secs: &[u64],
    ) -> Result<()> {
        let t0 = SystemTime::now() - Duration::from_secs(1000);
        for (p, &secs) in paths.iter().zip(created_after_t0_secs) {
            let exported = MDBShardFile::load_from_file(p, sfc)?.export_with_specific_expiration(
                sfm.shard_directory(),
                t0 + Duration::from_secs(2000),
                t0 + Duration::from_secs(secs),
                sfc,
            )?;
            sfm.register_shards(&[exported], ChunkIndexSizeEnforcement::Eviction).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_split_collections_dont_evict_while_chunk_index_has_room() -> Result<()> {
        let ctx = test_context();
        let sfc = get_shard_file_cache(&ctx.common);
        let tmp_dir = TempDir::with_prefix("shard_test_split_room")?;
        let (ref_shards, paths) = create_shards_for_eviction(&ctx, tmp_dir.path(), 4).await?;

        let shard_dir = tmp_dir.path().join("shards");
        let sfm = ShardFileManager::new_in_session_directory(&ctx, &shard_dir, false).await?;
        set_max_shards_per_collection(&sfm, 2).await?;

        // A shard older than everything registered before it, then a newer one, once the first collection is
        // full.  With room in the chunk index, nothing is evicted.
        register_with_creation_times(&sfm, &sfc, &paths, &[100, 200, 50, 300]).await?;
        for ref_shard in &ref_shards {
            assert_eq!(num_dedupable_chunks(&sfm, ref_shard).await?, 20);
        }
        assert_bookkeeping_consistent(&sfm).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_chunk_index_evicts_oldest_across_split_collections() -> Result<()> {
        let ctx = context_with_40_entry_chunk_index();
        let sfc = get_shard_file_cache(&ctx.common);
        let tmp_dir = TempDir::with_prefix("shard_test_split_cap")?;
        let (ref_shards, paths) = create_shards_for_eviction(&ctx, tmp_dir.path(), 4).await?;
        let (x0, x1, y, z) = (0, 1, 2, 3);

        let shard_dir = tmp_dir.path().join("shards");
        let sfm = ShardFileManager::new_in_session_directory(&ctx, &shard_dir, false).await?;
        set_max_shards_per_collection(&sfm, 2).await?;

        // X0 and X1 fill the first collection and the index.  Y, the oldest, evicts X0 and is then evicted by Z,
        // which leaves the two newest indexed although they sit in different collections.
        register_with_creation_times(&sfm, &sfc, &paths, &[100, 200, 50, 300]).await?;
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[x0]).await?, 0);
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[x1]).await?, 20);
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[y]).await?, 0);
        assert_eq!(num_dedupable_chunks(&sfm, &ref_shards[z]).await?, 20);
        assert_eq!(sfm.shard_bookkeeper.read().await?.shard_collections.len(), 2);
        assert_bookkeeping_consistent(&sfm).await?;

        Ok(())
    }
}
