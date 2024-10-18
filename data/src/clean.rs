use crate::cas_interface::Client;
use crate::chunking::{chunk_target_default, ChunkYieldType};
use crate::configurations::FileQueryPolicy;
use crate::constants::MIN_SPACING_BETWEEN_GLOBAL_DEDUP_QUERIES;
use crate::data_processing::{register_new_cas_block, CASDataAggregator};
use crate::errors::{DataProcessingError::*, Result};
use crate::metrics::FILTER_BYTES_CLEANED;
use crate::remote_shard_interface::RemoteShardInterface;
use crate::repo_salt::RepoSalt;
use crate::small_file_determination::{is_file_passthrough, is_possible_start_to_text_file};
use crate::PointerFile;
use cas_object::range_hash_from_chunks;
use lazy_static::lazy_static;
use mdb_shard::file_structs::{
    FileDataSequenceEntry, FileDataSequenceHeader, FileVerificationEntry, MDBFileInfo,
};
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use mdb_shard::{hash_is_global_dedup_eligible, ShardFileManager};
use merkledb::aggregate_hashes::file_node_hash;
use merkledb::constants::TARGET_CAS_BLOCK_SIZE;
use merklehash::MerkleHash;
use std::collections::HashMap;
use std::mem::take;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, error, info, warn};

// Chunking is the bottleneck, changing batch size doesn't have a big impact.
lazy_static! {
    pub static ref DEDUP_CHUNK_BATCH_SIZE: usize = std::env::var("XET_DEDUP_BATCHSIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
}

pub enum BufferItem<T: Send + Sync + 'static> {
    Value(T),
    Completed,
}

#[derive(Default, Debug)]
struct DedupFileTrackingInfo {
    file_hashes: Vec<(MerkleHash, usize)>,
    file_info: Vec<FileDataSequenceEntry>,
    current_cas_file_info_indices: Vec<usize>,
    file_size: u64,
    current_cas_block_hashes: HashMap<MerkleHash, usize>,
    cas_data: CASDataAggregator,
}

pub struct Cleaner {
    // Configurations
    small_file_threshold: usize,
    enable_global_dedup_queries: bool,
    cas_prefix: String,
    repo_salt: Option<RepoSalt>,

    // Utils
    shard_manager: Arc<ShardFileManager>,
    remote_shards: Arc<RemoteShardInterface>,
    cas: Arc<dyn Client + Send + Sync>,

    // External Data
    global_cas_data: Arc<Mutex<CASDataAggregator>>,

    // Internal workers
    chunk_data_queue: Sender<BufferItem<Vec<u8>>>,
    chunking_worker: Mutex<Option<JoinHandle<()>>>,
    dedup_worker: Mutex<Option<JoinHandle<()>>>,

    // Internal Data
    tracking_info: Mutex<DedupFileTrackingInfo>,
    small_file_buffer: Mutex<Option<Vec<u8>>>,

    // Auxiliary info
    file_name: Option<PathBuf>,
}

impl Cleaner {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        small_file_threshold: usize,
        enable_global_dedup_queries: bool,
        cas_prefix: String,
        repo_salt: Option<RepoSalt>,
        shard_manager: Arc<ShardFileManager>,
        remote_shards: Arc<RemoteShardInterface>,
        cas: Arc<dyn Client + Send + Sync>,
        cas_data: Arc<Mutex<CASDataAggregator>>,
        buffer_size: usize,
        file_name: Option<&Path>,
    ) -> Result<Arc<Self>> {
        let (data_p, data_c) = channel::<BufferItem<Vec<u8>>>(buffer_size);

        let (chunk_p, chunk_c) = channel::<Option<ChunkYieldType>>(buffer_size);

        let chunker = chunk_target_default(data_c, chunk_p);

        let cleaner = Arc::new(Cleaner {
            small_file_threshold,
            enable_global_dedup_queries,
            cas_prefix,
            repo_salt,
            shard_manager,
            remote_shards,
            cas,
            global_cas_data: cas_data,
            chunk_data_queue: data_p,
            chunking_worker: Mutex::new(Some(chunker)),
            dedup_worker: Mutex::new(None),
            tracking_info: Mutex::new(Default::default()),
            small_file_buffer: Mutex::new(Some(Vec::with_capacity(small_file_threshold))),
            file_name: file_name.map(|f| f.to_owned()),
        });

        Self::run(cleaner.clone(), chunk_c).await;

        Ok(cleaner)
    }

    pub async fn add_bytes(&self, data: Vec<u8>) -> Result<()> {
        self.task_is_running().await?;

        if !self.check_passthrough_status(&data).await? {
            self.add_data_to_chunking(BufferItem::Value(data)).await?
        }

        Ok(())
    }

    pub async fn result(&self) -> Result<String> {
        self.finish().await?;

        let mut small_file_buffer = self.small_file_buffer.lock().await;
        if let Some(buffer) = small_file_buffer.take() {
            let small_file = String::from_utf8(buffer)?;
            return Ok(small_file);
        }

        self.to_pointer_file().await
    }

    async fn run(cleaner: Arc<Self>, mut chunks: Receiver<Option<ChunkYieldType>>) {
        let cleaner_clone = cleaner.clone();
        let dedup_task = tokio::spawn(async move {
            loop {
                let mut chunk_vec = Vec::with_capacity(*DEDUP_CHUNK_BATCH_SIZE);

                let mut finished = false;

                for _ in 0..*DEDUP_CHUNK_BATCH_SIZE {
                    match chunks.try_recv() {
                        Ok(Some(chunk)) => chunk_vec.push(chunk),
                        Ok(None) | Err(TryRecvError::Disconnected) => {
                            finished = true;
                            break;
                        }
                        Err(TryRecvError::Empty) => {
                            if chunk_vec.is_empty() {
                                // need to wait a bit to make sure at least one chunk to process
                                match chunks.recv().await.flatten() {
                                    Some(chunk) => chunk_vec.push(chunk),
                                    None => {
                                        finished = true;
                                    }
                                }
                            }
                            break;
                        }
                    }
                }

                if !chunk_vec.is_empty() {
                    let res = cleaner_clone.dedup(&chunk_vec).await;
                    if res.is_err() {
                        error!("Clean task error: {res:?}");
                        break;
                    }
                }

                if finished {
                    break;
                }
            }
        });

        let mut worker = cleaner.dedup_worker.lock().await;

        *worker = Some(dedup_task);
    }

    async fn task_is_running(&self) -> Result<()> {
        let dedup_worker = self.dedup_worker.lock().await;

        let chunking_worker = self.chunking_worker.lock().await;

        if dedup_worker.is_none() || chunking_worker.is_none() {
            return Err(CleanTaskError("no active clean task".to_owned()));
        };

        Ok(())
    }

    async fn add_data_to_chunking(&self, it: BufferItem<Vec<u8>>) -> Result<()> {
        self.chunk_data_queue
            .send(it)
            .await
            .map_err(|e| InternalError(format!("{e}")))?;

        Ok(())
    }

    /// Check passthrough condition of data.
    /// Return true if the incoming data is already processed inside,
    /// otherwise return false and let the caller to handle the data.
    async fn check_passthrough_status(&self, data: &[u8]) -> Result<bool> {
        let mut small_file_buffer = self.small_file_buffer.lock().await;

        if let Some(mut buffer) = small_file_buffer.take() {
            buffer.extend_from_slice(data);

            if !is_possible_start_to_text_file(&buffer) || buffer.len() >= self.small_file_threshold
            {
                self.add_data_to_chunking(BufferItem::Value(buffer)).await?;

                // not passthrough, but just sent all buffered data + incoming data to chunker
                return Ok(true);
            }

            *small_file_buffer = Some(buffer);

            // may be passthrough, keep accumulating
            return Ok(true);
        }

        // not passthrough, already sent all buffered data to chunker
        Ok(false)
    }

    async fn dedup(&self, chunks: &[ChunkYieldType]) -> Result<()> {
        info!("Dedup {} chunks", chunks.len());
        let mut tracking_info = self.tracking_info.lock().await;

        let enable_global_dedup = self.enable_global_dedup_queries;
        let salt = self.repo_salt.unwrap_or_default();

        // Last chunk queried.
        let mut last_chunk_index_queried = isize::MIN;

        // All the previous chunk are stored here, use it as the global chunk index start.
        let global_chunk_index_start = tracking_info.file_hashes.len();

        let chunk_hashes = Vec::from_iter(chunks.iter().map(|(c, _)| c.hash));

        // Now, parallelize the querying of potential new shards on the server end with
        // querying for dedup information of the chunks, which are the two most expensive
        // parts of the process.  Then when we go into the next section, everything is essentially
        // a local lookup table so the remaining work should be quite fast.

        // This holds the results of the dedup queries.
        let mut deduped_blocks = vec![None; chunks.len()];

        // Do at most two passes; 1) with global dedup querying possibly enabled, and 2) possibly rerunning
        // if the global dedup query came back with a new shard.

        for first_pass in [true, false] {
            // Set up a join set for tracking any global dedup queries.
            let mut global_dedup_queries = JoinSet::<bool>::new();

            // Now, go through and test all of these for whether or not they can be deduplicated.
            let mut local_chunk_index = 0;
            while local_chunk_index < chunks.len() {
                let global_chunk_index = global_chunk_index_start + local_chunk_index;

                // First check to see if we don't already know what these blocks are from a previous pass.
                if let Some((n_deduped, _)) = &deduped_blocks[local_chunk_index] {
                    local_chunk_index += n_deduped;
                } else if let Some((n_deduped, fse)) = self
                    .shard_manager
                    .chunk_hash_dedup_query(&chunk_hashes[local_chunk_index..])
                    .await?
                {
                    if !first_pass {
                        // This means new shards were discovered.
                        debug!("clean_file ({:?}): {n_deduped} chunks deduped against shard discovered through global dedup.", self.file_name);
                    }
                    deduped_blocks[local_chunk_index] = Some((n_deduped, fse));
                    local_chunk_index += n_deduped;

                    // Now see if we can issue a background query against the global dedup server to see if
                    // any shards are present that give us more dedup ability.
                    //
                    // If we've already queried these against the global dedup, then we can proceed on without
                    // re-querying anything.  Only doing this on the first pass also gaurantees that in the case of errors
                    // on shard retrieval, we don't get stuck in a loop trying to download and reprocess.
                } else {
                    if enable_global_dedup          // Is enabled
                            && first_pass                   // Have we seen this on the previous pass?  If so, skip.
                            && (global_chunk_index == 0    // Query all hashes on first iteration.
                            || hash_is_global_dedup_eligible(&chunk_hashes[local_chunk_index]))
                            && (global_chunk_index as isize // Limit by enforcing at least 4MB between chunk queries.
                            >= last_chunk_index_queried + MIN_SPACING_BETWEEN_GLOBAL_DEDUP_QUERIES as isize)
                    {
                        // Now, query for a global dedup shard in the background to make sure that all the rest of this can continue.
                        let remote_shards = self.remote_shards.clone();
                        let query_chunk = chunk_hashes[local_chunk_index];

                        let file_name = self.file_name.clone();

                        global_dedup_queries.spawn(async move {
                                let Ok(query_result) = remote_shards.query_dedup_shard_by_chunk(&query_chunk, &salt).await.map_err(|e| {
                                    debug!("Error encountered attempting to query global dedup table: {e:?}; ignoring.");
                                    e
                                })
                                    else { return false; };

                                let Some(shard_hash) = query_result else {
                                    debug!("Queried shard for global dedup with hash {query_chunk:?}; nothing found.");
                                    return false;
                                };

                                // Okay, we have something, so go ahead and download it in the background.
                                debug!("global dedup: {file_name:?} deduplicated by shard {shard_hash}; registering.");
                                let Ok(_) = remote_shards.register_local_shard(&shard_hash).await.map_err(|e| {
                                    warn!("Error encountered attempting to download and register shard {shard_hash} for deduplication : {e:?}; ignoring.");
                                    e
                                })
                                    else { return false; };

                                debug!("global dedup: New shard {shard_hash} can be used for deduplication of {file_name:?}; reprocessing file.");

                                true
                            });

                        last_chunk_index_queried = global_chunk_index as isize
                    }

                    local_chunk_index += 1;
                }
            }

            // Now, see if any of the chunk queries have completed.
            let mut has_new_shards = false;
            if first_pass {
                while let Some(shard_probe_task) = global_dedup_queries.join_next().await {
                    has_new_shards |= shard_probe_task?;
                }
            }

            // If we have no new shards, then we're good to go.
            if !has_new_shards {
                break;
            } else {
                debug!(
                    "New shard(s) available for dedup on {:?}; reprocessing chunks.",
                    self.file_name
                );
            }
        }

        // Record all the file hashes.
        tracking_info
            .file_hashes
            .extend(chunks.iter().map(|(c, b)| (c.hash, b.len())));

        // Now, go through and process all the data.
        let mut cur_idx = 0;

        while cur_idx < chunks.len() {
            let mut n_bytes = 0;

            if let Some((n_deduped, fse)) = deduped_blocks[cur_idx].take() {
                // We found one or more chunk hashes present in a cas block somewhere.

                // Update all the metrics.
                #[allow(clippy::needless_range_loop)]
                for i in cur_idx..(cur_idx + n_deduped) {
                    n_bytes += chunks[i].1.len();
                }
                tracking_info.file_size += n_bytes as u64;

                // Do we modify the previous entry as this is the next logical chunk, or do we
                // start a new entry?
                if !tracking_info.file_info.is_empty()
                    && tracking_info.file_info.last().unwrap().cas_hash == fse.cas_hash
                    && tracking_info.file_info.last().unwrap().chunk_index_end
                        == fse.chunk_index_start
                {
                    // This block is the contiguous continuation of the last entry
                    let last_entry = tracking_info.file_info.last_mut().unwrap();
                    last_entry.unpacked_segment_bytes += n_bytes as u32;
                    last_entry.chunk_index_end = fse.chunk_index_end;
                } else {
                    // This block is new
                    tracking_info.file_info.push(fse);
                }

                cur_idx += n_deduped;
            } else {
                let (chunk, bytes) = &chunks[cur_idx];

                n_bytes = chunks[cur_idx].1.len();
                tracking_info.file_size += n_bytes as u64;

                // This is new data.
                let add_new_data;

                if let Some(idx) = tracking_info.current_cas_block_hashes.get(&chunk.hash) {
                    let idx = *idx;
                    // This chunk will get the CAS hash updated when the local CAS block
                    // is full and registered.
                    let file_info_len = tracking_info.file_info.len();
                    tracking_info
                        .current_cas_file_info_indices
                        .push(file_info_len);

                    tracking_info.file_info.push(FileDataSequenceEntry::new(
                        MerkleHash::default(),
                        n_bytes,
                        idx,
                        idx + 1,
                    ));
                    add_new_data = false;
                } else if !tracking_info.file_info.is_empty()
                    && tracking_info.file_info.last().unwrap().cas_hash == MerkleHash::default()
                    && tracking_info.file_info.last().unwrap().chunk_index_end as usize
                        == tracking_info.cas_data.chunks.len()
                {
                    // This is the next chunk in the CAS block we're building,
                    // in which case we can just modify the previous entry.
                    let last_entry = tracking_info.file_info.last_mut().unwrap();
                    last_entry.unpacked_segment_bytes += n_bytes as u32;
                    last_entry.chunk_index_end += 1;
                    add_new_data = true;
                } else {
                    // This block is unrelated to the previous one.
                    // This chunk will get the CAS hash updated when the local CAS block
                    // is full and registered.
                    let file_info_len = tracking_info.file_info.len();
                    tracking_info
                        .current_cas_file_info_indices
                        .push(file_info_len);

                    let chunk_len = tracking_info.cas_data.chunks.len();
                    tracking_info.file_info.push(FileDataSequenceEntry::new(
                        MerkleHash::default(),
                        n_bytes,
                        chunk_len,
                        chunk_len + 1,
                    ));
                    add_new_data = true;
                }

                if add_new_data {
                    // Add in the chunk and cas information.
                    let cas_data_chunks_len = tracking_info.cas_data.chunks.len();
                    tracking_info
                        .current_cas_block_hashes
                        .insert(chunk.hash, cas_data_chunks_len);
                    tracking_info.cas_data.chunks.push((chunk.hash, n_bytes));
                    tracking_info.cas_data.data.extend(bytes);

                    if tracking_info.cas_data.data.len() > TARGET_CAS_BLOCK_SIZE {
                        let cas_hash = register_new_cas_block(
                            &mut tracking_info.cas_data,
                            &self.shard_manager,
                            &self.cas,
                            &self.cas_prefix,
                        )
                        .await?;

                        for i in take(&mut tracking_info.current_cas_file_info_indices) {
                            tracking_info.file_info[i].cas_hash = cas_hash;
                        }
                        tracking_info.current_cas_block_hashes.clear();
                    }
                }

                // Next round.
                cur_idx += 1;
            }
        }

        Ok(())
    }

    async fn finish(&self) -> Result<()> {
        self.task_is_running().await?;

        // check if there is remaining data in buffer
        let mut small_file_buffer = self.small_file_buffer.lock().await;
        if let Some(buffer) = small_file_buffer.take() {
            if !is_file_passthrough(&buffer, self.small_file_threshold) {
                self.add_data_to_chunking(BufferItem::Value(buffer)).await?;
            } else {
                // put back for return value
                *small_file_buffer = Some(buffer);
            }
        }

        // signal finish
        self.add_data_to_chunking(BufferItem::Completed).await?;

        let mut chunking_worker = self.chunking_worker.lock().await;
        if let Some(task) = chunking_worker.take() {
            task.await.map_err(|e| InternalError(format!("{e:?}")))?;
        }

        let mut dedup_worker = self.dedup_worker.lock().await;
        if let Some(task) = dedup_worker.take() {
            task.await.map_err(|e| InternalError(format!("{e:?}")))?;
        }

        Ok(())
    }

    async fn summarize_dedup_info(&self) -> Result<(MerkleHash, u64)> {
        let mut tracking_info = self.tracking_info.lock().await;

        let file_hash = file_node_hash(
            &tracking_info.file_hashes,
            &self.repo_salt.unwrap_or_default(),
        )?;

        let file_size = tracking_info.file_size;

        // Is the file registered already?  If so, nothing needs to be added now.
        let file_already_registered = match self.remote_shards.file_query_policy {
            FileQueryPolicy::LocalFirst | FileQueryPolicy::LocalOnly => self
                .shard_manager
                .get_file_reconstruction_info(&file_hash)
                .await?
                .is_some(),
            FileQueryPolicy::ServerOnly => false,
        };

        if !file_already_registered {
            // Put an accumulated data into the struct-wide cas block for building a future chunk.
            let mut cas_data_accumulator = self.global_cas_data.lock().await;

            let shift = cas_data_accumulator.chunks.len() as u32;
            cas_data_accumulator
                .data
                .append(&mut tracking_info.cas_data.data);
            cas_data_accumulator
                .chunks
                .append(&mut tracking_info.cas_data.chunks);

            let segments: Vec<_> = tracking_info
                .file_info
                .iter()
                .map(|fi| {
                    // Transfering cas chunks from tracking_info.cas_data to cas_data_accumulator,
                    // shift chunk indices.
                    let s = if fi.cas_hash == MerkleHash::default() {
                        shift
                    } else {
                        0
                    };

                    let mut new_fi = fi.clone();
                    new_fi.chunk_index_start += s;
                    new_fi.chunk_index_end += s;

                    new_fi
                })
                .collect();

            let mut chunk_idx = 0;
            let verification = segments
                .iter()
                .map(|entry| {
                    let n_chunks = (entry.chunk_index_end - entry.chunk_index_start) as usize;
                    let chunk_hashes: Vec<_> = tracking_info.file_hashes
                        [chunk_idx..chunk_idx + n_chunks]
                        .iter()
                        .map(|(hash, _)| *hash)
                        .collect();
                    let range_hash = range_hash_from_chunks(&chunk_hashes);
                    chunk_idx += n_chunks;

                    FileVerificationEntry::new(range_hash)
                })
                .collect();

            let new_file_info = MDBFileInfo {
                metadata: FileDataSequenceHeader::new(
                    file_hash,
                    tracking_info.file_info.len(),
                    true,
                ),
                segments,
                verification,
            };

            cas_data_accumulator.pending_file_info.push((
                new_file_info,
                tracking_info.current_cas_file_info_indices.clone(),
            ));

            if cas_data_accumulator.data.len() >= TARGET_CAS_BLOCK_SIZE {
                let mut new_cas_data = take(cas_data_accumulator.deref_mut());
                drop(cas_data_accumulator); // Release the lock.
                register_new_cas_block(
                    &mut new_cas_data,
                    &self.shard_manager,
                    &self.cas,
                    &self.cas_prefix,
                )
                .await?;
            } else {
                drop(cas_data_accumulator);
            }
        }
        // we only add to the counters if we see changes
        FILTER_BYTES_CLEANED.inc_by(file_size);

        *tracking_info = Default::default();

        Ok((file_hash, file_size))
    }

    async fn to_pointer_file(&self) -> Result<String> {
        let (hash, filesize) = self.summarize_dedup_info().await?;
        let pointer_file = PointerFile::init_from_info(
            &self
                .file_name
                .clone()
                .map(|f| f.to_str().unwrap_or_default().to_owned())
                .unwrap_or_default(),
            &hash.hex(),
            filesize,
        );
        Ok(pointer_file.to_string())
    }
}
