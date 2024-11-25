use std::io::Write;
use std::mem::take;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use cas_client::Client;
use cas_types::FileRange;
use mdb_shard::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfo};
use mdb_shard::file_structs::MDBFileInfo;
use mdb_shard::ShardFileManager;
use merkledb::aggregate_hashes::cas_node_hash;
use merklehash::MerkleHash;
use tokio::sync::Mutex;
use tracing::{info, info_span, instrument, Instrument};
use utils::ThreadPool;

use crate::cas_interface::create_cas_client;
use crate::clean::Cleaner;
use crate::configurations::*;
use crate::errors::*;
use crate::metrics::{RUNTIME_CHUNKING, RUNTIME_DEDUP_QUERY, RUNTIME_HASHING, RUNTIME_SHA256, RUNTIME_XORB_UPLOAD};
use crate::remote_shard_interface::RemoteShardInterface;
use crate::shard_interface::create_shard_manager;
use crate::PointerFile;

#[derive(Default, Debug)]
pub struct CASDataAggregator {
    /// Bytes of all chunks accumulated in one CAS block concatenated together.
    pub data: Vec<u8>,
    /// Metadata of all chunks accumulated in one CAS block. Each entry is
    /// (chunk hash, chunk size).
    pub chunks: Vec<(MerkleHash, usize)>,
    // The file info of files that are still being processed.
    // As we're building this up, we assume that all files that do not have a size in the header are
    // not finished yet and thus cannot be uploaded.
    //
    // All the cases the default hash for a cas info entry will be filled in with the cas hash for
    // an entry once the cas block is finalized and uploaded.  These correspond to the indices given
    // alongwith the file info.
    // This tuple contains the file info (which may be modified) and the divisions in the chunks corresponding
    // to this file.
    pub pending_file_info: Vec<(MDBFileInfo, Vec<usize>)>,
}

impl CASDataAggregator {
    pub fn is_empty(&self) -> bool {
        self.data.is_empty() && self.chunks.is_empty() && self.pending_file_info.is_empty()
    }
}

/// Manages the translation of files between the
/// MerkleDB / pointer file format and the materialized version.
///
/// This class handles the clean and smudge options.
pub struct PointerFileTranslator {
    /* ----- Configurations ----- */
    config: TranslatorConfig,

    /* ----- Utils ----- */
    shard_manager: Arc<ShardFileManager>,
    remote_shards: Arc<RemoteShardInterface>,
    cas: Arc<dyn Client + Send + Sync>,

    /* ----- Deduped data shared across files ----- */
    global_cas_data: Arc<Mutex<CASDataAggregator>>,

    /* ----- Threadpool to use for concurrent execution ----- */
    threadpool: Arc<ThreadPool>,
}

// Constructors
impl PointerFileTranslator {
    pub async fn new(config: TranslatorConfig, threadpool: Arc<ThreadPool>) -> Result<PointerFileTranslator> {
        let shard_manager = Arc::new(create_shard_manager(&config.shard_storage_config).await?);

        let cas_client = create_cas_client(
            &config.cas_storage_config,
            &config.repo_info,
            shard_manager.clone(),
            threadpool.clone(),
        )?;

        let remote_shards = {
            if let Some(dedup) = &config.dedup_config {
                RemoteShardInterface::new(
                    config.file_query_policy,
                    &config.shard_storage_config,
                    Some(shard_manager.clone()),
                    Some(cas_client.clone()),
                    dedup.repo_salt,
                    threadpool.clone(),
                )
                .await?
            } else {
                RemoteShardInterface::new_query_only(
                    config.file_query_policy,
                    &config.shard_storage_config,
                    threadpool.clone(),
                )
                .await?
            }
        };

        Ok(Self {
            config,
            shard_manager,
            remote_shards,
            cas: cas_client,
            global_cas_data: Default::default(),
            threadpool,
        })
    }
}

/// Clean operations
impl PointerFileTranslator {
    /// Start to clean one file. When cleaning multiple files, each file should
    /// be associated with one Cleaner. This allows to launch multiple clean task
    /// simultaneously.
    ///
    /// The caller is responsible for memory usage management, the parameter "buffer_size"
    /// indicates the maximum number of Vec<u8> in the internal buffer.
    pub async fn start_clean(&self, buffer_size: usize, file_name: Option<&Path>) -> Result<Arc<Cleaner>> {
        let Some(ref dedup) = self.config.dedup_config else {
            return Err(DataProcessingError::DedupConfigError("empty dedup config".to_owned()));
        };

        Cleaner::new(
            dedup.small_file_threshold,
            matches!(dedup.global_dedup_policy, GlobalDedupPolicy::Always),
            self.config.cas_storage_config.prefix.clone(),
            dedup.repo_salt,
            self.shard_manager.clone(),
            self.remote_shards.clone(),
            self.cas.clone(),
            self.global_cas_data.clone(),
            buffer_size,
            file_name,
            self.threadpool.clone(),
        )
        .await
    }

    pub async fn finalize_cleaning(&self) -> Result<()> {
        // flush accumulated CAS data.
        let mut cas_data_accumulator = self.global_cas_data.lock().await;
        let mut new_cas_data = take(cas_data_accumulator.deref_mut());
        drop(cas_data_accumulator); // Release the lock.

        if !new_cas_data.is_empty() {
            register_new_cas_block(
                &mut new_cas_data,
                &self.shard_manager,
                &self.cas,
                &self.config.cas_storage_config.prefix,
            )
            .await?;
        }

        debug_assert!(new_cas_data.is_empty());

        // flush accumulated memory shard.
        self.shard_manager
            .flush()
            .instrument(info_span!("shard_manager::flush"))
            .await?;

        let s = Instant::now();
        self.upload().await?;
        let runtime_shard_upload = s.elapsed().as_millis();

        let runtime_chunking = RUNTIME_CHUNKING.get() / 1000000;
        RUNTIME_CHUNKING.reset();
        let runtime_hashing = RUNTIME_HASHING.get() / 1000000;
        RUNTIME_HASHING.reset();
        let runtime_sha256 = RUNTIME_SHA256.get() / 1000000;
        RUNTIME_SHA256.reset();
        let runtime_dedup_query = RUNTIME_DEDUP_QUERY.get() / 1000000;
        RUNTIME_DEDUP_QUERY.reset();
        let runtime_xorb_upload = RUNTIME_XORB_UPLOAD.get() / 1000000;
        RUNTIME_XORB_UPLOAD.reset();

        info!(
            "Runtimes:\n
        Chunking: {runtime_chunking} ms\n
        Hashing: {runtime_hashing} ms\n
        SHA256: {runtime_sha256} ms\n
        Dedup query: {runtime_dedup_query} ms\n
        Xorb upload: {runtime_xorb_upload} ms\n
        Shard upload: {runtime_shard_upload} ms"
        );

        Ok(())
    }

    async fn upload(&self) -> Result<()> {
        // First, get all the shards prepared and load them.
        let merged_shards_jh = self.remote_shards.merge_shards()?;

        // Make sure that all the uploads and everything are in a good state before proceeding with
        // anything changing the remote repository.
        //
        // Waiting until the CAS uploads finish avoids the following scenario:
        // 1. user 1 commit file A and push, but network drops after
        // sync_notes_to_remote before uploading cas finishes.
        // 2. user 2 tries to git add the same file A, which on filter pulls in
        // the new notes, and file A is 100% deduped so no CAS blocks will be created,
        // and push.
        //
        // This results in a bad repo state.
        self.upload_cas().await?;

        // Get a list of all the merged shards in order to upload them.
        let merged_shards = merged_shards_jh.await??;
        let merged_shard_len = merged_shards.len();

        // Now, these need to be sent to the remote.
        self.remote_shards
            .upload_and_register_shards(merged_shards)
            .instrument(info_span!("remote_shards::upload", "shard_len" = merged_shard_len))
            .await?;

        // Finally, we can move all the mdb shards from the session directory, which is used
        // by the upload_shard task, to the cache.
        self.remote_shards.move_session_shards_to_local_cache().await?;

        Ok(())
    }

    async fn upload_cas(&self) -> Result<()> {
        // We don't have staging client support yet.
        Ok(())
    }
}

/// Clean operation helpers
#[instrument(skip_all, name = "register_new_cas_block")]
pub(crate) async fn register_new_cas_block(
    cas_data: &mut CASDataAggregator,
    shard_manager: &Arc<ShardFileManager>,
    cas: &Arc<dyn Client + Send + Sync>,
    cas_prefix: &str,
) -> Result<MerkleHash> {
    let cas_hash = cas_node_hash(&cas_data.chunks[..]);

    let raw_bytes_len = cas_data.data.len();

    let metadata = CASChunkSequenceHeader::new(cas_hash, cas_data.chunks.len(), raw_bytes_len);

    let mut pos = 0;
    let chunks: Vec<_> = cas_data
        .chunks
        .iter()
        .map(|(h, len)| {
            let result = CASChunkSequenceEntry::new(*h, *len, pos);
            pos += *len;
            result
        })
        .collect();
    let cas_info = MDBCASInfo { metadata, chunks };

    pos = 0;
    let chunk_boundaries = cas_data
        .chunks
        .iter()
        .map(|(hash, len)| {
            pos += *len;
            (*hash, pos as u32)
        })
        .collect();

    if !cas_info.chunks.is_empty() {
        let s = Instant::now();
        cas.put(cas_prefix, &cas_hash, take(&mut cas_data.data), chunk_boundaries)
            .instrument(info_span!("cas_client::put", cas_hash=?cas_hash))
            .await?;
        RUNTIME_XORB_UPLOAD.inc_by(s.elapsed().as_nanos().try_into().unwrap());

        shard_manager.add_cas_block(cas_info).await?;
    } else {
        debug_assert_eq!(cas_hash, MerkleHash::default());
    }

    // Now register any new files as needed.
    for (mut fi, chunk_hash_indices) in take(&mut cas_data.pending_file_info) {
        for i in chunk_hash_indices {
            debug_assert_eq!(fi.segments[i].cas_hash, MerkleHash::default());
            fi.segments[i].cas_hash = cas_hash;
        }

        shard_manager.add_file_reconstruction_info(fi).await?;
    }

    cas_data.data.clear();
    cas_data.chunks.clear();
    cas_data.pending_file_info.clear();

    Ok(cas_hash)
}

/// Smudge operations
impl PointerFileTranslator {
    pub async fn smudge_file_from_pointer(
        &self,
        pointer: &PointerFile,
        writer: &mut Box<dyn Write + Send>,
        range: Option<FileRange>,
    ) -> Result<()> {
        self.smudge_file_from_hash(&pointer.hash()?, writer, range).await
    }

    pub async fn smudge_file_from_hash(
        &self,
        file_id: &MerkleHash,
        writer: &mut Box<dyn Write + Send>,
        range: Option<FileRange>,
    ) -> Result<()> {
        let http_client = cas_client::build_http_client(&None)?;
        self.cas.get_file(Arc::new(http_client), file_id, range, writer).await?;
        Ok(())
    }
}
