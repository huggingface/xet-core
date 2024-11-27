use std::io::Write;
use std::mem::take;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use cas_client::Client;
use cas_types::FileRange;
use mdb_shard::file_structs::MDBFileInfo;
use mdb_shard::ShardFileManager;
use merklehash::MerkleHash;
use tokio::sync::Mutex;
use tracing::{info, info_span, Instrument};
use utils::ThreadPool;

use crate::cas_interface::create_cas_client;
use crate::clean::Cleaner;
use crate::configurations::*;
use crate::constants::MAX_CONCURRENT_XORB_UPLOADS;
use crate::errors::*;
use crate::metrics::{RUNTIME_CHUNKING, RUNTIME_DEDUP_QUERY, RUNTIME_HASHING, RUNTIME_SHA256, RUNTIME_XORB_UPLOAD};
use crate::parallel_xorb_uploader::ParallelXorbUploader;
use crate::remote_shard_interface::RemoteShardInterface;
use crate::shard_interface::create_shard_manager;
use crate::PointerFile;

#[derive(Default, Debug)]
pub(crate) struct CASDataAggregator {
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
    xorb_uploader: Mutex<Option<Arc<ParallelXorbUploader>>>,

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
            xorb_uploader: Mutex::new(None),
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

        let mut xorb_uploader = self.xorb_uploader.lock().await;
        let uploader = match xorb_uploader.take() {
            Some(uploader) => uploader,
            None => {
                ParallelXorbUploader::new(
                    &self.config.cas_storage_config.prefix,
                    self.shard_manager.clone(),
                    self.cas.clone(),
                    self.threadpool.clone(),
                    *MAX_CONCURRENT_XORB_UPLOADS * 2, // set buffer size to double the concurrent uploads should be enough
                )
                .await
            },
        };
        *xorb_uploader = Some(uploader.clone());

        Cleaner::new(
            dedup.small_file_threshold,
            matches!(dedup.global_dedup_policy, GlobalDedupPolicy::Always),
            self.config.cas_storage_config.prefix.clone(),
            dedup.repo_salt,
            self.shard_manager.clone(),
            self.remote_shards.clone(),
            uploader,
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
        let new_cas_data = take(cas_data_accumulator.deref_mut());
        drop(cas_data_accumulator); // Release the lock.

        let Some(ref xorb_uploader) = *self.xorb_uploader.lock().await else {
            return Err(DataProcessingError::InternalError("no active xorb upload task".to_owned()));
        };

        if !new_cas_data.is_empty() {
            xorb_uploader.register_new_cas_block(new_cas_data).await?;
        }

        xorb_uploader.flush().await?;

        // flush accumulated memory shard.
        self.shard_manager
            .flush()
            .instrument(info_span!("shard_manager::flush"))
            .await?;

        let s = Instant::now();
        self.upload_shards().await?;
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

    async fn upload_shards(&self) -> Result<()> {
        // First, get all the shards prepared and load them.
        let merged_shards_jh = self.remote_shards.merge_shards()?;

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
