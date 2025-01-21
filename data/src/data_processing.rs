use std::io::Write;
use std::mem::take;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::Arc;

use cas_client::Client;
use cas_types::FileRange;
use jsonwebtoken::{decode, DecodingKey, Validation};
use lazy_static::lazy_static;
use mdb_shard::file_structs::MDBFileInfo;
use mdb_shard::ShardFileManager;
use merklehash::MerkleHash;
use tokio::sync::{Mutex, Semaphore};
use utils::progress::ProgressUpdater;
use utils::ThreadPool;

use crate::cas_interface::create_cas_client;
use crate::clean::Cleaner;
use crate::configurations::*;
use crate::constants::MAX_CONCURRENT_XORB_UPLOADS;
use crate::errors::*;
use crate::parallel_xorb_uploader::{ParallelXorbUploader, XorbUpload};
use crate::remote_shard_interface::RemoteShardInterface;
use crate::shard_interface::create_shard_manager;
use crate::PointerFile;

lazy_static! {
    pub static ref XORB_UPLOAD_RATE_LIMITER: Arc<Semaphore> = Arc::new(Semaphore::new(*MAX_CONCURRENT_XORB_UPLOADS));
}

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
    xorb_uploader: Arc<dyn XorbUpload + Send + Sync>,
    upload_progress_updater: Option<Arc<dyn ProgressUpdater>>,

    /* ----- Deduped data shared across files ----- */
    global_cas_data: Arc<Mutex<CASDataAggregator>>,

    /* ----- Threadpool to use for concurrent execution ----- */
    threadpool: Arc<ThreadPool>,

    /* ----- Telemetry ----- */
    repo_id: Option<String>,
}

// Constructors
impl PointerFileTranslator {
    pub async fn new(
        config: TranslatorConfig,
        threadpool: Arc<ThreadPool>,
        upload_progress_updater: Option<Arc<dyn ProgressUpdater>>,
        download_only: bool,
    ) -> Result<PointerFileTranslator> {
        let shard_manager = Arc::new(create_shard_manager(&config.shard_storage_config, download_only).await?);

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
                    download_only,
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

        let xorb_uploader = ParallelXorbUploader::new(
            &config.cas_storage_config.prefix,
            shard_manager.clone(),
            cas_client.clone(),
            XORB_UPLOAD_RATE_LIMITER.clone(),
            threadpool.clone(),
            upload_progress_updater.clone(),
        )
        .await;
        let repo_id = config.cas_storage_config.auth.clone().and_then(|auth| {
            let token = auth.token;
            let mut validation = Validation::default();
            validation.insecure_disable_signature_validation();

            decode::<serde_json::Map<String, serde_json::Value>>(
                &token,
                &DecodingKey::from_secret("".as_ref()), // Secret is not used here
                &validation,
            )
            .ok()
            .and_then(|decoded| {
                // Extract `repo_id` from the claims map
                decoded.claims.get("repoId").and_then(|value| value.as_str().map(String::from))
            })
        });

        Ok(Self {
            config,
            shard_manager,
            remote_shards,
            cas: cas_client,
            xorb_uploader,
            global_cas_data: Default::default(),
            threadpool,
            upload_progress_updater,
            repo_id,
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
            self.xorb_uploader.clone(),
            self.global_cas_data.clone(),
            buffer_size,
            file_name,
            self.threadpool.clone(),
            self.upload_progress_updater.clone(),
            self.repo_id.clone(),
        )
        .await
    }

    pub async fn finalize_cleaning(&self) -> Result<()> {
        // flush accumulated CAS data.
        let mut cas_data_accumulator = self.global_cas_data.lock().await;
        let new_cas_data = take(cas_data_accumulator.deref_mut());
        drop(cas_data_accumulator); // Release the lock.

        if !new_cas_data.is_empty() {
            self.xorb_uploader.register_new_cas_block(new_cas_data).await?;
        }

        self.xorb_uploader.flush().await?;

        // flush accumulated memory shard.
        self.shard_manager.flush().await?;

        self.upload_shards().await?;

        Ok(())
    }

    async fn upload_shards(&self) -> Result<()> {
        // First, get all the shards prepared and load them.
        let merged_shards_jh = self.remote_shards.merge_shards()?;

        // Get a list of all the merged shards in order to upload them.
        let merged_shards = merged_shards_jh.await??;

        // Now, these need to be sent to the remote.
        self.remote_shards.upload_and_register_shards(merged_shards).await?;

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
        progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Result<()> {
        self.smudge_file_from_hash(&pointer.hash()?, writer, range, progress_updater)
            .await
    }

    pub async fn smudge_file_from_hash(
        &self,
        file_id: &MerkleHash,
        writer: &mut Box<dyn Write + Send>,
        range: Option<FileRange>,
        progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Result<()> {
        let http_client = cas_client::build_http_client(&None)?;
        self.cas
            .get_file(Arc::new(http_client), file_id, range, writer, progress_updater)
            .await?;
        Ok(())
    }
}
