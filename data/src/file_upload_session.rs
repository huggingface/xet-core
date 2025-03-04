use std::mem::take;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::Arc;

use cas_client::Client;
use jsonwebtoken::{decode, DecodingKey, Validation};
use lazy_static::lazy_static;
use mdb_shard::file_structs::MDBFileInfo;
use mdb_shard::ShardFileManager;
use merklehash::MerkleHash;
use tokio::sync::{Mutex, Semaphore};
use utils::progress::ProgressUpdater;
use xet_threadpool::ThreadPool;

use crate::cas_interface::create_cas_client;
use crate::configurations::*;
use crate::constants::MAX_CONCURRENT_XORB_UPLOADS;
use crate::errors::*;
use crate::file_cleaner::SingleFileCleaner;
use crate::parallel_xorb_uploader::{ParallelXorbUploader, XorbUpload};
use crate::remote_shard_interface::RemoteShardInterface;
use crate::shard_interface::create_shard_manager;

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
/// This class handles the clean operations.  It's meant to be a single atomic session
/// that succeeds or fails as a unit;  i.e. all files get uploaded on finalization, and all shards
/// and xorbs needed to reconstruct those files are properly uploaded and registered.
pub struct FileUploadSession {
    /* ----- Configurations ----- */
    config: TranslatorConfig,
    dry_run: bool,

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
impl FileUploadSession {
    pub async fn new(
        config: TranslatorConfig,
        threadpool: Arc<ThreadPool>,
        upload_progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Result<FileUploadSession> {
        FileUploadSession::new_impl(config, threadpool, upload_progress_updater, false).await
    }

    pub async fn dry_run(
        config: TranslatorConfig,
        threadpool: Arc<ThreadPool>,
        upload_progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Result<FileUploadSession> {
        FileUploadSession::new_impl(config, threadpool, upload_progress_updater, true).await
    }

    async fn new_impl(
        config: TranslatorConfig,
        threadpool: Arc<ThreadPool>,
        upload_progress_updater: Option<Arc<dyn ProgressUpdater>>,
        dry_run: bool,
    ) -> Result<FileUploadSession> {
        let shard_manager = create_shard_manager(&config.shard_storage_config, false).await?;

        let cas_client = create_cas_client(&config.cas_storage_config, threadpool.clone(), dry_run)?;

        let remote_shards = {
            if let Some(dedup) = &config.dedup_config {
                RemoteShardInterface::new(
                    config.file_query_policy,
                    &config.shard_storage_config,
                    Some(shard_manager.clone()),
                    Some(cas_client.clone()),
                    dedup.repo_salt,
                    threadpool.clone(),
                    false,
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
            dry_run,
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
impl FileUploadSession {
    /// Start to clean one file. When cleaning multiple files, each file should
    /// be associated with one Cleaner. This allows to launch multiple clean task
    /// simultaneously.
    ///
    /// The caller is responsible for memory usage management, the parameter "buffer_size"
    /// indicates the maximum number of Vec<u8> in the internal buffer.
    pub async fn start_clean(&self, buffer_size: usize, file_name: Option<&Path>) -> Result<Arc<SingleFileCleaner>> {
        let Some(ref dedup) = self.config.dedup_config else {
            return Err(DataProcessingError::DedupConfigError("empty dedup config".to_owned()));
        };

        SingleFileCleaner::new(
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

    pub async fn finalize_cleaning(&self) -> Result<u64> {
        // flush accumulated CAS data.
        let mut cas_data_accumulator = self.global_cas_data.lock().await;
        let new_cas_data = take(cas_data_accumulator.deref_mut());
        drop(cas_data_accumulator); // Release the lock.

        // Upload if there is new data or info
        if !new_cas_data.is_empty() {
            self.xorb_uploader.register_new_cas_block(new_cas_data).await?;
        }

        let total_bytes_trans = self.xorb_uploader.flush().await?;

        // flush accumulated memory shard.
        self.shard_manager.flush().await?;

        if !self.dry_run {
            self.upload_shards().await?;
        }

        Ok(total_bytes_trans)
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

    pub async fn summarize_file_info_of_session(&self) -> Result<Vec<MDBFileInfo>> {
        self.shard_manager
            .all_file_info_of_session()
            .await
            .map_err(DataProcessingError::from)
    }
}

#[cfg(test)]
mod tests {

    use std::fs::{File, OpenOptions};
    use std::io::{Read, Write};
    use std::path::Path;
    use std::sync::{Arc, OnceLock};

    use xet_threadpool::ThreadPool;

    use crate::{FileDownloader, FileUploadSession, PointerFile};

    /// Return a shared threadpool to be reused as needed.
    fn get_threadpool() -> Arc<ThreadPool> {
        static THREADPOOL: OnceLock<Arc<ThreadPool>> = OnceLock::new();
        THREADPOOL
            .get_or_init(|| Arc::new(ThreadPool::new().expect("Error starting multithreaded runtime.")))
            .clone()
    }

    /// Cleans (converts) a regular file into a pointer file.
    ///
    /// * `input_path`: path to the original file
    /// * `output_path`: path to write the pointer file
    async fn test_clean_file(runtime: Arc<ThreadPool>, cas_path: &Path, input_path: &Path, output_path: &Path) {
        let read_data = std::fs::read(input_path).unwrap().to_vec();

        let mut pf_out = Box::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(output_path)
                .unwrap(),
        );

        let translator = FileUploadSession::new(TranslatorConfig::local_config(cas_path, true).unwrap(), runtime, None)
            .await
            .unwrap();

        let handle = translator.start_clean(1024, None).await.unwrap();

        // Read blocks from the source file and hand them to the cleaning handle
        handle.add_bytes(read_data).await.unwrap();

        let (pointer_file_contents, _) = handle.result().await.unwrap();
        translator.finalize_cleaning().await.unwrap();

        pf_out.write_all(pointer_file_contents.as_bytes()).unwrap();
    }

    /// Smudges (hydrates) a pointer file back into the original data.
    ///
    /// * `pointer_path`: path to the pointer file
    /// * `output_path`: path to write the hydrated/original file
    async fn test_smudge_file(runtime: Arc<ThreadPool>, cas_path: &Path, pointer_path: &Path, output_path: &Path) {
        let mut reader = File::open(pointer_path).unwrap();
        let writer: Box<dyn Write + Send + 'static> = Box::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(output_path)
                .unwrap(),
        );

        let mut input = String::new();
        reader.read_to_string(&mut input).unwrap();

        let pointer_file = PointerFile::init_from_string(&input, "");
        // If not a pointer file, do nothing
        if !pointer_file.is_valid() {
            return;
        }

        let translator = FileDownloader::new(TranslatorConfig::local_config(cas_path, true).unwrap(), runtime)
            .await
            .unwrap();

        translator
            .smudge_file_from_pointer(&pointer_file, &mut Box::new(writer), None, None)
            .await
            .unwrap();
    }

    use std::fs::{read, write};

    use tempfile::tempdir;

    /// Unit tests
    use super::*;

    #[test]
    fn test_clean_smudge_round_trip() {
        let temp = tempdir().unwrap();
        let original_data = b"Hello, world!";

        let runtime = get_threadpool();

        runtime
            .clone()
            .external_run_async_task(async move {
                let cas_path = temp.path().join("cas");

                // 1. Write an original file in the temp directory
                let original_path = temp.path().join("original.txt");
                write(&original_path, original_data).unwrap();

                // 2. Clean it (convert it to a pointer file)
                let pointer_path = temp.path().join("pointer.txt");
                test_clean_file(runtime.clone(), &cas_path, &original_path, &pointer_path).await;

                // 3. Smudge it (hydrate the pointer file) to a new file
                let hydrated_path = temp.path().join("hydrated.txt");
                test_smudge_file(runtime.clone(), &cas_path, &pointer_path, &hydrated_path).await;

                // 4. Verify that the round-tripped file matches the original
                let result_data = read(hydrated_path).unwrap();
                assert_eq!(original_data.to_vec(), result_data);
            })
            .unwrap();
    }
}
