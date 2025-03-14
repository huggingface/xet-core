use std::mem::take;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::thread::current;

use cas_client::Client;
use deduplication::constants::MAX_XORB_BYTES;
use deduplication::DeduplicationMetrics;
use jsonwebtoken::{decode, DecodingKey, Validation};
use lazy_static::lazy_static;
use mdb_shard::constants::MDB_SHARD_MIN_TARGET_SIZE;
use mdb_shard::file_structs::MDBFileInfo;
use mdb_shard::session_directory::consolidate_shards_in_directory;
use mdb_shard::ShardFileManager;
use prometheus::core::Atomic;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use utils::progress::ProgressUpdater;
use xet_threadpool::ThreadPool;

use crate::configurations::*;
use crate::constants::MAX_CONCURRENT_XORB_UPLOADS;
use crate::data_aggregator::CASDataAggregator;
use crate::errors::*;
use crate::file_cleaner::SingleFileCleaner;
use crate::parallel_xorb_uploader::{ParallelXorbUploader, XorbUpload};
use crate::remote_client_interface::create_remote_client;
use crate::remote_shard_interface::RemoteShardInterface;
use crate::repo_salt::RepoSalt;
use crate::shard_interface::create_shard_manager;

lazy_static! {
    pub static ref XORB_UPLOAD_RATE_LIMITER: Arc<Semaphore> = Arc::new(Semaphore::new(*MAX_CONCURRENT_XORB_UPLOADS));
}

// A struct that simply holds all the common information shared between cleaning sessions and other data upload tasks.
pub struct FileUploadSessionState {
    pub shard_manager: Arc<ShardFileManager>,

    pub remote_shards: Arc<RemoteShardInterface>,
    pub client: Arc<dyn Client + Send + Sync>,
    pub xorb_uploader: ParallelXorbUploader,
    pub upload_progress_updater: Option<Arc<dyn ProgressUpdater>>,

    /// Threadpool to use for the execution.
    pub threadpool: Arc<ThreadPool>,

    /// The repo id, if present.
    pub repo_id: Option<String>,

    // The repo salt, if specified.  Defaults to RepoSalt::default()
    pub repo_salt: RepoSalt,

    /// If true, don't actually upload anything; just collect statistics.
    pub dry_run: bool,

    /// The configuration settings, if needed.
    pub config: TranslatorConfig,
}

/// Manages the translation of files between the
/// MerkleDB / pointer file format and the materialized version.
///
/// This class handles the clean operations.  It's meant to be a single atomic session
/// that succeeds or fails as a unit;  i.e. all files get uploaded on finalization, and all shards
/// and xorbs needed to reconstruct those files are properly uploaded and registered.
pub struct FileUploadSession {
    /// The information needed by other processes in uploading data.
    state: Arc<FileUploadSessionState>,

    /// Deduplicated data shared across files.
    current_session_data: Mutex<DataAggregator>,

    /// Metrics for deduplication
    deduplication_metrics: Mutex<DeduplicationMetrics>,
}

// Constructors
impl FileUploadSession {
    pub async fn new(
        config: TranslatorConfig,
        threadpool: Arc<ThreadPool>,
        upload_progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Result<Arc<FileUploadSession>> {
        FileUploadSession::new_impl(config, threadpool, upload_progress_updater, false).await
    }

    pub async fn dry_run(
        config: TranslatorConfig,
        threadpool: Arc<ThreadPool>,
        upload_progress_updater: Option<Arc<dyn ProgressUpdater>>,
    ) -> Result<Arc<FileUploadSession>> {
        FileUploadSession::new_impl(config, threadpool, upload_progress_updater, true).await
    }

    async fn new_impl(
        config: TranslatorConfig,
        threadpool: Arc<ThreadPool>,
        upload_progress_updater: Option<Arc<dyn ProgressUpdater>>,
        dry_run: bool,
    ) -> Result<Arc<FileUploadSession>> {
        let shard_manager = create_shard_manager(&config.shard_storage_config, false).await?;

        let client = create_remote_client(&config, threadpool.clone(), dry_run)?;

        let remote_shards = RemoteShardInterface::new(
            config.file_query_policy,
            &config.shard_storage_config,
            Some(shard_manager.clone()),
            cas_client.clone(),
            config.dedup_config.repo_salt,
            threadpool.clone(),
            false,
        )
        .await?;

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

        let state = Arc::new(FileUploadSessionState {
            shard_manager,
            remote_shards,
            client,
            xorb_uploader,
            upload_progress_updater,
            threadpool,
            repo_id,
            repo_salt: RepoSalt::default(),
            dry_run,
            config,
        });

        Ok(Arc::new(Self {
            state,
            current_session_data: Arc::new(Mutex::new(DataAggregator::default())),
            deduplication_metrics: DeduplicationMetrics::default(),
        }))
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
    pub fn start_clean(&self, file_name: Option<&Path>) -> SingleFileCleaner {
        SingleFileCleaner::new(file_name, state.clone())
    }

    pub async fn register_file_clean_completion(
        &self,
        file_name: Option<String>,
        mut file_data: DataAggregator,
        dedup_metrics: DeduplicationMetrics,
    ) {
        let mut data_to_upload = None;

        // Merge in the remaining file data; uploading a new xorb if need be.
        {
            let mut current_session_data = self.current_session_data.lock().await;

            // Do we need to cut one of these to a xorb?
            if current_session_data.num_bytes() + file_data.num_bytes() > MAX_XORB_BYTES {
                // Cut the larger one as a xorb, uploading it and registering the files.
                if current_session_data.num_bytes() > file_data.num_bytes() {
                    std::mem::swap(&mut current_session_data, &mut file_data);
                }

                // Now file data is larger
                debug_assert_le!(current_session_data.num_bytes(), file_data.num_bytes());

                // Actually upload this outside the lock
                data_to_upload = Some(file_data);
            } else {
                current_session_data.merge_in(file_data);
            }
        }

        if let Some(data_agg) = data_to_upload {
            self.process_aggregated_data(data_agg).await?;
        }

        // Now, aggregate the new dedup metrics.
        self.deduplication_metrics.lock().await.merge_in(&dedup_metrics);
    }

    /// Process the aggregated data, uploading the data as a xorb and registering the files
    async fn process_aggregated_data(&self, data_agg: DataAggregator) -> Result<()> {
        let (xorb, new_files) = data_agg.finalize();

        self.state.xorb_uploader.register_new_xorb_for_upload(xorb).await?;

        for fi in new_files {
            self.state.shard_manager.add_file_reconstruction_info(fi).await?;
        }
    }

    /// Finalize the session, returning the aggregated metrics
    pub async fn finalize(mut self) -> Result<DeduplicationMetrics> {
        // Register the remaining xorbs for upload.
        let data_agg = self.current_session_data.into_inner();
        self.process_aggregated_data(data_agg).await?;

        // Now, make sure all the remaining xorbs are uploaded.
        let total_bytes_trans = self.state.xorb_uploader.finalize().await?;

        // Flush the accumulated shard information to disk.
        self.state.shard_manager.flush().await?;

        Ok(total_bytes_trans)
    }

    pub async fn upload_and_register_session_shards(&self) -> Result<()> {
        // Scan, merge, and fill out any shards in the session directory
        let shard_list =
            consolidate_shards_in_directory(&self.state.shard_manager.shard_directory(), MDB_SHARD_MIN_TARGET_SIZE)?;

        // Upload all the shards.
        let mut shard_uploads = JoinSet::<()>::new();

        for sl in shard_list {
            let salt = self.state.config.dedup_config.repo_salt.unwrap_or_default();
            let shard_client = self.state.client.clone();
            let shard_prefix = self.state.config.shard_storage_config.prefix.clone();

            shard_uploads.spawn(async move {
                debug!("Uploading shard {shard_prefix}/{:?} from staging area to CAS.", &si.shard_hash);
                let data = std::fs::read(&si.path)?;

                // Upload the shard.
                shard_client
                    .upload_shard(&shard_prefix, &si.shard_hash, false, &data, &salt)
                    .await?;

                info!("Shard {shard_prefix}/{:?} upload + sync completed successfully.", &si.shard_hash);

                // Now that that succeeded, move that shard to the cache directory.

                Ok(())
            });
        }

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

        let upload_session = FileUploadSession::new(TranslatorConfig::local_config(cas_path).unwrap(), runtime, None)
            .await
            .unwrap();

        let mut cleaner = upload_session.start_clean(None);

        // Read blocks from the source file and hand them to the cleaning handle
        cleaner.add_data(&read_data[..]).await.unwrap();

        let (pointer_file_contents, _) = cleaner.finish().await.unwrap();
        upload_session.finalize().await.unwrap();

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

        let translator = FileDownloader::new(TranslatorConfig::local_config(cas_path).unwrap(), runtime)
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
