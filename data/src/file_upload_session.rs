use std::collections::HashSet;
use std::mem::{swap, take};
use std::sync::Arc;

use cas_client::Client;
use deduplication::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS};
use deduplication::{DataAggregator, DeduplicationMetrics, RawXorbData};
use jsonwebtoken::{decode, DecodingKey, Validation};
use mdb_shard::file_structs::MDBFileInfo;
use merklehash::MerkleHash;
use more_asserts::*;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use utils::progress::{NoOpProgressUpdater, ProgressUpdaterVerificationWrapper, TrackingProgressUpdater};
use xet_threadpool::ThreadPool;

use crate::configurations::*;
use crate::constants::MAX_CONCURRENT_UPLOADS;
use crate::errors::*;
use crate::file_cleaner::SingleFileCleaner;
use crate::progress_tracking::{CompletionTracker, CompletionTrackerFileId};
use crate::prometheus_metrics;
use crate::remote_client_interface::create_remote_client;
use crate::shard_interface::SessionShardInterface;

lazy_static::lazy_static! {
     static ref UPLOAD_CONCURRENCY_LIMITER: Arc<Semaphore> = Arc::new(Semaphore::new(*MAX_CONCURRENT_UPLOADS));
}

/// Acquire a permit for uploading xorbs and shards to ensure that we don't overwhelm the server
/// or fill up the local host with in-memory data waiting to be uploaded.
///
/// The chosen Semaphore is fair, meaning xorbs and shards added first will be scheduled to upload first.
///
/// It's also important to acquire the permit before the task is launched; otherwise, we may spawn an unlimited
/// number of tasks that end up using up a ton of memory; this forces the pipeline to block here while the upload
/// is happening.
pub(crate) async fn acquire_upload_permit() -> Result<OwnedSemaphorePermit> {
    let upload_permit = UPLOAD_CONCURRENCY_LIMITER
        .clone()
        .acquire_owned()
        .await
        .map_err(|e| DataProcessingError::UploadTaskError(e.to_string()))?;
    Ok(upload_permit)
}

/// Manages the translation of files between the
/// MerkleDB / pointer file format and the materialized version.
///
/// This class handles the clean operations.  It's meant to be a single atomic session
/// that succeeds or fails as a unit;  i.e. all files get uploaded on finalization, and all shards
/// and xorbs needed to reconstruct those files are properly uploaded and registered.
pub struct FileUploadSession {
    // The parts of this that manage the
    pub(crate) client: Arc<dyn Client + Send + Sync>,
    pub(crate) shard_interface: SessionShardInterface,

    /// Threadpool to use for the execution.
    pub(crate) threadpool: Arc<ThreadPool>,

    /// The repo id, if present.
    pub(crate) repo_id: Option<String>,

    /// The configuration settings, if needed.
    pub(crate) config: Arc<TranslatorConfig>,

    /// Tracking upload completion between xorbs and files.
    pub(crate) completion_tracker: Arc<CompletionTracker>,

    /// Deduplicated data shared across files.
    current_session_data: Mutex<DataAggregator>,

    /// Metrics for deduplication
    deduplication_metrics: Mutex<DeduplicationMetrics>,

    /// Internal worker
    xorb_upload_tasks: Mutex<JoinSet<Result<()>>>,

    /// Xorbs that have been uploaded as part of this session -- and thus are tracked in the
    /// completion tracker.
    session_xorbs: Mutex<HashSet<MerkleHash>>,

    #[cfg(debug_assertions)]
    progress_verification_tracker: Arc<ProgressUpdaterVerificationWrapper>,
}

// Constructors
impl FileUploadSession {
    pub async fn new(
        config: Arc<TranslatorConfig>,
        threadpool: Arc<ThreadPool>,
        upload_progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
    ) -> Result<Arc<FileUploadSession>> {
        FileUploadSession::new_impl(config, threadpool, upload_progress_updater, false).await
    }

    pub async fn dry_run(
        config: Arc<TranslatorConfig>,
        threadpool: Arc<ThreadPool>,
        upload_progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
    ) -> Result<Arc<FileUploadSession>> {
        FileUploadSession::new_impl(config, threadpool, upload_progress_updater, true).await
    }

    async fn new_impl(
        config: Arc<TranslatorConfig>,
        threadpool: Arc<ThreadPool>,
        upload_progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
        dry_run: bool,
    ) -> Result<Arc<FileUploadSession>> {
        let progress_updater = upload_progress_updater.unwrap_or_else(|| Arc::new(NoOpProgressUpdater));

        // When debug assertions are enabled, track all the progress updates for consistency
        // and correctness.  This is checked at the end.
        #[cfg(debug_assertions)]
        let (progress_updater, progress_verification_tracker) = {
            let updater = ProgressUpdaterVerificationWrapper::new(progress_updater);

            (updater.clone() as Arc<dyn TrackingProgressUpdater>, updater)
        };

        let completion_tracker = Arc::new(CompletionTracker::new(progress_updater));

        let client = create_remote_client(&config, threadpool.clone(), dry_run)?;

        let shard_interface = SessionShardInterface::new(config.clone(), client.clone(), dry_run).await?;

        let repo_id = config.data_config.auth.clone().and_then(|auth| {
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

        Ok(Arc::new(Self {
            shard_interface,
            client,
            threadpool,
            repo_id,
            config,
            completion_tracker,
            current_session_data: Mutex::new(DataAggregator::default()),
            deduplication_metrics: Mutex::new(DeduplicationMetrics::default()),
            xorb_upload_tasks: Mutex::new(JoinSet::new()),
            session_xorbs: Mutex::new(HashSet::new()),

            #[cfg(debug_assertions)]
            progress_verification_tracker,
        }))
    }

    /// Start to clean one file. When cleaning multiple files, each file should
    /// be associated with one Cleaner. This allows to launch multiple clean task
    /// simultaneously.
    ///
    /// The caller is responsible for memory usage management, the parameter "buffer_size"
    /// indicates the maximum number of Vec<u8> in the internal buffer.
    pub async fn start_clean(self: &Arc<Self>, file_name: Arc<str>, size: u64) -> SingleFileCleaner {
        // Get a new file id for the completion tracking
        let file_id = self.completion_tracker.register_new_file(file_name.clone(), size).await;

        SingleFileCleaner::new(file_name, file_id, self.clone())
    }

    /// Registers a new xorb for upload, returning true if the xorb was added to the upload queue and false
    /// if it was already in the queue and didn't need to be uploaded again.
    pub(crate) async fn register_new_xorb_for_upload(self: &Arc<Self>, xorb: RawXorbData) -> Result<bool> {
        // First check the current xorb upload tasks to see if any can be cleaned up.
        {
            let mut upload_tasks = self.xorb_upload_tasks.lock().await;
            while let Some(result) = upload_tasks.try_join_next() {
                result??;
            }
        }

        let xorb_hash = xorb.hash();

        // Register that this xorb is part of this session, and thus tracked in the upload completion
        // part.
        //
        // In some circumstances, we can cut to instances of the same xorb, namely when there are two files
        // with the same starting data that get processed simultaneously.  When this happens, we only upload
        // the first one, returning early
        let new_xorb = self.session_xorbs.lock().await.insert(xorb_hash);

        if !new_xorb {
            return Ok(false);
        }

        // No need to process an empty xorb.  But check this after the session_xorbs tracker
        // to make sure the reporting is correct.
        if xorb.num_bytes() == 0 {
            self.completion_tracker.register_xorb_upload_completion(xorb_hash).await;
            return Ok(true);
        }

        let xorb_data = xorb.to_vec();
        let chunks_and_boundaries = xorb.cas_info.chunks_and_boundaries();

        drop(xorb);

        let session = self.clone();
        let upload_permit = acquire_upload_permit().await?;
        let cas_prefix = session.config.data_config.prefix.clone();

        self.xorb_upload_tasks.lock().await.spawn(async move {
            let n_bytes_transmitted = session
                .client
                .put(&cas_prefix, &xorb_hash, xorb_data, chunks_and_boundaries)
                .await?;

            drop(upload_permit);

            // Register that the xorb has been uploaded.
            session.completion_tracker.register_xorb_upload_completion(xorb_hash).await;

            // Record the number of bytes uploaded.
            session.deduplication_metrics.lock().await.xorb_bytes_uploaded += n_bytes_transmitted;
            Ok(())
        });

        Ok(true)
    }

    /// Meant to be called by the finalize() method of the SingleFileCleaner
    pub(crate) async fn register_single_file_clean_completion(
        self: &Arc<Self>,
        mut file_data: DataAggregator,
        dedup_metrics: &DeduplicationMetrics,
    ) -> Result<()> {
        // Merge in the remaining file data; uploading a new xorb if need be.
        {
            let mut current_session_data = self.current_session_data.lock().await;

            // Do we need to cut one of these to a xorb?
            if current_session_data.num_bytes() + file_data.num_bytes() > *MAX_XORB_BYTES
                || current_session_data.num_chunks() + file_data.num_chunks() > *MAX_XORB_CHUNKS
            {
                // Cut the larger one as a xorb, uploading it and registering the files.
                if current_session_data.num_bytes() > file_data.num_bytes() {
                    swap(&mut *current_session_data, &mut file_data);
                }

                // Now file data is larger
                debug_assert_le!(current_session_data.num_bytes(), file_data.num_bytes());

                // Actually upload this outside the lock
                drop(current_session_data);

                self.process_aggregated_data_as_xorb(file_data).await?;
            } else {
                current_session_data.merge_in(file_data);
            }
        }

        #[cfg(debug_assertions)]
        {
            let current_session_data = self.current_session_data.lock().await;
            debug_assert_le!(current_session_data.num_bytes(), *MAX_XORB_BYTES);
            debug_assert_le!(current_session_data.num_chunks(), *MAX_XORB_CHUNKS);
        }

        // Now, aggregate the new dedup metrics.
        self.deduplication_metrics.lock().await.merge_in(dedup_metrics);

        Ok(())
    }

    /// Process the aggregated data, uploading the data as a xorb and registering the files
    async fn process_aggregated_data_as_xorb(self: &Arc<Self>, data_agg: DataAggregator) -> Result<()> {
        let (xorb, new_files) = data_agg.finalize();
        let xorb_hash = xorb.hash();

        debug_assert_le!(xorb.num_bytes(), *MAX_XORB_BYTES);
        debug_assert_le!(xorb.data.len(), *MAX_XORB_CHUNKS);

        // Now, we need to scan all the file dependencies for dependencies on this xorb, as
        // these would not have been registered yet.
        self.register_new_xorb_for_upload(xorb).await?;

        let mut new_dependencies = Vec::with_capacity(new_files.len());

        {
            for (file_id, fi, bytes_in_xorb) in new_files {
                if xorb_hash == MerkleHash::default() || bytes_in_xorb != 0 {
                    new_dependencies.push((file_id, xorb_hash, bytes_in_xorb, false));
                }

                self.shard_interface.add_file_reconstruction_info(fi).await?;
            }
        }

        self.completion_tracker.register_dependencies(&new_dependencies).await;

        Ok(())
    }

    /// Register a xorb dependencies that is given as part of the dedup process.
    pub(crate) async fn register_xorb_dependencies(
        self: &Arc<Self>,
        file_id: CompletionTrackerFileId,
        xorb_dependencies: &[(MerkleHash, u64)],
    ) {
        // See what dependencies we own:
        let mut dependencies = Vec::with_capacity(xorb_dependencies.len());

        {
            let xorb_lookup = self.session_xorbs.lock().await;
            for &(xorb_hash, n_bytes) in xorb_dependencies {
                let is_uploaded_out_of_session = !xorb_lookup.contains(&xorb_hash);
                dependencies.push((file_id, xorb_hash, n_bytes, is_uploaded_out_of_session));
            }
        }

        self.completion_tracker.register_dependencies(&dependencies).await;
    }

    /// Finalize everthing.
    async fn finalize_impl(self: Arc<Self>, return_files: bool) -> Result<(DeduplicationMetrics, Vec<MDBFileInfo>)> {
        // Register the remaining xorbs for upload.
        let data_agg = take(&mut *self.current_session_data.lock().await);
        self.process_aggregated_data_as_xorb(data_agg).await?;

        // Now, make sure all the remaining xorbs are uploaded.
        let mut metrics = take(&mut *self.deduplication_metrics.lock().await);

        // Finalize the xorb uploads.
        let mut upload_tasks = take(&mut *self.xorb_upload_tasks.lock().await);

        while let Some(result) = upload_tasks.join_next().await {
            result??;
        }

        // Now that all the tasks there are completed, there shouldn't be any other references to this session
        // hanging around; i.e. the self in this shession should be used as if it's consuming the class, as it
        // effectively empties all the states.
        debug_assert_eq!(Arc::strong_count(&self), 1);

        let all_file_info = if return_files {
            self.shard_interface.session_file_info_list().await?
        } else {
            Vec::new()
        };

        // Upload and register the current shards in the session, moving them
        // to the cache.
        metrics.shard_bytes_uploaded = self.shard_interface.upload_and_register_session_shards().await?;
        metrics.total_bytes_uploaded = metrics.shard_bytes_uploaded + metrics.xorb_bytes_uploaded;

        // Update the global counters
        prometheus_metrics::FILTER_CAS_BYTES_PRODUCED.inc_by(metrics.new_bytes as u64);
        prometheus_metrics::FILTER_BYTES_CLEANED.inc_by(metrics.total_bytes as u64);

        #[cfg(debug_assertions)]
        {
            // Checks to make sure all the upload parts are complete.
            self.completion_tracker.assert_complete().await;

            // Checks that all the progress updates were received correctly.
            self.progress_verification_tracker.assert_complete();
        }

        Ok((metrics, all_file_info))
    }

    // Wait until everything currently in process is completed and uploaded, cutting a xorb for the remaining bit.
    // However, does not clean up the session so add_data can be called again.  Finalize must be called later.
    //
    // Used for testing.  Should be called only after all add_data calls have completed.
    pub async fn checkpoint(self: &Arc<Self>) -> Result<()> {
        // Cut the current data present as a xorb, upload it.
        let data_agg = take(&mut *self.current_session_data.lock().await);
        self.process_aggregated_data_as_xorb(data_agg).await?;

        // Wait for all inflight xorb uploads to complete.
        {
            let mut upload_tasks = self.xorb_upload_tasks.lock().await;

            while let Some(result) = upload_tasks.join_next().await {
                result??;
            }
        }

        Ok(())
    }

    pub async fn finalize(self: Arc<Self>) -> Result<DeduplicationMetrics> {
        Ok(self.finalize_impl(false).await?.0)
    }

    pub async fn finalize_with_file_info(self: Arc<Self>) -> Result<(DeduplicationMetrics, Vec<MDBFileInfo>)> {
        self.finalize_impl(true).await
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

        let mut cleaner = upload_session.start_clean("test".into(), read_data.len() as u64).await;

        // Read blocks from the source file and hand them to the cleaning handle
        cleaner.add_data(&read_data[..]).await.unwrap();

        let (pointer_file_contents, _metrics) = cleaner.finish().await.unwrap();
        upload_session.finalize().await.unwrap();

        pf_out.write_all(pointer_file_contents.to_string().as_bytes()).unwrap();
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
            .smudge_file_from_pointer(&pointer_file, &mut (Box::new(writer) as Box<dyn Write + Send>), None, None)
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
