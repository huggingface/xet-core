use std::borrow::Cow;
use std::fs::File;
use std::io::Read;
use std::mem::{swap, take};
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use cas_client::Client;
use cas_object::SerializedCasObject;
use deduplication::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS};
use deduplication::{DataAggregator, DeduplicationMetrics, RawXorbData};
use lazy_static::lazy_static;
use mdb_shard::Sha256;
use mdb_shard::file_structs::MDBFileInfo;
use more_asserts::*;
use progress_tracking::aggregator::AggregatingProgressUpdater;
use progress_tracking::upload_tracking::{CompletionTracker, FileXorbDependency};
#[cfg(debug_assertions)] // Used here to verify the update accuracy
use progress_tracking::verification_wrapper::ProgressUpdaterVerificationWrapper;
use progress_tracking::{NoOpProgressUpdater, TrackingProgressUpdater};
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{Instrument, Span, info_span, instrument};
use ulid::Ulid;
use xet_runtime::{GlobalSemaphoreHandle, XetRuntime, global_semaphore_handle, xet_config};

use crate::configurations::*;
use crate::errors::*;
use crate::file_cleaner::SingleFileCleaner;
use crate::remote_client_interface::create_remote_client;
use crate::shard_interface::SessionShardInterface;
use crate::{XetFileInfo, prometheus_metrics};

lazy_static! {
    pub static ref CONCURRENT_FILE_INGESTION_LIMITER: GlobalSemaphoreHandle =
        global_semaphore_handle!(xet_config().data.max_concurrent_file_ingestion);
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

    /// The configuration settings, if needed.
    pub(crate) config: Arc<TranslatorConfig>,

    /// Tracking upload completion between xorbs and files.
    pub(crate) completion_tracker: Arc<CompletionTracker>,

    /// Session aggregation
    progress_aggregator: Option<Arc<AggregatingProgressUpdater>>,

    /// Deduplicated data shared across files.
    current_session_data: Mutex<DataAggregator>,

    /// Metrics for deduplication
    deduplication_metrics: Mutex<DeduplicationMetrics>,

    /// Internal worker
    xorb_upload_tasks: Mutex<JoinSet<Result<()>>>,

    #[cfg(debug_assertions)]
    progress_verifier: Arc<ProgressUpdaterVerificationWrapper>,
}

// Constructors
impl FileUploadSession {
    pub async fn new(
        config: Arc<TranslatorConfig>,
        upload_progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
    ) -> Result<Arc<FileUploadSession>> {
        FileUploadSession::new_impl(config, upload_progress_updater, false).await
    }

    pub async fn dry_run(
        config: Arc<TranslatorConfig>,
        upload_progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
    ) -> Result<Arc<FileUploadSession>> {
        FileUploadSession::new_impl(config, upload_progress_updater, true).await
    }

    async fn new_impl(
        config: Arc<TranslatorConfig>,
        upload_progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
        dry_run: bool,
    ) -> Result<Arc<FileUploadSession>> {
        let session_id = config
            .session_id
            .as_ref()
            .map(Cow::Borrowed)
            .unwrap_or_else(|| Cow::Owned(Ulid::new().to_string()));

        let (progress_updater, progress_aggregator): (Arc<dyn TrackingProgressUpdater>, Option<_>) = {
            match upload_progress_updater {
                Some(updater) => {
                    let flush_interval = xet_config().data.progress_update_interval;
                    if !flush_interval.is_zero() && config.progress_config.aggregate {
                        let aggregator = AggregatingProgressUpdater::new(
                            updater,
                            flush_interval,
                            xet_config().data.progress_update_speed_sampling_window,
                        );
                        (aggregator.clone(), Some(aggregator))
                    } else {
                        (updater, None)
                    }
                },
                None => (Arc::new(NoOpProgressUpdater), None),
            }
        };

        // When debug assertions are enabled, track all the progress updates for consistency
        // and correctness.  This is checked at the end.
        #[cfg(debug_assertions)]
        let (progress_updater, progress_verification_tracker) = {
            let updater = ProgressUpdaterVerificationWrapper::new(progress_updater);

            (updater.clone() as Arc<dyn TrackingProgressUpdater>, updater)
        };

        let completion_tracker = Arc::new(CompletionTracker::new(progress_updater));

        let client = create_remote_client(&config, &session_id, dry_run)?;

        let shard_interface = SessionShardInterface::new(config.clone(), client.clone(), dry_run).await?;

        Ok(Arc::new(Self {
            shard_interface,
            client,
            config,
            completion_tracker,
            progress_aggregator,
            current_session_data: Mutex::new(DataAggregator::default()),
            deduplication_metrics: Mutex::new(DeduplicationMetrics::default()),
            xorb_upload_tasks: Mutex::new(JoinSet::new()),

            #[cfg(debug_assertions)]
            progress_verifier: progress_verification_tracker,
        }))
    }

    pub async fn upload_files(
        self: &Arc<Self>,
        files_and_sha256s: impl IntoIterator<Item = (impl AsRef<Path>, Option<Sha256>)> + Send,
    ) -> Result<Vec<XetFileInfo>> {
        let mut cleaning_tasks: Vec<JoinHandle<_>> = vec![];

        for (f, sha256) in files_and_sha256s.into_iter() {
            let file_path = f.as_ref().to_owned();
            let file_name: Arc<str> = Arc::from(file_path.to_string_lossy());

            // Get the file size, and go ahead and register it in the completion tracker so that we know the whole
            // repo size at the beginning.
            let file_size = std::fs::metadata(&file_path)?.len();

            // Get a new file id for the completion tracking.  This also registers the size against the total bytes
            // of the file.
            let file_id = self.completion_tracker.register_new_file(file_name.clone(), file_size).await;

            // Now, spawn a task
            let ingestion_concurrency_limiter =
                XetRuntime::current().global_semaphore(*CONCURRENT_FILE_INGESTION_LIMITER);
            let session = self.clone();

            cleaning_tasks.push(tokio::spawn(async move {
                // Enable tracing to record this file's ingestion speed.
                let span = info_span!(
                    "clean_file_task",
                    "file.name" = file_name.to_string(),
                    "file.len" = file_size,
                    "file.new_bytes" = tracing::field::Empty,
                    "file.deduped_bytes" = tracing::field::Empty,
                    "file.defrag_prevented_dedup_bytes" = tracing::field::Empty,
                    "file.new_chunks" = tracing::field::Empty,
                    "file.deduped_chunks" = tracing::field::Empty,
                    "file.defrag_prevented_dedup_chunks" = tracing::field::Empty,
                );
                // First, get a permit to process this file.
                let _processing_permit = ingestion_concurrency_limiter.acquire().await?;

                async move {
                    let mut reader = File::open(&file_path)?;

                    // Start the clean process for each file.
                    let mut cleaner = SingleFileCleaner::new(Some(file_name), file_id, sha256, session);
                    let mut bytes_read = 0;

                    while bytes_read < file_size {
                        // Allocate a block of bytes, read into it.
                        let bytes_left = file_size - bytes_read;
                        let n_bytes_read = (*xet_config().data.ingestion_block_size).min(bytes_left) as usize;

                        // Read in the data here; we are assuming the file doesn't change size
                        // on the disk while we are reading it.

                        // We allocate the buffer anew on each loop as it's converted without copying
                        // to a Bytes object, and thus we avoid further copies downstream.  We also
                        // guarantee that the buffer is filled completely with the read_exact. Therefore,
                        // we can use an unsafe trick here to allocate the vector without initializing it
                        // to a specific value and avoid that clearing.
                        let mut buffer = Vec::with_capacity(n_bytes_read);
                        #[allow(clippy::uninit_vec)]
                        unsafe {
                            buffer.set_len(n_bytes_read);
                        }

                        // Read it in.
                        reader.read_exact(&mut buffer)?;

                        bytes_read += buffer.len() as u64;

                        cleaner.add_data_impl(Bytes::from(buffer)).await?;
                    }

                    // Finish and return the result.
                    let (xfi, metrics) = cleaner.finish().await?;

                    // Record dedup information.
                    let span = Span::current();
                    span.record("file.new_bytes", metrics.new_bytes);
                    span.record("file.deduped_bytes ", metrics.deduped_bytes);
                    span.record("file.defrag_prevented_dedup_bytes", metrics.defrag_prevented_dedup_bytes);
                    span.record("file.new_chunks", metrics.new_chunks);
                    span.record("file.deduped_chunks", metrics.deduped_chunks);
                    span.record("file.defrag_prevented_dedup_chunks", metrics.defrag_prevented_dedup_chunks);

                    Result::Ok(xfi)
                }
                .instrument(span)
                .await
            }));
        }

        // Join all the cleaning tasks.
        let mut ret = Vec::with_capacity(cleaning_tasks.len());

        for task in cleaning_tasks {
            ret.push(task.await??);
        }

        Ok(ret)
    }

    /// Start to clean one file. When cleaning multiple files, each file should
    /// be associated with one Cleaner. This allows to launch multiple clean task
    /// simultaneously.
    ///
    /// The caller is responsible for memory usage management, the parameter "buffer_size"
    /// indicates the maximum number of Vec<u8> in the internal buffer.
    ///
    /// If a sha256 is provided, the value will be directly used in shard upload to
    /// avoid redundant computation.
    pub async fn start_clean(
        self: &Arc<Self>,
        file_name: Option<Arc<str>>,
        size: u64,
        sha256: Option<Sha256>,
    ) -> SingleFileCleaner {
        // Get a new file id for the completion tracking
        let file_id = self
            .completion_tracker
            .register_new_file(file_name.clone().unwrap_or_default(), size)
            .await;

        SingleFileCleaner::new(file_name, file_id, sha256, self.clone())
    }

    /// Registers a new xorb for upload, returning true if the xorb was added to the upload queue and false
    /// if it was already in the queue and didn't need to be uploaded again.
    #[instrument(skip_all, name="FileUploadSession::register_new_xorb_for_upload", fields(xorb_len = xorb.num_bytes()))]
    pub(crate) async fn register_new_xorb(
        self: &Arc<Self>,
        xorb: RawXorbData,
        file_dependencies: &[FileXorbDependency],
    ) -> Result<bool> {
        // First check the current xorb upload tasks to see if any can be cleaned up.
        {
            let mut upload_tasks = self.xorb_upload_tasks.lock().await;
            while let Some(result) = upload_tasks.try_join_next() {
                result??;
            }
        }

        let xorb_hash = xorb.hash();

        // Register that this xorb is part of this session and set up completion tracking.
        //
        // In some circumstances, we can cut to instances of the same xorb, namely when there are two files
        // with the same starting data that get processed simultaneously.  When this happens, we only upload
        // the first one, returning early here.
        let xorb_is_new = self
            .completion_tracker
            .register_new_xorb(xorb_hash, xorb.num_bytes() as u64)
            .await;

        // Make sure we add in all the dependencies.  This should happen after the xorb is registered but before
        // we start the upload.
        self.completion_tracker.register_dependencies(file_dependencies).await;

        if !xorb_is_new {
            return Ok(false);
        }

        // No need to process an empty xorb.  But check this after the session_xorbs tracker
        // to make sure the reporting is correct.
        if xorb.num_bytes() == 0 {
            self.completion_tracker.register_xorb_upload_completion(xorb_hash).await;
            return Ok(true);
        }

        // This xorb is in the session upload queue, so other threads can go ahead and dedup against it.
        // No session shard data gets uploaded until all the xorbs have been successfully uploaded, so
        // this is safe.
        let xorb_cas_info = Arc::new(xorb.cas_info.clone());
        self.shard_interface.add_cas_block(xorb_cas_info.clone()).await?;

        let xorb_hash = xorb.hash();

        // Serialize the object; this can be relatively expensive, so run it on a compute thread.
        let compression_scheme = self.config.data_config.compression;
        let with_footer = self.client.use_xorb_footer();
        let cas_object =
            tokio::task::spawn_blocking(move || SerializedCasObject::from_xorb(xorb, compression_scheme, with_footer))
                .await??;

        let session = self.clone();
        let upload_permit = self.client.acquire_upload_permit().await?;
        let cas_prefix = session.config.data_config.prefix.clone();
        let completion_tracker = self.completion_tracker.clone();

        self.xorb_upload_tasks.lock().await.spawn(
            async move {
                let n_bytes_transmitted = session
                    .client
                    .upload_xorb(&cas_prefix, cas_object, Some(completion_tracker), upload_permit)
                    .await?;

                // Register that the xorb has been uploaded.
                session.completion_tracker.register_xorb_upload_completion(xorb_hash).await;

                // Record the number of bytes uploaded.
                session.deduplication_metrics.lock().await.xorb_bytes_uploaded += n_bytes_transmitted as u64;

                // Add this as a completed cas block so that future sessions can resume quickly.
                session.shard_interface.add_uploaded_cas_block(xorb_cas_info).await?;

                Ok(())
            }
            .instrument(info_span!("FileUploadSession::upload_xorb_task", xorb.hash = xorb_hash.hex())),
        );

        Ok(true)
    }

    /// Meant to be called by the finalize() method of the SingleFileCleaner
    #[instrument(skip_all, name="FileUploadSession::register_single_file_clean_completion", fields(num_bytes = file_data.num_bytes(), num_chunks = file_data.num_chunks()))]
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
        // these would not have been registered yet as we just got the xorb hash.
        let mut new_dependencies = Vec::with_capacity(new_files.len());

        {
            for (file_id, fi, bytes_in_xorb) in new_files {
                new_dependencies.push(FileXorbDependency {
                    file_id,
                    xorb_hash,
                    n_bytes: bytes_in_xorb,
                    is_external: false,
                });

                // Record the reconstruction.
                self.shard_interface.add_file_reconstruction_info(fi).await?;
            }
        }

        // Register the xorb and start the upload process.
        self.register_new_xorb(xorb, &new_dependencies).await?;

        Ok(())
    }

    /// Register a xorb dependencies that is given as part of the dedup process.
    pub(crate) async fn register_xorb_dependencies(self: &Arc<Self>, xorb_dependencies: &[FileXorbDependency]) {
        self.completion_tracker.register_dependencies(xorb_dependencies).await;
    }

    /// Finalize everything.
    #[instrument(skip_all, name="FileUploadSession::finalize", fields(session.id))]
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
        // hanging around; i.e. the self in this session should be used as if it's consuming the class, as it
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
        prometheus_metrics::FILTER_CAS_BYTES_PRODUCED.inc_by(metrics.new_bytes);
        prometheus_metrics::FILTER_BYTES_CLEANED.inc_by(metrics.total_bytes);

        #[cfg(debug_assertions)]
        {
            // Checks to make sure all the upload parts are complete.
            self.completion_tracker.assert_complete().await;

            // Checks that all the progress updates were received correctly.
            self.progress_verifier.assert_complete().await;
        }

        // Make sure all the updates have been flushed through.
        self.completion_tracker.flush().await;

        // Clear this out so the background aggregation session fully finishes.
        if let Some(pa) = &self.progress_aggregator {
            pa.finalize().await;
            debug_assert!(pa.is_finished().await);
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

        self.completion_tracker.flush().await;

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

    use xet_runtime::XetRuntime;

    use crate::{FileDownloader, FileUploadSession, XetFileInfo};

    /// Return a shared threadpool to be reused as needed.
    fn get_threadpool() -> Arc<XetRuntime> {
        static THREADPOOL: OnceLock<Arc<XetRuntime>> = OnceLock::new();
        THREADPOOL
            .get_or_init(|| XetRuntime::new().expect("Error starting multithreaded runtime."))
            .clone()
    }

    /// Cleans (converts) a regular file into a pointer file.
    ///
    /// * `input_path`: path to the original file
    /// * `output_path`: path to write the pointer file
    async fn test_clean_file(cas_path: &Path, input_path: &Path, output_path: &Path) {
        let read_data = read(input_path).unwrap().to_vec();

        let mut pf_out = Box::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(output_path)
                .unwrap(),
        );

        let upload_session = FileUploadSession::new(TranslatorConfig::local_config(cas_path).unwrap().into(), None)
            .await
            .unwrap();

        let mut cleaner = upload_session
            .start_clean(Some("test".into()), read_data.len() as u64, None)
            .await;

        // Read blocks from the source file and hand them to the cleaning handle
        cleaner.add_data(&read_data[..]).await.unwrap();

        let (xet_file_info, _metrics) = cleaner.finish().await.unwrap();
        upload_session.finalize().await.unwrap();

        pf_out
            .write_all(serde_json::to_string(&xet_file_info).unwrap().as_bytes())
            .unwrap();
    }

    /// Smudges (hydrates) a pointer file back into the original data.
    ///
    /// * `pointer_path`: path to the pointer file
    /// * `output_path`: path to write the hydrated/original file
    async fn test_smudge_file(cas_path: &Path, pointer_path: &Path, output_path: &Path) {
        use file_reconstruction::DataOutput;

        let mut reader = File::open(pointer_path).unwrap();

        let mut input = String::new();
        reader.read_to_string(&mut input).unwrap();

        let xet_file = serde_json::from_str::<XetFileInfo>(&input).unwrap();

        let config = TranslatorConfig::local_config(cas_path).unwrap();
        let translator = FileDownloader::new(config.into()).await.unwrap();

        let output = DataOutput::write_in_file(output_path);

        translator
            .smudge_file_from_hash(
                &xet_file.merkle_hash().expect("File hash is not a valid file hash"),
                output_path.to_string_lossy().into(),
                output,
                None,
                None,
            )
            .await
            .unwrap();
    }

    use std::fs::{read, write};

    use tempfile::tempdir;

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
                test_clean_file(&cas_path, &original_path, &pointer_path).await;

                // 3. Smudge it (hydrate the pointer file) to a new file
                let hydrated_path = temp.path().join("hydrated.txt");
                test_smudge_file(&cas_path, &pointer_path, &hydrated_path).await;

                // 4. Verify that the round-tripped file matches the original
                let result_data = read(hydrated_path).unwrap();
                assert_eq!(original_data.to_vec(), result_data);
            })
            .unwrap();
    }
}
