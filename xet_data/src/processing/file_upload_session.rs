use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::mem::{swap, take};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use more_asserts::*;
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{Instrument, Span, info_span, instrument};
use xet_client::cas_client::{Client, ProgressCallback};
use xet_core_structures::metadata_shard::file_structs::MDBFileInfo;
use xet_core_structures::xorb_object::SerializedXorbObject;
use xet_runtime::core::{XetRuntime, xet_config};

use super::configurations::TranslatorConfig;
use super::file_cleaner::{Sha256Policy, SingleFileCleaner};
use super::remote_client_interface::create_remote_client;
use super::shard_interface::SessionShardInterface;
use super::{XetFileInfo, prometheus_metrics};
use crate::deduplication::constants::{
    MAX_XORB_BYTES, MAX_XORB_CHUNKS, XORB_CUT_THRESHOLD_BYTES, XORB_CUT_THRESHOLD_CHUNKS,
};
use crate::deduplication::{DataAggregator, DeduplicationMetrics, RawXorbData};
use crate::error::{DataError, Result};
use crate::progress_tracking::upload_tracking::{CompletionTracker, FileXorbDependency};
use crate::progress_tracking::{GroupProgress, GroupProgressReport, ItemProgressReport, UniqueID};

/// Manages the translation of files between the
/// MerkleDB / pointer file format and the materialized version.
///
/// This class handles the clean operations.  It's meant to be a single atomic session
/// that succeeds or fails as a unit;  i.e. all files get uploaded on finalization, and all shards
/// and xorbs needed to reconstruct those files are properly uploaded and registered.
pub struct FileUploadSession {
    pub(crate) client: Arc<dyn Client + Send + Sync>,
    pub(crate) shard_interface: SessionShardInterface,

    /// Tracking upload completion between xorbs and files.
    pub(crate) completion_tracker: Arc<CompletionTracker>,

    /// Aggregate progress across all files in this upload session.
    progress: Arc<GroupProgress>,

    /// Deduplicated data shared across files.
    current_session_data: Mutex<DataAggregator>,

    /// Metrics for deduplication
    deduplication_metrics: Mutex<DeduplicationMetrics>,

    /// Internal worker
    xorb_upload_tasks: Mutex<JoinSet<Result<()>>>,

    /// Set to true after finalize() has been called.
    finalized: AtomicBool,
}

// Constructors
impl FileUploadSession {
    pub async fn new(config: Arc<TranslatorConfig>) -> Result<Arc<FileUploadSession>> {
        FileUploadSession::new_impl(config, false).await
    }

    pub async fn dry_run(config: Arc<TranslatorConfig>) -> Result<Arc<FileUploadSession>> {
        FileUploadSession::new_impl(config, true).await
    }

    async fn new_impl(config: Arc<TranslatorConfig>, dry_run: bool) -> Result<Arc<FileUploadSession>> {
        let session_id = config
            .session
            .session_id
            .as_ref()
            .map(Cow::Borrowed)
            .unwrap_or_else(|| Cow::Owned(UniqueID::new().to_string()));

        let progress = GroupProgress::with_speed_config(
            xet_config().data.progress_update_speed_sampling_window,
            xet_config().data.progress_update_speed_min_observations,
        );
        let completion_tracker = Arc::new(CompletionTracker::new(progress.clone()));

        let client = create_remote_client(&config, &session_id, dry_run).await?;

        let shard_interface = SessionShardInterface::new(config.clone(), client.clone(), dry_run).await?;

        Ok(Arc::new(Self {
            shard_interface,
            client,
            completion_tracker,
            progress,
            current_session_data: Mutex::new(DataAggregator::default()),
            deduplication_metrics: Mutex::new(DeduplicationMetrics::default()),
            xorb_upload_tasks: Mutex::new(JoinSet::new()),
            finalized: AtomicBool::new(false),
        }))
    }

    pub async fn upload_files(
        self: &Arc<Self>,
        files_and_sha256: impl IntoIterator<Item = (impl AsRef<Path>, Sha256Policy)> + Send,
    ) -> Result<Vec<XetFileInfo>> {
        self.check_not_finalized()?;
        let mut cleaning_tasks: Vec<JoinHandle<_>> = vec![];

        for (f, sha256) in files_and_sha256.into_iter() {
            let file_path = f.as_ref().to_owned();
            let file_name: Arc<str> = Arc::from(file_path.to_string_lossy());

            let file_size = std::fs::metadata(&file_path)?.len();

            let updater = self.progress.new_item(UniqueID::new(), file_name.clone());
            let file_id = self.completion_tracker.register_new_file(updater, Some(file_size));

            let ingestion_concurrency_limiter = XetRuntime::current().common().file_ingestion_semaphore.clone();
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

                        cleaner.add_data_from_bytes(Bytes::from(buffer)).await?;
                    }

                    // Finish and return the result.
                    let (xfi, metrics) = cleaner.finish().await?;

                    // Record dedup information.
                    let span = Span::current();
                    span.record("file.new_bytes", metrics.new_bytes);
                    span.record("file.deduped_bytes", metrics.deduped_bytes);
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
    /// If a sha256 is provided via [`Sha256Policy::Provided`], the value will be directly
    /// used in shard upload to avoid redundant computation. [`Sha256Policy::Skip`] skips
    /// SHA-256 computation entirely and no metadata_ext is included in the shard.
    pub fn start_clean(
        self: &Arc<Self>,
        tracking_name: Option<Arc<str>>,
        size: Option<u64>,
        sha256: Sha256Policy,
    ) -> Result<(UniqueID, SingleFileCleaner)> {
        self.check_not_finalized()?;
        let id = UniqueID::new();
        let cleaner = self.start_clean_with_id(id, tracking_name, size, sha256);
        Ok((id, cleaner))
    }

    fn start_clean_with_id(
        self: &Arc<Self>,
        id: UniqueID,
        tracking_name: Option<Arc<str>>,
        size: Option<u64>,
        sha256: Sha256Policy,
    ) -> SingleFileCleaner {
        let updater = self.progress.new_item(id, tracking_name.clone().unwrap_or_default());
        let file_id = self.completion_tracker.register_new_file(updater, size);
        SingleFileCleaner::new(tracking_name, file_id, sha256, self.clone())
    }

    /// Spawns a task that reads `file_path` and uploads it.
    ///
    /// Returns the tracking ID and a join handle for the spawned task.
    pub async fn spawn_upload_from_path(
        self: &Arc<Self>,
        file_path: PathBuf,
        sha256: Sha256Policy,
    ) -> Result<(UniqueID, JoinHandle<Result<(XetFileInfo, DeduplicationMetrics)>>)> {
        self.check_not_finalized()?;
        let file_size = std::fs::metadata(&file_path)?.len();
        let tracking_name: Arc<str> = Arc::from(file_path.to_string_lossy().as_ref());
        let (id, cleaner) = self.start_clean(Some(tracking_name), Some(file_size), sha256)?;

        let rt = XetRuntime::current();
        let semaphore = rt.common().file_ingestion_semaphore.clone();
        let handle = rt.spawn(async move {
            let _permit = semaphore.acquire().await?;
            Self::feed_file_to_cleaner(cleaner, &file_path).await
        });

        Ok((id, handle))
    }

    /// Spawns a task that uploads `bytes` as a single file.
    ///
    /// Returns the tracking ID and a join handle for the spawned task.
    pub async fn spawn_upload_bytes(
        self: &Arc<Self>,
        bytes: Vec<u8>,
        sha256: Sha256Policy,
        tracking_name: Option<Arc<str>>,
    ) -> Result<(UniqueID, JoinHandle<Result<(XetFileInfo, DeduplicationMetrics)>>)> {
        self.check_not_finalized()?;
        let (id, mut cleaner) = self.start_clean(tracking_name, Some(bytes.len() as u64), sha256)?;

        let rt = XetRuntime::current();
        let semaphore = rt.common().file_ingestion_semaphore.clone();
        let handle = rt.spawn(async move {
            let _permit = semaphore.acquire().await?;
            cleaner.add_data(&bytes).await?;
            cleaner.finish().await
        });

        Ok((id, handle))
    }

    async fn feed_file_to_cleaner(
        mut cleaner: SingleFileCleaner,
        file_path: &Path,
    ) -> Result<(XetFileInfo, DeduplicationMetrics)> {
        let mut reader = File::open(file_path)?;
        let filesize = reader.metadata()?.len();
        let mut buffer = vec![0u8; u64::min(filesize, *xet_config().data.ingestion_block_size) as usize];

        loop {
            let n = reader.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            cleaner.add_data(&buffer[..n]).await?;
        }
        cleaner.finish().await
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
        let xorb_is_new = self.completion_tracker.register_new_xorb(xorb_hash, xorb.num_bytes() as u64);

        // Make sure we add in all the dependencies.  This should happen after the xorb is registered but before
        // we start the upload.
        self.completion_tracker.register_dependencies(file_dependencies);

        if !xorb_is_new {
            return Ok(false);
        }

        // No need to process an empty xorb.  But check this after the session_xorbs tracker
        // to make sure the reporting is correct.
        if xorb.num_bytes() == 0 {
            self.completion_tracker.register_xorb_upload_completion(xorb_hash);
            return Ok(true);
        }

        // This xorb is in the session upload queue, so other threads can go ahead and dedup against it.
        // No session shard data gets uploaded until all the xorbs have been successfully uploaded, so
        // this is safe.
        let xorb_info = Arc::new(xorb.xorb_info.clone());
        self.shard_interface.add_xorb_block(xorb_info.clone()).await?;

        // Serialize the object; this can be relatively expensive, so run it on a compute thread.
        // XORBs are sent without footer - the server/client reconstructs it from chunk data.
        let xorb_obj = XetRuntime::current()
            .spawn_blocking(move || SerializedXorbObject::from_xorb(xorb, false))
            .await??;

        let session = self.clone();
        let upload_permit = self.client.acquire_upload_permit().await?;
        let cas_prefix = xet_config().data.default_prefix.clone();
        let completion_tracker = self.completion_tracker.clone();
        let xorb_hash = xorb_obj.hash;
        let raw_num_bytes = xorb_obj.raw_num_bytes;
        let progress_callback: ProgressCallback = Arc::new(move |delta, _completed, total| {
            let raw_delta = (delta * raw_num_bytes).checked_div(total).unwrap_or(0);
            if raw_delta > 0 {
                completion_tracker
                    .clone()
                    .register_xorb_upload_progress_background(xorb_hash, raw_delta);
            }
        });

        self.xorb_upload_tasks.lock().await.spawn(
            async move {
                let n_bytes_transmitted = session
                    .client
                    .upload_xorb(&cas_prefix, xorb_obj, Some(progress_callback), upload_permit)
                    .await?;

                // Register that the xorb has been uploaded.
                session.completion_tracker.register_xorb_upload_completion(xorb_hash);

                // Record the number of bytes uploaded.
                session.deduplication_metrics.lock().await.xorb_bytes_uploaded += n_bytes_transmitted;

                // Add this as a completed cas block so that future sessions can resume quickly.
                session.shard_interface.add_uploaded_xorb_block(xorb_info).await?;

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
            if current_session_data.num_bytes() + file_data.num_bytes() > *XORB_CUT_THRESHOLD_BYTES
                || current_session_data.num_chunks() + file_data.num_chunks() > *XORB_CUT_THRESHOLD_CHUNKS
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
    pub(crate) fn register_xorb_dependencies(self: &Arc<Self>, xorb_dependencies: &[FileXorbDependency]) {
        self.completion_tracker.register_dependencies(xorb_dependencies);
    }

    /// Finalize everything.
    #[instrument(skip_all, name="FileUploadSession::finalize", fields(session.id))]
    async fn finalize_impl(
        self: Arc<Self>,
        return_files: bool,
    ) -> Result<(DeduplicationMetrics, Vec<MDBFileInfo>, GroupProgressReport)> {
        if self.finalized.swap(true, Ordering::AcqRel) {
            return Err(DataError::InvalidOperation("FileUploadSession already finalized".to_string()));
        }

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
            self.completion_tracker.assert_complete();
            self.progress.assert_complete();
        }

        let report = self.report();
        Ok((metrics, all_file_info, report))
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

    fn check_not_finalized(&self) -> Result<()> {
        if self.finalized.load(Ordering::Acquire) {
            return Err(DataError::InvalidOperation("FileUploadSession already finalized".to_string()));
        }
        Ok(())
    }

    pub fn progress(&self) -> &Arc<GroupProgress> {
        &self.progress
    }

    pub fn report(&self) -> GroupProgressReport {
        self.progress.report()
    }

    pub fn item_report(&self, id: UniqueID) -> Option<ItemProgressReport> {
        self.progress.item_report(id)
    }

    pub fn item_reports(&self) -> HashMap<UniqueID, ItemProgressReport> {
        self.progress.item_reports()
    }

    pub async fn finalize(self: Arc<Self>) -> Result<DeduplicationMetrics> {
        Ok(self.finalize_impl(false).await?.0)
    }

    pub async fn finalize_with_report(self: Arc<Self>) -> Result<(DeduplicationMetrics, GroupProgressReport)> {
        let (metrics, _file_info, report) = self.finalize_impl(false).await?;
        Ok((metrics, report))
    }

    pub async fn finalize_with_file_info(self: Arc<Self>) -> Result<(DeduplicationMetrics, Vec<MDBFileInfo>)> {
        let (metrics, file_info, _report) = self.finalize_impl(true).await?;
        Ok((metrics, file_info))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Write};
    use std::path::Path;
    use std::sync::{Arc, OnceLock};

    use xet_runtime::core::XetRuntime;

    use crate::processing::{FileDownloadSession, FileUploadSession, XetFileInfo};

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

        let upload_session = FileUploadSession::new(TranslatorConfig::local_config(cas_path).unwrap().into())
            .await
            .unwrap();

        let (_id, mut cleaner) = upload_session
            .start_clean(Some("test".into()), Some(read_data.len() as u64), Sha256Policy::Compute)
            .unwrap();

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
        let mut reader = File::open(pointer_path).unwrap();

        let mut input = String::new();
        reader.read_to_string(&mut input).unwrap();

        let xet_file = serde_json::from_str::<XetFileInfo>(&input).unwrap();

        let config = TranslatorConfig::local_config(cas_path).unwrap();
        let session = FileDownloadSession::new(config.into(), None).await.unwrap();

        let (_id, _n_bytes) = session.download_file(&xet_file, output_path).await.unwrap();
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
            .bridge_sync(async move {
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

    #[test]
    fn test_clean_skip_sha256_no_metadata_ext() {
        let temp = tempdir().unwrap();
        let data = b"Hello, skip sha256!";

        let runtime = get_threadpool();

        runtime
            .clone()
            .bridge_sync(async move {
                let cas_path = temp.path().join("cas");

                let upload_session = FileUploadSession::new(TranslatorConfig::local_config(&cas_path).unwrap().into())
                    .await
                    .unwrap();

                let (_id, mut cleaner) = upload_session
                    .start_clean(Some("test".into()), Some(data.len() as u64), Sha256Policy::Skip)
                    .unwrap();
                cleaner.add_data(data).await.unwrap();
                cleaner.finish().await.unwrap();

                // Verify that the shard has no metadata_ext (no SHA-256).
                let (_metrics, file_infos) = upload_session.finalize_with_file_info().await.unwrap();
                assert_eq!(file_infos.len(), 1);
                assert!(file_infos[0].metadata_ext.is_none(), "Skip should produce no metadata_ext");
            })
            .unwrap();
    }
}
