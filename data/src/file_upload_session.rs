use std::borrow::Cow;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::mem::{swap, take};
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use cas_client::{Client, ProgressCallback};
use cas_object::SerializedCasObject;
use deduplication::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS};
use deduplication::{DataAggregator, DeduplicationMetrics, RawXorbData};
use mdb_shard::Sha256;
use mdb_shard::file_structs::{FileDataSequenceEntry, MDBFileInfo};
use more_asserts::*;
use progress_tracking::aggregator::AggregatingProgressUpdater;
use progress_tracking::upload_tracking::{CompletionTracker, FileXorbDependency};
#[cfg(debug_assertions)] // Used here to verify the update accuracy
use progress_tracking::verification_wrapper::ProgressUpdaterVerificationWrapper;
use progress_tracking::{NoOpProgressUpdater, TrackingProgressUpdater};
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{Instrument, Span, info, info_span, instrument};
use ulid::Ulid;
use xet_runtime::{XetRuntime, xet_config};

use crate::configurations::*;
use crate::errors::*;
use crate::file_cleaner::SingleFileCleaner;
use crate::file_download_session::FileDownloadSession;
use crate::remote_client_interface::create_remote_client;
use crate::shard_interface::SessionShardInterface;
use crate::{XetFileInfo, prometheus_metrics};

/// Manages the translation of files between the
/// MerkleDB / pointer file format and the materialized version.
///
/// This class handles the clean operations.  It's meant to be a single atomic session
/// that succeeds or fails as a unit;  i.e. all files get uploaded on finalization, and all shards
/// and xorbs needed to reconstruct those files are properly uploaded and registered.
pub struct FileUploadSession {
    // The parts of this that manage the
    pub client: Arc<dyn Client + Send + Sync>,
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

        let client = create_remote_client(&config, &session_id, dry_run).await?;

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

            // Get a new file id for the completion tracking.  The size is not passed here;
            // it will be discovered incrementally via increment_file_size in add_data_impl.
            let file_id = self
                .completion_tracker
                .register_new_file(file_name.clone(), Some(file_size))
                .await;

            // Now, spawn a task
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
        size: Option<u64>,
        sha256: Option<Sha256>,
    ) -> SingleFileCleaner {
        // Get a new file id for the completion tracking.
        let file_id = self
            .completion_tracker
            .register_new_file(file_name.clone().unwrap_or_default(), size)
            .await;

        SingleFileCleaner::new(file_name, file_id, sha256, self.clone())
    }

    /// Uploads a file using delta deduplication: only changed portions go through
    /// the full upload pipeline while unchanged byte ranges are streamed from CAS
    /// and fed through the chunker so that the dedup layer recognizes them.
    ///
    /// `dirty_ranges` must be sorted, non-overlapping `(start, end)` byte ranges
    /// (exclusive end). Gaps between dirty ranges are considered clean and are
    /// downloaded from CAS via `download_session`.
    #[instrument(skip_all, name = "FileUploadSession::upload_file_delta",
        fields(new_file_size, num_dirty_ranges = dirty_ranges.len()))]
    pub async fn upload_file_delta(
        self: &Arc<Self>,
        download_session: &FileDownloadSession,
        old_file_info: &XetFileInfo,
        new_file_size: u64,
        dirty_ranges: &[(u64, u64)],
        staging_path: impl AsRef<Path>,
    ) -> Result<(XetFileInfo, DeduplicationMetrics)> {
        let staging_path = staging_path.as_ref().to_owned();
        let old_size = old_file_info.file_size();

        // Optimized path: for append-only writes (single dirty range starting at old_size),
        // pre-populate the dedup pipeline with chunk metadata from local shards instead of
        // downloading the entire original file from CAS.
        if let Some(result) = self
            .try_upload_file_delta_optimized(
                download_session,
                old_file_info,
                new_file_size,
                dirty_ranges,
                &staging_path,
            )
            .await?
        {
            return Ok(result);
        }

        // Fallback: streaming path that downloads clean ranges from CAS.
        let mut cleaner = self.start_clean(None, Some(new_file_size), None).await;

        let mut cursor: u64 = 0;

        for &(dirty_start, dirty_end) in dirty_ranges {
            // Clean gap before this dirty range: download from CAS
            if cursor < dirty_start {
                let clean_end = dirty_start.min(old_size);
                if cursor < clean_end {
                    let mut stream = download_session.download_stream_range(old_file_info, cursor..clean_end, None)?;
                    while let Some(chunk) = stream.next().await? {
                        cleaner.add_data(&chunk).await?;
                    }
                }
                cursor = dirty_start;
            }

            // Dirty range: read from staging file
            if cursor < dirty_end {
                let path = staging_path.clone();
                let offset = cursor;
                let len = (dirty_end - cursor) as usize;
                let data = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
                    let mut f = File::open(&path)?;
                    f.seek(SeekFrom::Start(offset))?;
                    let mut buf = vec![0u8; len];
                    f.read_exact(&mut buf)?;
                    Ok(buf)
                })
                .await
                .map_err(|e| DataProcessingError::InternalError(e.to_string()))??;

                cleaner.add_data(&data).await?;
                cursor = dirty_end;
            }
        }

        // Trailing clean gap after last dirty range
        let trailing_end = new_file_size.min(old_size);
        if cursor < trailing_end {
            let mut stream = download_session.download_stream_range(old_file_info, cursor..trailing_end, None)?;
            while let Some(chunk) = stream.next().await? {
                cleaner.add_data(&chunk).await?;
            }
        }

        cleaner.finish().await
    }

    /// Attempts the optimized delta upload path: pre-populate chunk metadata from
    /// local MDB shards instead of downloading from CAS. Only applies when:
    /// 1. The write is append-only (single dirty range starting at old file size)
    /// 2. The file's chunk info is available in local session/cache shards
    ///
    /// Returns `None` to signal fallback to the streaming path.
    async fn try_upload_file_delta_optimized(
        self: &Arc<Self>,
        download_session: &FileDownloadSession,
        old_file_info: &XetFileInfo,
        new_file_size: u64,
        dirty_ranges: &[(u64, u64)],
        staging_path: &Path,
    ) -> Result<Option<(XetFileInfo, DeduplicationMetrics)>> {
        let old_size = old_file_info.file_size();

        // Only optimize append-only writes: single dirty range starting at old_size.
        if dirty_ranges.len() != 1 || dirty_ranges[0].0 != old_size {
            return Ok(None);
        }

        // Resolve the old file hash.
        let old_file_hash = match old_file_info.merkle_hash() {
            Ok(h) => h,
            Err(_) => return Ok(None),
        };

        // Look up chunk-level reconstruction info from local shards.
        let chunk_info = match self.shard_interface.get_file_chunk_info(&old_file_hash).await? {
            Some(ci) => ci,
            None => return Ok(None),
        };

        if chunk_info.chunks.is_empty() {
            return Ok(None);
        }

        info!(
            "Delta upload: using optimized path, pre-populating {} chunks ({} bytes), \
             re-chunking last chunk ({} bytes) + {} bytes new data",
            chunk_info.chunks.len() - 1,
            chunk_info.total_size,
            chunk_info.chunks.last().map(|(_, s)| *s).unwrap_or(0),
            new_file_size - old_size,
        );

        let mut cleaner = self.start_clean(None, Some(new_file_size), None).await;

        // Pre-populate all chunks EXCEPT the last one. The last chunk's CDC boundary
        // was forced at EOF and will shift when new data is appended.
        let prefix_chunks = &chunk_info.chunks[..chunk_info.chunks.len() - 1];

        // Build prefix segments: we need to adjust segments to exclude the last chunk.
        // The simplest approach: recompute segments for the prefix chunks only.
        let prefix_chunk_count = prefix_chunks.len() as u32;
        let prefix_segments: Vec<_> = chunk_info
            .segments
            .iter()
            .filter_map(|seg| {
                if seg.chunk_index_start >= prefix_chunk_count {
                    // This segment is entirely in the last chunk or beyond.
                    None
                } else if seg.chunk_index_end > prefix_chunk_count {
                    // This segment spans the boundary: truncate it.
                    let truncated_end = prefix_chunk_count;
                    let truncated_bytes: u32 = prefix_chunks[seg.chunk_index_start as usize..]
                        .iter()
                        .map(|(_, s)| *s as u32)
                        .sum();
                    Some(FileDataSequenceEntry {
                        cas_hash: seg.cas_hash,
                        cas_flags: seg.cas_flags,
                        unpacked_segment_bytes: truncated_bytes,
                        chunk_index_start: seg.chunk_index_start,
                        chunk_index_end: truncated_end,
                    })
                } else {
                    Some(seg.clone())
                }
            })
            .collect();

        if !prefix_chunks.is_empty() {
            cleaner.add_pre_resolved_prefix(prefix_chunks, &prefix_segments).await?;
        }

        // Download only the last chunk (~64-128KB) for CDC boundary re-alignment.
        let last_chunk_size = chunk_info.chunks.last().map(|(_, s)| *s).unwrap_or(0);
        let seam_offset = chunk_info.total_size - last_chunk_size;

        if last_chunk_size > 0 {
            let mut stream =
                download_session.download_stream_range(old_file_info, seam_offset..chunk_info.total_size, None)?;
            while let Some(data) = stream.next().await? {
                cleaner.add_data(&data).await?;
            }
        }

        // Read new appended data from the staging file.
        let (dirty_start, dirty_end) = dirty_ranges[0];
        if dirty_start < dirty_end {
            let path = staging_path.to_owned();
            let offset = dirty_start;
            let len = (dirty_end - dirty_start) as usize;
            let data = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
                let mut f = File::open(&path)?;
                f.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; len];
                f.read_exact(&mut buf)?;
                Ok(buf)
            })
            .await
            .map_err(|e| DataProcessingError::InternalError(e.to_string()))??;

            cleaner.add_data(&data).await?;
        }

        Ok(Some(cleaner.finish().await?))
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

        // Serialize the object; this can be relatively expensive, so run it on a compute thread.
        // XORBs are sent without footer - the server/client reconstructs it from chunk data.
        let compression_scheme = self.config.data_config.compression;
        let cas_object = XetRuntime::current()
            .spawn_blocking(move || SerializedCasObject::from_xorb(xorb, compression_scheme, false))
            .await??;

        let session = self.clone();
        let upload_permit = self.client.acquire_upload_permit().await?;
        let cas_prefix = session.config.data_config.prefix.clone();
        let completion_tracker = self.completion_tracker.clone();
        let xorb_hash = cas_object.hash;
        let raw_num_bytes = cas_object.raw_num_bytes;
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
                    .upload_xorb(&cas_prefix, cas_object, Some(progress_callback), upload_permit)
                    .await?;

                // Register that the xorb has been uploaded.
                session.completion_tracker.register_xorb_upload_completion(xorb_hash).await;

                // Record the number of bytes uploaded.
                session.deduplication_metrics.lock().await.xorb_bytes_uploaded += n_bytes_transmitted;

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

    use crate::{FileDownloadSession, FileUploadSession, XetFileInfo};

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
            .start_clean(Some("test".into()), Some(read_data.len() as u64), None)
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
        let mut reader = File::open(pointer_path).unwrap();

        let mut input = String::new();
        reader.read_to_string(&mut input).unwrap();

        let xet_file = serde_json::from_str::<XetFileInfo>(&input).unwrap();

        let config = TranslatorConfig::local_config(cas_path).unwrap();
        let session = FileDownloadSession::new(config.into(), None, None).await.unwrap();

        session.download_file(&xet_file, output_path, None).await.unwrap();
    }

    use std::fs::{read, write};

    use tempfile::tempdir;

    use super::*;

    async fn upload_data(cas_path: &Path, data: &[u8]) -> XetFileInfo {
        let upload_session = FileUploadSession::new(TranslatorConfig::local_config(cas_path).unwrap().into(), None)
            .await
            .unwrap();

        let mut cleaner = upload_session
            .start_clean(Some("test".into()), Some(data.len() as u64), None)
            .await;
        cleaner.add_data(data).await.unwrap();
        let (xfi, _metrics) = cleaner.finish().await.unwrap();
        upload_session.finalize().await.unwrap();
        xfi
    }

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

    #[test]
    fn test_upload_file_delta_partial_modification() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");

                // Upload original file
                let original = b"AAAA BBBB CCCC DDDD";
                let old_xfi = upload_data(&cas_path, original).await;

                // Create modified staging file (change middle portion)
                let mut modified = original.to_vec();
                modified[5..9].copy_from_slice(b"XXXX");
                let staging_path = temp.path().join("staging");
                write(&staging_path, &modified).unwrap();

                // Delta upload: only the middle range [5,9) is dirty.
                // Share the client between download and upload sessions to avoid
                // opening the same LMDB environment twice.
                let config: Arc<TranslatorConfig> = TranslatorConfig::local_config(&cas_path).unwrap().into();
                let upload_session = FileUploadSession::new(config, None).await.unwrap();
                let download_session = FileDownloadSession::from_client(upload_session.client.clone(), None, None);

                let dirty_ranges = vec![(5u64, 9u64)];
                let (new_xfi, metrics) = upload_session
                    .upload_file_delta(&download_session, &old_xfi, modified.len() as u64, &dirty_ranges, &staging_path)
                    .await
                    .unwrap();

                assert_eq!(new_xfi.file_size(), modified.len() as u64);
                assert!(metrics.total_bytes > 0);

                drop(download_session);
                upload_session.finalize().await.unwrap();

                // Download the new file and verify contents
                let config2: Arc<TranslatorConfig> = TranslatorConfig::local_config(&cas_path).unwrap().into();
                let dl = FileDownloadSession::new(config2, None, None).await.unwrap();
                let out_path = temp.path().join("result.txt");
                dl.download_file(&new_xfi, &out_path, None).await.unwrap();
                assert_eq!(read(&out_path).unwrap(), modified);
            })
            .unwrap();
    }
}
