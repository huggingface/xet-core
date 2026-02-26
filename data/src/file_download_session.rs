use std::borrow::Cow;
use std::io::Write;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use cas_client::Client;
use cas_types::FileRange;
use file_reconstruction::{DownloadStream, FileReconstructor};
use progress_tracking::TrackingProgressUpdater;
use progress_tracking::download_tracking::DownloadProgressTracker;
use tracing::instrument;
use ulid::Ulid;
use xet_runtime::XetRuntime;

use crate::configurations::TranslatorConfig;
use crate::errors::*;
use crate::remote_client_interface::create_remote_client;
use crate::{XetFileInfo, prometheus_metrics};

/// Manages the downloading of files from CAS storage.
///
/// This struct parallels `FileUploadSession` for the download path. It holds the
/// CAS client, a shared progress tracker for all downloads in the session, and
/// gates concurrent downloads with a semaphore.
pub struct FileDownloadSession {
    client: Arc<dyn Client>,
    progress_tracker: Option<Arc<DownloadProgressTracker>>,
}

impl FileDownloadSession {
    pub async fn new(
        config: Arc<TranslatorConfig>,
        progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
    ) -> Result<Arc<Self>> {
        let session_id = config
            .session_id
            .as_ref()
            .map(Cow::Borrowed)
            .unwrap_or_else(|| Cow::Owned(Ulid::new().to_string()));

        let client = create_remote_client(&config, &session_id, false).await?;

        let progress_tracker = progress_updater.map(DownloadProgressTracker::new);

        Ok(Arc::new(Self {
            client,
            progress_tracker,
        }))
    }

    /// Creates a new download session from an existing CAS client.
    ///
    /// This is useful for tests or contexts where a client has already been created
    /// outside of the normal config-based flow.
    pub fn from_client(
        client: Arc<dyn Client>,
        progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
    ) -> Arc<Self> {
        let progress_tracker = progress_updater.map(DownloadProgressTracker::new);
        Arc::new(Self {
            client,
            progress_tracker,
        })
    }

    /// Downloads a complete file to the given path.
    ///
    /// If `tracking_id` is provided, it is used as the progress item name;
    /// otherwise the write path is used.
    #[instrument(skip_all, name = "FileDownloadSession::download_file", fields(hash = file_info.hash()))]
    pub async fn download_file(&self, file_info: &XetFileInfo, write_path: &Path, tracking_id: Ulid) -> Result<u64> {
        let semaphore = XetRuntime::current().common().file_download_semaphore.clone();
        let _permit = semaphore.acquire().await?;

        let reconstructor = self.setup_reconstructor(file_info, None, tracking_id, Some(write_path))?;
        let n_bytes = reconstructor.reconstruct_to_file(write_path, None).await?;
        prometheus_metrics::FILTER_BYTES_SMUDGED.inc_by(n_bytes);

        Ok(n_bytes)
    }

    /// Downloads a byte range of a file and writes it to the provided writer.
    ///
    /// The provided `source_range` is interpreted against the original file; output
    /// starts at the writer's current position.
    ///
    /// This path does not acquire the session-level file download semaphore.
    #[instrument(skip_all, name = "FileDownloadSession::download_to_writer",
        fields(hash = file_info.hash(), range_start = source_range.start, range_end = source_range.end))]
    pub async fn download_to_writer<W: Write + Send + 'static>(
        &self,
        file_info: &XetFileInfo,
        source_range: Range<u64>,
        writer: W,
        tracking_id: Ulid,
    ) -> Result<u64> {
        let range = FileRange::new(source_range.start, source_range.end);
        let reconstructor = self.setup_reconstructor(file_info, Some(range), tracking_id, None)?;
        let n_bytes = reconstructor.reconstruct_to_writer(writer).await?;
        prometheus_metrics::FILTER_BYTES_SMUDGED.inc_by(n_bytes);

        Ok(n_bytes)
    }

    /// Downloads a complete file to the given path, using a caller-provided progress updater
    /// instead of the session's shared progress tracker.
    ///
    /// This is intended for legacy callers that manage per-file progress updaters externally.
    #[instrument(skip_all, name = "FileDownloadSession::download_file_with_updater", fields(hash = file_info.hash()))]
    pub async fn download_file_with_updater(
        self: &Arc<Self>,
        file_info: &XetFileInfo,
        write_path: &Path,
        progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
    ) -> Result<u64> {
        let semaphore = XetRuntime::current().common().file_download_semaphore.clone();
        let _permit = semaphore.acquire().await?;

        let tracking_name = self.tracker_name(Some(write_path));

        let per_file_tracker = progress_updater.map(|updater| {
            let tracker = DownloadProgressTracker::new(updater);
            let task = tracker.new_download_task(Ulid::new(), tracking_name);
            task.update_item_size(file_info.file_size(), true);
            task
        });

        let file_id = file_info.merkle_hash()?;
        let mut reconstructor = FileReconstructor::new(&self.client, file_id);

        if let Some(tracker) = per_file_tracker {
            reconstructor = reconstructor.with_progress_updater(tracker);
        }

        let n_bytes = reconstructor.reconstruct_to_file(write_path, None).await?;
        prometheus_metrics::FILTER_BYTES_SMUDGED.inc_by(n_bytes);

        Ok(n_bytes)
    }

    /// Creates a streaming download of a file.
    ///
    /// Returns a [`DownloadStream`] that yields data chunks as the file is
    /// reconstructed. Reconstruction starts lazily on first
    /// [`DownloadStream::next`] / [`DownloadStream::blocking_next`] call
    /// (or when `start()` is called explicitly).
    ///
    /// This path does not acquire the session-level file download semaphore.
    #[instrument(skip_all, name = "FileDownloadSession::download_stream", fields(hash = file_info.hash()))]
    pub fn download_stream(&self, file_info: &XetFileInfo, tracking_id: Ulid) -> Result<DownloadStream> {
        let reconstructor = self.setup_reconstructor(file_info, None, tracking_id, None)?;
        Ok(reconstructor.reconstruct_to_stream())
    }

    fn tracker_name(&self, write_path: Option<&Path>) -> Arc<str> {
        write_path
            .map(|path| Arc::from(path.to_string_lossy().as_ref()))
            .unwrap_or_else(|| Arc::from(""))
    }

    /// Common setup: builds a `FileReconstructor` with the given options.
    fn setup_reconstructor(
        &self,
        file_info: &XetFileInfo,
        range: Option<FileRange>,
        tracking_id: Ulid,
        write_path: Option<&Path>,
    ) -> Result<FileReconstructor> {
        let file_id = file_info.merkle_hash()?;
        let tracking_name = self.tracker_name(write_path);
        let task_updater = self.progress_tracker.as_ref().map(|tracker| {
            let task = tracker.new_download_task(tracking_id, tracking_name);
            let size = range
                .map(|r| r.end.saturating_sub(r.start))
                .unwrap_or_else(|| file_info.file_size());
            task.update_item_size(size, true);
            task
        });

        let mut reconstructor = FileReconstructor::new(&self.client, file_id);

        if let Some(range) = range {
            reconstructor = reconstructor.with_byte_range(range);
        }

        if let Some(tracker) = task_updater {
            reconstructor = reconstructor.with_progress_updater(tracker);
        }

        Ok(reconstructor)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{read, write};
    use std::io::{Seek, SeekFrom};
    use std::sync::{Arc, OnceLock};

    use tempfile::tempdir;
    use xet_runtime::XetRuntime;

    use super::*;
    use crate::configurations::TranslatorConfig;
    use crate::{FileUploadSession, XetFileInfo};

    fn get_threadpool() -> Arc<XetRuntime> {
        static THREADPOOL: OnceLock<Arc<XetRuntime>> = OnceLock::new();
        THREADPOOL
            .get_or_init(|| XetRuntime::new().expect("Error starting multithreaded runtime."))
            .clone()
    }

    async fn upload_data(cas_path: &Path, data: &[u8]) -> XetFileInfo {
        let upload_session = FileUploadSession::new(TranslatorConfig::local_config(cas_path).unwrap().into(), None)
            .await
            .unwrap();

        let mut cleaner = upload_session
            .start_clean(Some("test".into()), data.len() as u64, None, Ulid::new())
            .await;
        cleaner.add_data(data).await.unwrap();
        let (xfi, _metrics) = cleaner.finish().await.unwrap();
        upload_session.finalize().await.unwrap();
        xfi
    }

    #[test]
    fn test_download_file() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"Hello, download session!";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                let out_path = temp.path().join("output.txt");
                let n_bytes = session.download_file(&xfi, &out_path, Ulid::new()).await.unwrap();

                assert_eq!(n_bytes, original_data.len() as u64);
                assert_eq!(read(&out_path).unwrap(), original_data);
            })
            .unwrap();
    }

    #[test]
    fn test_download_file_creates_parent_dirs() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"nested directory test";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                let out_path = temp.path().join("deep").join("nested").join("dir").join("output.txt");
                assert!(!out_path.parent().unwrap().exists());

                session.download_file(&xfi, &out_path, Ulid::new()).await.unwrap();

                assert_eq!(read(&out_path).unwrap(), original_data);
            })
            .unwrap();
    }

    #[test]
    fn test_download_file_with_tracking_id() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"tracking id test";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                let out_path = temp.path().join("tracked.txt");
                let n_bytes = session.download_file(&xfi, &out_path, Ulid::new()).await.unwrap();

                assert_eq!(n_bytes, original_data.len() as u64);
                assert_eq!(read(&out_path).unwrap(), original_data);
            })
            .unwrap();
    }

    #[test]
    fn test_download_to_writer() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"0123456789abcdef";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                let out_path = temp.path().join("partial_writer.txt");
                write(&out_path, vec![0u8; original_data.len()]).unwrap();

                let mut file = std::fs::OpenOptions::new().write(true).open(&out_path).unwrap();
                file.seek(SeekFrom::Start(4)).unwrap();

                let n_bytes = session.download_to_writer(&xfi, 4..12, file, Ulid::new()).await.unwrap();

                assert_eq!(n_bytes, 8);
                let result = read(&out_path).unwrap();
                assert_eq!(&result[4..12], &original_data[4..12]);
            })
            .unwrap();
    }

    #[test]
    fn test_download_to_writer_parallel_partitioned_file() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"abcdefghijklmnopqrstuvwxyz0123456789";

                let xfi = upload_data(&cas_path, original_data).await;
                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                let out_path = temp.path().join("partitioned.txt");
                write(&out_path, vec![0u8; original_data.len()]).unwrap();

                let n_parts = 5u64;
                let total = original_data.len() as u64;
                let mut tasks = Vec::new();

                for idx in 0..n_parts {
                    let start = (idx * total) / n_parts;
                    let end = ((idx + 1) * total) / n_parts;
                    if start == end {
                        continue;
                    }

                    let session = session.clone();
                    let xfi = xfi.clone();
                    let out_path = out_path.clone();
                    tasks.push(tokio::spawn(async move {
                        let mut writer = std::fs::OpenOptions::new().write(true).open(out_path).unwrap();
                        writer.seek(SeekFrom::Start(start)).unwrap();
                        session.download_to_writer(&xfi, start..end, writer, Ulid::new()).await
                    }));
                }

                for task in tasks {
                    task.await.unwrap().unwrap();
                }

                let result = read(&out_path).unwrap();
                assert_eq!(result, original_data);
            })
            .unwrap();
    }

    #[test]
    fn test_download_multiple_files_concurrent() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");

                let data_a = b"File A content for concurrent test";
                let data_b = b"File B content for concurrent test - different";

                let xfi_a = upload_data(&cas_path, data_a).await;
                let xfi_b = upload_data(&cas_path, data_b).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                let out_a = temp.path().join("out_a.txt");
                let out_b = temp.path().join("out_b.txt");

                let session_a = session.clone();
                let xfi_a_clone = xfi_a.clone();
                let out_a_clone = out_a.clone();
                let task_a =
                    tokio::spawn(async move { session_a.download_file(&xfi_a_clone, &out_a_clone, Ulid::new()).await });

                let session_b = session.clone();
                let xfi_b_clone = xfi_b.clone();
                let out_b_clone = out_b.clone();
                let task_b =
                    tokio::spawn(async move { session_b.download_file(&xfi_b_clone, &out_b_clone, Ulid::new()).await });

                task_a.await.unwrap().unwrap();
                task_b.await.unwrap().unwrap();

                assert_eq!(read(&out_a).unwrap(), data_a);
                assert_eq!(read(&out_b).unwrap(), data_b);
            })
            .unwrap();
    }

    // ==================== Download Stream Tests ====================

    #[test]
    fn test_download_stream_async() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"Hello, streaming download!";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                let mut stream = session.download_stream(&xfi, Ulid::new()).unwrap();

                let mut collected = Vec::new();
                while let Some(chunk) = stream.next().await.unwrap() {
                    collected.extend_from_slice(&chunk);
                }

                assert_eq!(collected, original_data);
            })
            .unwrap();
    }

    #[test]
    fn test_download_stream_blocking() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"Blocking stream test data";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                let stream = session.download_stream(&xfi, Ulid::new()).unwrap();

                let collected = tokio::task::spawn_blocking(move || {
                    let mut stream = stream;
                    let mut buf = Vec::new();
                    while let Some(chunk) = stream.blocking_next().unwrap() {
                        buf.extend_from_slice(&chunk);
                    }
                    buf
                })
                .await
                .unwrap();

                assert_eq!(collected, original_data);
            })
            .unwrap();
    }

    #[test]
    fn test_download_stream_auto_start_blocking() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"Auto-start stream test";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                let stream = session.download_stream(&xfi, Ulid::new()).unwrap();

                let collected = tokio::task::spawn_blocking(move || {
                    let mut stream = stream;
                    let mut buf = Vec::new();
                    while let Some(chunk) = stream.blocking_next().unwrap() {
                        buf.extend_from_slice(&chunk);
                    }
                    buf
                })
                .await
                .unwrap();

                assert_eq!(collected, original_data);
            })
            .unwrap();
    }

    #[test]
    fn test_download_stream_returns_none_after_finish() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"Extra none calls";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                let mut stream = session.download_stream(&xfi, Ulid::new()).unwrap();

                // Drain all data
                while stream.next().await.unwrap().is_some() {}

                // Subsequent calls should return Ok(None)
                assert!(stream.next().await.unwrap().is_none());
                assert!(stream.next().await.unwrap().is_none());
            })
            .unwrap();
    }

    #[test]
    fn test_download_stream_with_tracking_id() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"Stream with tracking";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                let mut stream = session.download_stream(&xfi, Ulid::new()).unwrap();

                let mut collected = Vec::new();
                while let Some(chunk) = stream.next().await.unwrap() {
                    collected.extend_from_slice(&chunk);
                }

                assert_eq!(collected, original_data);
            })
            .unwrap();
    }

    #[test]
    fn test_download_stream_multiple_concurrent() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");

                let data_a = b"Stream A for concurrent download";
                let data_b = b"Stream B for concurrent download - different";

                let xfi_a = upload_data(&cas_path, data_a).await;
                let xfi_b = upload_data(&cas_path, data_b).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                let mut stream_a = session.download_stream(&xfi_a, Ulid::new()).unwrap();
                let mut stream_b = session.download_stream(&xfi_b, Ulid::new()).unwrap();

                let task_a = tokio::spawn(async move {
                    let mut buf = Vec::new();
                    while let Some(chunk) = stream_a.next().await.unwrap() {
                        buf.extend_from_slice(&chunk);
                    }
                    buf
                });

                let task_b = tokio::spawn(async move {
                    let mut buf = Vec::new();
                    while let Some(chunk) = stream_b.next().await.unwrap() {
                        buf.extend_from_slice(&chunk);
                    }
                    buf
                });

                let result_a = task_a.await.unwrap();
                let result_b = task_b.await.unwrap();

                assert_eq!(result_a, data_a);
                assert_eq!(result_b, data_b);
            })
            .unwrap();
    }

    #[test]
    fn test_drop_stream_never_started_then_download() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"Drop-before-start cleanup test";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                // Create a stream but never start it, then drop.
                let stream = session.download_stream(&xfi, Ulid::new()).unwrap();
                drop(stream);

                // A subsequent file download must succeed, proving no resources leaked.
                let out_path = temp.path().join("after_drop.txt");
                session.download_file(&xfi, &out_path, Ulid::new()).await.unwrap();
                assert_eq!(read(&out_path).unwrap(), original_data);
            })
            .unwrap();
    }

    #[test]
    fn test_drop_stream_started_no_reads_then_download() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"Drop-after-start-no-reads cleanup test";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                // Create the stream, then drop without reading any chunks.
                let stream = session.download_stream(&xfi, Ulid::new()).unwrap();
                drop(stream);

                // Yield to let the runtime process the cancellation.
                tokio::task::yield_now().await;

                // A subsequent download must succeed.
                let out_path = temp.path().join("after_drop.txt");
                session.download_file(&xfi, &out_path, Ulid::new()).await.unwrap();
                assert_eq!(read(&out_path).unwrap(), original_data);
            })
            .unwrap();
    }

    #[test]
    fn test_drop_stream_mid_read_then_download() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"Drop-mid-read cleanup test data";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                // Read one chunk, then drop mid-stream.
                let mut stream = session.download_stream(&xfi, Ulid::new()).unwrap();
                let _chunk = stream.next().await;
                drop(stream);

                // Yield to let the runtime process the cancellation.
                tokio::task::yield_now().await;

                // A subsequent download must succeed.
                let out_path = temp.path().join("after_drop.txt");
                session.download_file(&xfi, &out_path, Ulid::new()).await.unwrap();
                assert_eq!(read(&out_path).unwrap(), original_data);
            })
            .unwrap();
    }

    #[test]
    fn test_drop_stream_multiple_cycles_then_download() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"Multi-cycle drop cleanup test";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                // Repeatedly create, start, optionally read, and drop streams.
                for i in 0..5u32 {
                    let mut stream = session.download_stream(&xfi, Ulid::new()).unwrap();
                    if i % 3 == 0 {
                        let _ = stream.next().await;
                    }
                    drop(stream);
                    tokio::task::yield_now().await;
                }

                // After many create/drop cycles, a full download must still work.
                let out_path = temp.path().join("after_cycles.txt");
                session.download_file(&xfi, &out_path, Ulid::new()).await.unwrap();
                assert_eq!(read(&out_path).unwrap(), original_data);
            })
            .unwrap();
    }

    #[test]
    fn test_drop_stream_blocking_mid_read_then_download() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"Blocking drop cleanup test data";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                // Read one chunk via blocking next() in a spawn_blocking, then drop.
                let stream = session.download_stream(&xfi, Ulid::new()).unwrap();

                tokio::task::spawn_blocking(move || {
                    let mut stream = stream;
                    let _chunk = stream.blocking_next().unwrap();
                    // stream is dropped here at the end of the closure
                })
                .await
                .unwrap();

                // Yield to let the runtime process the cancellation.
                tokio::task::yield_now().await;

                // A subsequent download must succeed.
                let out_path = temp.path().join("after_blocking_drop.txt");
                session.download_file(&xfi, &out_path, Ulid::new()).await.unwrap();
                assert_eq!(read(&out_path).unwrap(), original_data);
            })
            .unwrap();
    }

    #[test]
    fn test_cancel_stream_before_start_returns_none() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"Cancel-before-start stream test";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                let mut stream = session.download_stream(&xfi, Ulid::new()).unwrap();
                stream.cancel();
                assert!(stream.next().await.unwrap().is_none());
                assert!(stream.next().await.unwrap().is_none());
            })
            .unwrap();
    }

    #[test]
    fn test_cancel_stream_after_first_chunk_returns_none() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"Cancel-after-first-chunk stream test data";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into(), None).await.unwrap();

                let mut stream = session.download_stream(&xfi, Ulid::new()).unwrap();
                let _ = stream.next().await.unwrap();
                stream.cancel();
                assert!(stream.next().await.unwrap().is_none());
                assert!(stream.next().await.unwrap().is_none());

                let out_path = temp.path().join("after_cancel.txt");
                session.download_file(&xfi, &out_path, Ulid::new()).await.unwrap();
                assert_eq!(read(&out_path).unwrap(), original_data);
            })
            .unwrap();
    }
}
