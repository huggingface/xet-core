use std::borrow::Cow;
use std::io::Write;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use cas_client::Client;
use cas_types::FileRange;
use file_reconstruction::{DataOutput, FileReconstructor};
use progress_tracking::TrackingProgressUpdater;
use progress_tracking::aggregator::AggregatingProgressUpdater;
use progress_tracking::download_tracking::{DownloadProgressTracker, DownloadTaskUpdater};
use tracing::instrument;
use ulid::Ulid;
use xet_runtime::{XetRuntime, xet_config};

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
    pub async fn download_file(
        &self,
        file_info: &XetFileInfo,
        write_path: &Path,
        tracking_id: Option<&str>,
    ) -> Result<u64> {
        let output = DataOutput::write_in_file(write_path);
        let tracking_name = self.tracker_name(tracking_id, Some(write_path), None);
        let task_updater = self.progress_tracker.as_ref().map(|tracker| {
            let task = tracker.new_download_task(tracking_name);
            task.update_item_size(file_info.file_size(), true);
            task
        });

        self.run_download(file_info, output, None, task_updater).await
    }

    /// Downloads a byte range of a file and writes it to the provided writer.
    ///
    /// The provided `source_range` is interpreted against the original file; output
    /// starts at the writer's current position.
    #[instrument(skip_all, name = "FileDownloadSession::download_to_writer",
        fields(hash = file_info.hash(), range_start = source_range.start, range_end = source_range.end))]
    pub async fn download_to_writer<W: Write + Send + 'static>(
        &self,
        file_info: &XetFileInfo,
        source_range: Range<u64>,
        writer: W,
        tracking_name: Option<Arc<str>>,
    ) -> Result<u64> {
        let output = DataOutput::writer(writer);
        let range = FileRange::new(source_range.start, source_range.end);
        let tracking_name = self.tracker_name(None, None, tracking_name);
        let task_updater = self.progress_tracker.as_ref().map(|tracker| {
            let task = tracker.new_download_task(tracking_name);
            task.update_item_size(source_range.end.saturating_sub(source_range.start), true);
            task
        });

        self.run_download(file_info, output, Some(range), task_updater).await
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
        let output = DataOutput::write_in_file(write_path);
        let tracking_name = self.tracker_name(None, Some(write_path), None);

        // Wrap the progress updater in an aggregator to batch updates and reduce
        // callback overhead. Without this, each XORB chunk triggers a Rust→Python
        // callback that acquires the GIL, causing severe contention with many
        // concurrent file downloads.
        let (updater_for_tracker, aggregator) = match progress_updater {
            Some(updater) => {
                let flush_interval = xet_config().data.progress_update_interval;
                let sampling_window = xet_config().data.progress_update_speed_sampling_window;
                if !flush_interval.is_zero() {
                    let agg = AggregatingProgressUpdater::new(updater, flush_interval, sampling_window);
                    (Some(agg.clone() as Arc<dyn TrackingProgressUpdater>), Some(agg))
                } else {
                    (Some(updater), None)
                }
            },
            None => (None, None),
        };

        let per_file_tracker = updater_for_tracker.map(|updater| {
            let tracker = DownloadProgressTracker::new(updater);
            let task = tracker.new_download_task(tracking_name);
            task.update_item_size(file_info.file_size(), true);
            task
        });

        let result = self.run_download(file_info, output, None, per_file_tracker).await;

        // Finalize the aggregator to flush remaining updates and stop the background task.
        if let Some(agg) = aggregator {
            agg.finalize().await;
        }

        result
    }

    fn tracker_name(
        &self,
        tracking_id: Option<&str>,
        write_path: Option<&Path>,
        explicit_name: Option<Arc<str>>,
    ) -> Arc<str> {
        explicit_name
            .or_else(|| tracking_id.map(Arc::from))
            .or_else(|| write_path.map(|path| Arc::from(path.to_string_lossy().as_ref())))
            .unwrap_or_else(|| Arc::from(""))
    }

    /// Common download implementation: acquires the semaphore and runs the file reconstruction.
    async fn run_download(
        &self,
        file_info: &XetFileInfo,
        output: DataOutput,
        range: Option<FileRange>,
        progress_updater: Option<Arc<DownloadTaskUpdater>>,
    ) -> Result<u64> {
        let semaphore = XetRuntime::current().common().file_download_semaphore.clone();
        let _permit = semaphore.acquire().await?;

        let file_id = file_info.merkle_hash()?;
        let mut reconstructor = FileReconstructor::new(&self.client, file_id, output);

        if let Some(range) = range {
            reconstructor = reconstructor.with_byte_range(range);
        }

        if let Some(tracker) = progress_updater {
            reconstructor = reconstructor.with_progress_updater(tracker);
        }

        let n_bytes = reconstructor.run().await?;
        prometheus_metrics::FILTER_BYTES_SMUDGED.inc_by(n_bytes);

        Ok(n_bytes)
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

        let mut cleaner = upload_session.start_clean(Some("test".into()), data.len() as u64, None).await;
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
                let n_bytes = session.download_file(&xfi, &out_path, None).await.unwrap();

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

                session.download_file(&xfi, &out_path, None).await.unwrap();

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
                let n_bytes = session
                    .download_file(&xfi, &out_path, Some("my-custom-tracking-id"))
                    .await
                    .unwrap();

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

                let n_bytes = session
                    .download_to_writer(&xfi, 4..12, file, Some(Arc::from("partial-writer")))
                    .await
                    .unwrap();

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
                        session
                            .download_to_writer(&xfi, start..end, writer, Some(Arc::from(format!("part-{idx}"))))
                            .await
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
                    tokio::spawn(
                        async move { session_a.download_file(&xfi_a_clone, &out_a_clone, Some("file-a")).await },
                    );

                let session_b = session.clone();
                let xfi_b_clone = xfi_b.clone();
                let out_b_clone = out_b.clone();
                let task_b =
                    tokio::spawn(
                        async move { session_b.download_file(&xfi_b_clone, &out_b_clone, Some("file-b")).await },
                    );

                task_a.await.unwrap().unwrap();
                task_b.await.unwrap().unwrap();

                assert_eq!(read(&out_a).unwrap(), data_a);
                assert_eq!(read(&out_b).unwrap(), data_b);
            })
            .unwrap();
    }
}
