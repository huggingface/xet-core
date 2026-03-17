use std::borrow::Cow;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::task::JoinHandle;
use tracing::instrument;
use xet_client::cas_client::Client;
use xet_client::cas_types::FileRange;
use xet_runtime::core::XetRuntime;

use super::configurations::TranslatorConfig;
use super::errors::*;
use super::remote_client_interface::create_remote_client;
use super::{XetFileInfo, prometheus_metrics};
use crate::file_reconstruction::{DownloadStream, FileReconstructor};
use crate::progress_tracking::{GroupProgress, ItemProgressUpdater, UniqueID};

/// Manages the downloading of files from CAS storage.
///
/// This struct parallels `FileUploadSession` for the download path. It holds the
/// CAS client and a shared progress group for all downloads in the session.
pub struct FileDownloadSession {
    client: Arc<dyn Client>,
    progress: Arc<GroupProgress>,
    finalized: AtomicBool,
}

impl FileDownloadSession {
    pub async fn new(config: Arc<TranslatorConfig>) -> Result<Arc<Self>> {
        let session_id = config
            .session
            .session_id
            .as_ref()
            .map(Cow::Borrowed)
            .unwrap_or_else(|| Cow::Owned(UniqueID::new().to_string()));

        let client = create_remote_client(&config, &session_id, false).await?;
        let progress = GroupProgress::new();

        Ok(Arc::new(Self {
            client,
            progress,
            finalized: AtomicBool::new(false),
        }))
    }

    pub fn from_client(client: Arc<dyn Client>) -> Arc<Self> {
        let progress = GroupProgress::new();
        Arc::new(Self {
            client,
            progress,
            finalized: AtomicBool::new(false),
        })
    }

    pub fn report(&self) -> crate::progress_tracking::GroupProgressReport {
        self.progress.report()
    }

    pub fn item_report(&self, id: UniqueID) -> Option<crate::progress_tracking::ItemProgressReport> {
        self.progress.item_report(id)
    }

    pub fn item_reports(&self) -> std::collections::HashMap<UniqueID, crate::progress_tracking::ItemProgressReport> {
        self.progress.item_reports()
    }

    /// Spawns a download task that writes `file_info` to `write_path`.
    ///
    /// Acquires a permit from the global download semaphore before starting.
    /// Returns the tracking ID and the join handle for the spawned task.
    pub async fn download_file_background(
        self: &Arc<Self>,
        file_info: XetFileInfo,
        write_path: PathBuf,
    ) -> Result<(UniqueID, JoinHandle<Result<u64>>)> {
        self.check_not_finalized()?;
        let id = UniqueID::new();
        let session = self.clone();
        let rt = XetRuntime::current();
        let semaphore = rt.common().file_download_semaphore.clone();
        let handle = rt.spawn(async move {
            let _permit = semaphore.acquire().await?;
            session.download_file_with_id(&file_info, &write_path, id).await
        });
        Ok((id, handle))
    }

    /// Downloads a complete file to the given path.
    #[instrument(skip_all, name = "FileDownloadSession::download_file", fields(hash = file_info.hash()))]
    pub async fn download_file(&self, file_info: &XetFileInfo, write_path: &Path) -> Result<(UniqueID, u64)> {
        self.check_not_finalized()?;
        let id = UniqueID::new();
        let n_bytes = self.download_file_with_id(file_info, write_path, id).await?;
        Ok((id, n_bytes))
    }

    async fn download_file_with_id(&self, file_info: &XetFileInfo, write_path: &Path, id: UniqueID) -> Result<u64> {
        let name = Arc::from(write_path.to_string_lossy().as_ref());
        let progress_updater = self.progress.new_item(id, name);
        let reconstructor = self.setup_reconstructor(file_info, None, Some(progress_updater))?;
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
    ) -> Result<(UniqueID, u64)> {
        self.check_not_finalized()?;
        let range = FileRange::new(source_range.start, source_range.end);
        let id = UniqueID::new();
        let name = Arc::from("");
        let progress_updater = self.progress.new_item(id, name);
        let reconstructor = self.setup_reconstructor(file_info, Some(range), Some(progress_updater))?;
        let n_bytes = reconstructor.reconstruct_to_writer(writer).await?;
        prometheus_metrics::FILTER_BYTES_SMUDGED.inc_by(n_bytes);

        Ok((id, n_bytes))
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
    pub async fn download_stream(&self, file_info: &XetFileInfo) -> Result<(UniqueID, DownloadStream)> {
        self.check_not_finalized()?;
        let id = UniqueID::new();
        let progress_updater = self.progress.new_item(id, "stream");
        let reconstructor = self.setup_reconstructor(file_info, None, Some(progress_updater))?;
        Ok((id, reconstructor.reconstruct_to_stream()))
    }

    fn check_not_finalized(&self) -> Result<()> {
        if self.finalized.load(Ordering::Acquire) {
            return Err(DataProcessingError::InvalidOperation("FileDownloadSession already finalized".to_string()));
        }
        Ok(())
    }

    /// Finalizes the session; in debug builds, asserts all items are complete.
    pub async fn finalize(&self) -> Result<()> {
        if self.finalized.swap(true, Ordering::AcqRel) {
            return Err(DataProcessingError::InvalidOperation("FileDownloadSession already finalized".to_string()));
        }
        #[cfg(debug_assertions)]
        self.progress.assert_complete();
        Ok(())
    }

    fn setup_reconstructor(
        &self,
        file_info: &XetFileInfo,
        range: Option<FileRange>,
        progress_updater: Option<Arc<ItemProgressUpdater>>,
    ) -> Result<FileReconstructor> {
        let file_id = file_info.merkle_hash()?;
        let effective_range = range.unwrap_or_else(|| FileRange::new(0, file_info.file_size()));
        let size = effective_range.end - effective_range.start;
        if let Some(ref updater) = progress_updater {
            updater.update_item_size(size, true);
        }
        let mut reconstructor = FileReconstructor::new(&self.client, file_id).with_byte_range(effective_range);
        if let Some(updater) = progress_updater {
            reconstructor = reconstructor.with_progress_updater(updater);
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
    use xet_runtime::core::XetRuntime;

    use super::*;
    use crate::processing::configurations::TranslatorConfig;
    use crate::processing::file_cleaner::Sha256Policy;
    use crate::processing::{FileUploadSession, XetFileInfo};

    fn get_threadpool() -> Arc<XetRuntime> {
        static THREADPOOL: OnceLock<Arc<XetRuntime>> = OnceLock::new();
        THREADPOOL
            .get_or_init(|| XetRuntime::new().expect("Error starting multithreaded runtime."))
            .clone()
    }

    async fn upload_data(cas_path: &Path, data: &[u8]) -> XetFileInfo {
        let upload_session = FileUploadSession::new(TranslatorConfig::local_config(cas_path).unwrap().into())
            .await
            .unwrap();

        let (_id, mut cleaner) = upload_session
            .start_clean(Some("test".into()), data.len() as u64, Sha256Policy::Compute)
            .unwrap();
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
                let session = FileDownloadSession::new(config.into()).await.unwrap();

                let out_path = temp.path().join("output.txt");
                let (_id, n_bytes) = session.download_file(&xfi, &out_path).await.unwrap();

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
                let session = FileDownloadSession::new(config.into()).await.unwrap();

                let out_path = temp.path().join("deep").join("nested").join("dir").join("output.txt");
                assert!(!out_path.parent().unwrap().exists());

                session.download_file(&xfi, &out_path).await.unwrap();

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
                let session = FileDownloadSession::new(config.into()).await.unwrap();

                let out_path = temp.path().join("partial_writer.txt");
                write(&out_path, vec![0u8; original_data.len()]).unwrap();

                let mut file = std::fs::OpenOptions::new().write(true).open(&out_path).unwrap();
                file.seek(SeekFrom::Start(4)).unwrap();

                let (_id, n_bytes) = session.download_to_writer(&xfi, 4..12, file).await.unwrap();

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
                let session = FileDownloadSession::new(config.into()).await.unwrap();

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
                        session.download_to_writer(&xfi, start..end, writer).await
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
                let session = FileDownloadSession::new(config.into()).await.unwrap();

                let out_a = temp.path().join("out_a.txt");
                let out_b = temp.path().join("out_b.txt");

                let session_a = session.clone();
                let xfi_a_clone = xfi_a.clone();
                let out_a_clone = out_a.clone();
                let task_a = tokio::spawn(async move { session_a.download_file(&xfi_a_clone, &out_a_clone).await });

                let session_b = session.clone();
                let xfi_b_clone = xfi_b.clone();
                let out_b_clone = out_b.clone();
                let task_b = tokio::spawn(async move { session_b.download_file(&xfi_b_clone, &out_b_clone).await });

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
                let session = FileDownloadSession::new(config.into()).await.unwrap();

                let (_id, mut stream) = session.download_stream(&xfi).await.unwrap();

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
                let session = FileDownloadSession::new(config.into()).await.unwrap();

                let (_id, stream) = session.download_stream(&xfi).await.unwrap();

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
                let session = FileDownloadSession::new(config.into()).await.unwrap();

                let (_id, mut stream) = session.download_stream(&xfi).await.unwrap();

                while stream.next().await.unwrap().is_some() {}

                // Subsequent calls should return Ok(None)
                assert!(stream.next().await.unwrap().is_none());
                assert!(stream.next().await.unwrap().is_none());
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
                let session = FileDownloadSession::new(config.into()).await.unwrap();

                let (_id_a, mut stream_a) = session.download_stream(&xfi_a).await.unwrap();
                let (_id_b, mut stream_b) = session.download_stream(&xfi_b).await.unwrap();

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
    fn test_drop_stream_without_reading() {
        let runtime = get_threadpool();
        runtime
            .clone()
            .external_run_async_task(async {
                let temp = tempdir().unwrap();
                let cas_path = temp.path().join("cas");
                let original_data = b"Drop-without-reading cleanup test";

                let xfi = upload_data(&cas_path, original_data).await;

                let config = TranslatorConfig::local_config(&cas_path).unwrap();
                let session = FileDownloadSession::new(config.into()).await.unwrap();

                let (_id, stream) = session.download_stream(&xfi).await.unwrap();
                drop(stream);
                tokio::task::yield_now().await;

                let out_path = temp.path().join("after_drop.txt");
                session.download_file(&xfi, &out_path).await.unwrap();
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
                let session = FileDownloadSession::new(config.into()).await.unwrap();

                for i in 0..5u32 {
                    let (_id, mut stream) = session.download_stream(&xfi).await.unwrap();
                    if i % 3 == 0 {
                        let _ = stream.next().await;
                    }
                    drop(stream);
                    tokio::task::yield_now().await;
                }

                let out_path = temp.path().join("after_cycles.txt");
                session.download_file(&xfi, &out_path).await.unwrap();
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
                let session = FileDownloadSession::new(config.into()).await.unwrap();

                let (_id, stream) = session.download_stream(&xfi).await.unwrap();

                tokio::task::spawn_blocking(move || {
                    let mut stream = stream;
                    let _chunk = stream.blocking_next().unwrap();
                })
                .await
                .unwrap();

                tokio::task::yield_now().await;

                let out_path = temp.path().join("after_blocking_drop.txt");
                session.download_file(&xfi, &out_path).await.unwrap();
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
                let session = FileDownloadSession::new(config.into()).await.unwrap();

                let (_id, mut stream) = session.download_stream(&xfi).await.unwrap();
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
                let session = FileDownloadSession::new(config.into()).await.unwrap();

                let (_id, mut stream) = session.download_stream(&xfi).await.unwrap();
                let _ = stream.next().await.unwrap();
                stream.cancel();
                assert!(stream.next().await.unwrap().is_none());
                assert!(stream.next().await.unwrap().is_none());

                let out_path = temp.path().join("after_cancel.txt");
                session.download_file(&xfi, &out_path).await.unwrap();
                assert_eq!(read(&out_path).unwrap(), original_data);
            })
            .unwrap();
    }
}
