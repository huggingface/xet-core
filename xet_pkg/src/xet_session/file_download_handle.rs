//! XetFileDownload — handle for a background file-download task

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::task::JoinHandle;
use xet_data::DataError;
use xet_data::processing::{FileDownloadSession, XetFileInfo};
use xet_data::progress_tracking::{ItemProgressReport, UniqueID};

use super::task_runtime::{BackgroundTaskResolution, BackgroundTaskState, TaskRuntime, XetTaskState};
use crate::error::XetError;

type DownloadJoinHandle = JoinHandle<Result<u64, DataError>>;

/// Per-file result returned by [`XetDownloadGroup::finish`](crate::xet_session::XetDownloadGroup::finish).
#[derive(Clone, Debug)]
pub struct DownloadedFile {
    /// Local path where the file was written.
    pub dest_path: PathBuf,
    /// Xet file hash and size of the downloaded file.
    pub file_info: XetFileInfo,
}

/// Per-file result type returned by [`XetDownloadGroup::finish`](crate::xet_session::XetDownloadGroup::finish).
///
/// The `Arc` lets the same value be stored in both the `finish()` return map
/// and the per-task [`XetFileDownload`] without requiring the inner `Result`
/// to be `Clone`.
pub type DownloadResult = Arc<Result<DownloadedFile, XetError>>;

#[derive(Clone)]
pub struct XetFileDownload {
    /// Id of the task, can be used to retrieve per-task progress and result.
    pub task_id: UniqueID,
    pub(super) dest_path: PathBuf,
    pub(super) file_info: XetFileInfo,
    pub(super) download_session: Arc<FileDownloadSession>,
    pub(super) task_runtime: Arc<TaskRuntime>,
    pub(super) state: Arc<tokio::sync::Mutex<BackgroundTaskState<DownloadedFile, DownloadJoinHandle>>>,
}

impl XetFileDownload {
    pub fn task_id(&self) -> UniqueID {
        self.task_id
    }

    pub fn file_path(&self) -> PathBuf {
        self.dest_path.clone()
    }

    pub fn progress(&self) -> Option<ItemProgressReport> {
        self.download_session.item_report(self.task_id)
    }

    pub fn status(&self) -> Result<XetTaskState, XetError> {
        self.task_runtime.status_from_background_task(&self.state)
    }

    pub fn result(&self) -> Option<DownloadResult> {
        self.task_runtime.background_result(&self.state).map(Arc::new)
    }

    pub async fn finish(&self) -> Result<DownloadedFile, XetError> {
        let join_handle = match self
            .task_runtime
            .begin_background_task_resolution(&self.state, "download task already resolved")
            .await?
        {
            BackgroundTaskResolution::Cached(value) => return Ok(value),
            BackgroundTaskResolution::JoinHandle(handle) => handle,
        };

        let file_info = self.file_info.clone();
        let dest_path = self.dest_path.clone();
        let result = self
            .task_runtime
            .bridge_async_finalizing("download_handle_finish", async move {
                match join_handle.await {
                    Ok(Ok(n_bytes)) => Ok(DownloadedFile {
                        dest_path,
                        file_info: XetFileInfo {
                            hash: file_info.hash,
                            file_size: Some(n_bytes),
                            sha256: file_info.sha256,
                        },
                    }),
                    Ok(Err(e)) => Err(XetError::TaskError(e.to_string())),
                    Err(e) => Err(XetError::TaskError(e.to_string())),
                }
            })
            .await;

        self.task_runtime.finish_background_task_resolution(&self.state, &result).await;

        match result {
            Ok(value) => Ok(value),
            Err(_) => Err(self
                .task_runtime
                .background_result(&self.state)
                .and_then(Result::err)
                .unwrap_or_else(|| XetError::TaskError("download task failed".to_string()))),
        }
    }

    pub fn finish_blocking(&self) -> Result<DownloadedFile, XetError> {
        let join_handle = match self
            .task_runtime
            .begin_background_task_resolution_blocking(&self.state, "download task already resolved")?
        {
            BackgroundTaskResolution::Cached(value) => return Ok(value),
            BackgroundTaskResolution::JoinHandle(handle) => handle,
        };

        let file_info = self.file_info.clone();
        let dest_path = self.dest_path.clone();
        let result = self
            .task_runtime
            .bridge_sync_finalizing("download_handle_finish_blocking", async move {
                match join_handle.await {
                    Ok(Ok(n_bytes)) => Ok(DownloadedFile {
                        dest_path,
                        file_info: XetFileInfo {
                            hash: file_info.hash,
                            file_size: Some(n_bytes),
                            sha256: file_info.sha256,
                        },
                    }),
                    Ok(Err(e)) => Err(XetError::TaskError(e.to_string())),
                    Err(e) => Err(XetError::TaskError(e.to_string())),
                }
            });

        self.task_runtime
            .finish_background_task_resolution_blocking(&self.state, &result);

        match result {
            Ok(value) => Ok(value),
            Err(_) => Err(self
                .task_runtime
                .background_result(&self.state)
                .and_then(Result::err)
                .unwrap_or_else(|| XetError::TaskError("download task failed".to_string()))),
        }
    }

    pub fn cancel(&self) {
        self.task_runtime
            .cancel_background_task(&self.state, "download task cancelled by user", |join_handle| {
                join_handle.abort();
            });
    }
}

impl fmt::Debug for XetFileDownload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("XetFileDownload")
            .field("task_id", &self.task_id)
            .finish_non_exhaustive()
    }
}
