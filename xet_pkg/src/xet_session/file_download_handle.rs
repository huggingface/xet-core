//! XetFileDownload — handle for a background file-download task

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use xet_data::processing::{FileDownloadSession, XetFileInfo};
use xet_data::progress_tracking::{ItemProgressReport, UniqueID};

use super::task_runtime::{BackgroundTaskState, TaskRuntime, XetTaskState};
use crate::error::XetError;

/// Per-file download result returned by
/// [`XetFileDownloadGroup::finish`](crate::xet_session::XetFileDownloadGroup::finish).
#[derive(Clone, Debug)]
pub struct XetDownloadReport {
    /// Unique identifier for this download task.
    pub task_id: UniqueID,
    /// Local path where the file was written, if applicable.
    pub path: Option<PathBuf>,
    /// Xet file hash and size of the downloaded file.
    pub file_info: XetFileInfo,
    /// Per-file progress snapshot at the time of completion.
    pub progress: Option<ItemProgressReport>,
}

// ── XetFileDownloadInner ────────────────────────────────────────────────────

pub(super) struct XetFileDownloadInner {
    pub(super) task_id: UniqueID,
    pub(super) dest_path: PathBuf,
    pub(super) download_session: Arc<FileDownloadSession>,
    pub(super) state: tokio::sync::Mutex<BackgroundTaskState<XetDownloadReport>>,
}

// ── XetFileDownload (public wrapper) ────────────────────────────────────────

/// Handle for a background file-download task within an
/// [`XetFileDownloadGroup`](crate::xet_session::XetFileDownloadGroup).
///
/// Returned by
/// [`XetFileDownloadGroup::download_file_to_path`](crate::xet_session::XetFileDownloadGroup::download_file_to_path).
/// Use [`finish`](Self::finish) to wait for completion or
/// [`result`](Self::result) to poll without blocking.
pub struct XetFileDownload {
    pub(super) inner: Arc<XetFileDownloadInner>,
    pub(super) task_runtime: Arc<TaskRuntime>,
}

impl XetFileDownload {
    pub fn task_id(&self) -> UniqueID {
        self.inner.task_id
    }

    pub fn file_path(&self) -> PathBuf {
        self.inner.dest_path.clone()
    }

    pub fn progress(&self) -> Option<ItemProgressReport> {
        self.inner.download_session.item_report(self.inner.task_id)
    }

    pub fn status(&self) -> Result<XetTaskState, XetError> {
        self.task_runtime.status_from_background_task(&self.inner.state)
    }

    pub fn result(&self) -> Option<Result<XetDownloadReport, XetError>> {
        self.task_runtime.background_result(&self.inner.state)
    }

    /// Wait for the download to complete and return the report.
    ///
    /// Idempotent: returns the cached result if already resolved.
    pub async fn finish(&self) -> Result<XetDownloadReport, XetError> {
        let inner = self.inner.clone();
        self.task_runtime
            .bridge_async_finalizing("download_finish", true, async move { inner.state.lock().await.finish().await })
            .await
    }

    /// Blocking version of [`finish`](Self::finish).
    pub fn finish_blocking(&self) -> Result<XetDownloadReport, XetError> {
        let inner = self.inner.clone();
        self.task_runtime
            .bridge_sync_finalizing("download_finish", true, async move { inner.state.lock().await.finish().await })
    }

    pub fn cancel(&self) {
        self.task_runtime.cancel_background_task();
    }
}

impl fmt::Debug for XetFileDownload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("XetFileDownload")
            .field("task_id", &self.inner.task_id)
            .finish_non_exhaustive()
    }
}
