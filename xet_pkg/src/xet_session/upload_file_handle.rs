//! XetFileUpload — handle for a background file-upload task

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use tracing::info;
use xet_data::processing::FileUploadSession;
use xet_data::progress_tracking::{ItemProgressReport, UniqueID};

use super::task_runtime::{BackgroundTaskState, TaskRuntime, XetTaskState};
use super::upload_commit::XetFileMetadata;
use crate::error::XetError;

// ── XetFileUploadInner ──────────────────────────────────────────────────────

pub(super) struct XetFileUploadInner {
    pub(super) task_id: UniqueID,
    pub(super) file_path: Option<PathBuf>,
    pub(super) upload_session: Arc<FileUploadSession>,
    pub(super) state: tokio::sync::Mutex<BackgroundTaskState<XetFileMetadata>>,
}

// ── XetFileUpload (public wrapper) ──────────────────────────────────────────

/// Handle for a background file-upload task within an [`XetUploadCommit`].
///
/// Returned by [`XetUploadCommit::upload_from_path`] and
/// [`XetUploadCommit::upload_bytes`]. Use [`finalize_ingestion`](Self::finalize_ingestion)
/// to wait for ingestion completion.
///
/// Important: ingestion completion means the file has been chunked/deduplicated.
/// The file is not uploaded to CAS until [`XetUploadCommit::commit`] is called.
pub struct XetFileUpload {
    pub(super) inner: Arc<XetFileUploadInner>,
    pub(super) task_runtime: Arc<TaskRuntime>,
}

impl fmt::Debug for XetFileUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("XetFileUpload")
            .field("task_id", &self.inner.task_id)
            .finish_non_exhaustive()
    }
}

impl XetFileUpload {
    /// Unique identifier for this upload task, usable for progress lookups.
    pub fn task_id(&self) -> UniqueID {
        self.inner.task_id
    }

    /// Absolute file path for path-based uploads, if available.
    pub fn file_path(&self) -> Option<PathBuf> {
        self.inner.file_path.clone()
    }

    /// Per-file progress snapshot, or `None` if not yet available.
    pub fn progress(&self) -> Option<ItemProgressReport> {
        self.inner.upload_session.item_report(self.inner.task_id)
    }

    pub fn status(&self) -> Result<XetTaskState, XetError> {
        self.task_runtime.status_from_background_task(&self.inner.state)
    }

    /// Wait for ingestion to complete and return per-file [`XetFileMetadata`].
    ///
    /// Idempotent: returns the cached result if already resolved.
    ///
    /// Note: this does not upload file data to CAS; call [`XetUploadCommit::commit`]
    /// to upload all ingested files in the commit.
    pub async fn finalize_ingestion(&self) -> Result<XetFileMetadata, XetError> {
        info!(task_id = %self.task_id(), "File upload finalize_ingestion");
        let inner = self.inner.clone();
        self.task_runtime
            .bridge_async_finalizing("finalize_ingestion", true, async move { inner.state.lock().await.finish().await })
            .await
    }

    /// Blocking version of [`finalize_ingestion`](Self::finalize_ingestion).
    pub fn finalize_ingestion_blocking(&self) -> Result<XetFileMetadata, XetError> {
        info!(task_id = %self.task_id(), "File upload finalize_ingestion");
        if let Some(meta) = self.try_finish() {
            return Ok(meta);
        }
        let inner = self.inner.clone();
        self.task_runtime
            .bridge_sync_finalizing("finalize_ingestion_blocking", true, async move {
                inner.state.lock().await.finish().await
            })
    }

    /// Returns cached completion metadata if finalize succeeded.
    pub fn try_finish(&self) -> Option<XetFileMetadata> {
        self.task_runtime.background_success(&self.inner.state)
    }

    pub(super) fn abort_task(&self) {
        self.task_runtime.cancel_background_task();
    }
}
