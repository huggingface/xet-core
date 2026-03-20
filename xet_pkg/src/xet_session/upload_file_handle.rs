//! UploadFileHandle — handle for a background file-upload task

use std::fmt;
use std::sync::Arc;

use tokio::task::{AbortHandle, JoinHandle};
use xet_data::DataError;
use xet_data::deduplication::DeduplicationMetrics;
use xet_data::processing::{FileUploadSession, XetFileInfo};
use xet_data::progress_tracking::{ItemProgressReport, UniqueID};
use xet_runtime::core::XetRuntime;

use super::upload_commit::FileMetadata;
use crate::error::XetError;

// ── Private state ───────────────────────────────────────────────────────────

pub(super) enum FileTaskState {
    Running {
        join_handle: JoinHandle<Result<(XetFileInfo, DeduplicationMetrics), DataError>>,
        tracking_name: Option<String>,
    },
    Finished(Result<FileMetadata, XetError>),
}

pub(super) struct TrackedFileUpload {
    pub(super) state: Arc<tokio::sync::Mutex<FileTaskState>>,
    pub(super) abort_handle: AbortHandle,
}

// ── Resolution helper ───────────────────────────────────────────────────────

pub(super) async fn resolve_file_task(state: &tokio::sync::Mutex<FileTaskState>) -> Result<FileMetadata, XetError> {
    let mut guard = state.lock().await;
    if let FileTaskState::Finished(ref result) = *guard {
        return result.clone();
    }
    let prev =
        std::mem::replace(&mut *guard, FileTaskState::Finished(Err(XetError::other("task resolution in progress"))));
    let FileTaskState::Running {
        join_handle,
        tracking_name,
    } = prev
    else {
        unreachable!()
    };
    let result = match join_handle.await {
        Ok(Ok((xet_info, dedup_metrics))) => Ok(FileMetadata {
            xet_info,
            dedup_metrics,
            tracking_name,
        }),
        Ok(Err(e)) => Err(XetError::from(e)),
        Err(e) => Err(XetError::from(e)),
    };
    *guard = FileTaskState::Finished(result.clone());
    result
}

// ── UploadFileHandleInner ───────────────────────────────────────────────────

pub(super) struct UploadFileHandleInner {
    pub(super) task_id: UniqueID,
    pub(super) state: Arc<tokio::sync::Mutex<FileTaskState>>,
    pub(super) upload_session: Arc<FileUploadSession>,
}

impl UploadFileHandleInner {
    async fn finish(&self) -> Result<FileMetadata, XetError> {
        resolve_file_task(&self.state).await
    }

    fn try_finish(&self) -> Option<Result<FileMetadata, XetError>> {
        let guard = self.state.try_lock().ok()?;
        match &*guard {
            FileTaskState::Finished(result) => Some(result.clone()),
            FileTaskState::Running { .. } => None,
        }
    }

    fn get_progress(&self) -> Option<ItemProgressReport> {
        self.upload_session.item_report(self.task_id)
    }
}

// ── UploadFileHandle (public wrapper) ───────────────────────────────────────

/// Handle for a background file-upload task within an [`UploadCommit`].
///
/// Returned by [`UploadCommit::upload_from_path`] and
/// [`UploadCommit::upload_bytes`].  Call [`finish`](Self::finish) to wait for
/// ingestion (chunking + deduplication) to complete.  This does **not** mean
/// the data has been uploaded to the server — call [`UploadCommit::commit`]
/// for that.
///
/// `finish` is idempotent: subsequent calls return the same cached result.
///
/// This type is cheaply clonable; all clones share the same underlying state.
#[derive(Clone)]
pub struct UploadFileHandle {
    pub(super) inner: Arc<UploadFileHandleInner>,
    pub(super) runtime: Arc<XetRuntime>,
}

impl fmt::Debug for UploadFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadFileHandle")
            .field("task_id", &self.inner.task_id)
            .finish_non_exhaustive()
    }
}

impl UploadFileHandle {
    /// Unique identifier for this upload task, usable for progress lookups.
    pub fn task_id(&self) -> UniqueID {
        self.inner.task_id
    }

    /// Wait for ingestion to complete and return per-file [`FileMetadata`].
    ///
    /// Ingestion means the file has been fully chunked and deduplicated.
    /// It does **not** mean data has reached the server — call
    /// [`UploadCommit::commit`] for that.
    ///
    /// Idempotent: subsequent calls return the same cached result.
    pub async fn finish(&self) -> Result<FileMetadata, XetError> {
        self.inner.finish().await
    }

    /// Blocking version of [`finish`](Self::finish).
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn finish_blocking(&self) -> Result<FileMetadata, XetError> {
        let inner = Arc::clone(&self.inner);
        self.runtime.external_run_async_task(async move { inner.finish().await })?
    }

    /// Returns the result if ingestion has already completed, without blocking.
    pub fn try_finish(&self) -> Option<Result<FileMetadata, XetError>> {
        self.inner.try_finish()
    }

    /// Per-file progress snapshot, or `None` if not yet available.
    pub fn get_progress(&self) -> Option<ItemProgressReport> {
        self.inner.get_progress()
    }
}
