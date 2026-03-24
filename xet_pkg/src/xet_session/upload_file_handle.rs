//! UploadFileHandle — handle for a background file-upload task

use std::fmt;
use std::sync::{Arc, OnceLock};

use tokio::task::{AbortHandle, JoinHandle};
use tracing::info;
use xet_data::DataError;
use xet_data::deduplication::DeduplicationMetrics;
use xet_data::processing::{FileUploadCoordinator, XetFileInfo};
use xet_data::progress_tracking::{ItemProgressReport, UniqueID};

use super::task_runtime::TaskRuntime;
use super::upload_commit::FileMetadata;
use crate::error::XetError;

// ── Private state ───────────────────────────────────────────────────────────

type UploadJoinHandle = JoinHandle<Result<(XetFileInfo, DeduplicationMetrics), DataError>>;

pub(super) struct TrackedFileUpload {
    pub(super) result: Arc<OnceLock<FileMetadata>>,
    pub(super) join_handle: Arc<tokio::sync::Mutex<Option<UploadJoinHandle>>>,
    pub(super) tracking_name: Option<String>,
    pub(super) abort_handle: AbortHandle,
    pub(super) task_runtime: Arc<TaskRuntime>,
}

impl TrackedFileUpload {
    pub(super) async fn finish(&self) -> Result<FileMetadata, XetError> {
        let join_handle = self.join_handle.clone();
        let tracking_name = self.tracking_name.clone();
        let result_cell = self.result.clone();
        let meta = self
            .task_runtime
            .bridge_async_finalizing("tracked_upload_finish", async move {
                resolve_file_task(&join_handle, tracking_name).await
            })
            .await?;
        let _ = result_cell.set(meta.clone());
        Ok(meta)
    }
}

// ── Resolution helper ───────────────────────────────────────────────────────

pub(super) async fn resolve_file_task(
    join_handle: &tokio::sync::Mutex<Option<UploadJoinHandle>>,
    tracking_name: Option<String>,
) -> Result<FileMetadata, XetError> {
    let mut guard = join_handle.lock().await;
    let Some(join_handle) = guard.take() else {
        return Err(XetError::other("upload task already resolved"));
    };
    drop(guard);

    match join_handle.await {
        Ok(Ok((xet_info, dedup_metrics))) => Ok(FileMetadata {
            xet_info,
            dedup_metrics,
            tracking_name,
        }),
        Ok(Err(e)) => Err(XetError::from(e)),
        Err(e) => Err(XetError::from(e)),
    }
}

// ── UploadFileHandleInner ───────────────────────────────────────────────────

pub(super) struct UploadFileHandleInner {
    pub(super) task_id: UniqueID,
    pub(super) result: Arc<OnceLock<FileMetadata>>,
    pub(super) join_handle: Arc<tokio::sync::Mutex<Option<UploadJoinHandle>>>,
    pub(super) tracking_name: Option<String>,
    pub(super) upload_coordinator: Arc<FileUploadCoordinator>,
}

impl UploadFileHandleInner {
    async fn finish(self: &Arc<Self>) -> Result<FileMetadata, XetError> {
        resolve_file_task(&self.join_handle, self.tracking_name.clone()).await
    }

    fn try_finish(self: &Arc<Self>) -> Option<FileMetadata> {
        self.result.get().cloned()
    }

    fn get_progress(self: &Arc<Self>) -> Option<ItemProgressReport> {
        self.upload_coordinator.item_report(self.task_id)
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
/// This type is cheaply clonable; all clones share the same underlying state.
#[derive(Clone)]
pub struct UploadFileHandle {
    pub(super) inner: Arc<UploadFileHandleInner>,
    pub(super) task_runtime: Arc<TaskRuntime>,
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
    /// A second call returns [`XetError::AlreadyCompleted`] after a successful
    /// finish; use [`try_finish`](Self::try_finish) to read cached metadata
    /// without waiting again.
    pub async fn finish(&self) -> Result<FileMetadata, XetError> {
        info!(task_id = %self.task_id(), "File upload finish");
        let inner = Arc::clone(&self.inner);
        let result_cell = self.inner.result.clone();
        let meta = self
            .task_runtime
            .bridge_async_finalizing("upload_file_finish", async move { inner.finish().await })
            .await?;
        let _ = result_cell.set(meta.clone());
        Ok(meta)
    }

    /// Blocking version of [`finish`](Self::finish).
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn finish_blocking(&self) -> Result<FileMetadata, XetError> {
        info!(task_id = %self.task_id(), "File upload finish");
        let inner = Arc::clone(&self.inner);
        let result_cell = self.inner.result.clone();
        let meta = self
            .task_runtime
            .bridge_sync_finalizing("upload_file_finish_blocking", async move { inner.finish().await })?;
        let _ = result_cell.set(meta.clone());
        Ok(meta)
    }

    /// Returns the result if ingestion has already completed, without blocking.
    pub fn try_finish(&self) -> Option<FileMetadata> {
        self.inner.try_finish()
    }

    /// Per-file progress snapshot, or `None` if not yet available.
    pub fn get_progress(&self) -> Option<ItemProgressReport> {
        self.inner.get_progress()
    }
}
