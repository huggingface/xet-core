//! UploadFileHandle — handle for a background file-upload task

use std::fmt;
use std::sync::{Arc, OnceLock};

use tokio::task::{AbortHandle, JoinHandle};
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
    pub(super) result: Arc<OnceLock<Result<FileMetadata, XetError>>>,
    pub(super) join_handle: Arc<tokio::sync::Mutex<Option<UploadJoinHandle>>>,
    pub(super) tracking_name: Option<String>,
    pub(super) abort_handle: AbortHandle,
    pub(super) task_runtime: Arc<TaskRuntime>,
}

impl TrackedFileUpload {
    pub(super) async fn finish(&self) -> Result<FileMetadata, XetError> {
        let result = self.result.clone();
        let join_handle = self.join_handle.clone();
        let tracking_name = self.tracking_name.clone();
        self.task_runtime
            .bridge_async_terminal("tracked_upload_finish", result, async move {
                resolve_file_task(&join_handle, tracking_name).await
            })
            .await
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
    pub(super) result: Arc<OnceLock<Result<FileMetadata, XetError>>>,
    pub(super) join_handle: Arc<tokio::sync::Mutex<Option<UploadJoinHandle>>>,
    pub(super) tracking_name: Option<String>,
    pub(super) upload_coordinator: Arc<FileUploadCoordinator>,
}

impl UploadFileHandleInner {
    async fn finish(self: &Arc<Self>) -> Result<FileMetadata, XetError> {
        resolve_file_task(&self.join_handle, self.tracking_name.clone()).await
    }

    fn try_finish(self: &Arc<Self>) -> Option<Result<FileMetadata, XetError>> {
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
/// `finish` is idempotent: subsequent calls return the same cached result.
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
    /// Idempotent: subsequent calls return the same cached result.
    pub async fn finish(&self) -> Result<FileMetadata, XetError> {
        let inner = Arc::clone(&self.inner);
        let result = self.inner.result.clone();
        self.task_runtime
            .bridge_async_terminal("upload_file_finish", result, async move { inner.finish().await })
            .await
    }

    /// Blocking version of [`finish`](Self::finish).
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn finish_blocking(&self) -> Result<FileMetadata, XetError> {
        let inner = Arc::clone(&self.inner);
        let result = self.inner.result.clone();
        self.task_runtime.bridge_sync_terminal(
            "upload_file_finish_blocking",
            result,
            async move { inner.finish().await },
        )
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
