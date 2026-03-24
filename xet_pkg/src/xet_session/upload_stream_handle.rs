//! UploadStreamHandle — handle for incremental streaming uploads

use std::fmt;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use tracing::{debug, info};
use xet_data::processing::{FileUploadCoordinator, SingleFileCleaner};
use xet_data::progress_tracking::{ItemProgressReport, UniqueID};

use super::task_runtime::TaskRuntime;
use super::upload_commit::FileMetadata;
use crate::error::XetError;

// ── Private state ───────────────────────────────────────────────────────────

type CleanerState = Option<(SingleFileCleaner, Option<String>)>;

// ── UploadStreamHandleInner ─────────────────────────────────────────────────

pub(super) struct UploadStreamHandleInner {
    pub(super) task_id: UniqueID,
    pub(super) result: Arc<OnceLock<FileMetadata>>,
    pub(super) cleaner: Arc<tokio::sync::Mutex<CleanerState>>,
    pub(super) upload_coordinator: Arc<FileUploadCoordinator>,
    pub(super) task_runtime: Arc<TaskRuntime>,
}

impl UploadStreamHandleInner {
    async fn write(self: &Arc<Self>, data: Bytes) -> Result<(), XetError> {
        let mut guard = self.cleaner.lock().await;
        let Some((cleaner, _)) = guard.as_mut() else {
            return Err(XetError::other("stream already finished"));
        };
        cleaner.add_data_from_bytes(data).await.map_err(XetError::from)
    }

    async fn finish(self: &Arc<Self>) -> Result<FileMetadata, XetError> {
        let mut guard = self.cleaner.lock().await;
        let Some((cleaner, tracking_name)) = guard.take() else {
            return Err(XetError::other("stream already finished"));
        };
        drop(guard);

        match cleaner.finish().await {
            Ok((xet_info, dedup_metrics)) => Ok(FileMetadata {
                xet_info,
                dedup_metrics,
                tracking_name,
            }),
            Err(e) => Err(XetError::from(e)),
        }
    }

    fn try_finish(self: &Arc<Self>) -> Option<FileMetadata> {
        self.result.get().cloned()
    }

    fn get_progress(self: &Arc<Self>) -> Option<ItemProgressReport> {
        self.upload_coordinator.item_report(self.task_id)
    }

    fn abort(self: &Arc<Self>) {
        let _ = self.task_runtime.cancel_subtree();
        if let Ok(mut cleaner_guard) = self.cleaner.try_lock() {
            *cleaner_guard = None;
        }
    }
}

// ── UploadStreamHandle (public wrapper) ─────────────────────────────────────

/// Handle for a streaming upload within an [`UploadCommit`].
///
/// Returned by [`UploadCommit::upload_stream`].  Feed data with
/// [`write`](Self::write), then call [`finish`](Self::finish) to finalise
/// ingestion.  **`finish` must be called before [`UploadCommit::commit`]**;
/// committing with an unfinished stream handle is an error.
///
/// This type is cheaply clonable; all clones share the same underlying state.
#[derive(Clone)]
pub struct UploadStreamHandle {
    pub(super) inner: Arc<UploadStreamHandleInner>,
    pub(super) task_runtime: Arc<TaskRuntime>,
}

impl fmt::Debug for UploadStreamHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadStreamHandle")
            .field("task_id", &self.inner.task_id)
            .finish_non_exhaustive()
    }
}

impl UploadStreamHandle {
    /// Unique identifier for this upload task, usable for progress lookups.
    pub fn task_id(&self) -> UniqueID {
        self.inner.task_id
    }

    /// Feed data into the streaming upload pipeline.
    ///
    /// May be called any number of times before [`finish`](Self::finish).
    /// Returns an error if `finish` or [`abort`](Self::abort) has already
    /// been called.
    pub async fn write(&self, data: impl Into<Bytes>) -> Result<(), XetError> {
        let inner = Arc::clone(&self.inner);
        let data = data.into();
        debug!(task_id = %self.task_id(), bytes = data.len(), "Stream write");
        self.task_runtime
            .bridge_async("upload_stream_write", async move { inner.write(data).await })
            .await
    }

    /// Blocking version of [`write`](Self::write).
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn write_blocking(&self, data: impl Into<Bytes>) -> Result<(), XetError> {
        let data = data.into();
        debug!(task_id = %self.task_id(), bytes = data.len(), "Stream write");
        let inner = Arc::clone(&self.inner);
        self.task_runtime
            .bridge_sync("upload_stream_write_blocking", async move { inner.write(data).await })
    }

    /// Finalise the streaming upload and return per-file [`FileMetadata`].
    ///
    /// Must be called before [`UploadCommit::commit`].  A second call returns
    /// [`XetError::AlreadyCompleted`] after a successful finish; use
    /// [`try_finish`](Self::try_finish) to read cached metadata without
    /// finalizing again.
    pub async fn finish(&self) -> Result<FileMetadata, XetError> {
        info!(task_id = %self.task_id(), "Stream finish");
        let inner = Arc::clone(&self.inner);
        let result_cell = self.inner.result.clone();
        let meta = self
            .task_runtime
            .bridge_async_finalizing("upload_stream_finish", async move { inner.finish().await })
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
        info!(task_id = %self.task_id(), "Stream finish");
        let inner = Arc::clone(&self.inner);
        let result_cell = self.inner.result.clone();
        let meta = self
            .task_runtime
            .bridge_sync_finalizing("upload_stream_finish_blocking", async move { inner.finish().await })?;
        let _ = result_cell.set(meta.clone());
        Ok(meta)
    }

    /// Returns the result if the stream has been finished, without blocking.
    pub fn try_finish(&self) -> Option<FileMetadata> {
        self.inner.try_finish()
    }

    /// Per-file progress snapshot, or `None` if not yet available.
    pub fn get_progress(&self) -> Option<ItemProgressReport> {
        self.inner.get_progress()
    }

    /// Cancel the streaming upload.
    ///
    /// Drops the internal data pipeline.  Subsequent [`write`](Self::write) or
    /// [`finish`](Self::finish) calls may return [`XetError::UserCancelled`] or
    /// related errors.
    pub fn abort(&self) {
        info!(task_id = %self.task_id(), "Stream abort");
        self.inner.abort()
    }
}
