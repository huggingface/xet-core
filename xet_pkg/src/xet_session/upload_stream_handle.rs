//! UploadStreamHandle — handle for incremental streaming uploads

use std::fmt;
use std::sync::Arc;

use xet_data::processing::{FileUploadSession, SingleFileCleaner};
use xet_data::progress_tracking::{ItemProgressReport, UniqueID};
use xet_runtime::core::XetRuntime;

use super::upload_commit::FileMetadata;
use crate::error::XetError;

// ── Private state ───────────────────────────────────────────────────────────

pub(super) enum StreamState {
    Writing {
        cleaner: SingleFileCleaner,
        tracking_name: Option<String>,
    },
    Finished(Result<FileMetadata, XetError>),
}

// ── Resolution helper ───────────────────────────────────────────────────────

async fn resolve_stream_task(state: &tokio::sync::Mutex<StreamState>) -> Result<FileMetadata, XetError> {
    let mut guard = state.lock().await;
    if let StreamState::Finished(ref result) = *guard {
        return result.clone();
    }
    let prev = std::mem::replace(&mut *guard, StreamState::Finished(Err(XetError::other("stream finish in progress"))));
    let StreamState::Writing { cleaner, tracking_name } = prev else {
        unreachable!()
    };
    let result = match cleaner.finish().await {
        Ok((xet_info, dedup_metrics)) => Ok(FileMetadata {
            xet_info,
            dedup_metrics,
            tracking_name,
        }),
        Err(e) => Err(XetError::from(e)),
    };
    *guard = StreamState::Finished(result.clone());
    result
}

// ── UploadStreamHandleInner ─────────────────────────────────────────────────

pub(super) struct UploadStreamHandleInner {
    pub(super) task_id: UniqueID,
    pub(super) state: Arc<tokio::sync::Mutex<StreamState>>,
    pub(super) upload_session: Arc<FileUploadSession>,
}

impl UploadStreamHandleInner {
    async fn write(&self, data: &[u8]) -> Result<(), XetError> {
        let mut guard = self.state.lock().await;
        match &mut *guard {
            StreamState::Writing { cleaner, .. } => cleaner.add_data(data).await.map_err(XetError::from),
            StreamState::Finished(Err(e)) => Err(e.clone()),
            StreamState::Finished(Ok(_)) => Err(XetError::other("stream already finished")),
        }
    }

    async fn finish(&self) -> Result<FileMetadata, XetError> {
        resolve_stream_task(&self.state).await
    }

    fn try_finish(&self) -> Option<Result<FileMetadata, XetError>> {
        let guard = self.state.try_lock().ok()?;
        match &*guard {
            StreamState::Finished(result) => Some(result.clone()),
            StreamState::Writing { .. } => None,
        }
    }

    fn get_progress(&self) -> Option<ItemProgressReport> {
        self.upload_session.item_report(self.task_id)
    }

    fn abort(&self) {
        if let Ok(mut guard) = self.state.try_lock()
            && matches!(*guard, StreamState::Writing { .. })
        {
            *guard = StreamState::Finished(Err(XetError::Aborted));
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
/// `finish` is idempotent: subsequent calls return the same cached result.
///
/// This type is cheaply clonable; all clones share the same underlying state.
#[derive(Clone)]
pub struct UploadStreamHandle {
    pub(super) inner: Arc<UploadStreamHandleInner>,
    pub(super) runtime: Arc<XetRuntime>,
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
    pub async fn write(&self, data: &[u8]) -> Result<(), XetError> {
        self.inner.write(data).await
    }

    /// Blocking version of [`write`](Self::write).
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio async runtime.
    pub fn write_blocking(&self, data: &[u8]) -> Result<(), XetError> {
        let data = data.to_vec();
        let inner = Arc::clone(&self.inner);
        self.runtime.external_run_async_task(async move { inner.write(&data).await })?
    }

    /// Finalise the streaming upload and return per-file [`FileMetadata`].
    ///
    /// Must be called before [`UploadCommit::commit`].  Idempotent:
    /// subsequent calls return the same cached result.
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

    /// Returns the result if the stream has been finished, without blocking.
    pub fn try_finish(&self) -> Option<Result<FileMetadata, XetError>> {
        self.inner.try_finish()
    }

    /// Per-file progress snapshot, or `None` if not yet available.
    pub fn get_progress(&self) -> Option<ItemProgressReport> {
        self.inner.get_progress()
    }

    /// Cancel the streaming upload.
    ///
    /// Drops the internal data pipeline and stores an [`XetError::Aborted`]
    /// result.  Subsequent [`write`](Self::write) or [`finish`](Self::finish)
    /// calls will return the aborted error.
    pub fn abort(&self) {
        self.inner.abort()
    }
}
