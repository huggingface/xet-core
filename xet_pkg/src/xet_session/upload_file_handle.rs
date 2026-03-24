//! XetFileUpload — handle for a background file-upload task

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::task::JoinHandle;
use tracing::info;
use xet_data::DataError;
use xet_data::deduplication::DeduplicationMetrics;
use xet_data::processing::{FileUploadSession, XetFileInfo};
use xet_data::progress_tracking::{ItemProgressReport, UniqueID};

use super::task_runtime::{BackgroundTaskResolution, BackgroundTaskState, TaskRuntime, XetTaskState};
use super::upload_commit::XetFileMetadata;
use crate::error::XetError;

pub(super) type UploadJoinHandle = JoinHandle<Result<(XetFileInfo, DeduplicationMetrics), DataError>>;

/// Handle for a background file-upload task within an [`XetUploadCommit`].
///
/// Returned by [`XetUploadCommit::upload_from_path`] and
/// [`XetUploadCommit::upload_bytes`]. Use [`finalize_ingestion`](Self::finalize_ingestion)
/// to wait for ingestion completion.
///
/// Important: ingestion completion means the file has been chunked/deduplicated.
/// The file is not uploaded to CAS until [`XetUploadCommit::commit`] is called.
#[derive(Clone)]
pub struct XetFileUpload {
    pub(super) task_id: UniqueID,
    pub(super) file_path: Option<PathBuf>,
    pub(super) tracking_name: Option<String>,
    pub(super) upload_session: Arc<FileUploadSession>,
    pub(super) state: Arc<tokio::sync::Mutex<BackgroundTaskState<XetFileMetadata, UploadJoinHandle>>>,
    pub(super) task_runtime: Arc<TaskRuntime>,
}

impl fmt::Debug for XetFileUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("XetFileUpload")
            .field("task_id", &self.task_id)
            .finish_non_exhaustive()
    }
}

impl XetFileUpload {
    /// Unique identifier for this upload task, usable for progress lookups.
    pub fn task_id(&self) -> UniqueID {
        self.task_id
    }

    /// Absolute file path for path-based uploads, if available.
    pub fn file_path(&self) -> Option<PathBuf> {
        self.file_path.clone()
    }

    /// Per-file progress snapshot, or `None` if not yet available.
    pub fn progress(&self) -> Option<ItemProgressReport> {
        self.upload_session.item_report(self.task_id)
    }

    pub fn status(&self) -> Result<XetTaskState, XetError> {
        self.task_runtime.status_from_background_task(&self.state)
    }

    /// Wait for ingestion to complete and return per-file [`XetFileMetadata`].
    ///
    /// Note: this does not upload file data to CAS; call [`XetUploadCommit::commit`]
    /// to upload all ingested files in the commit.
    pub async fn finalize_ingestion(&self) -> Result<XetFileMetadata, XetError> {
        info!(task_id = %self.task_id(), "File upload finalize_ingestion");
        let join_handle = match self
            .task_runtime
            .begin_background_task_resolution(&self.state, "upload task already resolved")
            .await?
        {
            BackgroundTaskResolution::Cached(value) => return Ok(value),
            BackgroundTaskResolution::JoinHandle(handle) => handle,
        };

        let tracking_name = self.tracking_name.clone();
        let result = self
            .task_runtime
            .bridge_async_finalizing("file_upload_finalize_ingestion", async move {
                match join_handle.await {
                    Ok(Ok((xet_info, dedup_metrics))) => Ok(XetFileMetadata {
                        xet_info,
                        dedup_metrics,
                        tracking_name,
                    }),
                    Ok(Err(e)) => Err(XetError::from(e)),
                    Err(e) => Err(XetError::from(e)),
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
                .unwrap_or_else(|| XetError::TaskError("upload task failed".to_string()))),
        }
    }

    /// Blocking version of [`finalize_ingestion`](Self::finalize_ingestion).
    pub fn finalize_ingestion_blocking(&self) -> Result<XetFileMetadata, XetError> {
        info!(task_id = %self.task_id(), "File upload finalize_ingestion");
        let join_handle = match self
            .task_runtime
            .begin_background_task_resolution_blocking(&self.state, "upload task already resolved")?
        {
            BackgroundTaskResolution::Cached(value) => return Ok(value),
            BackgroundTaskResolution::JoinHandle(handle) => handle,
        };

        let tracking_name = self.tracking_name.clone();
        let result = self
            .task_runtime
            .bridge_sync_finalizing("file_upload_finalize_ingestion_blocking", async move {
                match join_handle.await {
                    Ok(Ok((xet_info, dedup_metrics))) => Ok(XetFileMetadata {
                        xet_info,
                        dedup_metrics,
                        tracking_name,
                    }),
                    Ok(Err(e)) => Err(XetError::from(e)),
                    Err(e) => Err(XetError::from(e)),
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
                .unwrap_or_else(|| XetError::TaskError("upload task failed".to_string()))),
        }
    }

    /// Returns cached completion metadata if finalize succeeded.
    pub fn try_finish(&self) -> Option<XetFileMetadata> {
        self.task_runtime.background_success(&self.state)
    }

    pub(super) fn abort_task(&self) {
        self.task_runtime
            .cancel_background_task(&self.state, "upload task cancelled by user", |join_handle| {
                join_handle.abort();
            });
    }
}
