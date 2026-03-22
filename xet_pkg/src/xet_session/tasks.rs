//! Progress tracking for upload commits and download groups.

use std::sync::{Arc, Mutex};

use xet_data::progress_tracking::UniqueID;

use super::SessionError;
use super::upload_commit::FileMetadata;
use crate::error::XetError;

/// Per-file upload result (aligned with download [`DownloadResult`]).
pub type UploadResult = std::sync::Arc<Result<FileMetadata, XetError>>;

/// Lifecycle state of a single upload or download task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskStatus {
    /// Task has been queued but has not started executing yet.
    Queued,
    /// Task is actively transferring data.
    Running,
    /// Task finished successfully.
    Completed,
    /// Task encountered an error and did not complete.
    Failed,
    /// Task was cancelled before it could complete.
    Cancelled,
}

impl TaskStatus {
    pub(super) fn mark_running(status: &Arc<Mutex<TaskStatus>>) {
        if let Ok(mut current) = status.lock()
            && matches!(*current, TaskStatus::Queued)
        {
            *current = TaskStatus::Running;
        }
    }

    pub(super) fn mark_terminal(status: &Arc<Mutex<TaskStatus>>, terminal_status: TaskStatus) {
        if let Ok(mut current) = status.lock()
            && !matches!(*current, TaskStatus::Cancelled)
        {
            *current = terminal_status;
        }
    }

    pub(super) fn mark_cancelled(status: &Arc<Mutex<TaskStatus>>) {
        if let Ok(mut current) = status.lock() {
            *current = TaskStatus::Cancelled;
        }
    }
}

#[derive(Debug)]
pub struct TaskHandle {
    pub(super) status: Option<Arc<Mutex<TaskStatus>>>,
    /// Id of the task, can be used to retrieve per-task progress and result.
    pub task_id: UniqueID,
}

impl TaskHandle {
    pub fn status(&self) -> Result<TaskStatus, SessionError> {
        if let Some(status) = &self.status {
            Ok(*status.lock()?)
        } else {
            Err(SessionError::other("status not available"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::sync::OnceLock;

    use xet_data::deduplication::DeduplicationMetrics;
    use xet_data::processing::XetFileInfo;

    use super::*;
    use crate::xet_session::FileMetadata;

    #[derive(Debug)]
    struct UploadTaskHandle {
        inner: TaskHandle,
        result: Arc<OnceLock<UploadResult>>,
    }

    impl Deref for UploadTaskHandle {
        type Target = TaskHandle;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl UploadTaskHandle {
        fn result(&self) -> Option<UploadResult> {
            self.result.get().cloned()
        }
    }

    #[test]
    fn test_task_handle_with_no_status_returns_error() {
        let handle = TaskHandle {
            status: None,
            task_id: UniqueID::new(),
        };
        assert!(handle.status().is_err());
    }

    #[test]
    fn test_upload_task_handle_result_none_before_commit() {
        let handle = UploadTaskHandle {
            inner: TaskHandle {
                status: None,
                task_id: UniqueID::new(),
            },
            result: Arc::new(OnceLock::new()),
        };
        assert!(handle.result().is_none());
    }

    #[test]
    fn test_upload_task_handle_result_some_after_result_set() {
        let result_arc = Arc::new(OnceLock::new());
        let handle = UploadTaskHandle {
            inner: TaskHandle {
                status: None,
                task_id: UniqueID::new(),
            },
            result: result_arc.clone(),
        };

        let metadata = Arc::new(Ok(FileMetadata {
            tracking_name: Some("file.bin".to_string()),
            xet_info: XetFileInfo {
                hash: "abc123".to_string(),
                file_size: Some(42),
                sha256: None,
            },
            dedup_metrics: DeduplicationMetrics::default(),
        }));
        result_arc.set(metadata).unwrap();

        let result = handle.result().unwrap();
        let meta = result.as_ref().as_ref().unwrap();
        assert_eq!(meta.xet_info.file_size, Some(42));
        assert_eq!(meta.xet_info.hash, "abc123");
    }
}
