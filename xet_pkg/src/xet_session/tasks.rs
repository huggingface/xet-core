//! Task handles for download groups and shared task types.

use std::ops::Deref;
use std::sync::{Arc, Mutex, OnceLock};

use xet_data::progress_tracking::UniqueID;

use super::download_group::DownloadResult;
use crate::error::XetError;

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

#[derive(Debug)]
pub struct DownloadTaskHandle {
    pub(super) inner: TaskHandle,
    pub(super) result: Arc<OnceLock<DownloadResult>>,
}

impl Deref for DownloadTaskHandle {
    type Target = TaskHandle;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TaskHandle {
    pub fn status(&self) -> Result<TaskStatus, XetError> {
        if let Some(status) = &self.status {
            Ok(*status.lock()?)
        } else {
            Err(XetError::other("status not available"))
        }
    }
}

impl DownloadTaskHandle {
    pub fn result(&self) -> Option<DownloadResult> {
        self.result.get().cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use xet_data::processing::XetFileInfo;

    use super::*;
    use crate::xet_session::DownloadedFile;

    #[test]
    fn test_task_handle_with_no_status_returns_error() {
        let handle = TaskHandle {
            status: None,
            task_id: UniqueID::new(),
        };
        assert!(handle.status().is_err());
    }

    #[test]
    fn test_download_task_handle_result_none_before_finish() {
        let handle = DownloadTaskHandle {
            inner: TaskHandle {
                status: None,
                task_id: UniqueID::new(),
            },
            result: Arc::new(OnceLock::new()),
        };
        assert!(handle.result().is_none());
    }

    #[test]
    fn test_download_task_handle_result_some_after_result_set() {
        let result_arc = Arc::new(OnceLock::new());
        let handle = DownloadTaskHandle {
            inner: TaskHandle {
                status: None,
                task_id: UniqueID::new(),
            },
            result: result_arc.clone(),
        };

        let download_result = Arc::new(Ok(DownloadedFile {
            dest_path: PathBuf::from("out/file.bin"),
            file_info: XetFileInfo {
                hash: "def456".to_string(),
                file_size: Some(99),
                sha256: None,
            },
        }));
        result_arc.set(download_result).unwrap();

        let result = handle.result().unwrap();
        let dl = result.as_ref().as_ref().unwrap();
        assert_eq!(dl.file_info.file_size, Some(99));
        assert_eq!(dl.dest_path, PathBuf::from("out/file.bin"));
    }
}
