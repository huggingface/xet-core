//! Progress tracking for upload commits and download groups.

use std::ops::Deref;
use std::sync::{Arc, Mutex, OnceLock};

use xet_data::progress_tracking::UniqueID;

use super::SessionError;
use super::download_group::DownloadResult;
use super::upload_commit::UploadResult;

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

#[derive(Debug)]
pub struct TaskHandle {
    pub(super) status: Option<Arc<Mutex<TaskStatus>>>,
    /// Id of the task, can be used to retrieve per-task progress and result.
    pub task_id: UniqueID,
}

#[derive(Debug)]
pub struct UploadTaskHandle {
    pub(super) inner: TaskHandle,
    pub(super) result: Arc<OnceLock<UploadResult>>,
}

impl Deref for UploadTaskHandle {
    type Target = TaskHandle;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
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
    pub fn status(&self) -> Result<TaskStatus, SessionError> {
        if let Some(status) = &self.status {
            Ok(*status.lock()?)
        } else {
            Err(SessionError::other("status not available"))
        }
    }
}

impl UploadTaskHandle {
    pub fn result(&self) -> Option<UploadResult> {
        self.result.get().cloned()
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
    use crate::xet_session::{DownloadedFile, FileMetadata};

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
            hash: "abc123".to_string(),
            file_size: 42,
            sha256: None,
        }));
        result_arc.set(metadata).unwrap();

        let result = handle.result().unwrap();
        let meta = result.as_ref().as_ref().unwrap();
        assert_eq!(meta.file_size, 42);
        assert_eq!(meta.hash, "abc123");
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
                file_size: 99,
                sha256: None,
            },
        }));
        result_arc.set(download_result).unwrap();

        let result = handle.result().unwrap();
        let dl = result.as_ref().as_ref().unwrap();
        assert_eq!(dl.file_info.file_size, 99);
        assert_eq!(dl.dest_path, PathBuf::from("out/file.bin"));
    }
}
