//! Progress tracking for upload commits and download groups.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use ulid::Ulid;
use xet_data::progress_tracking::{ProgressUpdate, TrackingProgressUpdater};

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
    pub(crate) status: Option<Arc<Mutex<TaskStatus>>>,
    pub(crate) group_progress: Arc<GroupProgress>,
    /// Id of the task, can be used to retrive per-task progress and result.
    pub task_id: Ulid,
}

#[derive(Debug)]
pub struct UploadTaskHandle {
    pub(crate) inner: TaskHandle,
    pub(crate) result: Arc<OnceLock<UploadResult>>,
}

impl Deref for UploadTaskHandle {
    type Target = TaskHandle;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug)]
pub struct DownloadTaskHandle {
    pub(crate) inner: TaskHandle,
    pub(crate) result: Arc<OnceLock<DownloadResult>>,
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

    pub fn progress(&self) -> Result<FileProgress, SessionError> {
        self.group_progress.file(self.task_id)
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

// ── Aggregate / per-file progress ───────────────────────────────────────────

pub struct ProgressSnapshot {
    total: TotalProgressSnapshot,
    files: HashMap<Ulid, FileProgress>,
}

impl ProgressSnapshot {
    pub fn total(&self) -> &TotalProgressSnapshot {
        &self.total
    }

    pub fn file(&self, task_id: Ulid) -> Result<&FileProgress, SessionError> {
        self.files.get(&task_id).ok_or(SessionError::InvalidTaskID(task_id))
    }
}

/// Snapshot of aggregate progress returned by [`TaskProgress::total`].
#[derive(Clone, Debug, Default)]
pub struct TotalProgressSnapshot {
    /// Total bytes known to process (includes deduplicated bytes).
    pub total_bytes: u64,
    /// Total bytes that have been processed so far.
    pub total_bytes_completed: u64,
    /// Bytes-processed completion rate, if available.
    pub total_bytes_completion_rate: Option<f64>,
    /// Total bytes that need to be transferred (uploaded/downloaded).
    pub total_transfer_bytes: u64,
    /// Total bytes that have been transferred so far.
    pub total_transfer_bytes_completed: u64,
    /// Transfer completion rate, if available.
    pub total_transfer_bytes_completion_rate: Option<f64>,
}

/// Snapshot of a single file's progress returned by [`TaskProgress::files`].
#[derive(Clone, Debug)]
pub struct FileProgress {
    /// File name as reported by the data layer.
    pub item_name: Arc<str>,
    /// Total size of this file in bytes.
    pub total_bytes: u64,
    /// Bytes of this file processed so far.
    pub bytes_completed: u64,
}

pub type FileProgressSnapshot = FileProgress;

/// Tracks per-file and aggregate transfer progress for upload commits and download groups.
///
/// Implements [`TrackingProgressUpdater`].
///
/// - Call [`GroupProgress::total`] for an aggregate snapshot (lock-free reads).
/// - Call [`GroupProgress::files`] for a per-file breakdown.
///
/// All integer counters are stored as [`AtomicU64`]; floating-point completion
/// rates use a `Mutex` (rarely written, never held across await points).
#[derive(Debug)]
pub struct GroupProgress {
    // Aggregate totals
    total_bytes: AtomicU64,
    total_bytes_completed: AtomicU64,
    total_transfer_bytes: AtomicU64,
    total_transfer_bytes_completed: AtomicU64,

    // Completion rates
    total_bytes_completion_rate: Mutex<Option<f64>>,
    total_transfer_bytes_completion_rate: Mutex<Option<f64>>,

    // Per-file: item_name → (total_bytes, bytes_completed)
    files: Mutex<HashMap<Ulid, FileProgress>>,
}

impl GroupProgress {
    /// Create a new tracker with all counters at zero.
    pub fn new() -> Self {
        Self {
            total_bytes: AtomicU64::new(0),
            total_bytes_completed: AtomicU64::new(0),
            total_transfer_bytes: AtomicU64::new(0),
            total_transfer_bytes_completed: AtomicU64::new(0),
            total_bytes_completion_rate: Mutex::new(None),
            total_transfer_bytes_completion_rate: Mutex::new(None),
            files: Mutex::new(HashMap::new()),
        }
    }

    /// Return a combined snapshot of aggregate and per-file progress.
    pub fn snapshot(&self) -> Result<ProgressSnapshot, SessionError> {
        let total = self.total();
        let files = self.files.lock()?.clone();
        Ok(ProgressSnapshot { total, files })
    }

    /// Return a combined snapshot of aggregate progress.
    fn total(&self) -> TotalProgressSnapshot {
        TotalProgressSnapshot {
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            total_bytes_completed: self.total_bytes_completed.load(Ordering::Relaxed),
            total_bytes_completion_rate: self.total_bytes_completion_rate.lock().ok().and_then(|g| *g),
            total_transfer_bytes: self.total_transfer_bytes.load(Ordering::Relaxed),
            total_transfer_bytes_completed: self.total_transfer_bytes_completed.load(Ordering::Relaxed),
            total_transfer_bytes_completion_rate: self
                .total_transfer_bytes_completion_rate
                .lock()
                .ok()
                .and_then(|g| *g),
        }
    }

    fn file(&self, tracking_id: Ulid) -> Result<FileProgressSnapshot, SessionError> {
        self.files
            .lock()?
            .get(&tracking_id)
            .cloned()
            .ok_or(SessionError::InvalidTaskID(tracking_id))
    }
}

impl Default for GroupProgress {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TrackingProgressUpdater for GroupProgress {
    async fn register_updates(&self, updates: ProgressUpdate) {
        // Update aggregate integer counters atomically.
        self.total_bytes.store(updates.total_bytes, Ordering::Relaxed);
        self.total_bytes_completed
            .store(updates.total_bytes_completed, Ordering::Relaxed);
        self.total_transfer_bytes.store(updates.total_transfer_bytes, Ordering::Relaxed);
        self.total_transfer_bytes_completed
            .store(updates.total_transfer_bytes_completed, Ordering::Relaxed);

        // Update floating-point rates (brief lock, not held across await).
        if let Ok(mut rate) = self.total_bytes_completion_rate.lock() {
            *rate = updates.total_bytes_completion_rate;
        }
        if let Ok(mut rate) = self.total_transfer_bytes_completion_rate.lock() {
            *rate = updates.total_transfer_bytes_completion_rate;
        }

        // Update per-file progress.
        if let Ok(mut items) = self.files.lock() {
            for item_update in updates.item_updates {
                let entry = items.entry(item_update.tracking_id).or_insert(FileProgress {
                    item_name: item_update.item_name.clone(),
                    total_bytes: item_update.total_bytes,
                    bytes_completed: item_update.bytes_completed,
                });
                entry.total_bytes = entry.total_bytes.max(item_update.total_bytes);
                entry.bytes_completed = entry.bytes_completed.max(item_update.bytes_completed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use xet_data::processing::XetFileInfo;
    use xet_data::progress_tracking::ItemProgressUpdate;

    use super::*;
    use crate::xet_session::{DownloadedFile, FileMetadata};

    // ── GroupProgress unit tests ─────────────────────────────────────────────

    #[test]
    // A freshly created GroupProgress has all-zero totals and no completion rates.
    fn test_snapshot_empty_initially() {
        let p = GroupProgress::new();
        let snapshot = p.snapshot().unwrap();
        let total = snapshot.total();
        assert_eq!(total.total_bytes, 0);
        assert_eq!(total.total_bytes_completed, 0);
        assert_eq!(total.total_transfer_bytes, 0);
        assert_eq!(total.total_transfer_bytes_completed, 0);
        assert!(total.total_bytes_completion_rate.is_none());
        assert!(total.total_transfer_bytes_completion_rate.is_none());
    }

    #[test]
    // Looking up an unknown tracking ID in a snapshot returns InvalidTaskID.
    fn test_snapshot_file_with_unknown_id_returns_error() {
        let p = GroupProgress::new();
        let snapshot = p.snapshot().unwrap();
        let unknown_id = Ulid::new();
        let result = snapshot.file(unknown_id);
        assert!(matches!(result, Err(SessionError::InvalidTaskID(_))));
    }

    #[tokio::test]
    // Per-file bytes_completed uses max semantics: a stale/lower update does not reduce progress.
    async fn test_register_updates_per_file_bytes_completed_never_decreases() {
        let p = GroupProgress::new();
        let task_id = Ulid::new();

        p.register_updates(ProgressUpdate {
            total_bytes: 100,
            total_bytes_completed: 80,
            item_updates: vec![ItemProgressUpdate {
                tracking_id: task_id,
                item_name: "file.bin".into(),
                total_bytes: 100,
                bytes_completed: 80,
                bytes_completion_increment: 80,
            }],
            ..Default::default()
        })
        .await;

        // Simulate a stale/out-of-order update carrying a smaller value.
        p.register_updates(ProgressUpdate {
            total_bytes: 100,
            total_bytes_completed: 40,
            item_updates: vec![ItemProgressUpdate {
                tracking_id: task_id,
                item_name: "file.bin".into(),
                total_bytes: 100,
                bytes_completed: 40, // lower than previously seen
                bytes_completion_increment: 0,
            }],
            ..Default::default()
        })
        .await;

        let snapshot = p.snapshot().unwrap();
        let file = snapshot.file(task_id).unwrap();
        // Max semantics: should still report 80, not the lower 40.
        assert_eq!(file.bytes_completed, 80);
    }

    // ── TaskHandle unit tests ────────────────────────────────────────────────

    #[test]
    // A TaskHandle with no status Arc (streaming upload) returns an error from status().
    fn test_task_handle_with_no_status_returns_error() {
        let progress = Arc::new(GroupProgress::new());
        let handle = TaskHandle {
            status: None,
            group_progress: progress,
            task_id: Ulid::new(),
        };
        assert!(handle.status().is_err());
    }

    #[test]
    // TaskHandle::progress() returns InvalidTaskID when the task_id has no registered progress.
    fn test_task_handle_progress_for_unknown_id_returns_error() {
        let progress = Arc::new(GroupProgress::new());
        let handle = TaskHandle {
            status: None,
            group_progress: progress,
            task_id: Ulid::new(), // not registered in GroupProgress
        };
        assert!(matches!(handle.progress(), Err(SessionError::InvalidTaskID(_))));
    }

    // ── UploadTaskHandle unit tests ──────────────────────────────────────────
    //
    // `UploadTaskHandle` wraps a `TaskHandle` and adds a `result` Arc that is
    // shared with the internal `InnerUploadTaskHandle` inside `UploadCommit`.
    // After `commit()` completes, the internal handle writes the per-file
    // `UploadResult` (= `Arc<Result<FileMetadata, SessionError>>`) into that
    // shared Arc so callers can read it directly from the task handle without
    // touching the `commit()` return value.
    //
    // There are therefore two equivalent ways to retrieve a per-task result:
    //   1. `commit()` returns `HashMap<Ulid, UploadResult>`; look up the task using `handle.task_id`.
    //   2. Call `handle.result()` directly after `commit()` returns.
    //
    // The tests below exercise the `result` Arc mechanics in isolation; see
    // `upload_commit.rs` for end-to-end integration tests of both patterns.

    #[test]
    // UploadTaskHandle::result() returns None before the result Arc is populated.
    fn test_upload_task_handle_result_none_before_commit() {
        let progress = Arc::new(GroupProgress::new());
        let handle = UploadTaskHandle {
            inner: TaskHandle {
                status: None,
                group_progress: progress,
                task_id: Ulid::new(),
            },
            result: Arc::new(OnceLock::new()),
        };
        assert!(handle.result().is_none());
    }

    #[test]
    // UploadTaskHandle::result() returns the value once the shared Arc is populated.
    fn test_upload_task_handle_result_some_after_result_set() {
        let progress = Arc::new(GroupProgress::new());
        let result_arc = Arc::new(OnceLock::new());
        let handle = UploadTaskHandle {
            inner: TaskHandle {
                status: None,
                group_progress: progress,
                task_id: Ulid::new(),
            },
            result: result_arc.clone(),
        };

        // Simulate commit() writing the result.
        let metadata = Arc::new(Ok(FileMetadata {
            tracking_name: Some("file.bin".to_string()),
            hash: "abc123".to_string(),
            file_size: 42,
        }));
        result_arc.set(metadata).unwrap();

        let result = handle.result().unwrap();
        let meta = result.as_ref().as_ref().unwrap();
        assert_eq!(meta.file_size, 42);
        assert_eq!(meta.hash, "abc123");
    }

    // ── DownloadTaskHandle unit tests ────────────────────────────────────────
    //
    // `DownloadTaskHandle` follows the same Arc-sharing pattern as
    // `UploadTaskHandle`.  Its `result` field holds a `DownloadResult`
    // (= `Arc<Result<DownloadedFile, SessionError>>`), populated by `finish()`.

    #[test]
    // DownloadTaskHandle::result() returns None before finish() populates the result Arc.
    fn test_download_task_handle_result_none_before_finish() {
        let progress = Arc::new(GroupProgress::new());
        let handle = DownloadTaskHandle {
            inner: TaskHandle {
                status: None,
                group_progress: progress,
                task_id: Ulid::new(),
            },
            result: Arc::new(OnceLock::new()),
        };
        assert!(handle.result().is_none());
    }

    #[test]
    // DownloadTaskHandle::result() returns the value once the shared Arc is populated.
    fn test_download_task_handle_result_some_after_result_set() {
        let progress = Arc::new(GroupProgress::new());
        let result_arc = Arc::new(OnceLock::new());
        let handle = DownloadTaskHandle {
            inner: TaskHandle {
                status: None,
                group_progress: progress,
                task_id: Ulid::new(),
            },
            result: result_arc.clone(),
        };

        // Simulate finish() writing the result.
        let download_result = Arc::new(Ok(DownloadedFile {
            dest_path: PathBuf::from("out/file.bin"),
            file_info: XetFileInfo {
                hash: "def456".to_string(),
                file_size: 99,
            },
        }));
        result_arc.set(download_result).unwrap();

        let result = handle.result().unwrap();
        let dl = result.as_ref().as_ref().unwrap();
        assert_eq!(dl.file_info.file_size, 99);
        assert_eq!(dl.dest_path, PathBuf::from("out/file.bin"));
    }

    // ── Full register_updates test ───────────────────────────────────────────

    #[tokio::test]
    // A single register_updates call populates all aggregate fields and per-file entries correctly.
    async fn test_commit_progress_register_updates() {
        let p = GroupProgress::new();

        let file_a = (Ulid::new(), "fileA.bin");
        let file_b = (Ulid::new(), "fileB.bin");
        let update = ProgressUpdate {
            total_bytes: 1000,
            total_bytes_completed: 400,
            total_bytes_completion_rate: Some(0.4),
            total_transfer_bytes: 800,
            total_transfer_bytes_completed: 300,
            total_transfer_bytes_completion_rate: Some(0.375),
            item_updates: vec![
                ItemProgressUpdate {
                    tracking_id: file_a.0,
                    item_name: file_a.1.into(),
                    total_bytes: 500,
                    bytes_completed: 200,
                    bytes_completion_increment: 200,
                },
                ItemProgressUpdate {
                    tracking_id: file_b.0,
                    item_name: file_b.1.into(),
                    total_bytes: 500,
                    bytes_completed: 200,
                    bytes_completion_increment: 200,
                },
            ],
            ..Default::default()
        };

        p.register_updates(update).await;

        let snapshot = p.snapshot().unwrap();
        let total = snapshot.total();
        assert_eq!(total.total_bytes, 1000);
        assert_eq!(total.total_bytes_completed, 400);
        assert_eq!(total.total_bytes_completion_rate, Some(0.4));
        assert_eq!(total.total_transfer_bytes, 800);
        assert_eq!(total.total_transfer_bytes_completed, 300);
        assert_eq!(total.total_transfer_bytes_completion_rate, Some(0.375));
        let fa = snapshot.file(file_a.0).unwrap();
        assert_eq!(fa.total_bytes, 500);
        assert_eq!(fa.bytes_completed, 200);
        let fb = snapshot.file(file_b.0).unwrap();
        assert_eq!(fb.total_bytes, 500);
        assert_eq!(fb.bytes_completed, 200);
    }
}
