//! Progress tracking without callbacks (GIL-free)

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use progress_tracking::{ProgressUpdate, TrackingProgressUpdater};

/// Lock-free progress tracker using atomic integers.
///
/// Designed to be shared between upload/download tasks and their callers.
/// All reads are non-blocking so Python can poll progress without acquiring the GIL.
pub struct AtomicProgress {
    bytes_completed: AtomicU64,
    bytes_total: AtomicU64,
}

impl AtomicProgress {
    /// Create a new progress tracker with all counters at zero.
    pub fn new() -> Self {
        Self {
            bytes_completed: AtomicU64::new(0),
            bytes_total: AtomicU64::new(0),
        }
    }

    /// Return the number of bytes processed so far.
    pub fn get_completed(&self) -> u64 {
        self.bytes_completed.load(Ordering::Relaxed)
    }

    /// Return the total number of bytes to process.
    pub fn get_total(&self) -> u64 {
        self.bytes_total.load(Ordering::Relaxed)
    }

    /// Increment the completed-bytes counter.
    pub fn add_completed(&self, bytes: u64) {
        self.bytes_completed.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Set the total-bytes counter.
    pub fn set_total(&self, total: u64) {
        self.bytes_total.store(total, Ordering::Relaxed);
    }
}

impl Default for AtomicProgress {
    fn default() -> Self {
        Self::new()
    }
}

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

/// Bridges the `data` crate's progress-tracking callback into [`AtomicProgress`].
///
/// Implements [`TrackingProgressUpdater`] so it can be passed to `FileDownloader`
/// — without ever touching the Python GIL.
pub struct AtomicProgressUpdater {
    progress: Arc<AtomicProgress>,
}

impl AtomicProgressUpdater {
    /// Create a new updater that writes into `progress`.
    pub fn new(progress: Arc<AtomicProgress>) -> Self {
        Self { progress }
    }
}

#[async_trait]
impl TrackingProgressUpdater for AtomicProgressUpdater {
    async fn register_updates(&self, updates: ProgressUpdate) {
        self.progress.set_total(updates.total_bytes);
        self.progress
            .bytes_completed
            .store(updates.total_bytes_completed, Ordering::Relaxed);
    }
}

// ── Aggregate / per-file progress ───────────────────────────────────────────

/// Snapshot of aggregate progress returned by [`CommitProgress::total`].
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

/// Snapshot of a single file's progress returned by [`CommitProgress::files`].
#[derive(Clone, Debug)]
pub struct FileProgressSnapshot {
    /// File name as reported by the data layer.
    pub item_name: Arc<str>,
    /// Total size of this file in bytes.
    pub total_bytes: u64,
    /// Bytes of this file processed so far.
    pub bytes_completed: u64,
}

/// Tracks per-file and aggregate upload progress received from [`FileUploadSession`].
///
/// Implements [`TrackingProgressUpdater`] so it can be passed directly to
/// [`FileUploadSession::new`](data::FileUploadSession::new).
///
/// - Call [`CommitProgress::total`] for an aggregate snapshot (lock-free reads).
/// - Call [`CommitProgress::files`] for a per-file breakdown.
///
/// All integer counters are stored as [`AtomicU64`]; floating-point completion
/// rates use a `Mutex` (rarely written, never held across await points).
pub struct CommitProgress {
    // Aggregate totals — lock-free reads
    total_bytes: AtomicU64,
    total_bytes_completed: AtomicU64,
    total_transfer_bytes: AtomicU64,
    total_transfer_bytes_completed: AtomicU64,

    // Completion rates — rarely written, never held across await
    total_bytes_completion_rate: Mutex<Option<f64>>,
    total_transfer_bytes_completion_rate: Mutex<Option<f64>>,

    // Per-file: item_name → (total_bytes, bytes_completed)
    items: Mutex<HashMap<Arc<str>, (u64, u64)>>,
}

impl CommitProgress {
    /// Create a new tracker with all counters at zero.
    pub fn new() -> Self {
        Self {
            total_bytes: AtomicU64::new(0),
            total_bytes_completed: AtomicU64::new(0),
            total_transfer_bytes: AtomicU64::new(0),
            total_transfer_bytes_completed: AtomicU64::new(0),
            total_bytes_completion_rate: Mutex::new(None),
            total_transfer_bytes_completion_rate: Mutex::new(None),
            items: Mutex::new(HashMap::new()),
        }
    }

    /// Return a snapshot of aggregate progress.
    ///
    /// Integer fields are read atomically (no lock); rate fields require a brief
    /// lock acquisition.
    pub fn total(&self) -> TotalProgressSnapshot {
        TotalProgressSnapshot {
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            total_bytes_completed: self.total_bytes_completed.load(Ordering::Relaxed),
            total_bytes_completion_rate: self
                .total_bytes_completion_rate
                .lock()
                .ok()
                .and_then(|g| *g),
            total_transfer_bytes: self.total_transfer_bytes.load(Ordering::Relaxed),
            total_transfer_bytes_completed: self.total_transfer_bytes_completed.load(Ordering::Relaxed),
            total_transfer_bytes_completion_rate: self
                .total_transfer_bytes_completion_rate
                .lock()
                .ok()
                .and_then(|g| *g),
        }
    }

    /// Return a per-file progress snapshot for every file seen so far.
    pub fn files(&self) -> Vec<FileProgressSnapshot> {
        match self.items.lock() {
            Ok(items) => items
                .iter()
                .map(|(name, &(total_bytes, bytes_completed))| FileProgressSnapshot {
                    item_name: name.clone(),
                    total_bytes,
                    bytes_completed,
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }
}

impl Default for CommitProgress {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TrackingProgressUpdater for CommitProgress {
    async fn register_updates(&self, updates: ProgressUpdate) {
        // Update aggregate integer counters atomically.
        self.total_bytes.store(updates.total_bytes, Ordering::Relaxed);
        self.total_bytes_completed
            .store(updates.total_bytes_completed, Ordering::Relaxed);
        self.total_transfer_bytes
            .store(updates.total_transfer_bytes, Ordering::Relaxed);
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
        if let Ok(mut items) = self.items.lock() {
            for item_update in updates.item_updates {
                let entry = items.entry(item_update.item_name).or_insert((0, 0));
                entry.0 = entry.0.max(item_update.total_bytes);
                entry.1 = entry.1.max(item_update.bytes_completed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_progress_initial_values() {
        let p = AtomicProgress::new();
        assert_eq!(p.get_completed(), 0);
        assert_eq!(p.get_total(), 0);
    }

    #[test]
    fn test_atomic_progress_add_completed() {
        let p = AtomicProgress::new();
        p.add_completed(100);
        assert_eq!(p.get_completed(), 100);
        p.add_completed(50);
        assert_eq!(p.get_completed(), 150);
    }

    #[test]
    fn test_atomic_progress_set_total() {
        let p = AtomicProgress::new();
        p.set_total(1024);
        assert_eq!(p.get_total(), 1024);
        p.set_total(2048);
        assert_eq!(p.get_total(), 2048);
    }

    #[test]
    fn test_atomic_progress_default() {
        let p = AtomicProgress::default();
        assert_eq!(p.get_completed(), 0);
        assert_eq!(p.get_total(), 0);
    }

    #[test]
    fn test_task_status_equality() {
        assert_eq!(TaskStatus::Queued, TaskStatus::Queued);
        assert_eq!(TaskStatus::Running, TaskStatus::Running);
        assert_eq!(TaskStatus::Completed, TaskStatus::Completed);
        assert_eq!(TaskStatus::Failed, TaskStatus::Failed);
        assert_eq!(TaskStatus::Cancelled, TaskStatus::Cancelled);
        assert_ne!(TaskStatus::Queued, TaskStatus::Running);
        assert_ne!(TaskStatus::Completed, TaskStatus::Failed);
    }

    #[test]
    fn test_task_status_copy() {
        let s = TaskStatus::Running;
        let s2 = s; // Copy
        assert_eq!(s, s2);
    }

    #[test]
    fn test_commit_progress_initial_values() {
        let p = CommitProgress::new();
        let total = p.total();
        assert_eq!(total.total_bytes, 0);
        assert_eq!(total.total_bytes_completed, 0);
        assert!(total.total_bytes_completion_rate.is_none());
        assert_eq!(total.total_transfer_bytes, 0);
        assert_eq!(total.total_transfer_bytes_completed, 0);
        assert!(total.total_transfer_bytes_completion_rate.is_none());
        assert!(p.files().is_empty());
    }

    #[tokio::test]
    async fn test_commit_progress_register_updates() {
        use progress_tracking::ItemProgressUpdate;
        use std::sync::Arc;

        let p = CommitProgress::new();

        let update = ProgressUpdate {
            total_bytes: 1000,
            total_bytes_completed: 400,
            total_bytes_completion_rate: Some(0.4),
            total_transfer_bytes: 800,
            total_transfer_bytes_completed: 300,
            total_transfer_bytes_completion_rate: Some(0.375),
            item_updates: vec![
                ItemProgressUpdate {
                    item_name: Arc::from("file_a.bin"),
                    total_bytes: 500,
                    bytes_completed: 200,
                    bytes_completion_increment: 200,
                },
                ItemProgressUpdate {
                    item_name: Arc::from("file_b.bin"),
                    total_bytes: 500,
                    bytes_completed: 200,
                    bytes_completion_increment: 200,
                },
            ],
            ..Default::default()
        };

        p.register_updates(update).await;

        let total = p.total();
        assert_eq!(total.total_bytes, 1000);
        assert_eq!(total.total_bytes_completed, 400);
        assert_eq!(total.total_bytes_completion_rate, Some(0.4));
        assert_eq!(total.total_transfer_bytes, 800);
        assert_eq!(total.total_transfer_bytes_completed, 300);
        assert_eq!(total.total_transfer_bytes_completion_rate, Some(0.375));

        let files = p.files();
        assert_eq!(files.len(), 2);
        let find = |name: &str| files.iter().find(|f| f.item_name.as_ref() == name).cloned();
        let fa = find("file_a.bin").unwrap();
        assert_eq!(fa.total_bytes, 500);
        assert_eq!(fa.bytes_completed, 200);
        let fb = find("file_b.bin").unwrap();
        assert_eq!(fb.total_bytes, 500);
        assert_eq!(fb.bytes_completed, 200);
    }
}
