//! Progress tracking without callbacks (GIL-free)

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use async_trait::async_trait;
use progress_tracking::{TrackingProgressUpdater, ProgressUpdate};

/// Lock-free atomic progress tracker
pub struct AtomicProgress {
    bytes_completed: AtomicU64,
    bytes_total: AtomicU64,
}

impl AtomicProgress {
    pub fn new() -> Self {
        Self {
            bytes_completed: AtomicU64::new(0),
            bytes_total: AtomicU64::new(0),
        }
    }

    pub fn get_completed(&self) -> u64 {
        self.bytes_completed.load(Ordering::Relaxed)
    }

    pub fn get_total(&self) -> u64 {
        self.bytes_total.load(Ordering::Relaxed)
    }

    pub fn add_completed(&self, bytes: u64) {
        self.bytes_completed.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn set_total(&self, total: u64) {
        self.bytes_total.store(total, Ordering::Relaxed);
    }
}

impl Default for AtomicProgress {
    fn default() -> Self {
        Self::new()
    }
}

/// Task status
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// Progress updater that updates atomics instead of calling Python callbacks
pub struct AtomicProgressUpdater {
    progress: Arc<AtomicProgress>,
}

impl AtomicProgressUpdater {
    pub fn new(progress: Arc<AtomicProgress>) -> Self {
        Self { progress }
    }
}

#[async_trait]
impl TrackingProgressUpdater for AtomicProgressUpdater {
    async fn register_updates(&self, updates: ProgressUpdate) {
        // Update the atomic progress counters based on the ProgressUpdate
        self.progress.set_total(updates.total_bytes);
        self.progress.bytes_completed.store(updates.total_bytes_completed, Ordering::Relaxed);
    }
}
