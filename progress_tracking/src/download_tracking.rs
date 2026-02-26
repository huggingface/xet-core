use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use more_asserts::debug_assert_le;
use ulid::Ulid;

use crate::{ItemProgressUpdate, ProgressUpdate, TrackingProgressUpdater};

/// Tracks the total progress across all download tasks.  Updates on individual download tasks
/// are forwarded to the inner progress updater, using this info to update the totals.
pub struct DownloadProgressTracker {
    inner: Arc<dyn TrackingProgressUpdater>,
    total_bytes: AtomicU64,
    total_transfer_bytes: AtomicU64,
    total_bytes_completed: AtomicU64,
    total_transfer_bytes_completed: AtomicU64,
}

impl DownloadProgressTracker {
    pub fn new(inner: Arc<dyn TrackingProgressUpdater>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            total_bytes: 0.into(),
            total_transfer_bytes: 0.into(),
            total_bytes_completed: 0.into(),
            total_transfer_bytes_completed: 0.into(),
        })
    }

    pub fn new_download_task(self: &Arc<Self>, tracking_id: Ulid, item_name: Arc<str>) -> Arc<DownloadTaskUpdater> {
        Arc::new(DownloadTaskUpdater::new(tracking_id, item_name, self.clone()))
    }

    #[inline]
    pub fn assert_complete(&self) {
        #[cfg(debug_assertions)]
        {
            assert_eq!(self.total_bytes_completed.load(Ordering::Relaxed), self.total_bytes.load(Ordering::Relaxed));
            assert_eq!(
                self.total_transfer_bytes_completed.load(Ordering::Relaxed),
                self.total_transfer_bytes.load(Ordering::Relaxed)
            );
        }
    }
}

/// The interface struct for a single file or file segment.  Holds a reference to the
/// group-level DownloadProgressTracker.
pub struct DownloadTaskUpdater {
    tracking_id: Ulid,
    item_name: Arc<str>,
    item_bytes: AtomicU64,
    item_transfer_bytes: AtomicU64,
    item_size_finalized: AtomicBool,
    bytes_completed: AtomicU64,
    transfer_bytes_completed: AtomicU64,
    tracker: Arc<DownloadProgressTracker>,
}

impl DownloadTaskUpdater {
    fn new(tracking_id: Ulid, item_name: Arc<str>, tracker: Arc<DownloadProgressTracker>) -> Self {
        Self {
            tracking_id,
            item_name,
            item_bytes: 0.into(),
            item_transfer_bytes: 0.into(),
            item_size_finalized: false.into(),
            bytes_completed: 0.into(),
            transfer_bytes_completed: 0.into(),
            tracker,
        }
    }

    #[cfg(debug_assertions)]
    pub fn correctness_verification_tracker() -> Arc<Self> {
        let null_tracker = crate::NoOpProgressUpdater::new();

        let testing_download_tracker = DownloadProgressTracker::new(null_tracker);
        testing_download_tracker.new_download_task(Ulid::new(), Arc::from(""))
    }

    /// Updates the total decompressed item size.
    ///
    /// When `is_final` is true, the provided value is treated as the definitive total and
    /// all subsequent calls to `update_item_size` are ignored.  Use `is_final = true` when
    /// the file size is known ahead of time (e.g. from metadata), and `is_final = false`
    /// when the total is being discovered incrementally during reconstruction.
    pub fn update_item_size(&self, total_item_bytes: u64, is_final: bool) {
        // If already finalized, ignore subsequent calls.
        if self.item_size_finalized.load(Ordering::Relaxed) {
            // Make sure that updates reflect known reality correctly.
            if is_final {
                debug_assert_eq!(self.item_bytes.load(Ordering::Relaxed), total_item_bytes);
            }
            return;
        }

        if is_final {
            self.item_size_finalized.store(true, Ordering::Relaxed);
        }

        let old_item_total = self.item_bytes.swap(total_item_bytes, Ordering::Release);

        if old_item_total == total_item_bytes {
            return;
        }

        // Should only increase.
        debug_assert_le!(old_item_total, total_item_bytes);

        let total_bytes_increment = total_item_bytes.saturating_sub(old_item_total);
        let old_total_bytes = self.tracker.total_bytes.fetch_add(total_bytes_increment, Ordering::Release);
        let total_bytes = old_total_bytes + total_bytes_increment;

        let item_update = ItemProgressUpdate {
            tracking_id: self.tracking_id,
            item_name: self.item_name.clone(),
            total_bytes: total_item_bytes,
            bytes_completed: self.bytes_completed.load(Ordering::Relaxed),
            bytes_completion_increment: 0,
        };

        let progress_update = ProgressUpdate {
            item_updates: vec![item_update],
            total_bytes,
            total_bytes_increment,
            total_bytes_completed: self.tracker.total_bytes_completed.load(Ordering::Relaxed),
            total_bytes_completion_increment: 0,
            total_bytes_completion_rate: None,
            total_transfer_bytes: self.tracker.total_transfer_bytes.load(Ordering::Relaxed),
            total_transfer_bytes_increment: 0,
            total_transfer_bytes_completed: self.tracker.total_transfer_bytes_completed.load(Ordering::Relaxed),
            total_transfer_bytes_completion_increment: 0,
            total_transfer_bytes_completion_rate: None,
        };

        let inner = self.tracker.inner.clone();
        tokio::spawn(async move { inner.register_updates(progress_update).await });
    }

    /// Updates the expected total transfer (network) bytes for this item.
    /// Called incrementally as xorb blocks are discovered during reconstruction.
    pub fn update_transfer_size(&self, item_transfer_bytes: u64) {
        let old_item_transfer = self.item_transfer_bytes.swap(item_transfer_bytes, Ordering::Relaxed);

        if old_item_transfer == item_transfer_bytes {
            return;
        }

        // Should only increase.
        debug_assert_le!(old_item_transfer, item_transfer_bytes);

        let total_transfer_bytes_increment = item_transfer_bytes.saturating_sub(old_item_transfer);
        let old_transfer_bytes = self
            .tracker
            .total_transfer_bytes
            .fetch_add(total_transfer_bytes_increment, Ordering::Relaxed);
        let total_transfer_bytes = old_transfer_bytes + total_transfer_bytes_increment;

        let progress_update = ProgressUpdate {
            item_updates: vec![],
            total_bytes: self.tracker.total_bytes.load(Ordering::Relaxed),
            total_bytes_increment: 0,
            total_bytes_completed: self.tracker.total_bytes_completed.load(Ordering::Relaxed),
            total_bytes_completion_increment: 0,
            total_bytes_completion_rate: None,
            total_transfer_bytes,
            total_transfer_bytes_increment,
            total_transfer_bytes_completed: self.tracker.total_transfer_bytes_completed.load(Ordering::Relaxed),
            total_transfer_bytes_completion_increment: 0,
            total_transfer_bytes_completion_rate: None,
        };

        let inner = self.tracker.inner.clone();
        tokio::spawn(async move { inner.register_updates(progress_update).await });
    }

    pub fn total_bytes_completed(&self) -> u64 {
        self.bytes_completed.load(Ordering::Relaxed)
    }

    /// Reports decompressed bytes written to disk.
    pub fn report_bytes_written(&self, increment: u64) {
        if increment == 0 {
            return;
        }

        let item_total_bytes = self.item_bytes.load(Ordering::Acquire);
        let old_completed = self.bytes_completed.fetch_add(increment, Ordering::Relaxed);
        let bytes_completed = old_completed + increment;

        if item_total_bytes > 0 {
            debug_assert_le!(bytes_completed, item_total_bytes);
        }

        let global_old_completed = self.tracker.total_bytes_completed.fetch_add(increment, Ordering::Relaxed);
        let total_bytes_completed = global_old_completed + increment;

        let total_bytes = self.tracker.total_bytes.load(Ordering::Acquire);
        debug_assert_le!(total_bytes_completed, total_bytes);

        let item_progress_update = ItemProgressUpdate {
            tracking_id: self.tracking_id,
            item_name: self.item_name.clone(),
            total_bytes: item_total_bytes,
            bytes_completed,
            bytes_completion_increment: increment,
        };

        let progress_update = ProgressUpdate {
            item_updates: vec![item_progress_update],
            total_bytes,
            total_bytes_increment: 0,
            total_bytes_completed,
            total_bytes_completion_increment: increment,
            total_bytes_completion_rate: None,
            total_transfer_bytes: self.tracker.total_transfer_bytes.load(Ordering::Relaxed),
            total_transfer_bytes_increment: 0,
            total_transfer_bytes_completed: self.tracker.total_transfer_bytes_completed.load(Ordering::Relaxed),
            total_transfer_bytes_completion_increment: 0,
            total_transfer_bytes_completion_rate: None,
        };

        let inner = self.tracker.inner.clone();
        tokio::spawn(async move { inner.register_updates(progress_update).await });
    }

    /// Reports transfer (network) bytes downloaded.
    pub fn report_transfer_progress(&self, transfer_increment: u64) {
        if transfer_increment == 0 {
            return;
        }

        let item_total_transfer_bytes = self.item_transfer_bytes.load(Ordering::Relaxed);
        let old_transfer_completed = self.transfer_bytes_completed.fetch_add(transfer_increment, Ordering::Relaxed);
        let transfer_bytes_completed = old_transfer_completed + transfer_increment;

        if item_total_transfer_bytes > 0 {
            debug_assert_le!(transfer_bytes_completed, item_total_transfer_bytes);
        }

        let global_old_transfer_completed = self
            .tracker
            .total_transfer_bytes_completed
            .fetch_add(transfer_increment, Ordering::Relaxed);
        let total_transfer_bytes_completed = global_old_transfer_completed + transfer_increment;

        let total_transfer_bytes = self.tracker.total_transfer_bytes.load(Ordering::Relaxed);
        debug_assert_le!(total_transfer_bytes_completed, total_transfer_bytes);

        let progress_update = ProgressUpdate {
            item_updates: vec![],
            total_bytes: self.tracker.total_bytes.load(Ordering::Relaxed),
            total_bytes_increment: 0,
            total_bytes_completed: self.tracker.total_bytes_completed.load(Ordering::Relaxed),
            total_bytes_completion_increment: 0,
            total_bytes_completion_rate: None,
            total_transfer_bytes,
            total_transfer_bytes_increment: 0,
            total_transfer_bytes_completed,
            total_transfer_bytes_completion_increment: transfer_increment,
            total_transfer_bytes_completion_rate: None,
        };

        let inner = self.tracker.inner.clone();
        tokio::spawn(async move { inner.register_updates(progress_update).await });
    }

    #[inline]
    pub fn assert_complete(&self) {
        #[cfg(debug_assertions)]
        {
            assert_eq!(self.bytes_completed.load(Ordering::Relaxed), self.item_bytes.load(Ordering::Relaxed));
            assert_eq!(
                self.transfer_bytes_completed.load(Ordering::Relaxed),
                self.item_transfer_bytes.load(Ordering::Relaxed)
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NoOpProgressUpdater;

    fn make_task(name: &str) -> (Arc<DownloadProgressTracker>, Arc<DownloadTaskUpdater>) {
        let tracker = DownloadProgressTracker::new(NoOpProgressUpdater::new());
        let task = tracker.new_download_task(Ulid::new(), Arc::from(name));
        (tracker, task)
    }

    // ==================== update_item_size tests ====================

    #[tokio::test]
    async fn test_update_item_size_monotonic_increase() {
        let (tracker, task) = make_task("file.bin");

        task.update_item_size(100, false);
        assert_eq!(task.item_bytes.load(Ordering::Relaxed), 100);
        assert_eq!(tracker.total_bytes.load(Ordering::Relaxed), 100);

        task.update_item_size(300, false);
        assert_eq!(task.item_bytes.load(Ordering::Relaxed), 300);
        assert_eq!(tracker.total_bytes.load(Ordering::Relaxed), 300);
    }

    #[tokio::test]
    async fn test_update_item_size_same_value_is_noop() {
        let (tracker, task) = make_task("file.bin");

        task.update_item_size(1000, false);
        assert_eq!(tracker.total_bytes.load(Ordering::Relaxed), 1000);

        task.update_item_size(1000, false);
        assert_eq!(tracker.total_bytes.load(Ordering::Relaxed), 1000);
    }

    #[tokio::test]
    async fn test_update_item_size_final_ignores_subsequent_calls() {
        let (tracker, task) = make_task("file.bin");

        // Set final size.
        task.update_item_size(1000, true);
        assert_eq!(task.item_bytes.load(Ordering::Relaxed), 1000);
        assert_eq!(tracker.total_bytes.load(Ordering::Relaxed), 1000);

        // Subsequent non-final calls are ignored.
        task.update_item_size(500, false);
        assert_eq!(task.item_bytes.load(Ordering::Relaxed), 1000);
        assert_eq!(tracker.total_bytes.load(Ordering::Relaxed), 1000);

        // A second is_final call with the same value is also ignored (no-op).
        task.update_item_size(1000, true);
        assert_eq!(task.item_bytes.load(Ordering::Relaxed), 1000);
        assert_eq!(tracker.total_bytes.load(Ordering::Relaxed), 1000);
    }

    #[tokio::test]
    async fn test_update_item_size_non_final_then_final() {
        let (tracker, task) = make_task("file.bin");

        // Incremental updates (non-final).
        task.update_item_size(200, false);
        task.update_item_size(500, false);
        assert_eq!(tracker.total_bytes.load(Ordering::Relaxed), 500);

        // Final update locks it in.
        task.update_item_size(1000, true);
        assert_eq!(tracker.total_bytes.load(Ordering::Relaxed), 1000);

        // Further calls ignored.
        task.update_item_size(1500, false);
        assert_eq!(tracker.total_bytes.load(Ordering::Relaxed), 1000);
    }

    #[tokio::test]
    #[should_panic(expected = "left <= right")]
    async fn test_update_item_size_decrease_panics_in_debug() {
        let (_tracker, task) = make_task("file.bin");

        task.update_item_size(1000, false);
        task.update_item_size(100, false);
    }

    // ==================== update_transfer_size tests ====================

    #[tokio::test]
    async fn test_update_transfer_size_monotonic_increase() {
        let (tracker, task) = make_task("file.bin");

        task.update_transfer_size(100);
        assert_eq!(task.item_transfer_bytes.load(Ordering::Relaxed), 100);
        assert_eq!(tracker.total_transfer_bytes.load(Ordering::Relaxed), 100);

        task.update_transfer_size(300);
        assert_eq!(task.item_transfer_bytes.load(Ordering::Relaxed), 300);
        assert_eq!(tracker.total_transfer_bytes.load(Ordering::Relaxed), 300);
    }

    #[tokio::test]
    async fn test_update_transfer_size_same_value_is_noop() {
        let (tracker, task) = make_task("file.bin");

        task.update_transfer_size(500);
        assert_eq!(tracker.total_transfer_bytes.load(Ordering::Relaxed), 500);

        task.update_transfer_size(500);
        assert_eq!(tracker.total_transfer_bytes.load(Ordering::Relaxed), 500);
    }

    // ==================== report_bytes_written tests ====================

    #[tokio::test]
    async fn test_report_bytes_written_accumulates() {
        let (tracker, task) = make_task("file.bin");
        task.update_item_size(1000, true);

        task.report_bytes_written(200);
        assert_eq!(task.total_bytes_completed(), 200);
        assert_eq!(tracker.total_bytes_completed.load(Ordering::Relaxed), 200);

        task.report_bytes_written(800);
        assert_eq!(task.total_bytes_completed(), 1000);
        assert_eq!(tracker.total_bytes_completed.load(Ordering::Relaxed), 1000);
    }

    #[tokio::test]
    async fn test_report_bytes_written_zero_is_noop() {
        let (tracker, task) = make_task("file.bin");
        task.update_item_size(100, false);

        task.report_bytes_written(0);
        assert_eq!(task.total_bytes_completed(), 0);
        assert_eq!(tracker.total_bytes_completed.load(Ordering::Relaxed), 0);
    }

    // ==================== report_transfer_progress tests ====================

    #[tokio::test]
    async fn test_report_transfer_progress_accumulates() {
        let (tracker, task) = make_task("file.bin");
        task.update_transfer_size(500);

        task.report_transfer_progress(100);
        assert_eq!(task.transfer_bytes_completed.load(Ordering::Relaxed), 100);
        assert_eq!(tracker.total_transfer_bytes_completed.load(Ordering::Relaxed), 100);

        task.report_transfer_progress(400);
        assert_eq!(task.transfer_bytes_completed.load(Ordering::Relaxed), 500);
        assert_eq!(tracker.total_transfer_bytes_completed.load(Ordering::Relaxed), 500);
    }

    #[tokio::test]
    async fn test_report_transfer_progress_zero_is_noop() {
        let (tracker, task) = make_task("file.bin");
        task.update_transfer_size(100);

        task.report_transfer_progress(0);
        assert_eq!(task.transfer_bytes_completed.load(Ordering::Relaxed), 0);
        assert_eq!(tracker.total_transfer_bytes_completed.load(Ordering::Relaxed), 0);
    }

    // ==================== Combined flow tests ====================

    #[tokio::test]
    async fn test_full_flow_incremental_discovery() {
        // Simulates the reconstruction flow where totals are discovered incrementally
        // by the manager, while the writer and xorb_block report progress separately.
        let (tracker, task) = make_task("file.bin");

        // First batch: discover 500 decompressed bytes, 300 transfer bytes.
        task.update_item_size(500, false);
        task.update_transfer_size(300);

        // Writer reports decompressed bytes; xorb reports transfer bytes.
        task.report_bytes_written(500);
        task.report_transfer_progress(300);

        // Second batch: totals grow.
        task.update_item_size(1000, false);
        task.update_transfer_size(700);

        task.report_bytes_written(500);
        task.report_transfer_progress(400);

        assert_eq!(task.total_bytes_completed(), 1000);
        assert_eq!(task.transfer_bytes_completed.load(Ordering::Relaxed), 700);

        task.assert_complete();
        tracker.assert_complete();
    }

    #[tokio::test]
    async fn test_full_flow_final_size_upfront() {
        // Simulates the data_client.rs flow: file size is known ahead of time (is_final=true),
        // transfer size is discovered incrementally by the manager.
        let (tracker, task) = make_task("file.bin");

        // data_client.rs: size known upfront.
        task.update_item_size(1000, true);
        assert_eq!(tracker.total_bytes.load(Ordering::Relaxed), 1000);

        // manager.rs discovers transfer sizes incrementally.
        task.update_transfer_size(300);
        task.update_transfer_size(700);

        // manager.rs also tries to set item_size, but it's ignored because final was set.
        task.update_item_size(500, false);
        assert_eq!(task.item_bytes.load(Ordering::Relaxed), 1000);

        // Writer reports bytes written; xorb reports transfer bytes.
        task.report_bytes_written(600);
        task.report_bytes_written(400);
        task.report_transfer_progress(300);
        task.report_transfer_progress(400);

        assert_eq!(task.total_bytes_completed(), 1000);
        assert_eq!(task.transfer_bytes_completed.load(Ordering::Relaxed), 700);

        task.assert_complete();
        tracker.assert_complete();
    }

    #[tokio::test]
    async fn test_interleaved_totals_and_progress() {
        let (tracker, task) = make_task("file.bin");

        // First batch discovered.
        task.update_item_size(400, false);
        task.update_transfer_size(200);

        // Start writing from first batch.
        task.report_bytes_written(200);
        task.report_transfer_progress(100);

        // Second batch discovered.
        task.update_item_size(800, false);
        task.update_transfer_size(500);

        // More progress.
        task.report_bytes_written(400);
        task.report_transfer_progress(250);

        // Final batch.
        task.update_item_size(1000, false);
        task.update_transfer_size(600);

        // Finish remaining.
        task.report_bytes_written(400);
        task.report_transfer_progress(250);

        assert_eq!(task.total_bytes_completed(), 1000);
        assert_eq!(task.transfer_bytes_completed.load(Ordering::Relaxed), 600);
        assert_eq!(tracker.total_bytes.load(Ordering::Relaxed), 1000);
        assert_eq!(tracker.total_transfer_bytes.load(Ordering::Relaxed), 600);

        task.assert_complete();
        tracker.assert_complete();
    }

    #[tokio::test]
    async fn test_transfer_bytes_less_than_total_bytes() {
        let (tracker, task) = make_task("file.bin");

        task.update_item_size(10000, true);
        task.update_transfer_size(3000);

        task.report_bytes_written(10000);
        task.report_transfer_progress(3000);

        assert_eq!(tracker.total_bytes.load(Ordering::Relaxed), 10000);
        assert_eq!(tracker.total_transfer_bytes.load(Ordering::Relaxed), 3000);

        task.assert_complete();
        tracker.assert_complete();
    }

    #[tokio::test]
    async fn test_multiple_tasks_independent_tracking() {
        let tracker = DownloadProgressTracker::new(NoOpProgressUpdater::new());
        let task1 = tracker.new_download_task(Ulid::new(), Arc::from("file1.bin"));
        let task2 = tracker.new_download_task(Ulid::new(), Arc::from("file2.bin"));

        task1.update_item_size(500, true);
        task1.update_transfer_size(200);
        task2.update_item_size(300, false);
        task2.update_transfer_size(100);

        assert_eq!(tracker.total_bytes.load(Ordering::Relaxed), 800);
        assert_eq!(tracker.total_transfer_bytes.load(Ordering::Relaxed), 300);

        task1.report_bytes_written(500);
        task1.report_transfer_progress(200);
        task2.report_bytes_written(300);
        task2.report_transfer_progress(100);

        task1.assert_complete();
        task2.assert_complete();
        tracker.assert_complete();
    }
}
