use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use more_asserts::debug_assert_le;

use crate::{ItemProgressUpdate, NoOpProgressUpdater, ProgressUpdate, TrackingProgressUpdater};

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

    pub fn new_download_task(self: &Arc<Self>, item_name: Arc<str>) -> Arc<DownloadTaskUpdater> {
        Arc::new(DownloadTaskUpdater::new(item_name, self.clone()))
    }

    pub fn assert_complete(&self) {
        assert_eq!(self.total_bytes_completed.load(Ordering::Relaxed), self.total_bytes.load(Ordering::Relaxed));
        assert_eq!(
            self.total_transfer_bytes_completed.load(Ordering::Relaxed),
            self.total_transfer_bytes.load(Ordering::Relaxed)
        );
    }
}

/// The interface struct for a single file or file segment.  Holds a reference to the
/// group-level DownloadProgressTracker.  
pub struct DownloadTaskUpdater {
    item_name: Arc<str>,
    item_bytes: AtomicU64,
    item_transfer_bytes: AtomicU64,
    bytes_completed: AtomicU64,
    transfer_bytes_completed: AtomicU64,
    tracker: Arc<DownloadProgressTracker>,
}

impl DownloadTaskUpdater {
    fn new(item_name: Arc<str>, tracker: Arc<DownloadProgressTracker>) -> Self {
        Self {
            item_name,
            item_bytes: 0.into(),
            item_transfer_bytes: 0.into(),
            bytes_completed: 0.into(),
            transfer_bytes_completed: 0.into(),
            tracker,
        }
    }

    pub fn correctness_verification_tracker() -> Arc<Self> {
        let null_tracker = NoOpProgressUpdater::new();

        let testing_download_tracker = DownloadProgressTracker::new(null_tracker);
        testing_download_tracker.new_download_task(Arc::from(""))
    }

    pub fn update_totals(&self, total_item_bytes: u64, item_transfer_bytes: u64) {
        let old_item_total = self.item_bytes.swap(total_item_bytes, Ordering::Relaxed);
        let old_item_transfer = self.item_transfer_bytes.swap(item_transfer_bytes, Ordering::Relaxed);

        if old_item_total == total_item_bytes && old_item_transfer == item_transfer_bytes {
            return;
        }
        // These should only increase.
        debug_assert_le!(old_item_total, total_item_bytes);
        debug_assert_le!(old_item_transfer, item_transfer_bytes);

        // The total bytes.
        let total_bytes_increment = total_item_bytes.saturating_sub(old_item_total);
        let old_total_bytes = self.tracker.total_bytes.fetch_add(total_bytes_increment, Ordering::Relaxed);
        let total_bytes = old_total_bytes + total_bytes_increment;

        // The total transfer bytes.
        let total_transfer_bytes_increment = item_transfer_bytes.saturating_sub(old_item_transfer);
        let old_transfer_bytes = self
            .tracker
            .total_transfer_bytes
            .fetch_add(total_transfer_bytes_increment, Ordering::Relaxed);
        let total_transfer_bytes = old_transfer_bytes + total_transfer_bytes_increment;

        // The item update, if appropriate.
        let item_updates = if old_item_total != total_item_bytes {
            vec![ItemProgressUpdate {
                item_name: self.item_name.clone(),
                total_bytes: total_item_bytes,
                bytes_completed: self.bytes_completed.load(Ordering::Relaxed),
                bytes_completion_increment: 0,
            }]
        } else {
            vec![]
        };

        let progress_update = ProgressUpdate {
            item_updates,
            total_bytes,
            total_bytes_increment,
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

    pub fn update_progress(&self, increment: u64, transfer_increment: u64) {
        if increment == 0 && transfer_increment == 0 {
            return;
        }

        let item_total_bytes = self.item_bytes.load(Ordering::Relaxed);
        let item_total_transfer_bytes = self.item_transfer_bytes.load(Ordering::Relaxed);
        let old_completed = self.bytes_completed.fetch_add(increment, Ordering::Relaxed);
        let old_transfer_completed = self.transfer_bytes_completed.fetch_add(transfer_increment, Ordering::Relaxed);
        let bytes_completed = old_completed + increment;
        let transfer_bytes_completed = old_transfer_completed + transfer_increment;

        if item_total_bytes > 0 {
            debug_assert_le!(bytes_completed, item_total_bytes);
        }

        if item_total_transfer_bytes > 0 {
            debug_assert_le!(transfer_bytes_completed, item_total_transfer_bytes);
        }

        let global_old_completed = self.tracker.total_bytes_completed.fetch_add(increment, Ordering::Relaxed);
        let global_old_transfer_completed = self
            .tracker
            .total_transfer_bytes_completed
            .fetch_add(transfer_increment, Ordering::Relaxed);
        let total_bytes_completed = global_old_completed + increment;
        let total_transfer_bytes_completed = global_old_transfer_completed + transfer_increment;

        let total_bytes = self.tracker.total_bytes.load(Ordering::Relaxed);
        let total_transfer_bytes = self.tracker.total_transfer_bytes.load(Ordering::Relaxed);

        debug_assert_le!(total_bytes_completed, total_bytes);
        debug_assert_le!(total_transfer_bytes_completed, total_transfer_bytes);

        let item_progress_update = ItemProgressUpdate {
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
