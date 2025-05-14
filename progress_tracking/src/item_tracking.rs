use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use more_asserts::debug_assert_le;

use crate::progress_info::{ProgressUpdate, ProgressUpdateBatch};
use crate::TrackingProgressUpdater;

/// This wraps a TrackingProgressUpdater, translating per-item updates to a full progress report.
#[derive(Debug)]
pub struct ItemProgressUpdater {
    total_bytes: AtomicU64,
    total_bytes_completed: AtomicU64,

    inner: Arc<dyn TrackingProgressUpdater>,
}

impl ItemProgressUpdater {
    pub fn new(inner: Arc<dyn TrackingProgressUpdater>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            total_bytes: 0.into(),
            total_bytes_completed: 0.into(),
        })
    }

    /// Cut a specific tracker for an individual item that is part of a collection of items.
    pub fn item_tracker(
        self: &Arc<Self>,
        item_name: Arc<str>,
        total_bytes: Option<u64>,
    ) -> Arc<SingleItemProgressUpdater> {
        if let Some(b) = total_bytes {
            self.total_bytes.fetch_add(b, Ordering::Relaxed);
        }

        Arc::new(SingleItemProgressUpdater {
            item_name,
            byte_count: 0.into(),
            completed_count: 0.into(),
            inner: self.clone(),
        })
    }

    async fn do_item_update(self: Arc<Self>, progress_update: ProgressUpdate) {
        let update_increment = progress_update.update_increment;

        let total_bytes_completed_old = self
            .total_bytes_completed
            .fetch_add(progress_update.update_increment, Ordering::Relaxed);

        self.inner
            .register_updates(ProgressUpdateBatch {
                item_updates: vec![progress_update],
                total_bytes: self.total_bytes.load(Ordering::Relaxed),
                total_bytes_completed: total_bytes_completed_old + update_increment,
                total_bytes_completion_increment: update_increment,
            })
            .await;
    }

    fn adjust_total_bytes(self: &Arc<Self>, increase_byte_total: u64) {
        self.total_bytes.fetch_add(increase_byte_total, Ordering::Relaxed);
    }
}

/// This struct allows us to wrap the larger progress updater in a simple form for
/// specific items.
#[derive(Debug)]
pub struct SingleItemProgressUpdater {
    item_name: Arc<str>,
    byte_count: AtomicU64,
    completed_count: AtomicU64,
    inner: Arc<ItemProgressUpdater>,
}

/// In case we just want to
impl SingleItemProgressUpdater {
    pub async fn update(&self, increment: u64) {
        let old_completed_count = self.completed_count.fetch_add(increment, Ordering::Relaxed);

        self.inner
            .clone()
            .do_item_update(ProgressUpdate {
                item_name: self.item_name.clone(),
                total_count: self.byte_count.load(Ordering::Relaxed),
                completed_count: old_completed_count + increment,
                update_increment: increment,
            })
            .await;
    }

    pub async fn set_total(&self, n_bytes: u64) {
        let old_value = self.byte_count.swap(n_bytes, Ordering::Relaxed);

        // Should only increment stuff here.
        debug_assert_le!(old_value, n_bytes);

        if old_value != n_bytes {
            self.inner.adjust_total_bytes(old_value - n_bytes);
        }
    }
}
