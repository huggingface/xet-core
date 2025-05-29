use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use more_asserts::debug_assert_le;

use crate::progress_info::{ItemProgressUpdate, ProgressUpdate};
use crate::TrackingProgressUpdater;

/// This wraps a TrackingProgressUpdater, translating per-item updates to a full progress report.
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
            n_bytes: 0.into(),
            completed_count: 0.into(),
            inner: self.clone(),
        })
    }

    async fn do_item_update(self: Arc<Self>, progress_update: ItemProgressUpdate) {
        let update_increment = progress_update.bytes_completion_increment;

        // For now, with this simple interface, just track both process and transfer updates as
        // exactly the same.  A later PR can split these out in the download path.

        let total_bytes_completed_old = self.total_bytes_completed.fetch_add(update_increment, Ordering::Relaxed);

        let total_bytes = self.total_bytes.load(Ordering::Relaxed);
        let total_bytes_completed = total_bytes_completed_old + update_increment;

        self.inner
            .register_updates(ProgressUpdate {
                item_updates: vec![progress_update],
                total_bytes,
                total_bytes_increment: 0,
                total_bytes_completed,
                total_bytes_completion_increment: update_increment,
                total_transfer_bytes: total_bytes,
                total_transfer_bytes_increment: 0,
                total_transfer_bytes_completed: total_bytes_completed,
                total_transfer_bytes_completion_increment: update_increment,
                ..Default::default()
            })
            .await;
    }

    async fn adjust_total_bytes(self: &Arc<Self>, increase_byte_total: u64) {
        let total_process_bytes_old = self.total_bytes.fetch_add(increase_byte_total, Ordering::Relaxed);

        let total_bytes = total_process_bytes_old + increase_byte_total;
        let total_bytes_completed = self.total_bytes_completed.load(Ordering::Relaxed);

        self.inner
            .register_updates(ProgressUpdate {
                item_updates: vec![],
                total_bytes,
                total_bytes_increment: increase_byte_total,
                total_bytes_completed,
                total_bytes_completion_increment: 0,
                total_transfer_bytes: total_bytes,
                total_transfer_bytes_increment: increase_byte_total,
                total_transfer_bytes_completed: total_bytes_completed,
                total_transfer_bytes_completion_increment: 0,
                ..Default::default()
            })
            .await;
    }
}

/// This struct allows us to wrap the larger progress updater in a simple form for
/// specific items.
pub struct SingleItemProgressUpdater {
    item_name: Arc<str>,
    n_bytes: AtomicU64,
    completed_count: AtomicU64,
    inner: Arc<ItemProgressUpdater>,
}

/// In case we just want to
impl SingleItemProgressUpdater {
    pub async fn update(&self, increment: u64) {
        let old_completed_count = self.completed_count.fetch_add(increment, Ordering::Relaxed);

        self.inner
            .clone()
            .do_item_update(ItemProgressUpdate {
                item_name: self.item_name.clone(),
                total_bytes: self.n_bytes.load(Ordering::Relaxed),
                bytes_completed: old_completed_count + increment,
                bytes_completion_increment: increment,
            })
            .await;
    }

    pub async fn set_total(&self, n_bytes: u64) {
        let old_value = self.n_bytes.swap(n_bytes, Ordering::Relaxed);

        // Should only increment stuff here.
        debug_assert_le!(old_value, n_bytes);

        if old_value != n_bytes {
            self.inner.adjust_total_bytes(old_value - n_bytes).await;
        }
    }
}
