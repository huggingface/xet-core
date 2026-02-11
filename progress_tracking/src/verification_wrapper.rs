use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use more_asserts::assert_le;
use tokio::sync::Mutex;

use crate::{ProgressUpdate, TrackingProgressUpdater};

/// Internal structure to track and validate progress data for one item.
#[derive(Debug)]
struct ItemProgressData {
    total_count: u64,
    last_completed: u64,
}

#[derive(Debug, Default)]
pub struct ProgressUpdaterVerificationWrapperImpl {
    items: HashMap<Arc<str>, ItemProgressData>,
    total_transfer_bytes: u64,
    total_transfer_bytes_completed: u64,
    total_bytes: u64,
    total_process_bytes_completed: u64,
}

/// A wrapper that forwards updates to an inner `TrackingProgressUpdater`
/// while also validating each update for correctness:
///
/// - `completed_count` must be non-decreasing and never exceed `total_count`.
/// - `completed_count` must match `last_completed + update_increment`.
/// - `total_count` must remain consistent (if it changes across updates for the same item, that's an error).
/// - Final verification (`assert_complete()`) ensures all items reached `completed_count == total_count`.
pub struct ProgressUpdaterVerificationWrapper {
    inner: Arc<dyn TrackingProgressUpdater>,
    tr: Mutex<ProgressUpdaterVerificationWrapperImpl>,
}

impl ProgressUpdaterVerificationWrapper {
    /// Creates a new verification wrapper around an existing `TrackingProgressUpdater`.
    /// All updates are validated and then forwarded to `inner`.
    pub fn new(inner: Arc<dyn TrackingProgressUpdater>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            tr: Mutex::new(ProgressUpdaterVerificationWrapperImpl::default()),
        })
    }

    /// Once all uploads are done, call this to ensure that every item is fully complete.
    /// Panics if any item is still incomplete (i.e. `last_completed < total_count`).
    pub async fn assert_complete(&self) {
        let tr = self.tr.lock().await;

        for (item_name, data) in tr.items.iter() {
            assert_eq!(
                data.last_completed, data.total_count,
                "Item '{}' is not fully complete: {}/{}",
                item_name, data.last_completed, data.total_count
            );
        }

        assert_eq!(tr.total_transfer_bytes_completed, tr.total_transfer_bytes);
    }
}

#[async_trait]
impl TrackingProgressUpdater for ProgressUpdaterVerificationWrapper {
    async fn register_updates(&self, update: ProgressUpdate) {
        // First, capture and validate
        let mut tr = self.tr.lock().await;

        for up in update.item_updates.iter() {
            let entry = tr.items.entry(up.item_name.clone()).or_insert(ItemProgressData {
                total_count: 0,
                last_completed: 0,
            });

            // If first time seeing total_count for this item, record it.
            // Otherwise, ensure it stays consistent.
            if entry.total_count == 0 {
                entry.total_count = up.total_bytes;
            } else {
                assert_eq!(
                    entry.total_count, up.total_bytes,
                    "Inconsistent total_count for '{}'; was {}, now {}",
                    up.item_name, entry.total_count, up.total_bytes
                );
            }

            // Check increments:
            // 1) `completed_count` should never go down
            assert!(
                up.bytes_completed >= entry.last_completed,
                "Item '{}' completed_count went backwards: old={}, new={}",
                up.item_name,
                entry.last_completed,
                up.bytes_completed
            );

            // 2) `completed_count` must not exceed `total_count`
            assert!(
                up.bytes_completed <= up.total_bytes,
                "Item '{}' completed_count {} exceeds total {}",
                up.item_name,
                up.bytes_completed,
                up.total_bytes
            );

            // 3) The increment must match the difference
            let expected_new = entry.last_completed + up.bytes_completion_increment;
            assert_eq!(
                up.bytes_completed, expected_new,
                "Item '{}': mismatch: last_completed={} + update_increment={} != completed_count={}",
                up.item_name, entry.last_completed, up.bytes_completion_increment, up.bytes_completed
            );

            // Update item record
            entry.last_completed = up.bytes_completed;
        }

        assert_le!(
            tr.total_transfer_bytes,
            update.total_transfer_bytes,
            "New total bytes {} a decrease from previous report of total bytes {}",
            update.total_transfer_bytes,
            tr.total_transfer_bytes
        );

        tr.total_transfer_bytes += update.total_transfer_bytes_increment;

        assert_eq!(
            tr.total_transfer_bytes, update.total_transfer_bytes,
            "New increment {} put tracked checked transfer bytes {} out of step from reported total bytes {}",
            update.total_transfer_bytes_increment, tr.total_transfer_bytes, update.total_transfer_bytes,
        );

        assert_le!(
            tr.total_transfer_bytes_completed,
            update.total_transfer_bytes_completed,
            "New total bytes completed {} a decrease from previous report of total bytes {}",
            update.total_transfer_bytes_completed,
            tr.total_transfer_bytes_completed
        );

        tr.total_transfer_bytes_completed += update.total_transfer_bytes_completion_increment;

        assert_eq!(
            tr.total_transfer_bytes_completed, update.total_transfer_bytes_completed,
            "Total bytes completed {} does not match tracked total bytes {}",
            update.total_transfer_bytes_completed, tr.total_transfer_bytes_completed
        );

        assert_le!(
            tr.total_bytes,
            update.total_bytes,
            "New total bytes {} a decrease from previous report of total bytes {}",
            update.total_bytes,
            tr.total_bytes
        );

        tr.total_bytes += update.total_bytes_increment;

        assert_eq!(
            tr.total_bytes, update.total_bytes,
            "New increment {} put checked total processing bytes {} out of step from reported total bytes {}",
            update.total_bytes_increment, tr.total_bytes, update.total_bytes,
        );

        assert_le!(
            tr.total_process_bytes_completed,
            update.total_bytes_completed,
            "New total bytes completed {} a decrease from previous report of total bytes {}",
            update.total_bytes_completed,
            tr.total_process_bytes_completed
        );

        tr.total_process_bytes_completed += update.total_bytes_completion_increment;

        assert_eq!(
            tr.total_process_bytes_completed, update.total_bytes_completed,
            "Total bytes completed {} does not match tracked total bytes {}",
            update.total_bytes_completed, tr.total_process_bytes_completed
        );

        // Now forward them to the inner updater
        self.inner.register_updates(update).await;
    }
    async fn flush(&self) {
        self.inner.flush().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ItemProgressUpdate;

    /// A trivial `TrackingProgressUpdater` for testing, which just stores all updates.
    /// In real code, this could log to a file, update a UI, etc.
    #[derive(Debug, Default)]
    struct DummyLogger {
        pub all_updates: Mutex<Vec<ItemProgressUpdate>>,
    }

    #[async_trait]
    impl TrackingProgressUpdater for DummyLogger {
        async fn register_updates(&self, updates: ProgressUpdate) {
            let mut guard = self.all_updates.lock().await;
            guard.extend_from_slice(&updates.item_updates);
        }
    }

    #[tokio::test]
    async fn test_verification_wrapper() {
        // Create an actual inner logger or progress sink
        let logger = Arc::new(DummyLogger::default());

        // Wrap it with our verification wrapper
        let wrapper = ProgressUpdaterVerificationWrapper::new(logger.clone());

        // Let's register some progress updates
        wrapper
            .register_updates(ProgressUpdate {
                item_updates: vec![
                    ItemProgressUpdate {
                        item_name: Arc::from("fileA"),
                        total_bytes: 100,
                        bytes_completed: 50,
                        bytes_completion_increment: 50,
                    },
                    ItemProgressUpdate {
                        item_name: Arc::from("fileB"),
                        total_bytes: 200,
                        bytes_completed: 100,
                        bytes_completion_increment: 100,
                    },
                ],
                total_transfer_bytes: 100,
                total_transfer_bytes_increment: 100,
                total_transfer_bytes_completed: 50,
                total_transfer_bytes_completion_increment: 50,
                total_bytes: 200,
                total_bytes_increment: 200,
                total_bytes_completed: 100,
                total_bytes_completion_increment: 100,
                ..Default::default()
            })
            .await;

        // Shouldn't be complete yet. We'll do one more set of updates to finalize.
        wrapper
            .register_updates(ProgressUpdate {
                item_updates: vec![
                    ItemProgressUpdate {
                        item_name: Arc::from("fileA"),
                        total_bytes: 100,
                        bytes_completed: 100,
                        bytes_completion_increment: 50,
                    },
                    ItemProgressUpdate {
                        item_name: Arc::from("fileB"),
                        total_bytes: 200,
                        bytes_completed: 200,
                        bytes_completion_increment: 100,
                    },
                ],
                total_transfer_bytes: 150,
                total_transfer_bytes_increment: 50,
                total_transfer_bytes_completed: 150,
                total_transfer_bytes_completion_increment: 100,
                total_bytes: 200,
                total_bytes_increment: 0,
                total_bytes_completed: 200,
                total_bytes_completion_increment: 100,
                ..Default::default()
            })
            .await;

        // Now all items should be fully complete
        wrapper.assert_complete().await;

        // We can also inspect the inner logger's captured updates:
        let final_updates = logger.all_updates.lock().await;
        assert_eq!(final_updates.len(), 4, "We sent 4 updates total");
    }
}
