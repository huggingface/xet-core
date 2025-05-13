use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

/// A class to make all the bookkeeping clear with the
#[derive(Clone, Debug)]
pub struct ProgressUpdate {
    pub item_name: Arc<str>,
    pub total_count: u64,
    pub completed_count: u64,
    pub update_increment: u64,
}

/// A simple progress updater that simply reports when
/// progress has occured.
#[async_trait]
pub trait SimpleProgressUpdater: Debug + Send + Sync {
    /// updater takes 1 parameter which is an increment value to progress
    /// **not the total progress value**
    async fn update(&self, increment: u64);

    /// Optionally sets the total number of items available.
    async fn set_total(&self, _n_units: u64) {}
}

/// The trait that a progress updater that reports per-item progress completion.
#[async_trait]
pub trait TrackingProgressUpdater: Debug + Send + Sync {
    /// Register a set of updates as a list of ProgressUpdate instances, which
    /// contain the name and progress information.    
    async fn register_updates(&self, updates: &[ProgressUpdate]);
}

/// This struct allows us to wrap the larger progress updater in a simple form for
/// specific items.
#[derive(Debug)]
pub struct ItemProgressUpdater {
    item_name: Arc<str>,
    total_count: AtomicU64,
    completed_count: AtomicU64,
    inner: Arc<dyn TrackingProgressUpdater>,
}

impl ItemProgressUpdater {
    /// In case we need to just track completion of a single item within a function,
    /// this method creates such a class to enable updates.
    pub fn new(inner: Arc<dyn TrackingProgressUpdater>, item_name: Arc<str>, total_count: Option<u64>) -> Arc<Self> {
        let s = Self {
            item_name,
            total_count: AtomicU64::new(total_count.unwrap_or(0)),
            completed_count: AtomicU64::new(0),
            inner,
        };

        Arc::new(s)
    }
}

/// In case we just want to
#[async_trait]
impl SimpleProgressUpdater for ItemProgressUpdater {
    async fn update(&self, increment: u64) {
        self.completed_count.fetch_add(increment, Ordering::Relaxed);

        let progress_update = ProgressUpdate {
            item_name: self.item_name.clone(),
            total_count: self.total_count.load(Ordering::Relaxed),
            completed_count: self.completed_count.load(Ordering::Relaxed),
            update_increment: increment,
        };

        self.inner.register_updates(&[progress_update]).await;
    }

    async fn set_total(&self, n_units: u64) {
        self.total_count.store(n_units, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
pub struct NoOpProgressUpdater;

impl NoOpProgressUpdater {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl SimpleProgressUpdater for NoOpProgressUpdater {
    async fn update(&self, _increment: u64) {}
}

#[async_trait]
impl TrackingProgressUpdater for NoOpProgressUpdater {
    async fn register_updates(&self, _updates: &[ProgressUpdate]) {}
}

/// Internal structure to track and validate progress data for one item.
#[derive(Debug)]
struct ItemProgressData {
    total_count: u64,
    last_completed: u64,
}

/// A wrapper that forwards updates to an inner `TrackingProgressUpdater`
/// while also validating each update for correctness:
///
/// - `completed_count` must be non-decreasing and never exceed `total_count`.
/// - `completed_count` must match `last_completed + update_increment`.
/// - `total_count` must remain consistent (if it changes across updates for the same item, that's an error).
/// - Final verification (`assert_complete()`) ensures all items reached `completed_count == total_count`.
#[derive(Debug)]
pub struct ProgressUpdaterVerificationWrapper {
    inner: Arc<dyn TrackingProgressUpdater>,
    items: Mutex<HashMap<Arc<str>, ItemProgressData>>,
}

impl ProgressUpdaterVerificationWrapper {
    /// Creates a new verification wrapper around an existing `TrackingProgressUpdater`.
    /// All updates are validated and then forwarded to `inner`.
    pub fn new(inner: Arc<dyn TrackingProgressUpdater>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            items: Mutex::new(HashMap::new()),
        })
    }

    /// Once all uploads are done, call this to ensure that every item is fully complete.
    /// Panics if any item is still incomplete (i.e. `last_completed < total_count`).
    pub async fn assert_complete(&self) {
        let map = self.items.lock().await;
        for (item_name, data) in &*map {
            assert_eq!(
                data.last_completed, data.total_count,
                "Item '{}' is not fully complete: {}/{}",
                item_name, data.last_completed, data.total_count
            );
        }
    }
}

#[async_trait]
impl TrackingProgressUpdater for ProgressUpdaterVerificationWrapper {
    async fn register_updates(&self, updates: &[ProgressUpdate]) {
        // First, capture and validate
        let mut map = self.items.lock().await;
        for up in updates {
            let entry = map.entry(up.item_name.clone()).or_insert(ItemProgressData {
                total_count: 0,
                last_completed: 0,
            });

            // If first time seeing total_count for this item, record it.
            // Otherwise, ensure it stays consistent.
            if entry.total_count == 0 {
                entry.total_count = up.total_count;
            } else {
                assert_eq!(
                    entry.total_count, up.total_count,
                    "Inconsistent total_count for '{}'; was {}, now {}",
                    up.item_name, entry.total_count, up.total_count
                );
            }

            // Check increments:
            // 1) `completed_count` should never go down
            assert!(
                up.completed_count >= entry.last_completed,
                "Item '{}' completed_count went backwards: old={}, new={}",
                up.item_name,
                entry.last_completed,
                up.completed_count
            );

            // 2) `completed_count` must not exceed `total_count`
            assert!(
                up.completed_count <= up.total_count,
                "Item '{}' completed_count {} exceeds total {}",
                up.item_name,
                up.completed_count,
                up.total_count
            );

            // 3) The increment must match the difference
            let expected_new = entry.last_completed + up.update_increment;
            assert_eq!(
                up.completed_count, expected_new,
                "Item '{}': mismatch: last_completed={} + update_increment={} != completed_count={}",
                up.item_name, entry.last_completed, up.update_increment, up.completed_count
            );

            // Update item record
            entry.last_completed = up.completed_count;
        }

        // Now forward them to the inner updater
        self.inner.register_updates(updates).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A trivial `TrackingProgressUpdater` for testing, which just stores all updates.
    /// In real code, this could log to a file, update a UI, etc.
    #[derive(Debug, Default)]
    struct DummyLogger {
        pub all_updates: Mutex<Vec<ProgressUpdate>>,
    }

    #[async_trait]
    impl TrackingProgressUpdater for DummyLogger {
        async fn register_updates(&self, updates: &[ProgressUpdate]) {
            let mut guard = self.all_updates.lock().await;
            guard.extend_from_slice(updates);
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
            .register_updates(&[
                ProgressUpdate {
                    item_name: Arc::from("fileA"),
                    total_count: 100,
                    completed_count: 50,
                    update_increment: 50, // from 0->50
                },
                ProgressUpdate {
                    item_name: Arc::from("fileB"),
                    total_count: 200,
                    completed_count: 100,
                    update_increment: 100, // from 0->100
                },
            ])
            .await;

        // Shouldn't be complete yet. We'll do one more set of updates to finalize.
        wrapper
            .register_updates(&[
                ProgressUpdate {
                    item_name: Arc::from("fileA"),
                    total_count: 100,
                    completed_count: 100,
                    update_increment: 50, // from 50->100
                },
                ProgressUpdate {
                    item_name: Arc::from("fileB"),
                    total_count: 200,
                    completed_count: 200,
                    update_increment: 100, // from 100->200
                },
            ])
            .await;

        // Now all items should be fully complete
        wrapper.assert_complete().await;

        // We can also inspect the inner logger's captured updates:
        let final_updates = logger.all_updates.lock().await;
        assert_eq!(final_updates.len(), 4, "We sent 4 updates total");
    }
}
