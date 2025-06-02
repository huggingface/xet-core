use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::{ProgressUpdate, TrackingProgressUpdater};

/// A wrapper around an `Arc<dyn TrackingProgressUpdater>` that efficiently aggregates progress
/// updates over time and flushes the aggregated updates periodically or on demand.
///
/// This struct buffers incoming [`ProgressUpdate`] values and merges them by item name
/// so that repeated updates for the same item are merged.  
///
/// The aggregated updates to the wrapped inner updater on a fixed interval.
///
/// ### Usage:
///
/// let inner_updater: Arc<dyn TrackingProgressUpdater> = Arc::new(MyUpdater {});
/// let aggregator = AggregatingProgressUpdater::new(inner_updater, Duration::from_millis(200));
///
/// // Register updates as needed...
/// aggregator.register_updates(my_update).await;
#[derive(Debug)]
pub struct AggregatingProgressUpdater {
    inner: Option<Arc<dyn TrackingProgressUpdater>>,
    state: Arc<Mutex<AggregationState>>,
    bg_update_loop_handle: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Default)]
struct AggregationState {
    pending: ProgressUpdate,
    item_lookup: HashMap<Arc<str>, usize>,
    finished: bool,
}

impl AggregationState {
    fn merge_in(&mut self, mut other: ProgressUpdate) {
        debug_assert!(!self.finished);

        for item in other.item_updates.drain(..) {
            match self.item_lookup.entry(item.item_name.clone()) {
                HashMapEntry::Occupied(entry) => {
                    self.pending.item_updates[*entry.get()].merge_in(item);
                },
                HashMapEntry::Vacant(entry) => {
                    entry.insert_entry(self.pending.item_updates.len());
                    self.pending.item_updates.push(item);
                },
            }
        }
        // Already merged in all the other updates; do this one now.
        self.pending.merge_in(other);
    }
}

impl AggregatingProgressUpdater {
    /// Start a new aggregating progress updater that flushes the updates to  
    pub fn new(inner: Arc<dyn TrackingProgressUpdater>, flush_interval: Duration) -> Arc<Self> {
        let state = Arc::new(Mutex::new(AggregationState::default()));
        let state_clone = Arc::clone(&state);
        let inner_clone = Arc::clone(&inner);

        let bg_update_loop = tokio::spawn(async move {
            // Wake up every 100ms to check to see if we're complete.
            let mut interval = tokio::time::interval_at(Instant::now() + flush_interval, flush_interval);

            loop {
                interval.tick().await;
                let is_complete = Self::flush_impl(&inner_clone, &state_clone).await;

                if is_complete {
                    break;
                }
            }
        });

        Arc::new(Self {
            inner: Some(inner),
            state,
            bg_update_loop_handle: Mutex::new(Some(bg_update_loop)),
        })
    }

    /// Creates a class that only aggregates the stats to be used to hold and track the total stats during and after a
    /// session.
    pub fn new_aggregation_only() -> Arc<Self> {
        Arc::new(Self {
            inner: None,
            state: Arc::new(Mutex::new(AggregationState::default())),
            bg_update_loop_handle: Mutex::new(None),
        })
    }

    async fn get_aggregated_state_impl(state: &Arc<Mutex<AggregationState>>) -> (ProgressUpdate, bool) {
        let mut state_guard = state.lock().await;

        if state_guard.pending.is_empty() {
            return (ProgressUpdate::default(), state_guard.finished);
        }

        let flushed = std::mem::take(&mut state_guard.pending);

        // Preallocate enough that we minimize reallocations
        state_guard.pending.item_updates = Vec::with_capacity((4 * flushed.item_updates.len()) / 3);

        // Clear out the lookup table.
        state_guard.item_lookup.clear();

        (flushed, state_guard.finished)
    }

    async fn flush_impl(inner: &Arc<dyn TrackingProgressUpdater>, state: &Arc<Mutex<AggregationState>>) -> bool {
        let (flushed, is_complete) = Self::get_aggregated_state_impl(state).await;
        inner.register_updates(flushed).await;
        is_complete
    }

    pub async fn get_aggregated_state(&self) -> ProgressUpdate {
        Self::get_aggregated_state_impl(&self.state).await.0
    }

    // Ensure everything is completed.
    pub async fn is_finished(&self) -> bool {
        self.state.lock().await.finished && self.bg_update_loop_handle.lock().await.is_none()
    }

    pub async fn finalize(&self) {
        self.state.lock().await.finished = true;

        if let Some(bg_jh) = self.bg_update_loop_handle.lock().await.take() {
            let _ = bg_jh.await;
        }
    }
}

#[async_trait::async_trait]
impl TrackingProgressUpdater for AggregatingProgressUpdater {
    async fn register_updates(&self, updates: ProgressUpdate) {
        let mut state = self.state.lock().await;
        state.merge_in(updates);
    }
    async fn flush(&self) {
        if let Some(inner) = &self.inner {
            Self::flush_impl(inner, &self.state).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::ItemProgressUpdate;

    #[derive(Debug)]
    struct MockUpdater {
        flushed: Mutex<Option<ProgressUpdate>>,
    }

    #[async_trait::async_trait]
    impl TrackingProgressUpdater for MockUpdater {
        async fn register_updates(&self, update: ProgressUpdate) {
            if update.is_empty() {
                return;
            }

            let mut guard = self.flushed.lock().await;
            assert!(guard.is_none(), "Expected only one non-empty update.");
            *guard = Some(update);
        }
    }

    #[tokio::test]
    async fn test_single_ordered_flush_and_totals() {
        let mock = Arc::new(MockUpdater {
            flushed: Mutex::new(None),
        });

        // Create an aggregator that aggregates updates every 50 ms; it should send one update that aggregates the three
        // below.
        let aggregator = AggregatingProgressUpdater::new(mock.clone(), Duration::from_millis(50));

        // First update: fileA
        aggregator
            .register_updates(ProgressUpdate {
                item_updates: vec![ItemProgressUpdate {
                    item_name: Arc::from("fileA.txt"),
                    total_bytes: 100,
                    bytes_completed: 10,
                    bytes_completion_increment: 10,
                }],
                total_bytes: 100,
                total_bytes_increment: 100,
                total_bytes_completed: 10,
                total_bytes_completion_increment: 10,
                total_transfer_bytes: 50,
                total_transfer_bytes_increment: 50,
                total_transfer_bytes_completed: 5,
                total_transfer_bytes_completion_increment: 5,
            })
            .await;

        tokio::time::sleep(Duration::from_millis(10)).await;

        // Second update: fileB
        aggregator
            .register_updates(ProgressUpdate {
                item_updates: vec![ItemProgressUpdate {
                    item_name: Arc::from("fileB.txt"),
                    total_bytes: 200,
                    bytes_completed: 50,
                    bytes_completion_increment: 50,
                }],
                total_bytes: 300,
                total_bytes_increment: 200,
                total_bytes_completed: 60,
                total_bytes_completion_increment: 50,
                total_transfer_bytes: 150,
                total_transfer_bytes_increment: 100,
                total_transfer_bytes_completed: 30,
                total_transfer_bytes_completion_increment: 25,
            })
            .await;

        tokio::time::sleep(Duration::from_millis(10)).await;

        // Third update: fileC
        aggregator
            .register_updates(ProgressUpdate {
                item_updates: vec![
                    ItemProgressUpdate {
                        item_name: Arc::from("fileC.txt"),
                        total_bytes: 300,
                        bytes_completed: 90,
                        bytes_completion_increment: 90,
                    },
                    ItemProgressUpdate {
                        item_name: Arc::from("fileA.txt"),
                        total_bytes: 100,
                        bytes_completed: 30,
                        bytes_completion_increment: 20,
                    },
                ],
                total_bytes: 600,
                total_bytes_increment: 300,
                total_bytes_completed: 170,
                total_bytes_completion_increment: 110,
                total_transfer_bytes: 300,
                total_transfer_bytes_increment: 150,
                total_transfer_bytes_completed: 85,
                total_transfer_bytes_completion_increment: 55,
            })
            .await;

        // Wait long enough for flush to trigger
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Get flushed update
        let flushed = mock.flushed.lock().await.take().expect("No update was flushed");

        // === Total fields ===
        assert_eq!(flushed.total_bytes, 600);
        assert_eq!(flushed.total_bytes_increment, 600);
        assert_eq!(flushed.total_bytes_completed, 170);
        assert_eq!(flushed.total_bytes_completion_increment, 170);

        assert_eq!(flushed.total_transfer_bytes, 300);
        assert_eq!(flushed.total_transfer_bytes_increment, 300);
        assert_eq!(flushed.total_transfer_bytes_completed, 85);
        assert_eq!(flushed.total_transfer_bytes_completion_increment, 85);

        // === Item updates ===
        assert_eq!(flushed.item_updates.len(), 3);

        let a = &flushed.item_updates[0];
        assert_eq!(a.item_name.as_ref(), "fileA.txt");
        assert_eq!(a.total_bytes, 100);
        assert_eq!(a.bytes_completed, 30);
        assert_eq!(a.bytes_completion_increment, 30);

        let b = &flushed.item_updates[1];
        assert_eq!(b.item_name.as_ref(), "fileB.txt");
        assert_eq!(b.total_bytes, 200);
        assert_eq!(b.bytes_completed, 50);
        assert_eq!(b.bytes_completion_increment, 50);

        let c = &flushed.item_updates[2];
        assert_eq!(c.item_name.as_ref(), "fileC.txt");
        assert_eq!(c.total_bytes, 300);
        assert_eq!(c.bytes_completed, 90);
        assert_eq!(c.bytes_completion_increment, 90);
    }
}
