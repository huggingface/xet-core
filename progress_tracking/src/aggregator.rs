use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use more_asserts::*;
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
pub struct AggregatingProgressUpdater {
    inner: Option<Arc<dyn TrackingProgressUpdater>>,
    state: Arc<Mutex<AggregationState>>,
    bg_update_loop_handle: Mutex<Option<JoinHandle<()>>>,
}

struct SpeedWindowSample {
    sample_time: Instant,
    total_bytes_completed: u64,
    total_transfer_bytes_completed: u64,
}

#[derive(Default)]
struct AggregationState {
    pending: ProgressUpdate,
    item_lookup: HashMap<Arc<str>, usize>,
    finished: bool,

    /// A round-robin sampling window
    speed_window_samples: Vec<SpeedWindowSample>,
    speed_sample_size: usize,

    /// The tick index.  Elements are stored at
    tick_index: usize,
}

impl AggregationState {
    fn new(speed_sample_size: usize) -> Self {
        debug_assert_ge!(speed_sample_size, 1);

        Self {
            speed_window_samples: Vec::with_capacity(speed_sample_size),
            speed_sample_size,
            ..Default::default()
        }
    }

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

    fn get_state(&mut self) -> ProgressUpdate {
        let mut update = std::mem::take(&mut self.pending);

        // Copy back the accumulated stats in case this is called before another update happens.
        self.pending.total_bytes = update.total_bytes;
        self.pending.total_bytes_completed = update.total_bytes_completed;
        self.pending.total_transfer_bytes = update.total_transfer_bytes;
        self.pending.total_transfer_bytes_completed = update.total_transfer_bytes_completed;

        // Now update the speed estimation if possible.
        if self.speed_sample_size != 0 {
            let now = Instant::now();
            let earliest_idx = self.tick_index % self.speed_sample_size;

            if !self.speed_window_samples.is_empty() {
                // Run this as a fixed size ring buffer.
                let earliest = &self.speed_window_samples[earliest_idx];

                let time_passed = (now.saturating_duration_since(earliest.sample_time)).as_secs_f64().max(0.001);

                update.total_bytes_completion_rate = Some(
                    (update.total_bytes_completed.saturating_sub(earliest.total_bytes_completed)) as f64 / time_passed,
                );

                update.total_transfer_bytes_completion_rate = Some(
                    (update
                        .total_transfer_bytes_completed
                        .saturating_sub(earliest.total_transfer_bytes_completed)) as f64
                        / time_passed,
                );
            }

            // Add the current update to the ring
            let speed_sample = SpeedWindowSample {
                sample_time: now,
                total_bytes_completed: update.total_bytes_completed,
                total_transfer_bytes_completed: update.total_transfer_bytes_completed,
            };

            if self.speed_window_samples.len() < self.speed_sample_size {
                self.speed_window_samples.push(speed_sample);
            } else {
                // Cycle the insertion point in the ring.
                self.speed_window_samples[earliest_idx] = speed_sample;
                self.tick_index += 1;
            }
        }

        // Preallocate enough that we minimize reallocations
        self.pending.item_updates = Vec::with_capacity((4 * update.item_updates.len()) / 3);

        // Clear out the lookup table.
        self.item_lookup.clear();

        // Return the update.
        update
    }
}

impl AggregatingProgressUpdater {
    /// Start a new aggregating progress updater that flushes the updates to  
    pub fn new(
        inner: Arc<dyn TrackingProgressUpdater>,
        flush_interval: Duration,
        speed_sampling_window: Duration,
    ) -> Arc<Self> {
        let speed_sample_size =
            1 + (speed_sampling_window.as_secs_f64() / flush_interval.as_secs_f64()).ceil() as usize;

        let state = Arc::new(Mutex::new(AggregationState::new(speed_sample_size)));

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

        (state_guard.get_state(), state_guard.finished)
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
    use std::sync::atomic::{AtomicU64, Ordering};
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

            *self.flushed.lock().await = Some(update);
        }
    }

    impl MockUpdater {
        async fn last_update(&self) -> ProgressUpdate {
            self.flushed.lock().await.clone().unwrap()
        }
    }

    #[tokio::test]
    async fn test_single_ordered_flush_and_totals() {
        let mock = Arc::new(MockUpdater {
            flushed: Mutex::new(None),
        });

        // Create an aggregator that aggregates updates every 50 ms; it should send one update that aggregates the three
        // below.
        let aggregator =
            AggregatingProgressUpdater::new(mock.clone(), Duration::from_millis(50), Duration::from_millis(200));

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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
            })
            .await;

        // Wait long enough for flush to trigger
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Get flushed update
        let flushed = mock.last_update().await;

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

    // A test to test that the speed estimation is correct.
    #[tokio::test]
    async fn test_speed_estimation() {
        let mock = Arc::new(MockUpdater {
            flushed: Mutex::new(None),
        });

        // Create an aggregator that aggregates updates every 50 ms; it should send one update that aggregates the three
        // below.
        let aggregator =
            AggregatingProgressUpdater::new(mock.clone(), Duration::from_millis(1), Duration::from_millis(100));

        let completed_bytes = Arc::new(AtomicU64::new(0));
        let completed_transfer_bytes = Arc::new(AtomicU64::new(0));

        let add_updates = |total_bytes_per_ms: f64, transfer_bytes_per_ms: f64, n_ms: u64| {
            let completed_bytes_ = completed_bytes.clone();
            let completed_transfer_bytes_ = completed_transfer_bytes.clone();
            let aggregator = aggregator.clone();

            let update_start_time = Instant::now();
            let start_completed_bytes = completed_bytes_.load(Ordering::Relaxed);
            let start_completed_transfer_bytes = completed_transfer_bytes_.load(Ordering::Relaxed);

            async move {
                loop {
                    let now = Instant::now();
                    let ms_elapsed = now.saturating_duration_since(update_start_time).as_secs_f64() * 1000.;
                    if ms_elapsed >= n_ms as f64 {
                        break;
                    }
                    let cb = start_completed_bytes + (ms_elapsed * total_bytes_per_ms) as u64;
                    let ctb = start_completed_transfer_bytes + (ms_elapsed * transfer_bytes_per_ms) as u64;

                    let prev_cb = completed_bytes_.swap(cb, Ordering::Relaxed);
                    let prev_ctb = completed_transfer_bytes_.swap(ctb, Ordering::Relaxed);

                    aggregator
                        .register_updates(ProgressUpdate {
                            total_bytes_completed: cb,
                            total_bytes_completion_increment: cb - prev_cb,
                            total_transfer_bytes_completed: ctb,
                            total_transfer_bytes_completion_increment: ctb - prev_ctb,
                            ..Default::default()
                        })
                        .await;

                    completed_bytes_.store(cb, Ordering::Relaxed);
                    completed_transfer_bytes_.store(ctb, Ordering::Relaxed);
                }
            }
        };

        let check_rate_values = |expected_completion_rate: u64, expected_transfer_rate: u64| {
            let mock = mock.clone();
            async move {
                let update = mock.last_update().await;

                let assert_close = |ctx: &str, a: f64, b: f64| {
                    assert_le!((a - b).abs() / (a.abs() + b.abs()), 0.5, "Values not within 25% ({ctx}): {a} != {b}");
                };

                assert_close(
                    "completion",
                    update.total_bytes_completion_rate.unwrap_or_default() / 1000., /* Reported in seconds, we want
                                                                                     * in millis */
                    expected_completion_rate as f64,
                );

                assert_close(
                    "transfer",
                    update.total_transfer_bytes_completion_rate.unwrap_or_default() / 1000.,
                    expected_transfer_rate as f64,
                );
            }
        };

        add_updates(1000., 100., 50).await;
        check_rate_values(1000, 100).await;

        add_updates(1000., 100., 50).await;
        check_rate_values(1000, 100).await;

        // Increase the rate, this should go up linearly.
        add_updates(2000., 200., 25).await;
        check_rate_values(1250, 125).await;
        add_updates(2000., 200., 25).await;
        check_rate_values(1500, 150).await;
        add_updates(2000., 200., 25).await;
        check_rate_values(1750, 175).await;
        add_updates(2000., 200., 25).await;
        check_rate_values(2000, 200).await;
    }
}
