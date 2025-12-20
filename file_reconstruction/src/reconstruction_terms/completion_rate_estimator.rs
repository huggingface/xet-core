use std::sync::{Arc, Mutex};

use tokio::time::Duration;
use utils::ExpWeightedMovingAvg;

// This function gives the full estimate of the
pub struct BlockCompletionRateEstimator {
    inner: ExpWeightedMovingAvg,
}

impl BlockCompletionRateEstimator {
    pub fn new(observation_half_life: f64) -> Arc<Self> {
        let inner = 
        Arc::new(Self { inner })
    }

    /// Updates the estimator with a new observation.
    pub fn update(&self, completion_time_ms: u64, block_size: u64) {
        let rate = (completion_time_ms as f64) / (block_size as f64).max(0.0001);
        self.inner.lock().map(|mut inner| inner.update(rate));
    }

    /// Gets the target block size based on the current estimated completion rate.
    ///
    /// Returns 0 if there is no data yet.
    pub fn get_target_block_size(&self, target_completion_time: Duration) -> u64 {
        let Ok(inner_lg) = self.inner.lock() else {
            return 0;
        };

        let estimated_ms_per_byte = inner_lg.value();

        if estimated_ms_per_byte <= 0.0 {
            return 0;
        }

        let target_ms = target_completion_time.as_millis_f64();

        (target_ms / estimated_ms_per_byte).ceil() as u64
    }
}
