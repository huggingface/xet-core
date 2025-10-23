use std::time::Duration;

use tokio::time::Instant;

/// A latency predictor using a numerically stable, exponentially decayed linear regression:
///
/// We fit a model of the form:
///   duration_secs ≈ base_time_secs + size_bytes * inv_throughput
/// which is equivalent to:
///   duration_secs ≈ intercept + slope * size_bytes
///
/// Internally, we use a stable, online update method based on weighted means and covariances:
/// - mean_x, mean_y: weighted means of size and duration
/// - s_xx, s_xy: exponentially decayed sums of (x - mean_x)^2 and (x - mean_x)(y - mean_y)
///
/// We apply decay on each update using exp2(-elapsed / half_life).
///
/// This avoids numerical instability from large sums and is robust to shifting distributions.
// Use MB for the scale of the size; this is more numerically stable.
pub struct LatencyPredictor {
    // decayed weighted sums
    sw: f64,
    sx: f64,
    sy: f64,
    sxx: f64,
    sxy: f64,

    // learned params (physical model: secs = base + inv_tp * MiB)
    base_time_secs: f64,
    inv_throughput_secs_per_mib: f64,

    decay_half_life_secs: f64,
    last_update: Instant,
}

pub struct LatencyPredictorSnapshot {
    base_time_secs: f64,
    inv_throughput_secs_per_mib: f64,
}

impl LatencyPredictorSnapshot {
    pub fn predicted_latency(&self, size_bytes: u64, avg_concurrent: f64) -> f64 {
        // Feature x: number of bytes transferred in this time, assuming that multiple similar
        // connections are active.  This is just a way to treat the
        let x = (size_bytes as f64) / BASE_SIZE_UNIT;

        let y_pred = self.base_time_secs + self.inv_throughput_secs_per_mib * x;

        debug_assert!(y_pred >= 0.);

        (y_pred * avg_concurrent.max(1.)).max(0.)
    }
}

// Scale the X part to maintain numerical stability; this effectively says
// the base part is in MB.
const BASE_SIZE_UNIT: f64 = 1024. * 1024.;

impl LatencyPredictor {
    pub fn new(decay_half_life: Duration) -> Self {
        Self {
            sw: 0.0,
            sx: 0.0,
            sy: 0.0,
            sxx: 0.0,
            sxy: 0.0,
            base_time_secs: 0.0,
            inv_throughput_secs_per_mib: 0.0,
            decay_half_life_secs: decay_half_life.as_secs_f64(),
            last_update: Instant::now(),
        }
    }

    /// Updates the latency model with a new observation.
    ///
    /// Applies exponential decay to prior statistics and incorporates the new sample
    /// using a numerically stable linear regression formula.
    ///
    /// - `size_bytes`: the size of the completed transmission.
    /// - `duration`: the time taken to complete the transmission.
    /// - `avg_concurrent`: an estimate of the average number of concurrent connections during transfer.
    pub fn update(&mut self, size_bytes: u64, duration: Duration, avg_concurrent: f64) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs_f64();
        let decay = (-elapsed / self.decay_half_life_secs).exp2();

        // decay previous weighted sums
        self.sw *= decay;
        self.sx *= decay;
        self.sy *= decay;
        self.sxx *= decay;
        self.sxy *= decay;

        // new sample (weight could be !=1; see below)
        let w = 1.0;
        let x = (size_bytes as f64) * (1.0 / (1024.0 * 1024.0)); // MiB
        let y = duration.as_secs_f64().max(1e-9) / avg_concurrent;

        // accumulate
        self.sw += w;
        self.sx += w * x;
        self.sy += w * y;
        self.sxx += w * x * x;
        self.sxy += w * x * y;

        // re-fit under nonneg constraints
        self.refit_nonneg();

        self.last_update = now;
    }

    fn refit_nonneg(&mut self) {
        const EPS: f64 = 1e-12;
        if self.sw < EPS {
            self.base_time_secs = 0.0;
            self.inv_throughput_secs_per_mib = 0.0;
            return;
        }

        let xbar = self.sx / self.sw;
        let ybar = self.sy / self.sw;

        let sxx_c = self.sxx - self.sx * self.sx / self.sw;
        let sxy_c = self.sxy - self.sx * self.sy / self.sw;

        if sxx_c <= EPS {
            // no x variation: only intercept
            let a = ybar.max(0.0);
            self.base_time_secs = a;
            self.inv_throughput_secs_per_mib = 0.0;
            return;
        }

        let b_hat = sxy_c / sxx_c;
        let a_hat = ybar - b_hat * xbar;

        if a_hat >= 0.0 && b_hat >= 0.0 {
            self.base_time_secs = a_hat;
            self.inv_throughput_secs_per_mib = b_hat;
            return;
        }

        // Boundary: a = 0 ⇒ re-fit slope
        let mut b_a0 = if self.sxx > EPS { self.sxy / self.sxx } else { 0.0 };
        if b_a0 < 0.0 {
            b_a0 = 0.0;
        }

        // Boundary: b = 0 ⇒ a = mean(y)
        let mut a_b0 = ybar;
        if a_b0 < 0.0 {
            a_b0 = 0.0;
        }

        // Choose boundary with smaller SSE *if* we track syy; otherwise heuristic:
        // If a_hat < 0 and b_hat >= 0, prefer a=0 solution.
        // If b_hat < 0, prefer b=0 solution.
        if b_hat < 0.0 {
            // slope invalid; go with b=0
            self.base_time_secs = a_b0;
            self.inv_throughput_secs_per_mib = 0.0;
        } else {
            // intercept invalid; go with a=0
            self.base_time_secs = 0.0;
            self.inv_throughput_secs_per_mib = b_a0;
        }
    }

    /// Predicts the expected completion time for a given transfer size and concurrency level.
    ///
    /// First predicts the overall latency of a transfer in seconds, assuming that there is no
    /// concurrency and connections scale linearly.
    ///
    /// - `size_bytes`: the size of the transfer.
    /// - `avg_concurrent`: the number of concurrent connections.
    pub fn predicted_latency(&self, size_bytes: u64, avg_concurrent: f64) -> Option<f64> {
        self.model_snapshot().map(|ss| ss.predicted_latency(size_bytes, avg_concurrent))
    }

    pub fn predicted_bandwidth(&self) -> Option<f64> {
        let query_bytes = 10 * 1024 * 1024;

        // How long would it take to transmit this at full bandwidth
        let min_latency = self.predicted_latency(query_bytes, 1.)?;

        // Report bytes per sec in this model.
        Some(query_bytes as f64 / min_latency.max(1e-6))
    }

    pub fn model_snapshot(&self) -> Option<LatencyPredictorSnapshot> {
        if self.sw == 0. {
            return None;
        }

        Some(LatencyPredictorSnapshot {
            base_time_secs: self.base_time_secs,
            inv_throughput_secs_per_mib: self.inv_throughput_secs_per_mib,
        })
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{self, Duration as TokioDuration};

    use super::*;

    #[test]
    fn test_estimator_update() {
        let mut estimator = LatencyPredictor::new(Duration::from_secs_f64(10.0));
        estimator.update(1_000_000, Duration::from_millis(500), 1.);
        let expected = estimator.predicted_latency(1_000_000, 1.).unwrap();
        assert!(expected > 0.0);
    }

    #[test]
    fn test_converges_to_constant_observation() {
        for concurrency in [1., 5., 100.] {
            let mut predictor = LatencyPredictor::new(Duration::from_secs_f64(10.0));
            for _ in 0..10 {
                predictor.update(1000, Duration::from_secs_f64(1.0), concurrency);
            }
            let prediction = predictor.predicted_latency(1000, concurrency).unwrap();
            assert!((prediction - 1.0).abs() < 0.01);
        }
    }

    #[tokio::test]
    async fn test_decay_weighting_effect() {
        time::pause();
        let mut predictor = LatencyPredictor::new(Duration::from_secs_f64(2.0));
        predictor.update(1000, Duration::from_secs_f64(2.0), 1.);
        time::advance(TokioDuration::from_secs(2)).await;
        predictor.update(1000, Duration::from_secs_f64(1.0), 1.);
        let predicted = predictor.predicted_latency(1000, 1.).unwrap();
        assert!(predicted > 1.0 && predicted < 1.6);
    }

    #[test]
    fn test_scaling_with_concurrency() {
        let mut predictor = LatencyPredictor::new(Duration::from_secs_f64(10.0));
        for _ in 0..10 {
            predictor.update(1000, Duration::from_secs_f64(1.0), 1.);
        }
        let predicted_1 = predictor.predicted_latency(1000, 1.).unwrap();
        let predicted_2 = predictor.predicted_latency(1000, 2.).unwrap();
        let predicted_4 = predictor.predicted_latency(1000, 4.).unwrap();
        assert!(predicted_2 > predicted_1);
        assert!(predicted_4 > predicted_2);
    }
}
