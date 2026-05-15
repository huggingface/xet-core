use std::time::Duration;

// See note in `xet_core_structures/src/utils/exp_weighted_moving_avg.rs`:
// `tokio::time::Instant` on native (so tests can `tokio::time::advance`),
// `web_time::Instant` on wasm (`std::time::Instant::now` panics there).
#[cfg(not(target_family = "wasm"))]
use tokio::time::Instant;
#[cfg(target_family = "wasm")]
use web_time::Instant;
use xet_core_structures::ExpWeightedMovingAvg;

pub(crate) const DEFAULT_SPEED_HALF_LIFE: Duration = Duration::from_secs(10);
pub(crate) const DEFAULT_MIN_OBSERVATIONS_FOR_RATE: u32 = 4;

/// Tracks smoothed byte-rate using an exponentially-weighted moving average.
///
/// On each [`update`](Self::update) call the tracker computes the byte deltas
/// since the last call and feeds `(delta_bytes, elapsed_secs)` into the EWMA
/// via [`update_with_weight`](ExpWeightedMovingAvg::update_with_weight).
/// The resulting `value() = Σ(decayed bytes) / Σ(decayed time)` is a smoothed
/// bytes-per-second rate where older observations decay with the configured
/// half-life.
///
/// Two independent channels are tracked: *bytes* (logical/decompressed) and
/// *transfer bytes* (network/compressed).
///
/// The first observation's elapsed time is clamped to at least the half-life
/// so the rate starts conservatively low and converges upward. Rates are not
/// reported until at least `min_observations_for_rate` observations have been
/// recorded (default [`DEFAULT_MIN_OBSERVATIONS_FOR_RATE`]).
pub(crate) struct SpeedTracker {
    bytes_rate: ExpWeightedMovingAvg,
    transfer_rate: ExpWeightedMovingAvg,
    last_bytes_completed: u64,
    last_transfer_bytes_completed: u64,
    last_report_time: Instant,
    observation_count: u32,
    min_initial_interval_secs: f64,
    min_observations_for_rate: u32,
}

impl SpeedTracker {
    pub fn new(half_life: Duration) -> Self {
        Self {
            bytes_rate: ExpWeightedMovingAvg::new_time_decay(half_life),
            transfer_rate: ExpWeightedMovingAvg::new_time_decay(half_life),
            last_bytes_completed: 0,
            last_transfer_bytes_completed: 0,
            last_report_time: Instant::now(),
            observation_count: 0,
            min_initial_interval_secs: half_life.as_secs_f64(),
            min_observations_for_rate: DEFAULT_MIN_OBSERVATIONS_FOR_RATE,
        }
    }

    pub fn with_min_observations(mut self, n: u32) -> Self {
        self.min_observations_for_rate = n;
        self
    }

    /// Feed current cumulative byte counts. Computes deltas from the
    /// previously seen values and the elapsed wall-clock time, then updates
    /// both EWMA channels.
    ///
    /// On the first observation the elapsed time is clamped to at least the
    /// half-life so the rate starts near zero and converges upward, avoiding
    /// wild initial spikes. If elapsed time is zero, this call is a no-op.
    pub fn update(&mut self, bytes_completed: u64, transfer_bytes_completed: u64) {
        let now = Instant::now();
        let mut elapsed = (now - self.last_report_time).as_secs_f64();

        if elapsed > 0.0 {
            if self.observation_count == 0 {
                elapsed = elapsed.max(self.min_initial_interval_secs);
            }

            let bytes_delta = bytes_completed.saturating_sub(self.last_bytes_completed);
            let transfer_delta = transfer_bytes_completed.saturating_sub(self.last_transfer_bytes_completed);

            self.bytes_rate.update_with_weight(bytes_delta as f64, elapsed);
            self.transfer_rate.update_with_weight(transfer_delta as f64, elapsed);

            self.last_bytes_completed = bytes_completed;
            self.last_transfer_bytes_completed = transfer_bytes_completed;
            self.last_report_time = now;
            self.observation_count = self.observation_count.saturating_add(1);
        }
    }

    /// Current smoothed rates in bytes/sec. Returns `(bytes_rate, transfer_rate)`.
    /// Both are `None` until at least `min_observations_for_rate` observations
    /// with nonzero elapsed time have been recorded.
    pub fn rates(&self) -> (Option<f64>, Option<f64>) {
        if self.observation_count >= self.min_observations_for_rate {
            (Some(self.bytes_rate.value()), Some(self.transfer_rate.value()))
        } else {
            (None, None)
        }
    }
}

#[cfg(test)]
mod tests {
    use more_asserts::{assert_ge, assert_le, assert_lt};
    use tokio::time::{Duration, advance, pause};

    use super::*;

    const HALF_LIFE: Duration = Duration::from_secs(10);
    const TICK: Duration = Duration::from_millis(200);

    fn bytes_rate(tracker: &SpeedTracker) -> Option<f64> {
        tracker.rates().0
    }

    fn transfer_rate(tracker: &SpeedTracker) -> Option<f64> {
        tracker.rates().1
    }

    // ── Basic behaviour ────────────────────────────────────────────

    #[tokio::test]
    async fn no_rate_before_any_observation() {
        pause();
        let tracker = SpeedTracker::new(HALF_LIFE);
        let (br, tr) = tracker.rates();
        assert!(br.is_none());
        assert!(tr.is_none());
    }

    #[tokio::test]
    async fn rates_none_until_min_observations() {
        pause();
        let mut tracker = SpeedTracker::new(HALF_LIFE);
        let bytes_per_tick = 2_000u64;
        let mut total = 0u64;

        for _ in 0..DEFAULT_MIN_OBSERVATIONS_FOR_RATE {
            assert!(bytes_rate(&tracker).is_none());
            advance(TICK).await;
            total += bytes_per_tick;
            tracker.update(total, 0);
        }

        assert!(bytes_rate(&tracker).is_some());
    }

    #[tokio::test]
    async fn configurable_min_observations() {
        pause();
        let min_obs = 8u32;
        let mut tracker = SpeedTracker::new(HALF_LIFE).with_min_observations(min_obs);
        let bytes_per_tick = 2_000u64;
        let mut total = 0u64;

        for _ in 0..min_obs - 1 {
            advance(TICK).await;
            total += bytes_per_tick;
            tracker.update(total, 0);
        }
        assert!(bytes_rate(&tracker).is_none());

        advance(TICK).await;
        total += bytes_per_tick;
        tracker.update(total, 0);
        assert!(bytes_rate(&tracker).is_some());
    }

    #[tokio::test]
    async fn constant_rate_converges() {
        pause();
        let mut tracker = SpeedTracker::new(HALF_LIFE);

        let rate = 10_000.0;
        let bytes_per_tick = (rate * TICK.as_secs_f64()) as u64;

        let mut total = 0u64;
        for _ in 0..500 {
            advance(TICK).await;
            total += bytes_per_tick;
            tracker.update(total, 0);
        }

        let measured = bytes_rate(&tracker).unwrap();
        assert!((measured - rate).abs() / rate < 0.01);
    }

    #[tokio::test]
    async fn two_channels_tracked_independently() {
        pause();
        let mut tracker = SpeedTracker::new(HALF_LIFE);

        let bytes_target = 20_000.0;
        let transfer_target = 5_000.0;
        let bytes_per_tick = (bytes_target * TICK.as_secs_f64()) as u64;
        let transfer_per_tick = (transfer_target * TICK.as_secs_f64()) as u64;

        let mut total_bytes = 0u64;
        let mut total_transfer = 0u64;

        for _ in 0..250 {
            advance(TICK).await;
            total_bytes += bytes_per_tick;
            total_transfer += transfer_per_tick;
            tracker.update(total_bytes, total_transfer);
        }

        let br = bytes_rate(&tracker).unwrap();
        let tr = transfer_rate(&tracker).unwrap();
        assert!((br - bytes_target).abs() / bytes_target < 0.05);
        assert!((tr - transfer_target).abs() / transfer_target < 0.05);
    }

    // ── Warm-up / initial rate ─────────────────────────────────────

    #[tokio::test]
    async fn initial_rate_ramps_up_smoothly() {
        pause();
        let mut tracker = SpeedTracker::new(HALF_LIFE).with_min_observations(1);

        let rate = 10_000.0;
        let bytes_per_tick = (rate * TICK.as_secs_f64()) as u64;
        let mut total = 0u64;
        let mut prev_rate = 0.0;

        for i in 0..250 {
            advance(TICK).await;
            total += bytes_per_tick;
            tracker.update(total, 0);

            let r = bytes_rate(&tracker).unwrap();

            if i == 0 {
                assert_lt!(r, rate * 0.20);
            }

            if i > 0 {
                assert_ge!(r, prev_rate * 0.99);
            }

            prev_rate = r;
        }

        assert!((prev_rate - rate).abs() / rate < 0.05);
    }

    #[tokio::test]
    async fn no_initial_spike() {
        pause();
        let mut tracker = SpeedTracker::new(HALF_LIFE).with_min_observations(1);

        advance(TICK).await;
        tracker.update(50_000, 0);

        let r = bytes_rate(&tracker).unwrap();
        let max_expected = 50_000.0 / HALF_LIFE.as_secs_f64();
        assert_le!(r, max_expected * 1.01);
    }

    // ── Smoothing / stability ──────────────────────────────────────

    #[tokio::test]
    async fn burst_then_silence_smooths_gradually() {
        pause();
        let mut tracker = SpeedTracker::new(HALF_LIFE).with_min_observations(1);

        advance(TICK).await;
        tracker.update(100_000, 0);
        let peak = bytes_rate(&tracker).unwrap();

        let mut prev = peak;
        for _ in 0..10 {
            advance(TICK).await;
            tracker.update(100_000, 0);
            let r = bytes_rate(&tracker).unwrap();
            assert_le!(r, prev);
            prev = r;
        }

        assert_lt!(prev, peak);
        assert!(prev > 0.0);
    }

    #[tokio::test]
    async fn rate_stays_stable_under_uniform_feed() {
        pause();
        let mut tracker = SpeedTracker::new(HALF_LIFE);

        let rate = 50_000.0;
        let bytes_per_tick = (rate * TICK.as_secs_f64()) as u64;
        let mut total = 0u64;

        for _ in 0..500 {
            advance(TICK).await;
            total += bytes_per_tick;
            tracker.update(total, 0);
        }

        for _ in 0..50 {
            advance(TICK).await;
            total += bytes_per_tick;
            tracker.update(total, 0);
            let r = bytes_rate(&tracker).unwrap();
            assert!((r - rate).abs() / rate < 0.01);
        }
    }

    #[tokio::test]
    async fn speed_change_adapts() {
        pause();
        let mut tracker = SpeedTracker::new(HALF_LIFE);

        let slow = 1_000.0;
        let fast = 10_000.0;
        let slow_per_tick = (slow * TICK.as_secs_f64()) as u64;
        let fast_per_tick = (fast * TICK.as_secs_f64()) as u64;
        let mut total = 0u64;

        for _ in 0..300 {
            advance(TICK).await;
            total += slow_per_tick;
            tracker.update(total, 0);
        }
        let before = bytes_rate(&tracker).unwrap();
        assert!((before - slow).abs() / slow < 0.05);

        for _ in 0..250 {
            advance(TICK).await;
            total += fast_per_tick;
            tracker.update(total, 0);
        }
        let after = bytes_rate(&tracker).unwrap();
        assert!((after - fast).abs() / fast < 0.05);
    }

    // ── Decay / half-life ──────────────────────────────────────────

    #[tokio::test]
    async fn stall_decays_rate_toward_zero() {
        pause();
        let mut tracker = SpeedTracker::new(HALF_LIFE);

        let rate = 20_000.0;
        let bytes_per_tick = (rate * TICK.as_secs_f64()) as u64;
        let mut total = 0u64;

        for _ in 0..200 {
            advance(TICK).await;
            total += bytes_per_tick;
            tracker.update(total, 0);
        }
        let active_rate = bytes_rate(&tracker).unwrap();
        assert!(active_rate > rate * 0.5);

        for _ in 0..150 {
            advance(TICK).await;
            tracker.update(total, 0);
        }
        let stalled = bytes_rate(&tracker).unwrap();
        assert_lt!(stalled, active_rate * 0.15);
    }

    #[tokio::test]
    async fn shorter_half_life_decays_faster() {
        pause();
        let mut fast_tracker = SpeedTracker::new(Duration::from_secs(2));
        let mut slow_tracker = SpeedTracker::new(Duration::from_secs(20));

        let bytes_per_tick = 2_000u64;
        let mut total = 0u64;

        for _ in 0..200 {
            advance(TICK).await;
            total += bytes_per_tick;
            fast_tracker.update(total, 0);
            slow_tracker.update(total, 0);
        }

        for _ in 0..25 {
            advance(TICK).await;
            fast_tracker.update(total, 0);
            slow_tracker.update(total, 0);
        }

        let fast_rate = bytes_rate(&fast_tracker).unwrap();
        let slow_rate = bytes_rate(&slow_tracker).unwrap();
        assert_lt!(fast_rate, slow_rate);
    }

    // ── Smoothness metric ──────────────────────────────────────────

    #[tokio::test]
    async fn jitter_in_arrivals_smoothed_out() {
        pause();
        let mut tracker = SpeedTracker::new(HALF_LIFE);

        let target_rate = 10_000.0;
        let avg_bytes_per_tick = (target_rate * TICK.as_secs_f64()) as u64;
        let mut total = 0u64;

        let mut rates = Vec::new();

        for i in 0..300 {
            advance(TICK).await;
            if i % 2 == 0 {
                total += avg_bytes_per_tick * 2;
            }
            tracker.update(total, 0);

            if i >= 200 {
                rates.push(bytes_rate(&tracker).unwrap());
            }
        }

        let mean: f64 = rates.iter().sum::<f64>() / rates.len() as f64;

        assert!((mean - target_rate).abs() / target_rate < 0.05);

        let variance: f64 = rates.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / rates.len() as f64;
        let cv = variance.sqrt() / mean;
        assert_lt!(cv, 0.05);
    }

    // ── Edge cases ─────────────────────────────────────────────────

    #[tokio::test]
    async fn zero_elapsed_update_is_noop() {
        pause();
        let mut tracker = SpeedTracker::new(HALF_LIFE);
        tracker.update(1000, 500);
        assert!(bytes_rate(&tracker).is_none());
    }

    #[tokio::test]
    async fn resume_after_long_stall_picks_up_new_rate() {
        pause();
        let mut tracker = SpeedTracker::new(HALF_LIFE);

        let bytes_per_tick = 2_000u64;
        let mut total = 0u64;

        for _ in 0..300 {
            advance(TICK).await;
            total += bytes_per_tick;
            tracker.update(total, 0);
        }

        for _ in 0..500 {
            advance(TICK).await;
            tracker.update(total, 0);
        }

        let stalled = bytes_rate(&tracker).unwrap();
        assert_lt!(stalled, 100.0);

        let slow_per_tick = 1_000u64;
        for _ in 0..250 {
            advance(TICK).await;
            total += slow_per_tick;
            tracker.update(total, 0);
        }

        let r = bytes_rate(&tracker).unwrap();
        let expected = slow_per_tick as f64 / TICK.as_secs_f64();
        assert!((r - expected).abs() / expected < 0.05);
    }

    #[tokio::test]
    async fn large_tick_interval_works() {
        pause();
        let mut tracker = SpeedTracker::new(HALF_LIFE).with_min_observations(1);

        advance(Duration::from_secs(15)).await;
        tracker.update(150_000, 75_000);

        let br = bytes_rate(&tracker).unwrap();
        let tr = transfer_rate(&tracker).unwrap();
        assert_ge!(br, 9_900.0);
        assert_le!(br, 10_100.0);
        assert_ge!(tr, 4_900.0);
        assert_le!(tr, 5_100.0);
    }
}
