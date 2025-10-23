use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{debug, info};
use utils::adjustable_semaphore::{AdjustableSemaphore, AdjustableSemaphorePermit};
use utils::ExpWeightedMovingAvg;

use crate::adaptive_concurrency::latency_prediction::{LatencyPredictor, LatencyPredictorSnapshot};
use crate::constants::*;
use crate::CasClientError;

/// The internal state of the concurrency controller.
struct ConcurrencyControllerState {
    /// A running model of the current bandwidth.  Uses an exponentially weighted average to predict the
    latency_predictor: LatencyPredictor,

    // A running average of how far the latency prediction differs from the predicted latency.
    // This is tracked as ln(actual / predicted).  Essentially, when this is less than zero --
    // i.e. actual < predicted
    // -- then we increase concurrency; when this is greater than ln(1.1)
    // -- i.e. actual > 1.1 * predicted -- we decrease concurrency.
    deviance_tracking: ExpWeightedMovingAvg,

    // The last time we adjusted the permits.
    last_adjustment_time: Instant,

    // The last time we reported the concurrency in the log; just log this once every 10 seconds.
    last_logging_time: Instant,
}

impl ConcurrencyControllerState {
    fn new() -> Self {
        let latency_half_life = Duration::from_millis(*CONCURRENCY_CONTROL_LATENCY_TRACKING_HALF_LIFE_MS);
        let success_half_life = Duration::from_millis(*CONCURRENCY_CONTROL_SUCCESS_TRACKING_HALF_LIFE_MS);

        Self {
            latency_predictor: LatencyPredictor::new(latency_half_life),
            deviance_tracking: ExpWeightedMovingAvg::new(success_half_life),
            last_adjustment_time: Instant::now(),
            last_logging_time: Instant::now(),
        }
    }
}

/// A controller for dynamically adjusting the amount of concurrency on upload and download paths.
///
/// This controller uses two statistical models that adapt over time using exponentially weighted
/// moving averages.  The first is a model that predicts the overall current bandwith, and the second  and the second is
/// a model of the deviance between the actual transfer time and the predicted time based on a linear scaling of the
/// concurrency.
///
/// The key insight is this:
///
/// 1. When a network connection is underutilized, the latency scales sublinearly with the number of parallel
///    connections. In other words, adding another transfer does not affect the speed of the other transfers
///    significantly.
/// 2. When a network connection is fully utilized, then the latency scales linearly with the concurrency. In other
///    words, adding increasing the concurrency from N to N+1 would cause the latency of all the other transfers to
///    increase by a factor of (N+1) / N.
/// 3. When a network connection is oversaturated, then the latency scales superlinearly -- in other words, adding an
///    additional connection causes the overall throughput to decrease.
///
/// Now, because latency is a noisy observation, we track a running clipped average of the deviance between
/// predicted time and the actual time, and increase the concurrency when this is reliably sublinear and decrease it
/// when it is superlinear.  This is clipped to avoid having a single observation weight it too much; failures
/// and retries max out the deviance.
pub struct AdaptiveConcurrencyController {
    // The current state, including tracking information and when previous adjustments were made.
    // Also holds related constants
    state: Mutex<ConcurrencyControllerState>,

    // The semaphore from which new permits are issued.
    concurrency_semaphore: Arc<AdjustableSemaphore>,

    // constants used to calculate how long things should be expected to take.
    min_concurrency_increase_delay: Duration,
    min_concurrency_decrease_delay: Duration,

    // A logging tag for logging adjustments.
    logging_tag: &'static str,
}

impl AdaptiveConcurrencyController {
    pub fn new(logging_tag: &'static str, concurrency: usize, concurrency_bounds: (usize, usize)) -> Arc<Self> {
        // Make sure these values are sane, as they can be loaded from environment variables.
        let min_concurrency = concurrency_bounds.0.max(1);
        let max_concurrency = concurrency_bounds.1.max(min_concurrency);
        let current_concurrency = concurrency.clamp(min_concurrency, max_concurrency);

        info!("Initializing Adaptive Concurrency Controller for {logging_tag} with starting concurrency = {current_concurrency}; min = {min_concurrency}, max = {max_concurrency}");

        Arc::new(Self {
            state: Mutex::new(ConcurrencyControllerState::new()),
            concurrency_semaphore: AdjustableSemaphore::new(current_concurrency, (min_concurrency, max_concurrency)),

            min_concurrency_increase_delay: Duration::from_millis(*CONCURRENCY_CONTROL_MIN_INCREASE_WINDOW_MS),
            min_concurrency_decrease_delay: Duration::from_millis(*CONCURRENCY_CONTROL_MIN_DECREASE_WINDOW_MS),
            logging_tag,
        })
    }

    /// Acquire a connection permit based on the current concurrency.
    pub async fn acquire_connection_permit(self: &Arc<Self>) -> Result<ConnectionPermit, CasClientError> {
        let permit = self.concurrency_semaphore.acquire().await?;

        Ok(ConnectionPermit {
            permit,
            controller: Arc::clone(self),
            transfer_start_time: Instant::now(),
            starting_concurrency: self.concurrency_semaphore.active_permits(),
            latency_model_at_start: self.state.lock().await.latency_predictor.model_snapshot(),
        })
    }

    /// The current concurrency; there may be more permits out there due to the lazy resolution of decrements, but those
    /// are resolved before any new permits are issued.
    pub fn total_permits(&self) -> usize {
        self.concurrency_semaphore.total_permits()
    }

    /// The number of permits available currently.  Used mainly for testing.
    pub fn available_permits(&self) -> usize {
        self.concurrency_semaphore.available_permits()
    }

    /// Update  
    async fn report_and_update(&self, permit: &ConnectionPermit, n_bytes_if_known: Option<u64>, is_success: bool) {
        let actual_completion_time = permit.transfer_start_time.elapsed();

        let mut state_lg = self.state.lock().await;

        let max_dev = 1. + CONCURRENCY_CONTROL_DEVIANCE_MAX_SPREAD.max(0.);
        let min_dev = 1. / max_dev;

        let ok_dev = 1. + CONCURRENCY_CONTROL_DEVIANCE_TARGET_SPREAD.max(0.);
        let incr_dev = 1. / ok_dev;

        // First, calculate the predicted vs. actual time completion for this model.
        let deviance_ratio = {
            if let Some(n_bytes) = n_bytes_if_known {
                let cur_concurrency = self.concurrency_semaphore.active_permits();
                let avg_concurrency = ((cur_concurrency + permit.starting_concurrency) as f64) / 2.;

                if is_success {
                    let t_actual = actual_completion_time.as_secs_f64().max(1e-4);

                    let concurrency = permit.starting_concurrency as f64;

                    // Get the predicted time using the model when this started.
                    let t_pred = permit
                        .latency_model_at_start
                        .as_ref()
                        .map(|lm| lm.predicted_latency(n_bytes, concurrency))
                        .unwrap_or(t_actual);

                    let dev_ratio = t_actual / t_pred.max(1e-6);

                    state_lg
                        .latency_predictor
                        .update(n_bytes, actual_completion_time, avg_concurrency);

                    dev_ratio
                } else {
                    // If it's not a success, then update the deviance with the penalty factor.
                    max_dev
                }
            } else {
                // This would be a failure case, so update the
                debug_assert!(!is_success);

                max_dev
            }
        };

        // Update the deviance with this value; we're tracking the log of the ratio due
        // to the additive averaging.
        state_lg.deviance_tracking.update(deviance_ratio.clamp(min_dev, max_dev).ln()); // deviance_ratio.ln());

        // Now, get the current predicted deviance and see what the range is.
        let cur_deviance = state_lg.deviance_tracking.value().exp();

        if is_success && cur_deviance < incr_dev {
            // Attempt to increase the deviance.
            if state_lg.last_adjustment_time.elapsed() > self.min_concurrency_increase_delay {
                self.concurrency_semaphore.increment_total_permits();
                state_lg.last_adjustment_time = Instant::now();

                debug!(
                    "Concurrency control for {}: Increased concurrency to {}; latency deviance = {cur_deviance}.",
                    self.logging_tag,
                    self.concurrency_semaphore.total_permits()
                );
            }
        } else if !is_success && cur_deviance > ok_dev {
            // Attempt to decrease the deviance.
            if state_lg.last_adjustment_time.elapsed() > self.min_concurrency_decrease_delay {
                self.concurrency_semaphore.decrement_total_permits();
                state_lg.last_adjustment_time = Instant::now();

                debug!(
                    "Concurrency control for {}: Lowered concurrency to {}; latency deviance = {cur_deviance}.",
                    self.logging_tag,
                    self.concurrency_semaphore.total_permits()
                );
            }
        }

        if state_lg.last_logging_time.elapsed() > Duration::from_millis(*CONCURRENCY_CONTROL_LOGGING_INTERVAL_MS) {
            info!(
                "Concurrency control for {}: Current concurrency = {}; predicted bandwidth = {}; deviance = {}",
                self.logging_tag,
                self.concurrency_semaphore.total_permits(),
                state_lg.latency_predictor.predicted_bandwidth().unwrap_or_default(),
                state_lg.deviance_tracking.value().exp()
            );
        }
    }
}

/// A permit for a connection.  This can be used to track the start time of a transfer and report back
/// to the original controller whether it's needed.
pub struct ConnectionPermit {
    permit: AdjustableSemaphorePermit,
    controller: Arc<AdaptiveConcurrencyController>,
    transfer_start_time: Instant,
    starting_concurrency: usize,
    latency_model_at_start: Option<LatencyPredictorSnapshot>,
}

impl ConnectionPermit {
    /// Call this right before starting a transfer; records start time.
    pub(crate) fn transfer_starting(&mut self) {
        self.transfer_start_time = Instant::now();
    }

    /// Call this after a successful transfer, providing the byte count.
    pub(crate) async fn report_completion(self, n_bytes: u64, success: bool) {
        self.controller.clone().report_and_update(&self, Some(n_bytes), success).await;
    }

    pub(crate) async fn report_retryable_failure(&self) {
        self.controller.clone().report_and_update(self, None, false).await;
    }
}

// Testing routines.
#[cfg(test)]
mod test_constants {

    pub const TR_HALF_LIFE_MS: u64 = 10;
    pub const INCR_SPACING_MS: u64 = 200;
    pub const DECR_SPACING_MS: u64 = 100;

    pub const TARGET_TIME_MS_S: u64 = 5;
    pub const TARGET_TIME_MS_L: u64 = 20;

    pub const LARGE_N_BYTES: u64 = 10000;
}

#[cfg(test)]
impl ConcurrencyControllerState {
    #[cfg(test)]
    fn new_testing() -> Self {
        use crate::adaptive_concurrency::controller::test_constants::TR_HALF_LIFE_MS;

        let emwa_half_life = Duration::from_millis(TR_HALF_LIFE_MS);

        Self {
            latency_predictor: LatencyPredictor::new(emwa_half_life),
            deviance_tracking: ExpWeightedMovingAvg::new(emwa_half_life),
            last_adjustment_time: Instant::now(),
            last_logging_time: Instant::now(),
        }
    }
}

#[cfg(test)]
impl AdaptiveConcurrencyController {
    pub fn new_testing(concurrency: usize, concurrency_bounds: (usize, usize)) -> Arc<Self> {
        Arc::new(Self {
            // Start with 2x the minimum; increase over time.
            state: Mutex::new(ConcurrencyControllerState::new_testing()),
            concurrency_semaphore: AdjustableSemaphore::new(concurrency, concurrency_bounds),
            min_concurrency_increase_delay: Duration::from_millis(test_constants::INCR_SPACING_MS),
            min_concurrency_decrease_delay: Duration::from_millis(test_constants::DECR_SPACING_MS),
            logging_tag: "testing",
        })
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{self, advance, Duration};

    use super::test_constants::*;
    use super::*;

    pub const B: u64 = 1000;

    #[tokio::test]
    async fn test_permit_increase_to_max_on_repeated_success() {
        time::pause();

        let controller = AdaptiveConcurrencyController::new_testing(1, (1, 4));

        for i in 0..10 {
            let permit = controller.acquire_connection_permit().await.unwrap();
            // Increase the duration, so we're always going faster than predicted
            advance(Duration::from_millis(12 - i)).await;
            permit.report_completion(B, true).await;

            advance(Duration::from_millis(INCR_SPACING_MS + 1)).await;
        }

        // Expected the permits to increase to exactly 4 after 5 successful completions.
        assert_eq!(controller.total_permits(), 4);
        assert_eq!(controller.available_permits(), 4);
    }

    #[tokio::test]
    async fn test_permit_increase_to_max_slowly() {
        time::pause();

        let controller = AdaptiveConcurrencyController::new_testing(1, (1, 50));

        // Advance on so that the first success will trigger an adjustment.
        advance(Duration::from_millis(INCR_SPACING_MS + 1)).await;

        for i in 0..5 {
            let permit = controller.acquire_connection_permit().await.unwrap();
            // Increase the duration, so we're always going faster than predicted
            advance(Duration::from_millis(12 - i)).await;
            permit.report_completion(B, true).await;

            // Don't advance, so it should have only incremented by one as not enough time
            // will have passed for more.
        }

        assert_eq!(controller.available_permits(), 2);
        assert_eq!(controller.total_permits(), 2);

        // Now, advance the clock by enough time to allow another change.
        advance(Duration::from_millis(INCR_SPACING_MS + 1)).await;

        for i in 5..10 {
            let permit = controller.acquire_connection_permit().await.unwrap();
            // Increase the duration, so we're always going faster than predicted
            advance(Duration::from_millis(12 - i)).await;
            permit.report_completion(B, true).await;

            // Don't advance, so it should have only incremented by one as not enough time
            // will have passed for more.
        }

        // The window above should have had exactly two increases; one at the first success and one within the next
        // interval.
        assert_eq!(controller.available_permits(), 3);
        assert_eq!(controller.total_permits(), 3);
    }

    #[tokio::test]
    async fn test_permit_increase_on_slow_but_good_enough() {
        time::pause();

        let controller = AdaptiveConcurrencyController::new_testing(5, (5, 10));

        for _ in 0..5 {
            let permit = controller.acquire_connection_permit().await.unwrap();
            advance(Duration::from_millis(TARGET_TIME_MS_L - 1)).await;
            permit.report_completion(LARGE_N_BYTES, true).await;
            advance(Duration::from_millis(INCR_SPACING_MS)).await;
        }
    }

    #[tokio::test]
    async fn test_permit_decrease_on_explicit_failure() {
        time::pause();

        let controller = AdaptiveConcurrencyController::new_testing(10, (5, 10));

        // This should drop the number of permits down to the minimum.
        for i in 1..=5 {
            let permit = controller.acquire_connection_permit().await.unwrap();
            advance(Duration::from_millis(DECR_SPACING_MS + 1)).await;
            permit.report_completion(LARGE_N_BYTES, false).await;

            // Each of the above should drop down the number of permits
            assert_eq!(controller.available_permits(), 10 - i);
        }

        assert_eq!(controller.available_permits(), 5);
    }

    #[tokio::test]
    async fn test_retryable_failures_count_against_success() {
        time::pause();

        let controller = AdaptiveConcurrencyController::new_testing(4, (1, 4));

        let permit = controller.acquire_connection_permit().await.unwrap();

        advance(Duration::from_millis(DECR_SPACING_MS + 1)).await;

        // One failure; should cause a decrease in the number of permits.
        permit.report_retryable_failure().await;

        // Number available should have decreased from 4 to 3 due to the retryable failure, 2 available
        // due to permit acquired above.
        assert_eq!(controller.total_permits(), 3);
        assert_eq!(controller.available_permits(), 2);

        // Another failure; should not cause a decrease in the number of permits
        // yet due to the previous decrease without any time passing.
        permit.report_retryable_failure().await;

        assert_eq!(controller.total_permits(), 3);
        assert_eq!(controller.available_permits(), 2);

        // Acquire the rest of the permits.
        let permit_1 = controller.acquire_connection_permit().await.unwrap();
        let _permit_2 = controller.acquire_connection_permit().await.unwrap();

        assert_eq!(controller.total_permits(), 3);
        assert_eq!(controller.available_permits(), 0);

        // Report one as a retryable failure.
        advance(Duration::from_millis(DECR_SPACING_MS + 1)).await;
        permit_1.report_retryable_failure().await;

        assert_eq!(controller.total_permits(), 2);
        assert_eq!(controller.available_permits(), 0);

        // Now, resolve this permit, with a success. However, this shouldn't change anything, including the number of
        // available permits.
        permit.report_completion(0, true).await;
        assert_eq!(controller.total_permits(), 2);
        assert_eq!(controller.available_permits(), 0);

        // Shouldn't cause a change due to previous change happening immediately before this.
        permit_1.report_completion(0, true).await;
        assert_eq!(controller.total_permits(), 2);
        assert_eq!(controller.available_permits(), 1);
    }
}
