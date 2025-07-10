use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{debug, info};
use utils::adjustable_semaphore::{AdjustableSemaphore, AdjustableSemaphorePermit};

use crate::constants::*;
use crate::CasClientError;

/// The internal state of the concurrency controller.
struct ConcurrencyControllerState {
    // A running tally of how many of the last transfers were within their target completion time.
    tracked_successful_transfers: VecDeque<(Instant, bool)>,

    // Constants that determine controls on the tracking window
    tracking_window_time: Duration,
    tracking_window_size: usize,

    // The last time we adjusted the permits.
    last_adjustment_time: Instant,

    // The last time we reported the concurrency in the log; just log this once every 10 seconds.
    last_logging_time: Instant,
}

impl ConcurrencyControllerState {
    fn new() -> Self {
        let tracking_window_size = *CONCURRENCY_CONTROL_TRACKING_SIZE;
        let tracking_window_time = Duration::from_millis(*CONCURRENCY_CONTROL_TRACKING_WINDOW_MS);

        Self {
            tracked_successful_transfers: VecDeque::with_capacity(tracking_window_size),
            last_adjustment_time: Instant::now(),
            last_logging_time: Instant::now(),
            tracking_window_time,
            tracking_window_size,
        }
    }
}

/// A controller for robustly adjusting the amount of concurrancy on upload and download paths.
///
/// By default, the controller dynamically adjusts the concurrency within bounds so that 80% of the
/// transfers are completed within 20 seconds; it increases the concurrency as long as this criteria
/// is met.  When more than 20% of the transfers begin taking longer than that, concurrency is reduced.
/// Concurrency adjustments are throttled so that increasing the concurrency happens only every
/// 500ms, and decreasing it happens at most once every 250ms.   (These values are all defaults; see
/// the constants and their definitions in constants.rs).
///  
/// More formally:
///  
/// A "success" is a transfer that completed successfully within a specified amount of time that
/// is determined by the size of the transfer.  
///   - For 64MB uploads and downloads, this is defined as completion within
///     CONCURRENCY_CONTROL_TARGET_TIME_LARGE_TRANSFER_MS.
///   - For 0B transfers, this is defined as CONCURRENCY_CONTROL_TARGET_TIME_SMALL_TRANSFER_MS.
///   - The expected time is scaled linearly between these two endpoints based on size.
///
/// The last CONCURRENCY_CONTROL_TRACKING_SIZE successess or failures are tracked to estimate the
/// success_ratio, and only events within CONCURRENCY_CONTROL_TRACKING_WINDOW_MS are considered.  A
/// retry attempt is counted as a failure.
///
/// When a transfer is completed, the concurrency is updated based on the recent success_ratio and
/// whether that transfer was a success.  However, increases are made at most once every
/// CONCURRENCY_CONTROL_MIN_INCREASE_WINDOW_MS and decreases at most every
/// CONCURRENCY_CONTROL_MIN_DECREASE_WINDOW_MS.
pub struct AdaptiveConcurrencyController {
    // The current state, including tracking information and when previous adjustments were made.
    // Also holds related constants
    state: Arc<Mutex<ConcurrencyControllerState>>,

    // The semaphore from which new permits are issued.
    concurrency_semaphore: Arc<AdjustableSemaphore>,

    // Constants determining whether a transfer is a success (succeeds and within time limit) or a
    // failure.
    target_time_large: Duration,
    target_time_small: Duration,
    large_transfer_n_bytes: u64,

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
            state: Arc::new(Mutex::new(ConcurrencyControllerState::new())),
            concurrency_semaphore: AdjustableSemaphore::new(current_concurrency, (min_concurrency, max_concurrency)),
            target_time_large: Duration::from_millis(*CONCURRENCY_CONTROL_TARGET_TIME_LARGE_TRANSFER_MS),
            target_time_small: Duration::from_millis(*CONCURRENCY_CONTROL_TARGET_TIME_SMALL_TRANSFER_MS),
            large_transfer_n_bytes: *CONCURRENCY_CONTROL_LARGE_TRANSFER_NUM_BYTES,

            min_concurrency_increase_delay: Duration::from_millis(*CONCURRENCY_CONTROL_MIN_INCREASE_WINDOW_MS),
            min_concurrency_decrease_delay: Duration::from_millis(*CONCURRENCY_CONTROL_MIN_DECREASE_WINDOW_MS),
            logging_tag,
        })
    }

    /// Acquire a connection permit based on the current concurrancy.
    pub async fn acquire_connection_permit(self: &Arc<Self>) -> Result<ConnectionPermit, CasClientError> {
        let permit = self.concurrency_semaphore.acquire().await?;

        Ok(ConnectionPermit {
            permit,
            controller: Arc::clone(self),
            transfer_start_time: Instant::now(),
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
}

impl ConcurrencyControllerState {
    /// Add a new report to the state, returning the current success ratio
    fn add_report(&mut self, success: bool) -> f64 {
        // Refine to the current size.
        if self.tracked_successful_transfers.len() == self.tracking_window_size {
            self.tracked_successful_transfers.pop_front();
        }

        // Refine to the current time window.
        while let Some((ts, _)) = self.tracked_successful_transfers.front() {
            if ts.elapsed() > self.tracking_window_time {
                self.tracked_successful_transfers.pop_front();
            } else {
                break;
            }
        }

        self.tracked_successful_transfers.push_back((Instant::now(), success));

        // Now calculate the current success ratio.
        let n = self.tracked_successful_transfers.len();

        let success_count: usize = self.tracked_successful_transfers.iter().map(|b| if b.1 { 1 } else { 0 }).sum();

        (success_count as f64) / (n as f64)
    }
}

impl AdaptiveConcurrencyController {
    /// Consider an adjustment to the concurrency if possible.
    async fn update_concurrency(&self, success: bool) {
        let mut state_lg = self.state.lock().await;

        let success_ratio = state_lg.add_report(success);

        if success && (success_ratio > 0.8) {
            // Consider adjusting the concurrency
            if state_lg.last_adjustment_time.elapsed() > self.min_concurrency_increase_delay {
                // Enough time has passed, so add a new permit.
                if self.concurrency_semaphore.increment_total_permits() {
                    state_lg.last_adjustment_time = Instant::now();
                    debug!(
                        "Increasing concurrency for {} to {} due to successful completion and success_ratio = {:.2}",
                        self.logging_tag,
                        self.concurrency_semaphore.total_permits(),
                        success_ratio
                    );
                }
            }
        } else {
            // Had a failure, so attempt to decrease the number of permits.
            if state_lg.last_adjustment_time.elapsed() > self.min_concurrency_decrease_delay {
                let decrease_reason = {
                    if success {
                        format!("success_ratio = {success_ratio:.2} < 0.8")
                    } else {
                        format!("failed transfer with success_ratio = {success_ratio:.2}")
                    }
                };

                // Enough time has passed, so add a new permit.
                if self.concurrency_semaphore.decrement_total_permits() {
                    state_lg.last_adjustment_time = Instant::now();
                    debug!(
                        "Decreasing concurrency for {} to {} due to {decrease_reason}",
                        self.logging_tag,
                        self.concurrency_semaphore.total_permits()
                    );
                }
            }
        }

        if state_lg.last_logging_time.elapsed() > Duration::from_millis(*CONCURRENCY_CONTROL_LOGGING_INTERVAL_MS) {
            info!(
                "Concurrency for {} at {}; current success ratio = {success_ratio:.2}",
                self.logging_tag,
                self.concurrency_semaphore.total_permits()
            );
            state_lg.last_logging_time = Instant::now()
        }
    }

    /// Record a failure related to connection speed (e.g. a timeout) that will be retried.  
    /// With this, we can't decrease the number of permits, so just record there has been a failure.
    async fn report_retryable_failure(&self) {
        self.update_concurrency(false).await;
    }

    /// Report task completion.
    async fn report_completion(&self, conn_permit: ConnectionPermit, n_bytes: u64, success: bool) {
        // Did this complete in the target time window?
        let duration = conn_permit.transfer_start_time.elapsed();

        // Is this transfer within the target time window?
        let success = success && (duration < self.target_duration(n_bytes));

        // Attempt an adjustment
        self.update_concurrency(success).await;
    }

    /// The target max duration for a transfer.
    fn target_duration(&self, n_bytes: u64) -> Duration {
        let a = self.target_time_small.as_secs_f64();
        let b = self.target_time_large.as_secs_f64();
        let s = ((n_bytes as f64) / (self.large_transfer_n_bytes as f64)).clamp(0., 1.);

        Duration::from_secs_f64(a + s * (b - a))
    }
}

/// A permit for a connection.  This can be used to track the start time of a transfer and report back
/// to the original controller whether it's needed.
pub struct ConnectionPermit {
    permit: AdjustableSemaphorePermit,
    controller: Arc<AdaptiveConcurrencyController>,
    transfer_start_time: Instant,
}

impl ConnectionPermit {
    /// Call this right before starting a transfer; records start time.
    pub(crate) fn transfer_starting(&mut self) {
        self.transfer_start_time = Instant::now();
    }

    /// Call this after a successful transfer, providing the byte count.
    pub(crate) async fn report_completion(self, n_bytes: u64, success: bool) {
        self.controller.clone().report_completion(self, n_bytes, success).await;
    }

    pub(crate) async fn report_retryable_failure(&self) {
        self.controller.report_retryable_failure().await;
    }
}

// Testing routines.

#[cfg(test)]
mod test_constants {

    pub const TARGET_TIME_MS_L: u64 = 20;
    pub const TARGET_TIME_MS_S: u64 = 5;
    pub const INCR_SPACING_MS: u64 = 4;
    pub const DECR_SPACING_MS: u64 = 2;
    pub const TRACKING_WINDOW_MS: u64 = 200;
    pub const TRACKING_WINDOW_SIZE: usize = 16;
    pub const LARGE_N_BYTES: u64 = 1000;
}

#[cfg(test)]
impl ConcurrencyControllerState {
    fn new_testing() -> Self {
        let tracking_window_size = test_constants::TRACKING_WINDOW_SIZE;
        let tracking_window_time = Duration::from_millis(test_constants::TRACKING_WINDOW_MS);

        Self {
            tracked_successful_transfers: VecDeque::with_capacity(tracking_window_size),
            last_adjustment_time: Instant::now(),
            tracking_window_time,
            tracking_window_size,
            last_logging_time: Instant::now(),
        }
    }
}

#[cfg(test)]
impl AdaptiveConcurrencyController {
    pub fn new_testing(concurrency: usize, concurrency_bounds: (usize, usize)) -> Arc<Self> {
        Arc::new(Self {
            // Start with 2x the minimum; increase over time.
            state: Arc::new(Mutex::new(ConcurrencyControllerState::new_testing())),
            concurrency_semaphore: AdjustableSemaphore::new(concurrency, concurrency_bounds),
            target_time_large: Duration::from_millis(test_constants::TARGET_TIME_MS_L),
            target_time_small: Duration::from_millis(test_constants::TARGET_TIME_MS_S),
            large_transfer_n_bytes: test_constants::LARGE_N_BYTES,
            min_concurrency_increase_delay: Duration::from_millis(test_constants::INCR_SPACING_MS),
            min_concurrency_decrease_delay: Duration::from_millis(test_constants::DECR_SPACING_MS),
            logging_tag: "testing",
        })
    }

    pub async fn set_tracked(&self, tracked_state: &[bool]) {
        self.state.lock().await.tracked_successful_transfers =
            tracked_state.iter().map(|ts| (Instant::now(), *ts)).collect();
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{self, advance, Duration};

    use super::test_constants::*;
    use super::*;

    #[tokio::test]
    async fn test_permit_increase_to_max_on_repeated_success() {
        time::pause();

        let controller = AdaptiveConcurrencyController::new_testing(1, (1, 4));

        for _ in 0..10 {
            let permit = controller.acquire_connection_permit().await.unwrap();
            advance(Duration::from_millis(1)).await;
            permit.report_completion(LARGE_N_BYTES, true).await;
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

        let t = Instant::now();

        while t.elapsed() < Duration::from_millis(INCR_SPACING_MS + 2) {
            let permit = controller.acquire_connection_permit().await.unwrap();
            advance(Duration::from_millis(1)).await;
            permit.report_completion(LARGE_N_BYTES, true).await;
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
    async fn test_permit_decrease_on_success_but_too_slow() {
        time::pause();

        let controller = AdaptiveConcurrencyController::new_testing(10, (5, 10));

        for i in 1..=5 {
            advance(Duration::from_millis(DECR_SPACING_MS + 1)).await;

            let permit = controller.acquire_connection_permit().await.unwrap();

            advance(Duration::from_millis(TARGET_TIME_MS_S + 1)).await;

            // This was successful, but too slow for a small packet, so it should decrease the concurrancy.
            permit.report_completion(0, true).await;

            let p = controller.available_permits();
            assert_eq!(p, 10 - i);
        }

        let ending = controller.available_permits();
        assert_eq!(ending, 5);
    }

    #[tokio::test]
    async fn test_decrease_on_mixed_results() {
        time::pause();

        let controller = AdaptiveConcurrencyController::new_testing(3, (2, 4));

        for i in 0..10 {
            let permit = controller.acquire_connection_permit().await.unwrap();
            if i % 2 == 0 {
                advance(Duration::from_millis(1)).await;
                permit.report_completion(LARGE_N_BYTES, true).await;
            } else {
                advance(Duration::from_millis(TARGET_TIME_MS_L + 10)).await;
                permit.report_completion(LARGE_N_BYTES, true).await;
            }
            advance(Duration::from_millis(INCR_SPACING_MS + DECR_SPACING_MS)).await;
        }

        let final_permits = controller.available_permits();
        assert_eq!(final_permits, 2);
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
        let permit_2 = controller.acquire_connection_permit().await.unwrap();

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

        // A success, but due to the previous number of reported failures, should still
        // cause a decrease.
        advance(Duration::from_millis(DECR_SPACING_MS + 1)).await;
        permit_2.report_completion(0, true).await;

        assert_eq!(controller.total_permits(), 1);
        assert_eq!(controller.available_permits(), 0);

        // Shouldn't cause a change due to previous change happening immediately before this.
        permit_1.report_completion(0, true).await;
        assert_eq!(controller.total_permits(), 1);
        assert_eq!(controller.available_permits(), 1);

        // Set the controller state to all true.
        controller.set_tracked(&[true]).await;
        advance(Duration::from_millis(INCR_SPACING_MS + DECR_SPACING_MS + 1)).await;
    }
}
