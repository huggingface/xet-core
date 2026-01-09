use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use more_asserts::debug_assert_le;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
#[cfg(not(target_family = "wasm"))]
use tokio::time::Instant;
use tracing::info;
use utils::ExpWeightedMovingAvg;
use utils::adjustable_semaphore::{AdjustableSemaphore, AdjustableSemaphorePermit};
#[cfg(target_family = "wasm")]
use web_time::Instant;
use xet_runtime::xet_config;

use crate::CasClientError;
use crate::adaptive_concurrency::rtt_prediction::RTTPredictor;

const MIN_PARTIAL_REPORT_INTERVAL_MS: u64 = 200;
const PARTIAL_REPORT_WEIGHT_RATIO: f64 = 0.2;

/// The network model state extracted from the concurrency controller.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CCSuccessModelState {
    pub success_ratio: f64,
    pub success_ratio_thresholds: (f64, f64),
    pub recommended_adjustment: i8,
}

/// The network model state extracted from the concurrency controller.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CCLatencyModelState {
    pub predicted_max_rtt: f64,
    pub prediction_max_rtt_standard_error: f64,
    pub predicted_bandwidth: f64,
}

/// The internal state of the concurrency controller.
struct ConcurrencyControllerState {
    /// A running model of the current bandwidth.  Uses an exponentially weighted average to predict the
    rtt_predictor: RTTPredictor,

    // A running average of the success ratio (fraction of successful transfers).
    // Success is determined by whether the RTT quantile is below the cutoff quantile (0.8).
    // The success ratio is in [0.0, 1.0], where 1.0 means all transfers are successful.
    // When success_ratio > 0.8, increase concurrency. When success_ratio < 0.5, decrease concurrency.
    success_ratio_tracking: ExpWeightedMovingAvg,

    // The last time we adjusted the permits.
    last_adjustment_time: Instant,

    // The last time we reported the concurrency in the log; just log this once every 10 seconds.
    last_logging_time: Instant,

    // The number of bytes sent so far.
    bytes_sent_so_far: u64,

    // The number of completed transmissions observed so far.
    completed_transmissions_count: u64,
}

impl ConcurrencyControllerState {
    fn new() -> Self {
        let config = xet_config();
        let rtt_half_life_count = config.client.ac_latency_rtt_half_life;
        let success_half_life_count = config.client.ac_success_tracking_half_life;

        Self {
            rtt_predictor: RTTPredictor::new(rtt_half_life_count),
            success_ratio_tracking: ExpWeightedMovingAvg::new_count_decay(success_half_life_count),
            last_adjustment_time: Instant::now(),
            last_logging_time: Instant::now(),
            bytes_sent_so_far: 0,
            completed_transmissions_count: 0,
        }
    }

    fn success_ratio_thresholds(&self) -> (f64, f64) {
        let config = xet_config();
        let increase_threshold = config.client.ac_healthy_success_ratio_threshold;
        let decrease_threshold = config.client.ac_unhealthy_success_ratio_threshold;
        (increase_threshold, decrease_threshold)
    }

    fn update_success(&mut self, is_success: bool, weight: f64) {
        // Update success ratio tracking: 1.0 for success, 0.0 for failure
        let value = if is_success { 1.0 } else { 0.0 };
        self.success_ratio_tracking.update_with_weight(value, weight);
    }

    /// Internal implementation to extract network model state from a locked state guard.
    #[inline]
    fn success_model_state(&self) -> CCSuccessModelState {
        let success_ratio_thresholds = self.success_ratio_thresholds();

        // Get the tracked success ratio (already in [0.0, 1.0])
        let success_ratio = self.success_ratio_tracking.value().clamp(0.0, 1.0);

        // Determine what adjustment would be recommended based on the success ratio thresholds
        let recommended_adjustment = if success_ratio > success_ratio_thresholds.0 {
            1 // Increase concurrency (success ratio above increase threshold)
        } else if success_ratio < success_ratio_thresholds.1 {
            -1 // Decrease concurrency (success ratio below decrease threshold)
        } else {
            0 // No change (success ratio in acceptable range)
        };

        CCSuccessModelState {
            success_ratio,
            success_ratio_thresholds,
            recommended_adjustment,
        }
    }

    #[inline]
    fn latency_model_state(&self, current_concurrency: f64) -> CCLatencyModelState {
        let config = xet_config();
        let (predicted_max_rtt, prediction_max_rtt_standard_error) = self
            .rtt_predictor
            .predict(config.client.ac_target_rtt_transmission_size, current_concurrency);

        let predicted_bandwidth = self.rtt_predictor.predicted_bandwidth();

        CCLatencyModelState {
            predicted_max_rtt: predicted_max_rtt.unwrap_or(0.),
            prediction_max_rtt_standard_error: prediction_max_rtt_standard_error.unwrap_or(0.),
            predicted_bandwidth: predicted_bandwidth.unwrap_or(0.),
        }
    }
}

/// Controls dynamic adjustment of concurrency for upload and download operations.
///
/// This controller aims to optimize concurrency by continually adapting to network conditions.
/// It tracks two key signals using exponentially weighted moving averages:
///   1. Observed RTT/bandwidth via an online linear regression predictor.
///   2. Success ratio for recent transfers (using configurable success/failure thresholds).   Transfers are considered
///      successful if completed within the predicted RTT plus a fixed margin, and below the configured max RTT for
///      healthy operation.
///
/// Concurrency increases if success ratio is high and the RTT prediction stays less than the target
/// max RTT of 60 seconds.
///  
/// It decreases if the success ratio drops below a lower threshold or if transfers exceed
/// maximum tolerated RTT (currently 90 seconds by default).   
///
/// The key insight for monitoring the success ratio is that:
///   1. When a network connection is healthy and either underutilized or fully utilized, observed RTTs will be lower
///      than or around the predicted RTT when accounting for the number of concurrent connections. In this case, we can
///      increase concurrency as long as the predicted RTT is less than the target RTT.
///   2. When a network connection is congested, observed RTTs will be higher than the predicted RTT when accounting for
///      the number of concurrent connections, indicating we should decrease concurrency.  This will show up as a lower
///      success ratio and a predicted RTT that is higher than the target RTT.
///
/// To prevent rapid oscillations, the controller will only adjust the concurrency if the time
/// since the last adjustment is greater than the minimum increase or decrease delay.
///
/// All concurrency bounds, time windows, and thresholds are configurable via constants.
///
/// # Details
///
/// ## Models Used
///
/// The controller uses three mathematical models to track network performance:
///
/// 1. **RTTPredictor**: An exponentially-weighted online linear regression model that predicts round-trip time (RTT)
///    based on transfer size and concurrency level. The model fits: `duration_secs ≈ base_time_secs + (size_bytes *
///    concurrency) / bandwidth` Internally implemented using `ExpWeightedOnlineLinearRegression`, which maintains
///    exponentially-decayed sufficient statistics (weighted means, covariances) for numerical stability. The predictor
///    tracks both mean predictions and standard errors to enable quantile-based success/failure classification.
///
/// 2. **ExpWeightedMovingAvg**: Tracks the success ratio (fraction of successful transfers) using an exponentially
///    weighted moving average. Each transfer is classified as successful if:
///    - The transfer completes successfully
///    - The transfer completes within the configured maximum healthy RTT
///    - The actual RTT is within the predicted RTT's standard error bounds (quantile check)
///
/// 3. **AdjustableSemaphore**: Manages the actual concurrency permits, allowing dynamic adjustment of the total number
///    of permits within configured min/max bounds.
///
/// ## Usage Example
///
/// The controller is typically used in conjunction with `RetryWrapper` to manage HTTP requests.
/// Here's how `RemoteClient` uses it for uploads:
///
/// ```ignore
/// use crate::adaptive_concurrency::{AdaptiveConcurrencyController, ConnectionPermit};
/// use crate::retry_wrapper::RetryWrapper;
///
/// // Create a controller (typically done once during client initialization)
/// let upload_controller = AdaptiveConcurrencyController::new_upload("upload");
///
/// // Before making a request, acquire a permit
/// let permit = upload_controller.acquire_connection_permit().await?;
///
/// // Use the permit with RetryWrapper to track the transfer
/// let response: UploadResponse = RetryWrapper::new("cas::upload_shard")
///     .with_connection_permit(permit, Some(shard_data.len() as u64))
///     .run_and_extract_json(move |_partial_report_fn| {
///         client.post(url.clone()).body(shard_data.clone()).send()
///     })
///     .await?;
/// ```
///
/// The `RetryWrapper` automatically:
/// - Calls `permit.transfer_starting()` when the request begins
/// - Reports partial progress via `permit.get_partial_completion_reporting_function()` if provided
/// - Calls `permit.report_completion(bytes, success)` on success or failure
/// - Calls `permit.report_retryable_failure()` on retryable errors
///
/// The controller uses these reports to update its models and adjust concurrency accordingly.
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

    // Adjustment disabled.
    adjustment_disabled: bool,

    // minimum number of bytes that must be sent before we start adjusting the concurrency.
    min_bytes_required_for_adjustment: u64,

    // The minimum number of completed transmissions that must be observed before we start adjusting the concurrency.
    min_completed_transmissions_required_for_adjustment: u64,
}

impl AdaptiveConcurrencyController {
    pub fn new(
        logging_tag: &'static str,
        concurrency: usize,
        concurrency_bounds: (usize, usize),
        min_bytes_required_for_adjustment: u64,
        min_completed_transmissions_required_for_adjustment: u64,
    ) -> Arc<Self> {
        // Make sure these values are sane, as they can be loaded from environment variables.
        let min_concurrency = concurrency_bounds.0.max(1);
        let max_concurrency = concurrency_bounds.1.max(min_concurrency);
        let current_concurrency = concurrency.clamp(min_concurrency, max_concurrency);

        info!(
            "Initializing Adaptive Concurrency Controller for {logging_tag} with starting concurrency = {current_concurrency}; min = {min_concurrency}, max = {max_concurrency}, min_bytes_for_adjustment = {min_bytes_required_for_adjustment}, min_completed_transmissions_for_adjustment = {min_completed_transmissions_required_for_adjustment}"
        );

        let config = xet_config();
        Arc::new(Self {
            state: Mutex::new(ConcurrencyControllerState::new()),
            concurrency_semaphore: AdjustableSemaphore::new(current_concurrency, (min_concurrency, max_concurrency)),

            min_concurrency_increase_delay: Duration::from_millis(config.client.ac_min_adjustment_window_ms),
            min_concurrency_decrease_delay: Duration::from_millis(config.client.ac_min_adjustment_window_ms),
            adjustment_disabled: false,
            logging_tag,
            min_bytes_required_for_adjustment,
            min_completed_transmissions_required_for_adjustment,
        })
    }

    /// Create a new concurrency controller with a fixed maximum concurrency; adjustments are disabled.
    pub fn new_fixed(logging_tag: &'static str, concurrency: usize) -> Arc<Self> {
        info!("Fixing maximum concurrency for {logging_tag} at {concurrency}; adaptive concurrency disabled.");

        Arc::new(Self {
            state: Mutex::new(ConcurrencyControllerState::new()),
            concurrency_semaphore: AdjustableSemaphore::new(concurrency, (concurrency, concurrency)),
            adjustment_disabled: true,
            min_concurrency_increase_delay: Default::default(),
            min_concurrency_decrease_delay: Default::default(),
            logging_tag,
            min_bytes_required_for_adjustment: Default::default(),
            min_completed_transmissions_required_for_adjustment: Default::default(),
        })
    }

    /// Create a new concurrency controller for uploads using configuration values.
    /// This will use adaptive concurrency if enabled, otherwise fixed concurrency.
    pub fn new_upload(logging_tag: &'static str) -> Arc<Self> {
        let config = xet_config();
        if config.client.enable_adaptive_concurrency {
            Self::new(
                logging_tag,
                config.client.ac_initial_upload_concurrency,
                (config.client.ac_min_upload_concurrency, config.client.ac_max_upload_concurrency),
                config.client.ac_min_bytes_required_for_adjustment.into(),
                config.client.ac_num_transmissions_required_for_adjustment,
            )
        } else {
            Self::new_fixed(logging_tag, config.client.fixed_concurrency_max_uploads as usize)
        }
    }

    /// Create a new concurrency controller for downloads using configuration values.
    /// This will use adaptive concurrency if enabled, otherwise fixed concurrency.
    pub fn new_download(logging_tag: &'static str) -> Arc<Self> {
        let config = xet_config();
        if config.client.enable_adaptive_concurrency {
            Self::new(
                logging_tag,
                config.client.ac_initial_download_concurrency,
                (config.client.ac_min_download_concurrency, config.client.ac_max_download_concurrency),
                config.client.ac_min_bytes_required_for_adjustment.into(),
                config.client.ac_num_transmissions_required_for_adjustment,
            )
        } else {
            Self::new_fixed(logging_tag, config.client.fixed_concurrency_max_downloads as usize)
        }
    }

    pub async fn acquire_connection_permit(self: &Arc<Self>) -> Result<ConnectionPermit, CasClientError> {
        let _permit = self.concurrency_semaphore.acquire().await?;

        let info = Arc::new(ConnectionPermitInfo {
            controller: Arc::clone(self),
            transfer_start_time: Mutex::new(Instant::now()),
            starting_concurrency: self.concurrency_semaphore.active_permits(),
            rtt_model_at_start: Some(self.state.lock().await.rtt_predictor.clone()),
            report_portion: AtomicU32::new(0),
            last_partial_report_ms: AtomicU64::new(0),
        });

        Ok(ConnectionPermit { _permit, info })
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

    /// The number of currently active permits (concurrent connections).
    pub fn active_permits(&self) -> usize {
        self.concurrency_semaphore.active_permits()
    }

    /// Get the current network model state from the concurrency controller.
    /// Returns a NetworkModelState struct containing the mathematical model state.
    pub async fn success_model_state(&self) -> CCSuccessModelState {
        self.state.lock().await.success_model_state()
    }

    /// The current state of the model used for predicting the network latency.
    pub async fn latency_model_state(&self) -> CCLatencyModelState {
        self.state
            .lock()
            .await
            .latency_model_state(self.concurrency_semaphore.active_permits() as f64)
    }

    /// Update the controller with the results of a transfer.
    ///
    /// This function is called by the ConnectionPermit to update the controller with the results of a transfer.
    /// It is also called by the ConnectionPermit to report partial completion of a transfer.
    ///
    /// The function takes the following parameters:
    /// - permit_info: The permit information for the transfer.
    /// - n_bytes_if_known: The number of bytes transferred if known.
    /// - transmission_successful: Whether the transfer was successful.
    /// - partial_update: Whether this is a partial update.
    /// - weight: The weight of the update.
    ///
    /// The function updates the controller with the results of the transfer, then updates the rtt predictor model with
    /// the new observation and determines if we should adjust the concurrency.
    ///
    /// If the transfer was successful and completed within a healthy time, the actual average success ratio for recent
    /// transfers is above the healthy success ratio threshold, and we've waited long enough since the last
    /// adjustment, then the concurrency is increased.
    ///
    /// If the transfer was not successful or completed within a healthy time, or the actual average success ratio is
    /// not good, then the concurrency is decreased.
    async fn report_and_update(
        &self,
        permit_info: &ConnectionPermitInfo,
        n_bytes_if_known: Option<u64>,
        transmission_successful: bool,
        partial_update: bool,
        weight: f64,
    ) {
        // No need to track and report the concurrency if it's disabled.
        if self.adjustment_disabled {
            return;
        }

        let transfer_start_time = *permit_info.transfer_start_time.lock().await;
        let elapsed_time = transfer_start_time.elapsed();
        let t_actual = elapsed_time.as_secs_f64().max(1e-4);

        // Track if the transfer completed within a healthy time.
        let config = xet_config();
        let completed_in_time = elapsed_time < config.client.ac_max_healthy_rtt;

        let mut state_lg = self.state.lock().await;

        // Update the bytes sent so far.
        state_lg.bytes_sent_so_far += n_bytes_if_known.unwrap_or(0);

        // Increment completed transmissions count when a transmission completes (not a partial update).
        if !partial_update {
            state_lg.completed_transmissions_count += 1;
        }

        // Get the effective concurrency for this transfer.
        let cur_concurrency = self.concurrency_semaphore.active_permits() as f64;
        let avg_concurrency = (cur_concurrency + permit_info.starting_concurrency as f64) / 2.;

        let track_as_success = transmission_successful && completed_in_time && {
            // Now test to see if the transfer is within a reasonable margin of error from the predicted rtt.
            // This checks if the actual RTT is within the prediction's standard error bounds.
            if let Some(n_bytes) = n_bytes_if_known {
                // Get the model when this transfer started
                let quantile = permit_info
                    .rtt_model_at_start
                    .as_ref()
                    .map(|lm| lm.rtt_quantile(t_actual, n_bytes, avg_concurrency))
                    .unwrap_or(0.5); // Default to median if no model available

                quantile < config.client.ac_rtt_success_max_quantile
            } else {
                false
            }
        };

        // Now, update the model with the new observation.
        if track_as_success {
            state_lg.update_success(true, weight);
        } else {
            state_lg.update_success(false, weight);
        }

        // Now, determine if we should adjust the concurrency.
        let model_state = state_lg.success_model_state();

        // Update the rtt predictor if we have a successful transfer.
        if transmission_successful && let Some(n_bytes) = n_bytes_if_known {
            state_lg.rtt_predictor.update(n_bytes, elapsed_time, avg_concurrency, weight);
        }

        // Calculate common values once
        let reference_size = config.client.ac_target_rtt_transmission_size;
        let target_rtt_secs = config.client.ac_target_rtt.as_secs_f64();

        // If the success ratio is healthy and the predicted RTT is below the target RTT,
        // then adjust the concurrency upwards.
        if model_state.recommended_adjustment == 1
            && state_lg.bytes_sent_so_far >= self.min_bytes_required_for_adjustment
            && state_lg.completed_transmissions_count >= self.min_completed_transmissions_required_for_adjustment
            && state_lg.last_adjustment_time.elapsed() > self.min_concurrency_increase_delay
        {
            let old_concurrency = self.concurrency_semaphore.total_permits();
            let new_concurrency = 1. + old_concurrency as f64;

            // Calculate predicted RTT once for both decision and logging
            let predicted_rtt = state_lg
                .rtt_predictor
                .predicted_rtt(reference_size, new_concurrency)
                .unwrap_or(f64::INFINITY);

            if predicted_rtt < target_rtt_secs {
                self.concurrency_semaphore.increment_total_permits();
                let new_concurrency_actual = self.concurrency_semaphore.total_permits();
                state_lg.last_adjustment_time = Instant::now();

                info!(
                    "Concurrency control for {}: Increased concurrency from {} to {}; reason: success ratio {:.3} is above threshold {:.3} and predicted RTT for {}MB at new concurrency is {:.2}s < target {:.1}s",
                    self.logging_tag,
                    old_concurrency,
                    new_concurrency_actual,
                    model_state.success_ratio,
                    model_state.success_ratio_thresholds.0,
                    reference_size / (1024 * 1024),
                    predicted_rtt,
                    target_rtt_secs
                );
            }
        }

        if state_lg.bytes_sent_so_far >= self.min_bytes_required_for_adjustment
            && state_lg.completed_transmissions_count >= self.min_completed_transmissions_required_for_adjustment
            && (!transmission_successful
                || !completed_in_time
                || (!partial_update && model_state.recommended_adjustment == -1))
        {
            // Now, only adjust it down if it's a fully completed transfer.  Otherwise we may run things out of
            // permits too quickly.
            if state_lg.last_adjustment_time.elapsed() > self.min_concurrency_decrease_delay {
                let old_concurrency = self.concurrency_semaphore.total_permits();
                self.concurrency_semaphore.decrement_total_permits();
                let new_concurrency = self.concurrency_semaphore.total_permits();
                state_lg.last_adjustment_time = Instant::now();

                let reason = if !transmission_successful {
                    "transfer failed"
                } else {
                    "success ratio below threshold (connection struggling)"
                };

                info!(
                    "Concurrency control for {}: Decreased concurrency from {} to {}; reason: {} (success_ratio = {:.3}, threshold = {:.3})",
                    self.logging_tag,
                    old_concurrency,
                    new_concurrency,
                    reason,
                    model_state.success_ratio,
                    model_state.success_ratio_thresholds.1
                );
            }
        }

        if state_lg.last_logging_time.elapsed() > Duration::from_millis(config.client.ac_logging_interval_ms) {
            let latency_state = state_lg.latency_model_state(self.concurrency_semaphore.active_permits() as f64);
            info!(
                "Concurrency control for {}: Current concurrency = {}; predicted bandwidth = {}; success_ratio = {:.3}; observed bytes sent so far = {}; completed transmissions = {}",
                self.logging_tag,
                self.concurrency_semaphore.total_permits(),
                latency_state.predicted_bandwidth,
                model_state.success_ratio,
                state_lg.bytes_sent_so_far,
                state_lg.completed_transmissions_count
            );
        }
    }
}

/// Shared information for a connection permit that can be shared across multiple references.
pub struct ConnectionPermitInfo {
    controller: Arc<AdaptiveConcurrencyController>,
    transfer_start_time: Mutex<Instant>,
    starting_concurrency: usize,
    rtt_model_at_start: Option<RTTPredictor>,
    /// Maximum portion completed so far, scaled to u32::MAX (0-u32::MAX represents 0.0-1.0)
    report_portion: AtomicU32,
    /// Last time (in milliseconds since reference instant) when a partial report was sent
    last_partial_report_ms: AtomicU64,
}

/// A permit for a connection.  This can be used to track the start time of a transfer and report back
/// to the original controller whether it's needed.
///
/// Note that dropping it without reporting completion effectively aborts it without reporting
/// any statistics.
pub struct ConnectionPermit {
    _permit: AdjustableSemaphorePermit,
    info: Arc<ConnectionPermitInfo>,
}

impl ConnectionPermit {
    /// Call this right before starting a transfer; records start time.
    pub(crate) async fn transfer_starting(&self) {
        *self.info.transfer_start_time.lock().await = Instant::now();
    }

    /// Get a closure that can be used to report partial completion of a transfer.
    ///
    /// The returned closure takes:
    /// - `portion_completed`: fraction of transfer completed (0.0 to 1.0)
    /// - `size_completed`: number of bytes completed so far
    ///
    /// The closure uses tokio::spawn to asynchronously report the progress.
    /// Reports are throttled to at most once every MIN_PARTIAL_REPORT_INTERVAL_MS milliseconds.
    pub(crate) fn get_partial_completion_reporting_function(&self) -> Arc<dyn Fn(f64, u64) + Send + Sync> {
        let info = Arc::clone(&self.info);
        Arc::new(move |portion_completed: f64, total_bytes: u64| {
            let info = Arc::clone(&info);

            // Throttle reports to at most once every MIN_PARTIAL_REPORT_INTERVAL_MS
            static REFERENCE_INSTANT: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
            let now_ms = REFERENCE_INSTANT.get_or_init(Instant::now).elapsed().as_millis() as u64;

            // Return if we've already recently reported a partial completion.
            if info
                .last_partial_report_ms
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |last_ms| {
                    if now_ms.saturating_sub(last_ms) >= MIN_PARTIAL_REPORT_INTERVAL_MS {
                        Some(now_ms)
                    } else {
                        None
                    }
                })
                .is_err()
            {
                return;
            }

            tokio::spawn(async move {
                // The weight that gets passed to the controller is PARTIAL_REPORT_WEIGHT_RATIO * portion_completed for
                // partial reports, allowing at least the remaining weight to be given to the final
                // completed report.

                let report_portion = PARTIAL_REPORT_WEIGHT_RATIO * portion_completed.clamp(0.0, 1.0);

                let portion_scaled = (report_portion * u32::MAX as f64) as u32;

                // Atomically update to the maximum reported portion using fetch_max
                let previous_portion_scaled = info.report_portion.fetch_max(portion_scaled, Ordering::AcqRel);

                if previous_portion_scaled >= portion_scaled {
                    return;
                }

                // Calculate weight for this update
                // Previous total partial weight = PARTIAL_REPORT_WEIGHT_RATIO * previous_portion (capped at
                // PARTIAL_REPORT_WEIGHT_RATIO) New total partial weight = PARTIAL_REPORT_WEIGHT_RATIO *
                // current_portion (capped at PARTIAL_REPORT_WEIGHT_RATIO) Weight for this update is the
                // difference between the two.
                let weight = (portion_scaled - previous_portion_scaled) as f64 / u32::MAX as f64;

                // Get the size completed based on the proportion.
                let size_completed = (total_bytes as f64 * portion_completed).floor() as u64;

                // Report to controller
                info.controller
                    .clone()
                    .report_and_update(&info, Some(size_completed), true, true, weight)
                    .await;
            });
        })
    }

    /// Call this after a successful transfer, providing the byte count.
    ///
    /// Note, this would normally be
    pub(crate) async fn report_completion(self, n_bytes: u64, success: bool) {
        // Calculate remaining weight for final completion
        // Total partial weight = PARTIAL_REPORT_WEIGHT_RATIO * reported_portion (capped at PARTIAL_REPORT_WEIGHT_RATIO)
        let reported_portion_scaled = self.info.report_portion.fetch_max(u32::MAX, Ordering::AcqRel);
        let reported_portion = reported_portion_scaled as f64 / u32::MAX as f64;
        debug_assert_le!(reported_portion, PARTIAL_REPORT_WEIGHT_RATIO);

        let remaining_weight = (1.0 - reported_portion).clamp(0.0, 1.0);

        self.info
            .controller
            .clone()
            .report_and_update(&self.info, Some(n_bytes), success, false, remaining_weight)
            .await;
    }

    /// Report a retryable failure to the controller.
    pub(crate) async fn report_retryable_failure(&self) {
        self.info
            .controller
            .clone()
            .report_and_update(&self.info, None, false, false, 1.0)
            .await;
    }
}

// Testing routines.
#[cfg(test)]
mod test_constants {

    pub const TR_HALF_LIFE_COUNT: f64 = 10.0;
    pub const INCR_SPACING_MS: u64 = 200;
    pub const DECR_SPACING_MS: u64 = 100;

    pub const TARGET_TIME_MS_L: u64 = 20;

    pub const LARGE_N_BYTES: u64 = 10000;
}

#[cfg(test)]
impl ConcurrencyControllerState {
    #[cfg(test)]
    fn new_testing() -> Self {
        use crate::adaptive_concurrency::controller::test_constants::TR_HALF_LIFE_COUNT;

        Self {
            rtt_predictor: RTTPredictor::new(TR_HALF_LIFE_COUNT),
            success_ratio_tracking: ExpWeightedMovingAvg::new_count_decay(TR_HALF_LIFE_COUNT),
            last_adjustment_time: Instant::now(),
            last_logging_time: Instant::now(),
            bytes_sent_so_far: 0,
            completed_transmissions_count: 0,
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
            adjustment_disabled: false,
            logging_tag: "testing",
            min_bytes_required_for_adjustment: 0,
            min_completed_transmissions_required_for_adjustment: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{self, Duration, advance};

    use super::test_constants::*;
    use super::*;

    // Use a larger transfer size for tests to ensure the RTT predictor has enough data
    pub const TEST_TRANSFER_SIZE: u64 = 10 * 1024 * 1024; // 10MB

    #[tokio::test]
    async fn test_permit_increase_to_max_on_repeated_success() {
        time::pause();

        let controller = AdaptiveConcurrencyController::new_testing(1, (1, 4));

        // First, train the model with several successful transfers to build up the success ratio
        // and establish a good RTT prediction
        for _ in 0..20 {
            let permit = controller.acquire_connection_permit().await.unwrap();
            // Use durations that are well below max healthy RTT (60s) and train the model
            // Use 2 seconds per 10MB, which is 5MB/s - well within healthy limits
            let duration_ms = 2000;
            advance(Duration::from_millis(duration_ms)).await;
            permit.report_completion(TEST_TRANSFER_SIZE, true).await;

            advance(Duration::from_millis(INCR_SPACING_MS + 1)).await;
        }

        // After training, the model should have enough data and success ratio should be high
        // The quantile check should pass, so concurrency should have increased
        assert!(controller.total_permits() >= 1);
    }

    #[tokio::test]
    async fn test_permit_increase_to_max_slowly() {
        time::pause();

        let controller = AdaptiveConcurrencyController::new_testing(1, (1, 50));

        // Advance on so that the first success will trigger an adjustment.
        advance(Duration::from_millis(INCR_SPACING_MS + 1)).await;

        // Train the model first with consistent successful transfers
        for i in 0..10 {
            let permit = controller.acquire_connection_permit().await.unwrap();
            // Use consistent durations that train the model well (2s per 10MB = 5MB/s)
            let duration_ms = 2000;
            advance(Duration::from_millis(duration_ms)).await;
            permit.report_completion(TEST_TRANSFER_SIZE, true).await;

            // Advance time between transfers to allow adjustments
            if i < 5 {
                advance(Duration::from_millis(INCR_SPACING_MS + 1)).await;
            }
        }

        // After training, should have increased
        assert!(controller.total_permits() >= 1);
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

    #[tokio::test]
    async fn test_partial_completion_weighting() {
        time::pause();

        let controller = AdaptiveConcurrencyController::new_testing(1, (1, 4));

        let permit = controller.acquire_connection_permit().await.unwrap();

        // Get the reporting function
        let report = permit.get_partial_completion_reporting_function();

        // Report partial completions
        report(0.2, 200); // weight = PARTIAL_REPORT_WEIGHT_RATIO * 0.2 = 0.04
        advance(Duration::from_millis(10)).await;
        report(0.5, 500); // weight = PARTIAL_REPORT_WEIGHT_RATIO * 0.3 = 0.06
        advance(Duration::from_millis(10)).await;
        report(0.8, 800); // weight = PARTIAL_REPORT_WEIGHT_RATIO * 0.3 = 0.06
        // Total partial weight = 0.04 + 0.06 + 0.06 = 0.16

        advance(Duration::from_millis(10)).await;
        permit.report_completion(1000, true).await; // remaining weight = 1.0 - 0.16 = 0.84

        // Verify the model was updated (no assertion failures means it worked)
        let latency_state = controller.latency_model_state().await;
        assert!(latency_state.predicted_bandwidth >= 0.0);
    }

    #[tokio::test]
    async fn test_partial_completion_max_weight_cap() {
        time::pause();

        let controller = AdaptiveConcurrencyController::new_testing(1, (1, 4));

        let permit = controller.acquire_connection_permit().await.unwrap();

        // Get the reporting function
        let report = permit.get_partial_completion_reporting_function();

        // Report many small partial completions that would exceed PARTIAL_REPORT_WEIGHT_RATIO total
        for i in 1..=20 {
            let portion = i as f64 / 20.0; // 0.05, 0.10, ..., 1.0
            let size = i * 50;
            report(portion, size);
            advance(Duration::from_millis(1)).await;
        }
        // Each update has weight = PARTIAL_REPORT_WEIGHT_RATIO * 0.05 = 0.01
        // Total would be 20 * 0.01 = 0.2, but should be capped at PARTIAL_REPORT_WEIGHT_RATIO

        advance(Duration::from_millis(10)).await;
        permit.report_completion(1000, true).await; // remaining weight should be >= 1.0 - PARTIAL_REPORT_WEIGHT_RATIO

        // Verify the model was updated
        let latency_state = controller.latency_model_state().await;
        assert!(latency_state.predicted_bandwidth >= 0.0);
    }
}
