use std::time::Duration;

use statrs::function::erf::erf;

use crate::adaptive_concurrency::exp_weighted_olr::ExpWeightedOnlineLinearRegression;

/// An RTT (round trip time) predictor using a numerically stable, exponentially decayed linear regression:
///
/// We fit a model of the form:
///   duration_secs ≈ base_time_secs + size_bytes * inv_throughput
/// which is equivalent to:
///   duration_secs ≈ intercept + slope * size_bytes
///
/// Internally, we use a stable, online update method based on weighted means and covariances:
/// - mean_x, mean_y: weighted means of size and duration
/// - s_xx, s_xy: exponentially decayed sums of (x - mean_x)^2 and (x - mean_x)(y - mean_y)
/// - s_yy: exponentially decayed sum of (y - mean_y)^2 (for variance estimation)
///
/// We apply decay on each update using exp2(-1 / half_life_count) where each update counts as 1 sample.
///
/// This avoids numerical instability from large sums and is robust to shifting distributions.
///
/// ## Variance Estimation
///
/// The predictor tracks variance to enable quantile-based concurrency control:
///
/// 1. **Residual Variance (σ²)**: Estimated from the weighted residual sum of squares (RSS):
///    - RSS = Σ w_i (y_i - ŷ_i)² = syy - a*sy - b*sxy
///    - σ² = RSS / (sw - 2), where sw is the effective sample size (sum of weights)
///
/// 2. **Prediction Standard Error**: For a prediction at point x:
///    - SE(pred) = σ * sqrt(1/sw + (x - x̄)²/sxx_c)
///    - This accounts for uncertainty in both the model parameters and the specific prediction point
///
/// 3. **Exponential Weighting**: With exponential decay, older samples have less weight:
///    - Each sample has weight that decays as 2^(-age/half_life)
///    - The sum of weights sw = Σ w_i serves as the effective sample size
///    - Exponential decay is already accounted for in how sw evolves over time
///    - Standard error and variance calculations both use sw
// Use MB for the scale of the size; this is more numerically stable.
#[derive(Clone)]
pub struct RTTPredictor {
    // Core regression model (handles regression math)
    model: ExpWeightedOnlineLinearRegression,
}

// Scale the X part to maintain numerical stability; this effectively says
// the base part is in MB.
const BASE_SIZE_UNIT: f64 = 1024. * 1024.;

impl RTTPredictor {
    /// Create a new RTT predictor with count-based decay.
    ///
    /// - `decay_half_life_count`: the half-life in effective samples (sum of weights). After adding samples with total
    ///   weight equal to this value, the weight of previous observations will be halved.
    pub fn new(decay_half_life_count: f64) -> Self {
        // Note: We use a very large half_life for the regression model since we handle decay manually
        // with weight-proportional decay. The regression model's decay is not used.
        Self {
            model: ExpWeightedOnlineLinearRegression::new(decay_half_life_count),
        }
    }

    /// Updates the RTT model with a new observation.
    ///
    /// - `size_bytes`: the size of the completed transmission.
    /// - `duration`: the time taken to complete the transmission.
    /// - `avg_concurrent`: an estimate of the average number of concurrent connections during transfer.
    /// - `weight`: the weight to assign to this observation, clamped to [0.0, 1.0].
    pub fn update(&mut self, size_bytes: u64, duration: Duration, avg_concurrent: f64, weight: f64) {
        let w = weight.max(0.0);
        let x = (size_bytes as f64) / BASE_SIZE_UNIT; // MiB
        let concurrency_factor = avg_concurrent.max(1.0);
        let x_eff = x * concurrency_factor;
        let y = duration.as_secs_f64().max(1e-9);

        self.model.update(w, x_eff, y);
    }

    /// Predicts the expected RTT (mean) and standard error for a given transfer size and concurrency level.
    ///
    /// Returns `(Option<mean>, Option<std_dev>)` where:
    /// - `mean`: the predicted RTT in seconds, or `None` if the model cannot make a prediction
    /// - `std_dev`: the standard error of the prediction, or `None` if it cannot be computed
    ///
    /// - `size_bytes`: the size of the transfer.
    /// - `avg_concurrent`: the number of concurrent connections.
    pub fn predict(&self, size_bytes: u64, avg_concurrent: f64) -> (Option<f64>, Option<f64>) {
        let x = (size_bytes as f64) / BASE_SIZE_UNIT;
        let concurrency_factor = avg_concurrent.max(1.0);
        let x_eff = x * concurrency_factor;

        let (mean, std_dev) = self.model.predict(x_eff);
        (mean.map(|m| m.max(0.0)), std_dev)
    }

    /// Predicts the expected RTT (round trip time) for a given transfer size and concurrency level.
    ///
    /// - `size_bytes`: the size of the transfer.
    /// - `avg_concurrent`: the number of concurrent connections.
    pub fn predicted_rtt(&self, size_bytes: u64, avg_concurrent: f64) -> Option<f64> {
        self.predict(size_bytes, avg_concurrent).0
    }

    /// Returns the standard error of a prediction at the given size and concurrency.
    ///
    /// - `size_bytes`: the size of the transfer.
    /// - `avg_concurrent`: the number of concurrent connections.
    pub fn prediction_standard_error(&self, size_bytes: u64, avg_concurrent: f64) -> Option<f64> {
        self.predict(size_bytes, avg_concurrent).1
    }

    pub fn predicted_bandwidth(&self) -> Option<f64> {
        let query_bytes = 10 * 1024 * 1024;

        // How long would it take to transmit this at full bandwidth
        let min_rtt = self.predicted_rtt(query_bytes, 1.)?;

        // Report bytes per sec in this model.
        Some(query_bytes as f64 / min_rtt.max(1e-6))
    }

    /// Computes the quantile (0.0 to 1.0) of an observed RTT under the predicted normal distribution.
    ///
    /// Returns the probability that a random sample from N(predicted, se²) would be less than the observed RTT.
    /// This is computed using the normal CDF: P(X < observed) where X ~ N(predicted, se²).
    ///
    /// If the uncertainty is infinite, the standard error is too large relative to the prediction,
    /// or there are not enough samples, returns 0.5 (indicating the observation is at the median).
    ///
    /// - `observed_rtt_secs`: The actual observed RTT in seconds
    /// - `size_bytes`: The size of the transfer in bytes
    /// - `avg_concurrent`: The average concurrency during the transfer
    pub fn rtt_quantile(&self, observed_rtt_secs: f64, size_bytes: u64, avg_concurrent: f64) -> f64 {
        let (mean_opt, std_dev_opt) = self.predict(size_bytes, avg_concurrent);

        let Some(predicted) = mean_opt else {
            return 0.5;
        };

        let Some(se) = std_dev_opt else {
            return 0.5;
        };

        // Check if uncertainty is too high (infinite or very large relative to prediction)
        if !se.is_finite() || se <= 0.0 {
            return 0.5;
        }

        // Compute z-score: (observed - predicted) / se
        let z = (observed_rtt_secs - predicted) / se;

        // Compute normal CDF using error function
        // CDF(x) = 0.5 * (1 + erf(z / sqrt(2)))
        let quantile = 0.5 * (1.0 + erf(z / std::f64::consts::SQRT_2));

        quantile.clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_abs_diff_eq;

    use super::*;

    #[test]
    fn test_estimator_update() {
        let mut estimator = RTTPredictor::new(10.0);
        estimator.update(1_000_000, Duration::from_millis(500), 1., 1.0);
        estimator.update(2_000_000, Duration::from_millis(1000), 1., 1.0); // Need at least 2 samples with different x
        let expected = estimator.predicted_rtt(1_000_000, 1.).unwrap();
        assert!(expected > 0.0);
    }

    #[test]
    fn test_converges_to_constant_observation() {
        for concurrency in [1., 5., 100.] {
            let mut predictor = RTTPredictor::new(10.0);
            // Need at least 2 different sizes to fit a line
            predictor.update(1000, Duration::from_secs_f64(1.0), concurrency, 1.0);
            predictor.update(2000, Duration::from_secs_f64(2.0), concurrency, 1.0);
            for _ in 0..8 {
                predictor.update(1000, Duration::from_secs_f64(1.0), concurrency, 1.0);
            }
            let prediction = predictor.predicted_rtt(1000, concurrency).unwrap();
            assert_abs_diff_eq!(prediction, 1.0, epsilon = 0.01);
        }
    }

    #[test]
    fn test_scaling_with_size() {
        let mut predictor = RTTPredictor::new(10.0);
        // Train with varying sizes to ensure the model learns both base_time and throughput
        // Use different sizes with proportional durations to learn a throughput model
        // Assume 10 MB/s throughput and 0.1s base time
        let sizes = vec![
            (1024 * 1024, 0.2),      // 1 MB in 0.2s (0.1 base + 0.1 transfer)
            (5 * 1024 * 1024, 0.6),  // 5 MB in 0.6s (0.1 base + 0.5 transfer)
            (10 * 1024 * 1024, 1.1), // 10 MB in 1.1s (0.1 base + 1.0 transfer)
        ];
        for (size, duration) in sizes {
            for _ in 0..5 {
                predictor.update(size, Duration::from_secs_f64(duration), 1., 1.0);
            }
        }

        // Test that predictions scale with size
        let predicted_1mb = predictor.predicted_rtt(1024 * 1024, 1.).unwrap();
        let predicted_5mb = predictor.predicted_rtt(5 * 1024 * 1024, 1.).unwrap();
        let predicted_10mb = predictor.predicted_rtt(10 * 1024 * 1024, 1.).unwrap();

        // Larger sizes should result in higher predicted RTT
        assert!(predicted_5mb > predicted_1mb);
        assert!(predicted_10mb > predicted_5mb);

        // Predictions should be reasonable (positive)
        assert!(predicted_1mb > 0.0);
        assert!(predicted_5mb > 0.0);
        assert!(predicted_10mb > 0.0);
    }

    #[test]
    fn test_scaling_with_concurrency() {
        let mut predictor = RTTPredictor::new(10.0);
        // Train with varying sizes to ensure the model learns both base_time and throughput
        // Use different sizes with proportional durations to learn a throughput model
        // Assume 10 MB/s throughput and 0.1s base time
        let sizes = vec![
            (1024 * 1024, 0.2),      // 1 MB in 0.2s (0.1 base + 0.1 transfer)
            (5 * 1024 * 1024, 0.6),  // 5 MB in 0.6s (0.1 base + 0.5 transfer)
            (10 * 1024 * 1024, 1.1), // 10 MB in 1.1s (0.1 base + 1.0 transfer)
        ];
        for (size, duration) in sizes {
            for _ in 0..5 {
                predictor.update(size, Duration::from_secs_f64(duration), 1., 1.0);
            }
        }

        // Test with a medium size (5 MB)
        let test_size = 5 * 1024 * 1024;
        let predicted_1 = predictor.predicted_rtt(test_size, 1.).unwrap();
        let predicted_2 = predictor.predicted_rtt(test_size, 2.).unwrap();
        let predicted_4 = predictor.predicted_rtt(test_size, 4.).unwrap();

        // With concurrency scaling, higher concurrency should result in higher predicted RTT
        // because each connection gets less bandwidth (bandwidth / concurrency)
        // predicted_2 should be approximately 2x the transfer time component (if base_time is small)
        // predicted_4 should be approximately 4x the transfer time component
        assert!(predicted_2 > predicted_1, "predicted_2 ({}) should be > predicted_1 ({})", predicted_2, predicted_1);
        assert!(predicted_4 > predicted_2, "predicted_4 ({}) should be > predicted_2 ({})", predicted_4, predicted_2);

        // Verify that the scaling is approximately linear (within reasonable bounds)
        // If base_time is small, predicted_2 should be roughly 2x predicted_1
        // If base_time is large, the ratio will be less than 2x
        let ratio_2_1 = predicted_2 / predicted_1;
        let ratio_4_2 = predicted_4 / predicted_2;
        // Ratios should be > 1.0 (increasing) but may be less than 2.0 if base_time dominates
        assert!(ratio_2_1 > 1.0 && ratio_2_1 <= 2.5, "ratio_2_1 should be between 1.0 and 2.5, got {}", ratio_2_1);
        assert!(ratio_4_2 > 1.0 && ratio_4_2 <= 2.5, "ratio_4_2 should be between 1.0 and 2.5, got {}", ratio_4_2);
    }

    #[test]
    fn test_variance_tracking() {
        let mut predictor = RTTPredictor::new(10.0);

        // Add some observations with consistent relationship
        // Model: y = 0.1 + 0.1 * x (base 0.1s, 10 MB/s throughput)
        for size_mb in [1, 2, 5, 10] {
            let size_bytes = size_mb * 1024 * 1024;
            let duration = 0.1 + 0.1 * size_mb as f64; // 0.1s base + 0.1s per MB
            predictor.update(size_bytes, Duration::from_secs_f64(duration), 1., 1.0);
        }

        // Should be able to make predictions
        assert!(predictor.predicted_rtt(5 * 1024 * 1024, 1.0).is_some());

        // Standard error should be positive
        let se = predictor.prediction_standard_error(5 * 1024 * 1024, 1.0).unwrap();
        assert!(se >= 0.0);
    }

    #[test]
    fn test_rtt_quantile() {
        let mut predictor = RTTPredictor::new(10.0);

        // Train on consistent data: y = 0.1 + 0.1 * x
        for size_mb in [1, 2, 5, 10] {
            let size_bytes = size_mb * 1024 * 1024;
            let duration = 0.1 + 0.1 * size_mb as f64;
            predictor.update(size_bytes, Duration::from_secs_f64(duration), 1., 1.0);
        }

        let test_size = 5 * 1024 * 1024;
        let predicted = predictor.predicted_rtt(test_size, 1.0).unwrap();

        // When observed equals predicted, quantile should be around 0.5 (median)
        let quantile_at_predicted = predictor.rtt_quantile(predicted, test_size, 1.0);
        assert!(
            (quantile_at_predicted - 0.5).abs() < 0.1,
            "Quantile at predicted should be ~0.5, got {}",
            quantile_at_predicted
        );

        // When observed is much lower than predicted, quantile should be < 0.5
        let quantile_low = predictor.rtt_quantile(predicted * 0.5, test_size, 1.0);
        assert!(quantile_low < 0.5, "Quantile for low RTT should be < 0.5, got {}", quantile_low);

        // When observed is much higher than predicted, quantile should be > 0.5
        let quantile_high = predictor.rtt_quantile(predicted * 2.0, test_size, 1.0);
        assert!(quantile_high > 0.5, "Quantile for high RTT should be > 0.5, got {}", quantile_high);

        // Quantile should be monotonic
        assert!(quantile_low < quantile_at_predicted);
        assert!(quantile_at_predicted < quantile_high);
    }

    #[test]
    fn test_rtt_quantile_with_insufficient_samples() {
        let mut predictor = RTTPredictor::new(10.0);

        // Add only 1 sample (not enough)
        predictor.update(1024 * 1024, Duration::from_secs_f64(0.2), 1., 1.0);

        // Should return 0.5 when insufficient samples
        let quantile = predictor.rtt_quantile(0.5, 1024 * 1024, 1.0);
        assert_eq!(quantile, 0.5, "Should return 0.5 with insufficient samples");
    }

    /// Helper function to create a sample pool with random values
    /// Returns a Vec of (size_bytes, duration) tuples
    /// - sizes: uniform random between 32 MB and 64 MB (converted to bytes)
    /// - duration: size_mb * 1.0 + noise seconds, where noise = std_dev * random_normal
    fn create_sample_pool(num_samples: usize, scale_std_dev: f64, seed: u64) -> Vec<(u64, Duration)> {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        use rand_distr::{Distribution, Normal};
        let mut rng = StdRng::seed_from_u64(seed);
        let normal = Normal::new(0.0, scale_std_dev).unwrap();
        let mut samples = Vec::new();
        for _ in 0..num_samples {
            let size_mb = rng.random_range(32.0..64.0);
            let size_bytes = (size_mb * 1024.0 * 1024.0) as u64;
            let noise = normal.sample(&mut rng);
            let duration_secs = (size_mb * 1.0 + noise).max(0.1);
            samples.push((size_bytes, Duration::from_secs_f64(duration_secs)));
        }
        samples
    }

    /// Helper function to create a predictor from a sample pool and weights
    fn create_predictor(half_life: f64, sample_pool: &[(u64, Duration)], weights: &[f64]) -> RTTPredictor {
        let mut predictor = RTTPredictor::new(half_life);
        for ((size_bytes, duration), &w) in sample_pool.iter().zip(weights.iter()) {
            predictor.update(*size_bytes, *duration, 1.0, w);
        }
        predictor
    }

    /// Utility function to check if two models are approximately the same
    fn assert_models_similar(p1: &RTTPredictor, p2: &RTTPredictor, seed: u64) {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(seed);

        // Check regression models are similar
        assert!(p1.model.approx_equals(&p2.model, 1e-5), "Regression models should be approximately equal");

        // Test a few predictions
        for _ in 0..10 {
            let test_size_mb = rng.random_range(32.0..64.0);
            let test_size = (test_size_mb * 1024.0 * 1024.0) as u64;
            let concurrency = rng.random_range(1.0..10.0);
            let pred1 = p1.predicted_rtt(test_size, concurrency);
            let pred2 = p2.predicted_rtt(test_size, concurrency);
            if let (Some(p1_val), Some(p2_val)) = (pred1, pred2) {
                assert_abs_diff_eq!(p1_val, p2_val, epsilon = 1e-6);
            }
            let se1 = p1.prediction_standard_error(test_size, concurrency);
            let se2 = p2.prediction_standard_error(test_size, concurrency);
            if let (Some(s1_val), Some(s2_val)) = (se1, se2) {
                assert_abs_diff_eq!(s1_val, s2_val, epsilon = 1e-6);
            }
        }
    }

    #[test]
    fn test_weighted_mean_and_variance_fundamentals() {
        // Test that predictions are correct for identical samples
        // Use very long half-life so decay doesn't matter
        let half_life = 1e10;

        // Test with identical samples - prediction should equal the sample value
        let mut predictor = RTTPredictor::new(half_life);
        let test_size = 10 * 1024 * 1024; // 10 MB
        let test_duration = Duration::from_secs_f64(1.1); // 1.1 seconds

        // Need at least 2 different sizes to fit a line, then add more identical samples
        predictor.update(test_size, test_duration, 1.0, 1.0);
        predictor.update(test_size * 2, Duration::from_secs_f64(test_duration.as_secs_f64() * 2.0), 1.0, 1.0);
        for _ in 0..8 {
            predictor.update(test_size, test_duration, 1.0, 1.0);
        }

        // Prediction should equal the sample duration
        let predicted = predictor.predicted_rtt(test_size, 1.0).unwrap();
        assert_abs_diff_eq!(predicted, test_duration.as_secs_f64(), epsilon = 1e-6);

        // Standard error should be low for many identical samples
        let se = predictor.prediction_standard_error(test_size, 1.0).unwrap();
        assert!(se < 0.1, "Standard error should be low for many identical samples");

        // Test with different weights on same value
        let mut predictor2 = RTTPredictor::new(half_life);
        predictor2.update(test_size, test_duration, 1.0, 0.1);
        predictor2.update(test_size * 2, Duration::from_secs_f64(test_duration.as_secs_f64() * 2.0), 1.0, 0.1); // Need different x
        predictor2.update(test_size, test_duration, 1.0, 0.9);

        // Prediction should still equal the sample duration
        let predicted2 = predictor2.predicted_rtt(test_size, 1.0).unwrap();
        assert_abs_diff_eq!(predicted2, test_duration.as_secs_f64(), epsilon = 1e-6);
    }

    #[test]
    fn test_decay_and_weight_scaling() {
        let half_life1 = 100.0;
        let half_life2 = 1000.0;
        let weights = [0.1; 20];
        let sample_pool = create_sample_pool(20, 0.1, 1);

        let predictor1 = create_predictor(half_life1, &sample_pool, &weights);
        let scaled_weights: Vec<f64> = weights.iter().map(|&w| w * 10.0).collect();
        let predictor2 = create_predictor(half_life2, &sample_pool, &scaled_weights);

        let pred1 = predictor1.predicted_rtt(48 * 1024 * 1024, 1.0).unwrap();
        let pred2 = predictor2.predicted_rtt(48 * 1024 * 1024, 1.0).unwrap();
        assert_abs_diff_eq!(pred1, pred2, epsilon = pred1 * 0.01);
    }

    #[test]
    fn test_order_independence_with_slow_decay() {
        let half_life = 1e10;
        let weights = [1.0; 20];
        let sample_pool = create_sample_pool(20, 0.1, 8);

        let predictor1 = create_predictor(half_life, &sample_pool, &weights);

        let reversed_pool: Vec<_> = sample_pool.into_iter().rev().collect();
        let predictor2 = create_predictor(half_life, &reversed_pool, &weights);

        assert_models_similar(&predictor1, &predictor2, 100);
    }

    #[test]
    fn test_variance_order_independence() {
        let half_life = 1e10;
        let weights = [1.0; 20];
        let sample_pool = create_sample_pool(20, 0.1, 4);

        let predictor1 = create_predictor(half_life, &sample_pool, &weights);

        let reversed_pool: Vec<_> = sample_pool.into_iter().rev().collect();
        let predictor2 = create_predictor(half_life, &reversed_pool, &weights);

        assert_models_similar(&predictor1, &predictor2, 100);
    }

    #[test]
    fn test_variance_same_samples_different_weight_order() {
        let half_life = 1e10;
        let weights = [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 1.0];
        let sample_pool = create_sample_pool(11, 0.1, 5);

        let predictor1 = create_predictor(half_life, &sample_pool, &weights);

        let reversed_pool: Vec<_> = sample_pool.iter().rev().copied().collect();
        let reversed_weights: Vec<f64> = weights.iter().rev().copied().collect();
        let predictor2 = create_predictor(half_life, &reversed_pool, &reversed_weights);

        assert_models_similar(&predictor1, &predictor2, 100);
    }

    #[test]
    fn test_variance_same_samples_different_weight_scale() {
        let half_life = 1e10;
        let weights1 = [0.5; 11];
        let weights2 = [1.0; 11];
        let sample_pool = create_sample_pool(50, 0.1, 6);

        let predictor1 = create_predictor(half_life, &sample_pool, &weights1);
        let predictor2 = create_predictor(half_life, &sample_pool, &weights2);

        // Both should predict similar values (weighted means should be the same)
        let test_size = 48 * 1024 * 1024;
        let pred1 = predictor1.predicted_rtt(test_size, 1.0).unwrap();
        let pred2 = predictor2.predicted_rtt(test_size, 1.0).unwrap();
        assert_abs_diff_eq!(pred1, pred2, epsilon = 1e-6);

        // Standard errors may differ due to different weight distributions
        let se1 = predictor1.prediction_standard_error(test_size, 1.0).unwrap();
        let se2 = predictor2.prediction_standard_error(test_size, 1.0).unwrap();
        // Both should be reasonable values
        assert!(se1 > 0.0 && se1.is_finite());
        assert!(se2 > 0.0 && se2.is_finite());
    }

    #[test]
    fn test_variance_with_partial_weights() {
        let half_life = 1e10;
        let weights = [0.5, 0.25, 0.5, 1.0, 0.5, 0.4, 0.5, 0.6, 0.7, 0.5, 1.0];
        let sample_pool = create_sample_pool(11, 0.1, 7);

        let predictor1 = create_predictor(half_life, &sample_pool, &weights);

        let reversed_pool: Vec<_> = sample_pool.iter().rev().copied().collect();
        let reversed_weights: Vec<f64> = weights.iter().rev().copied().collect();
        let predictor2 = create_predictor(half_life, &reversed_pool, &reversed_weights);

        let weights3 = [1.0; 11];
        let predictor3 = create_predictor(half_life, &sample_pool, &weights3);
        let predictor4 = create_predictor(half_life, &reversed_pool, &weights3);

        // All predictors should predict similar values
        let test_size = 48 * 1024 * 1024;
        let pred1 = predictor1.predicted_rtt(test_size, 1.0).unwrap();
        let pred2 = predictor2.predicted_rtt(test_size, 1.0).unwrap();
        let pred3 = predictor3.predicted_rtt(test_size, 1.0).unwrap();
        let pred4 = predictor4.predicted_rtt(test_size, 1.0).unwrap();
        assert_abs_diff_eq!(pred1, pred2, epsilon = 1e-6);
        assert_abs_diff_eq!(pred3, pred4, epsilon = 1e-6);
        let se1 = predictor1.prediction_standard_error(test_size, 1.0).unwrap();
        let se2 = predictor2.prediction_standard_error(test_size, 1.0).unwrap();
        let se3 = predictor3.prediction_standard_error(test_size, 1.0).unwrap();
        let se4 = predictor4.prediction_standard_error(test_size, 1.0).unwrap();

        assert_abs_diff_eq!(se1, se2, epsilon = se1.min(se2) * 0.1);
        assert_abs_diff_eq!(se3, se4, epsilon = se3.min(se4) * 0.1);

        let pred1 = predictor1.predicted_rtt(test_size, 1.0).unwrap();
        let pred2 = predictor2.predicted_rtt(test_size, 1.0).unwrap();
        let pred3 = predictor3.predicted_rtt(test_size, 1.0).unwrap();
        let pred4 = predictor4.predicted_rtt(test_size, 1.0).unwrap();

        assert_abs_diff_eq!(pred1, pred2, epsilon = pred1.min(pred2) * 0.05);
        assert_abs_diff_eq!(pred3, pred4, epsilon = pred3.min(pred4) * 0.05);
    }

    #[test]
    fn test_linear_regression_against_fixed_calculation() {
        let half_life = 1e10;
        let sample_pool = create_sample_pool(10, 0.1, 9);
        let weights = [1.0; 10];

        let predictor = create_predictor(half_life, &sample_pool, &weights);

        // Manually calculate weighted linear regression
        let mut sw = 0.0;
        let mut sx = 0.0;
        let mut sy = 0.0;
        let mut sxx = 0.0;
        let mut sxy = 0.0;

        for (size_bytes, duration) in sample_pool.iter() {
            let x = (*size_bytes as f64) / (1024.0 * 1024.0);
            let y = duration.as_secs_f64();
            sw += 1.0;
            sx += x;
            sy += y;
            sxx += x * x;
            sxy += x * y;
        }

        let xbar = sx / sw;
        let ybar = sy / sw;
        let sxx_c = sxx - sx * sx / sw;
        let sxy_c = sxy - sx * sy / sw;

        let b_expected = sxy_c / sxx_c;
        let a_expected = ybar - b_expected * xbar;

        // Test that predictions match the expected linear regression
        let test_size = 48 * 1024 * 1024;
        let x_test = (test_size as f64) / (1024.0 * 1024.0);
        let y_expected = a_expected + b_expected * x_test;
        let y_predicted = predictor.predicted_rtt(test_size, 1.0).unwrap();
        assert_abs_diff_eq!(y_predicted, y_expected, epsilon = 1e-6);
    }

    #[test]
    fn test_weights_scale_like_multiple_samples() {
        let half_life = 1e10;
        let sample_pool = create_sample_pool(5, 0.1, 10);

        // Version 1: Each sample appears twice, first with weight 0.25, second with weight 0.75
        let mut samples1 = Vec::new();
        let mut weights1 = Vec::new();
        for (size_bytes, duration) in sample_pool.iter() {
            samples1.push((*size_bytes, *duration));
            weights1.push(0.25);
            samples1.push((*size_bytes, *duration));
            weights1.push(0.75);
        }

        // Version 2: Each sample appears twice, both with weight 0.5
        let mut samples2 = Vec::new();
        let weights2 = [0.5; 10];
        for (size_bytes, duration) in sample_pool.iter() {
            samples2.push((*size_bytes, *duration));
            samples2.push((*size_bytes, *duration));
        }

        let predictor1 = create_predictor(half_life, &samples1, &weights1);
        let predictor2 = create_predictor(half_life, &samples2, &weights2);

        // Both should have the same weighted means and predictions
        // Note: sw2 (sum of squared weights) will differ:
        //   Version 1: 0.25² + 0.75² = 0.625
        //   Version 2: 0.5² + 0.5² = 0.5
        // This means standard errors will differ, but predictions (means) should be the same.
        let test_size = 48 * 1024 * 1024;
        let pred1 = predictor1.predicted_rtt(test_size, 1.0).unwrap();
        let pred2 = predictor2.predicted_rtt(test_size, 1.0).unwrap();
        assert_abs_diff_eq!(pred1, pred2, epsilon = 1e-6);

        // Test predictions at various points (should be the same)
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(100);
        for _ in 0..10 {
            let test_size_mb = rng.random_range(32.0..64.0);
            let test_size = (test_size_mb * 1024.0 * 1024.0) as u64;
            let concurrency = rng.random_range(1.0..10.0);
            assert_abs_diff_eq!(
                predictor1.predicted_rtt(test_size, concurrency).unwrap(),
                predictor2.predicted_rtt(test_size, concurrency).unwrap(),
                epsilon = 1e-6
            );
        }

        // Standard errors may differ slightly due to different sw2, but with many samples
        // the difference may be very small (numerical precision)
        let test_size = 48 * 1024 * 1024;
        let se1 = predictor1.prediction_standard_error(test_size, 1.0).unwrap();
        let se2 = predictor2.prediction_standard_error(test_size, 1.0).unwrap();
        // Both should be reasonable values
        assert!(se1 > 0.0 && se1.is_finite());
        assert!(se2 > 0.0 && se2.is_finite());
    }
}
