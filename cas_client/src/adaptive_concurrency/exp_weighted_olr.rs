/// Online, exponentially-decayed weighted linear regression in 1D:
///   y ~ beta0 + beta1 * x
///
/// Uses an exponential forgetting factor lambda chosen from a
/// half-life specified in *samples*.
#[derive(Debug, Clone)]
pub struct ExpWeightedOnlineLinearRegression {
    /// Exponential decay factor per sample: lambda = 2^(-1/half_life)
    lambda: f64,

    // Decayed weighted sums
    sw: f64,  // sum w
    sx: f64,  // sum w * x
    sy: f64,  // sum w * y
    sxx: f64, // sum w * x^2
    sxy: f64, // sum w * x * y
    syy: f64, // sum w * y^2
}

impl ExpWeightedOnlineLinearRegression {
    /// Create a new decayed online linear regression model.
    ///
    /// `half_life` is in *samples*: a point D samples old will
    /// have its effective weight reduced by 50%.
    pub fn new(half_life: f64) -> Self {
        assert!(half_life > 0.0, "half_life must be positive");
        let lambda = (-1.0 / half_life).exp2();

        Self {
            lambda,
            sw: 0.0,
            sx: 0.0,
            sy: 0.0,
            sxx: 0.0,
            sxy: 0.0,
            syy: 0.0,
        }
    }

    /// Update the model with a new observation (weight, x, y).
    ///
    /// `weight` is the *base* weight; decay is handled internally.
    pub fn update(&mut self, weight: f64, x: f64, y: f64) {
        // First apply decay to all sufficient statistics

        self.sw *= self.lambda;
        self.sx *= self.lambda;
        self.sy *= self.lambda;
        self.sxx *= self.lambda;
        self.sxy *= self.lambda;
        self.syy *= self.lambda;

        // Then add the new weighted contribution
        let wx = weight * x;
        let wy = weight * y;

        self.sw += weight;
        self.sx += wx;
        self.sy += wy;
        self.sxx += wx * x; // w * x^2
        self.sxy += wx * y; // w * x * y
        self.syy += wy * y; // w * y^2
    }

    /// Predict the mean and standard deviation of the *fitted mean* at x.
    ///
    /// Returns:
    ///   - (None, None) if the model is not identifiable yet (normal matrix singular).
    ///   - (Some(mean), None) if coefficients can be estimated but df <= 0, so we can't compute a standard deviation
    ///     yet.
    ///   - (Some(mean), Some(std_dev)) otherwise.
    pub fn predict(&self, x0: f64) -> (Option<f64>, Option<f64>) {
        // Need a well-conditioned normal matrix to estimate beta.
        let delta = self.sw * self.sxx - self.sx * self.sx;
        if delta.abs() < 1e-12 {
            // Can't estimate beta0/beta1 at all.
            return (None, None);
        }

        // Estimate coefficients beta0, beta1
        let beta0 = (self.sxx * self.sy - self.sx * self.sxy) / delta;
        let beta1 = (self.sw * self.sxy - self.sx * self.sy) / delta;
        let mean = beta0 + beta1 * x0;

        // Effective degrees of freedom: sw - 2 (two parameters)
        let df = self.sw - 2.0;
        if df <= 0.0 {
            // Mean is defined, but we don't trust a variance estimate yet.
            return (Some(mean), None);
        }

        // Residual sum of squares (decayed)
        let rss = self.syy - beta0 * self.sy - beta1 * self.sxy;

        // Guard against numerical issues; sigma^2 cannot be negative.
        let sigma2 = (rss / df).max(0.0);

        // Variance of fitted mean at x0:
        //
        // var_mean = sigma^2 * (Sxx - 2 Sx x0 + Sw x0^2) / delta
        let quad = self.sxx - 2.0 * self.sx * x0 + self.sw * x0 * x0;
        let mut var_mean = sigma2 * quad / delta;

        if var_mean < 0.0 {
            // Numerical noise; clamp at zero.
            var_mean = 0.0;
        }

        let std_dev = var_mean.sqrt();
        (Some(mean), Some(std_dev))
    }

    /// Optionally: expose current coefficients (beta0, beta1) if desired.
    #[allow(dead_code)]
    pub fn coefficients(&self) -> Option<(f64, f64)> {
        let delta = self.sw * self.sxx - self.sx * self.sx;
        if delta.abs() < 1e-12 {
            return None;
        }

        let beta0 = (self.sxx * self.sy - self.sx * self.sxy) / delta;
        let beta1 = (self.sw * self.sxy - self.sx * self.sy) / delta;
        Some((beta0, beta1))
    }

    /// Check if two models are approximately equal for testing purposes.
    ///
    /// Compares all internal state (sufficient statistics) with a tolerance.
    #[cfg(test)]
    pub fn approx_equals(&self, other: &Self, epsilon: f64) -> bool {
        (self.sw - other.sw).abs() < epsilon
            && (self.sx - other.sx).abs() < epsilon
            && (self.sy - other.sy).abs() < epsilon
            && (self.sxx - other.sxx).abs() < epsilon
            && (self.sxy - other.sxy).abs() < epsilon
            && (self.syy - other.syy).abs() < epsilon
            && (self.lambda - other.lambda).abs() < epsilon
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_abs_diff_eq;

    use super::ExpWeightedOnlineLinearRegression;

    /// Offline / batch computation of the decayed weighted regression
    /// and prediction at x0, using the same model:
    ///
    /// y ~ beta0 + beta1 * x
    ///
    /// with exponential decay defined by `half_life`.
    ///
    /// This is the "ground truth" implementation that does NOT use
    /// incremental updates, but directly applies the decay factor
    /// to each sample based on its age.
    ///
    /// samples: in arrival order: [(w0, x0, y0), (w1, x1, y1), ..., (w_{N-1}, x_{N-1}, y_{N-1})]
    fn offline_decayed_predict(samples: &[(f64, f64, f64)], half_life: f64, x0: f64) -> (Option<f64>, Option<f64>) {
        let n = samples.len();
        if n == 0 {
            return (None, None);
        }

        let lambda = 0.5_f64.powf(1.0 / half_life);

        // Decayed weighted sums:
        // S_w   = sum_i (lambda^{N-1-i} * w_i)
        // S_x   = sum_i (lambda^{N-1-i} * w_i * x_i)
        // etc.
        let mut sw = 0.0;
        let mut sx = 0.0;
        let mut sy = 0.0;
        let mut sxx = 0.0;
        let mut sxy = 0.0;
        let mut syy = 0.0;

        for (idx, &(w, x, y)) in samples.iter().enumerate() {
            let age = (n - 1 - idx) as f64; // newest sample has age 0
            let coeff = lambda.powf(age);
            let w_eff = coeff * w;

            sw += w_eff;
            sx += w_eff * x;
            sy += w_eff * y;
            sxx += w_eff * x * x;
            sxy += w_eff * x * y;
            syy += w_eff * y * y;
        }

        let delta = sw * sxx - sx * sx;
        if delta.abs() < 1e-12 {
            return (None, None);
        }

        // Coefficients
        let beta0 = (sxx * sy - sx * sxy) / delta;
        let beta1 = (sw * sxy - sx * sy) / delta;

        let mean = beta0 + beta1 * x0;

        // Effective df: S_w - 2
        let df = sw - 2.0;
        if df <= 0.0 {
            // Not enough effective weight to estimate variance
            return (Some(mean), None);
        }

        let rss = syy - beta0 * sy - beta1 * sxy;
        let sigma2 = (rss / df).max(0.0);

        let quad = sxx - 2.0 * sx * x0 + sw * x0 * x0;
        let mut var_mean = sigma2 * quad / delta;
        if var_mean < 0.0 {
            var_mean = 0.0;
        }
        let std_dev = var_mean.sqrt();
        (Some(mean), Some(std_dev))
    }

    #[test]
    fn test_simple_line_noisy_with_decay() {
        // Underlying relationship ~ y = 1 + 2x, with some noise
        let samples = vec![
            (1.0, 0.0, 1.1),
            (1.0, 1.0, 2.9),
            (1.0, 2.0, 5.2),
            (1.0, 3.0, 7.0),
            (1.0, 4.0, 9.1),
        ];

        let half_life = 3.0;
        let x0 = 2.5;

        // Ground truth from batch / offline computation
        let (mean_off, std_off) = offline_decayed_predict(&samples, half_life, x0);

        // Online model
        let mut model = ExpWeightedOnlineLinearRegression::new(half_life);
        for &(w, x, y) in &samples {
            model.update(w, x, y);
        }

        let (mean_on, std_on) = model.predict(x0);

        // At this point we expect both mean and std dev to be defined
        assert!(mean_off.is_some() && std_off.is_some());
        assert!(mean_on.is_some() && std_on.is_some());

        let mean_off = mean_off.unwrap();
        let mean_on = mean_on.unwrap();
        let std_off = std_off.unwrap();
        let std_on = std_on.unwrap();

        assert_abs_diff_eq!(mean_on, mean_off, epsilon = 1e-10);
        assert_abs_diff_eq!(std_on, std_off, epsilon = 1e-10);
    }

    #[test]
    fn test_heavier_recent_sample() {
        // Series where the last sample gets a large weight.
        // Decay should make the fit lean more towards newer points.
        let samples = vec![
            (1.0, 0.0, 0.0),
            (1.0, 1.0, 1.0),
            (1.0, 2.0, 2.0),
            (5.0, 3.0, 10.0), // heavy outlier at the end
        ];

        let half_life = 2.0;
        let x0 = 2.0;

        let (mean_off, std_off) = offline_decayed_predict(&samples, half_life, x0);

        let mut model = ExpWeightedOnlineLinearRegression::new(half_life);
        for &(w, x, y) in &samples {
            model.update(w, x, y);
        }

        let (mean_on, std_on) = model.predict(x0);

        assert!(mean_off.is_some() && std_off.is_some());
        assert!(mean_on.is_some() && std_on.is_some());

        let mean_off = mean_off.unwrap();
        let mean_on = mean_on.unwrap();
        let std_off = std_off.unwrap();
        let std_on = std_on.unwrap();

        assert_abs_diff_eq!(mean_on, mean_off, epsilon = 1e-10);
        assert_abs_diff_eq!(std_on, std_off, epsilon = 1e-10);
    }

    #[test]
    fn test_insufficient_df_gives_mean_only() {
        // One sample: can fit beta0, beta1? No, delta is zero.
        // So we get (None, None).
        let half_life = 5.0;
        let mut model = ExpWeightedOnlineLinearRegression::new(half_life);

        model.update(1.0, 1.0, 2.0);

        let (mean1, std1) = model.predict(1.0);
        assert!(mean1.is_none());
        assert!(std1.is_none());

        // Add a second sample with different x so that we can fit a line,
        // but effective df is still <= 0, so std should be None while mean is Some.
        model.update(1.0, 2.0, 4.0);

        let (mean2, std2) = model.predict(1.5);
        assert!(mean2.is_some());
        assert!(std2.is_none());

        // Cross-check against offline version for the same two samples
        let samples = vec![(1.0, 1.0, 2.0), (1.0, 2.0, 4.0)];
        let (mean_off, std_off) = offline_decayed_predict(&samples, half_life, 1.5);

        assert!(mean_off.is_some());
        assert!(std_off.is_none());
        assert_abs_diff_eq!(mean2.unwrap(), mean_off.unwrap(), epsilon = 1e-10);
    }

    #[test]
    fn test_singular_x_all_same() {
        // All x are the same -> normal equations matrix is singular.
        let samples = vec![(1.0, 1.0, 1.0), (1.0, 1.0, 2.0), (1.0, 1.0, 3.0)];

        let half_life = 3.0;
        let x0 = 1.0;

        let (mean_off, std_off) = offline_decayed_predict(&samples, half_life, x0);
        assert!(mean_off.is_none());
        assert!(std_off.is_none());

        let mut model = ExpWeightedOnlineLinearRegression::new(half_life);
        for &(w, x, y) in &samples {
            model.update(w, x, y);
        }

        let (mean_on, std_on) = model.predict(x0);
        assert!(mean_on.is_none());
        assert!(std_on.is_none());
    }

    #[test]
    fn test_matches_offline_across_multiple_x0() {
        // Another sanity check: compare predictions at several x0
        // for the same data and half-life.
        let samples = vec![
            (0.5, 0.0, 0.2),
            (1.0, 1.0, 1.1),
            (2.0, 2.0, 3.9),
            (1.0, 3.0, 6.2),
            (0.5, 4.0, 8.1),
        ];
        let half_life = 4.0;

        let x_values = [0.0, 1.0, 2.5, 4.0];

        let mut model = ExpWeightedOnlineLinearRegression::new(half_life);
        for &(w, x, y) in &samples {
            model.update(w, x, y);
        }

        for &x0 in &x_values {
            let (mean_off, std_off) = offline_decayed_predict(&samples, half_life, x0);
            let (mean_on, std_on) = model.predict(x0);

            assert_eq!(mean_off.is_some(), mean_on.is_some());
            assert_eq!(std_off.is_some(), std_on.is_some());

            if let (Some(m_off), Some(m_on)) = (mean_off, mean_on) {
                assert_abs_diff_eq!(m_on, m_off, epsilon = 1e-10);
            }
            if let (Some(s_off), Some(s_on)) = (std_off, std_on) {
                assert_abs_diff_eq!(s_on, s_off, epsilon = 1e-10);
            }
        }
    }
}
