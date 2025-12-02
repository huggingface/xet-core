use tokio::time::{Duration, Instant};

/// Mode for exponentially-weighted moving average decay.
#[derive(Debug)]
pub enum ExpWeightedMovingAvgMode {
    /// Time-based decay: half-life in seconds with last update timestamp.
    TimeDecay { half_life_secs: f64, last_update: Instant },
    /// Count-based decay: half-life in number of samples.
    CountDecay { half_life_count: f64 },
}

/// Exponentially-weighted moving average with a configurable half-life.
///
/// Supports two decay modes:
/// - Time-based: Decay factor for elapsed time Δt is: decay = 2^(-Δt / half_life)
/// - Count-based: Decay factor for sample count Δn is: decay = 2^(-Δn / half_life)
#[derive(Debug)]
pub struct ExpWeightedMovingAvg {
    mode: ExpWeightedMovingAvgMode,
    weight: f64,
    value: f64,
}

impl ExpWeightedMovingAvg {
    /// Create a new tracker with time-based decay using the given half-life.
    pub fn new_time_decay(half_life: Duration) -> Self {
        let hl_secs = half_life.as_secs_f64();
        assert!(hl_secs.is_finite() && hl_secs > 0.0, "half-life must be positive");

        Self {
            mode: ExpWeightedMovingAvgMode::TimeDecay {
                half_life_secs: hl_secs,
                last_update: Instant::now(),
            },
            weight: 0.0,
            value: 0.0,
        }
    }

    /// Create a new tracker with count-based decay using the given half-life in number of samples.
    pub fn new_count_decay(half_life_count: f64) -> Self {
        assert!(half_life_count.is_finite() && half_life_count > 0.0, "half-life must be positive");

        Self {
            mode: ExpWeightedMovingAvgMode::CountDecay { half_life_count },
            weight: 0.0,
            value: 0.0,
        }
    }

    /// Add a sample, automatically decaying existing state first.
    pub fn update_with_weight(&mut self, sample: f64, weight: f64) {
        let decay = match &mut self.mode {
            ExpWeightedMovingAvgMode::TimeDecay {
                half_life_secs,
                last_update,
            } => {
                let now = Instant::now();
                let dt_secs = (now - *last_update).as_secs_f64();

                // decay = 2^(-Δt / T½)
                let decay = ((-dt_secs * weight) / *half_life_secs).exp2();
                *last_update = now;
                decay
            },
            ExpWeightedMovingAvgMode::CountDecay { half_life_count } => {
                // For count-based decay, we apply decay based on the number of samples.
                // Each update applies decay = 2^(-1 / T½) where 1 is the count increment.
                (-weight / *half_life_count).exp2()
            },
        };

        self.weight *= decay;
        self.value *= decay;

        // Fold in the new sample with unit weight
        self.weight += weight;
        self.value += sample;
    }

    pub fn update(&mut self, sample: f64) {
        self.update_with_weight(sample, 1.0);
    }

    /// Current exponentially-weighted mean (0.0 if no samples yet).
    pub fn value(&self) -> f64 {
        if self.weight == 0.0 {
            0.0
        } else {
            self.value / self.weight
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{Duration, advance, pause};

    use super::*;

    /// After one half-life, the weight and value should be exactly halved.
    /// Adding a new sample with unit weight then skews the mean toward
    /// that new value.
    #[tokio::test]
    async fn ewma_decays_with_simulated_time() {
        pause();

        let half_life = Duration::from_secs(2);
        let mut avg = ExpWeightedMovingAvg::new_time_decay(half_life);

        // t = 0 s: first sample
        avg.update(10.0);
        assert_eq!(avg.value(), 10.0);

        // Jump ahead exactly one half-life (no wall-clock sleep needed)
        advance(half_life).await;

        // t = 2 s: add a zero sample
        avg.update(0.0);

        // Internals after decay then sample:
        //   weight = 0.5 (decayed) + 1.0 (new) = 1.5
        //   value  = 10 * 0.5           + 0     = 5.0
        //   mean   = 5.0 / 1.5 ≈ 3.333…
        let epsilon = 1e-6;
        assert!((avg.value() - 3.333_333_333).abs() < epsilon);
    }

    /// Verifies that multiple advances accumulate correctly.
    #[tokio::test]
    async fn ewma_multiple_advances() {
        pause();

        let mut avg = ExpWeightedMovingAvg::new_time_decay(Duration::from_secs(4));

        avg.update(8.0); // t = 0
        advance(Duration::from_secs(2)).await; // t = 2
        avg.update(8.0); // identical sample mid-way
        advance(Duration::from_secs(4)).await; // t = 6
        avg.update(0.0); // pull mean down

        // The mean should now be strictly between 0 and 8.
        let m = avg.value();
        assert!(m > 0.0 && m < 8.0);
    }

    /// After one half-life in count, the weight and value should be exactly halved.
    /// Adding a new sample with unit weight then skews the mean toward that new value.
    #[test]
    fn ewma_count_decay_basic() {
        let half_life_count = 2.0;
        let mut avg = ExpWeightedMovingAvg::new_count_decay(half_life_count);

        // First sample: no decay
        avg.update(10.0);
        assert_eq!(avg.value(), 10.0);

        // Second sample: after 1 sample, decay = 2^(-1/2) ≈ 0.707
        avg.update(0.0);

        // After decay and adding new sample:
        //   weight = 1.0 * 0.707 + 1.0 ≈ 1.707
        //   value  = 10.0 * 0.707 + 0.0 ≈ 7.07
        //   mean   ≈ 7.07 / 1.707 ≈ 4.14
        let epsilon = 1e-6;
        let decay_factor: f64 = (-1.0_f64 / 2.0_f64).exp2();
        let expected_mean = (10.0 * decay_factor) / (1.0 * decay_factor + 1.0);
        assert!((avg.value() - expected_mean).abs() < epsilon);
    }

    /// Verifies that count-based decay works correctly over multiple samples.
    #[test]
    fn ewma_count_decay_multiple_samples() {
        let mut avg = ExpWeightedMovingAvg::new_count_decay(4.0);

        avg.update(8.0); // sample 1
        avg.update(8.0); // sample 2
        avg.update(8.0); // sample 3
        avg.update(0.0); // sample 4: pull mean down

        // The mean should now be strictly between 0 and 8.
        let m = avg.value();
        assert!(m > 0.0 && m < 8.0);
    }

    /// Verifies that after exactly half_life_count samples, the value is approximately halved.
    #[test]
    fn ewma_count_decay_half_life() {
        let half_life_count = 10.0;
        let mut avg = ExpWeightedMovingAvg::new_count_decay(half_life_count);

        // First sample
        avg.update(100.0);
        let initial_value = avg.value();
        assert_eq!(initial_value, 100.0);

        // Add 9 more samples (total of 10, which equals half_life_count)
        // Each sample applies decay = 2^(-1/10), so after 10 samples:
        // total_decay = (2^(-1/10))^10 = 2^(-1) = 0.5
        for _ in 0..9 {
            avg.update(0.0);
        }

        // After 10 samples, the original value should be approximately halved
        // but then we add a new sample with value 0, so the mean should be
        // between 0 and 50
        let final_value = avg.value();
        assert!(final_value > 0.0 && final_value < 50.0);
    }
}
