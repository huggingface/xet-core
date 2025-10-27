use tokio::time::{Duration, Instant};

/// Exponentially-weighted moving average with a configurable half-life.
///
/// Decay factor for elapsed time Δt is:
///     decay = 2^(-Δt / half_life)
#[derive(Debug)]
pub struct ExpWeightedMovingAvg {
    half_life_secs: f64, // T½ in seconds
    weight: f64,
    value: f64,
    last_update: Instant,
}

impl ExpWeightedMovingAvg {
    /// Create a new tracker with the given half-life.
    pub fn new(half_life: Duration) -> Self {
        let hl_secs = half_life.as_secs_f64();
        assert!(hl_secs.is_finite() && hl_secs > 0.0, "half-life must be positive");

        Self {
            half_life_secs: hl_secs,
            weight: 0.0,
            value: 0.0,
            last_update: Instant::now(),
        }
    }

    /// Add a sample, automatically decaying existing state first.
    pub fn update(&mut self, sample: f64) {
        let now = Instant::now();
        let dt_secs = (now - self.last_update).as_secs_f64();

        // decay = 2^(-Δt / T½)
        let decay = (-dt_secs / self.half_life_secs).exp2();
        self.weight *= decay;
        self.value *= decay;

        // Fold in the new sample with unit weight
        self.weight += 1.0;
        self.value += sample;
        self.last_update = now;
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
    use tokio::time::{advance, pause, Duration};

    use super::*;

    /// After one half-life, the weight and value should be exactly halved.
    /// Adding a new sample with unit weight then skews the mean toward
    /// that new value.
    #[tokio::test]
    async fn ewma_decays_with_simulated_time() {
        pause();

        let half_life = Duration::from_secs(2);
        let mut avg = ExpWeightedMovingAvg::new(half_life);

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

        let mut avg = ExpWeightedMovingAvg::new(Duration::from_secs(4));

        avg.update(8.0); // t = 0
        advance(Duration::from_secs(2)).await; // t = 2
        avg.update(8.0); // identical sample mid-way
        advance(Duration::from_secs(4)).await; // t = 6
        avg.update(0.0); // pull mean down

        // The mean should now be strictly between 0 and 8.
        let m = avg.value();
        assert!(m > 0.0 && m < 8.0);
    }
}
