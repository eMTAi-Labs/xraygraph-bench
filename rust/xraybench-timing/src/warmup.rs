/// Configuration for the CUSUM change-point detector.
#[derive(Debug, Clone)]
pub struct CusumConfig {
    /// Minimum number of samples before declaring steady-state.
    pub min_samples: usize,
    /// Number of standard deviations for the CUSUM threshold.
    pub threshold_sigma: f64,
    /// Fraction of mean to use as drift allowance.
    pub drift_fraction: f64,
}

impl Default for CusumConfig {
    fn default() -> Self {
        Self {
            min_samples: 5,
            threshold_sigma: 4.0,
            drift_fraction: 0.5,
        }
    }
}

/// CUSUM (Cumulative Sum) change-point detector for identifying when a benchmark
/// has warmed up and entered a steady state.
///
/// Algorithm:
/// - We use a sliding-window approach: keep a recent window of values.
/// - Once we have `min_samples` values in the window, compute the windowed
///   mean and variance.
/// - CUSUM tracks deviations from the *initial window's* baseline.
/// - When CUSUM signals a shift (regime change), we start a new window and
///   wait for stability in the new regime.
/// - A regime is "stable" when consecutive samples have low CV (coefficient
///   of variation) over `min_samples` recent observations.
#[derive(Debug)]
pub struct CusumDetector {
    config: CusumConfig,
    /// All observed values.
    pub values: Vec<f64>,
    /// Positive CUSUM accumulator.
    pub cusum_pos: f64,
    /// Negative CUSUM accumulator.
    pub cusum_neg: f64,
    /// Welford online mean (global, for reference only).
    pub running_mean: f64,
    /// Welford M2 accumulator (global).
    pub m2: f64,
    /// Index at which steady-state was declared.
    pub change_point: Option<usize>,
    /// The reference mean used for current CUSUM window.
    baseline_mean: f64,
    /// The reference stddev used for current CUSUM window.
    baseline_stddev: f64,
    /// Start index of current stability window.
    window_start: usize,
    /// Count of consecutive samples within threshold of baseline.
    stable_count: usize,
}

impl CusumDetector {
    /// Create a new detector with the given configuration.
    pub fn new(config: CusumConfig) -> Self {
        Self {
            config,
            values: Vec::new(),
            cusum_pos: 0.0,
            cusum_neg: 0.0,
            running_mean: 0.0,
            m2: 0.0,
            change_point: None,
            baseline_mean: 0.0,
            baseline_stddev: 0.0,
            window_start: 0,
            stable_count: 0,
        }
    }

    /// Compute mean and variance of a slice.
    fn stats(vals: &[f64]) -> (f64, f64) {
        if vals.is_empty() {
            return (0.0, 0.0);
        }
        let n = vals.len() as f64;
        let mean = vals.iter().sum::<f64>() / n;
        let var = if vals.len() > 1 {
            vals.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0)
        } else {
            0.0
        };
        (mean, var.sqrt())
    }

    /// Observe a new value. Returns `true` when steady-state is reached.
    pub fn observe(&mut self, value: f64) -> bool {
        let n = self.values.len() + 1;
        self.values.push(value);

        // Global Welford update (informational)
        let delta = value - self.running_mean;
        self.running_mean += delta / n as f64;
        let delta2 = value - self.running_mean;
        self.m2 += delta * delta2;

        // Not enough data yet
        if n < self.config.min_samples {
            return false;
        }

        // Already declared steady-state
        if self.change_point.is_some() {
            return true;
        }

        // ── Establish or update baseline from the current window ──────────────
        let window = &self.values[self.window_start..];
        let (win_mean, win_stddev) = Self::stats(window);

        // If baseline not yet set (first time we have enough samples in window)
        if self.window_start == 0 && window.len() == self.config.min_samples {
            self.baseline_mean = win_mean;
            self.baseline_stddev = win_stddev;
        }

        // Drift k and threshold h derived from baseline
        let k = if self.baseline_stddev > 1e-9 {
            (self.config.drift_fraction / 2.0) * self.baseline_stddev
        } else {
            // Zero-variance baseline: use fraction of mean as drift
            (self.config.drift_fraction * self.baseline_mean.abs() * 0.1).max(1e-10)
        };
        let h = if self.baseline_stddev > 1e-9 {
            self.config.threshold_sigma * self.baseline_stddev
        } else {
            (self.config.threshold_sigma
                * self.config.drift_fraction
                * self.baseline_mean.abs()
                * 0.1)
                .max(1e-9)
        };

        // Update CUSUM relative to baseline mean
        self.cusum_pos = (self.cusum_pos + value - self.baseline_mean - k).max(0.0);
        self.cusum_neg = (self.cusum_neg - value + self.baseline_mean - k).max(0.0);

        // ── Detect regime shift ───────────────────────────────────────────────
        if self.cusum_pos > h || self.cusum_neg > h {
            // Regime changed — reset CUSUM and start a new window from here
            self.cusum_pos = 0.0;
            self.cusum_neg = 0.0;
            self.window_start = n - 1;
            self.stable_count = 0;
            self.baseline_mean = value;
            self.baseline_stddev = 0.0;
            return false;
        }

        // ── Check for stability in current window ─────────────────────────────
        // We are stable if the recent min_samples values have low CV
        // relative to the threshold.
        if window.len() >= self.config.min_samples {
            let cv = if win_mean.abs() > 1e-10 {
                win_stddev / win_mean.abs()
            } else {
                0.0
            };

            // Stable if:
            // - stddev is essentially zero (all identical), OR
            // - CV is below threshold_sigma * drift_fraction / 10
            let cv_threshold = self.config.threshold_sigma * self.config.drift_fraction / 10.0;
            if win_stddev < 1e-9 || cv <= cv_threshold {
                self.stable_count += 1;
            } else {
                self.stable_count = 0;
            }

            if self.stable_count >= self.config.min_samples {
                self.change_point = Some(n - 1);
                return true;
            }
        }

        false
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_series_detects_quickly() {
        let mut detector = CusumDetector::new(CusumConfig::default());
        let mut reached = false;
        for i in 0..20 {
            let val = 100.0 + (i as f64 * 0.0001); // nearly stable
            if detector.observe(val) {
                reached = true;
                break;
            }
        }
        assert!(
            reached,
            "should detect steady-state within 20 stable values"
        );
    }

    #[test]
    fn warmup_then_stable_detects_transition() {
        let mut detector = CusumDetector::new(CusumConfig::default());
        // Warmup phase: high values
        for _ in 0..5 {
            detector.observe(500.0);
        }
        // Stable phase: low values
        let mut reached = false;
        for i in 0..50 {
            if detector.observe(100.0 + i as f64 * 0.001) {
                reached = true;
                break;
            }
        }
        assert!(
            reached,
            "should detect steady-state after transition from warmup to stable"
        );
    }

    #[test]
    fn all_identical_is_stable() {
        let mut detector = CusumDetector::new(CusumConfig::default());
        let mut reached = false;
        for _ in 0..10 {
            if detector.observe(42.0) {
                reached = true;
                break;
            }
        }
        assert!(reached, "10 identical values should reach steady-state");
    }

    #[test]
    fn noisy_but_stable_eventually_detects() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut detector = CusumDetector::new(CusumConfig::default());
        let mut reached = false;

        for i in 0u64..200 {
            // Deterministic "noise" using DefaultHasher
            let mut h = DefaultHasher::new();
            i.hash(&mut h);
            let hash = h.finish();
            // Noise in range [-5.0, 5.0]
            let noise = ((hash % 1000) as f64 / 100.0) - 5.0;
            let val = 100.0 + noise;
            if detector.observe(val) {
                reached = true;
                break;
            }
        }
        assert!(
            reached,
            "noisy but stable series should eventually reach steady-state"
        );
    }

    #[test]
    fn respects_min_samples() {
        let config = CusumConfig {
            min_samples: 10,
            threshold_sigma: 4.0,
            drift_fraction: 0.5,
        };
        let mut detector = CusumDetector::new(config);

        // Feed fewer than min_samples values
        for i in 0..9 {
            let result = detector.observe(100.0 + i as f64 * 0.001);
            assert!(
                !result,
                "should not trigger before min_samples (iteration {i})"
            );
        }
    }
}
