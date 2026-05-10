use crate::calibration::calibrate;
use crate::clock::{monotonic_ns, timing_fence};
use crate::warmup::{CusumConfig, CusumDetector};
use xraybench_types::{CalibrationResult, Measurement, Phase, Result};

/// Configuration for the measurement harness.
#[derive(Debug, Clone)]
pub struct HarnessConfig {
    /// Maximum number of warm-up iterations before giving up.
    pub max_warmup_iterations: u32,
    /// Maximum number of steady-state iterations to collect.
    pub max_steady_iterations: u32,
    /// Target CI half-width as a fraction of the mean (e.g., 0.02 = 2%).
    pub target_ci_fraction: f64,
    /// CUSUM configuration for warm-up detection.
    pub cusum_config: CusumConfig,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            max_warmup_iterations: 100,
            max_steady_iterations: 1000,
            target_ci_fraction: 0.02,
            cusum_config: CusumConfig::default(),
        }
    }
}

/// Results from a complete measurement run.
#[derive(Debug)]
pub struct HarnessResult {
    /// Calibration data gathered before the run.
    pub calibration: CalibrationResult,
    /// The single cold run (first execution).
    pub cold: Measurement,
    /// Measurements collected during warm-up.
    pub warmup: Vec<Measurement>,
    /// Measurements collected during steady-state.
    pub steady_state: Vec<Measurement>,
}

/// Time a single call to `work_fn` and return a `Measurement`.
fn time_one<F>(
    work_fn: &mut F,
    iteration: u32,
    phase: Phase,
    cal: &CalibrationResult,
) -> Result<Measurement>
where
    F: FnMut() -> std::result::Result<(), String>,
{
    timing_fence();
    let t1 = monotonic_ns()?;
    work_fn().map_err(xraybench_types::BenchError::InvalidData)?;
    let t2 = monotonic_ns()?;
    timing_fence();

    let duration_ns = t2.saturating_sub(t1);

    Ok(Measurement {
        timestamp_ns: t1,
        duration_ns,
        clock_resolution_ns: cal.clock_resolution_ns,
        clock_overhead_ns: cal.clock_overhead_ns,
        fence_overhead_ns: cal.fence_overhead_ns,
        iteration,
        phase,
    })
}

/// Run a complete measurement campaign for `work_fn`.
///
/// Steps:
/// 1. Calibrate clock
/// 2. Cold run
/// 3. Warm-up loop until CUSUM detects steady-state or `max_warmup_iterations`
/// 4. Steady-state collection until CI target met or `max_steady_iterations`
pub fn measure<F>(config: HarnessConfig, mut work_fn: F) -> Result<HarnessResult>
where
    F: FnMut() -> std::result::Result<(), String>,
{
    // 1. Calibrate
    let cal = calibrate()?;

    // 2. Cold run
    let cold = time_one(&mut work_fn, 0, Phase::Cold, &cal)?;

    // 3. Warm-up loop
    let mut warmup: Vec<Measurement> = Vec::new();
    let mut cusum = CusumDetector::new(config.cusum_config.clone());

    for i in 0..config.max_warmup_iterations {
        let m = time_one(&mut work_fn, i + 1, Phase::WarmUp, &cal)?;
        let duration = m.duration_ns as f64;
        warmup.push(m);

        if cusum.observe(duration) {
            break;
        }
    }

    // 4. Steady-state collection with adaptive CI check
    let mut steady_state: Vec<Measurement> = Vec::new();
    let warmup_count = warmup.len() as u32;

    for i in 0..config.max_steady_iterations {
        let m = time_one(&mut work_fn, warmup_count + i + 1, Phase::SteadyState, &cal)?;
        steady_state.push(m);

        // Adaptive CI check: only after at least 10 samples
        let n = steady_state.len();
        if n >= 10 {
            let durations: Vec<f64> = steady_state.iter().map(|m| m.duration_ns as f64).collect();
            let mean = durations.iter().sum::<f64>() / n as f64;
            if mean > 0.0 {
                let variance =
                    durations.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
                let stddev = variance.sqrt();
                let ci_half = 1.96 * stddev / (n as f64).sqrt();
                if ci_half < config.target_ci_fraction * mean {
                    break;
                }
            }
        }
    }

    Ok(HarnessResult {
        calibration: cal,
        cold,
        warmup,
        steady_state,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    /// Simulate ~10μs of work via spin.
    fn work_10us() -> std::result::Result<(), String> {
        let target_ns = 10_000u64; // 10 μs
        let start = crate::clock::monotonic_ns().unwrap();
        loop {
            let now = crate::clock::monotonic_ns().unwrap();
            if now.saturating_sub(start) >= target_ns {
                break;
            }
            std::hint::spin_loop();
        }
        Ok(())
    }

    #[test]
    fn harness_measures_cold_and_warm() {
        let config = HarnessConfig {
            max_warmup_iterations: 20,
            max_steady_iterations: 30,
            target_ci_fraction: 0.10,
            cusum_config: CusumConfig::default(),
        };
        let result = measure(config, work_10us).expect("measure should succeed");

        // Should have a cold measurement
        assert_eq!(result.cold.phase, Phase::Cold);
        assert!(result.cold.duration_ns > 0);

        // Should have some warmup and steady-state measurements
        assert!(
            !result.steady_state.is_empty(),
            "should have steady-state samples"
        );
    }

    #[test]
    fn harness_handles_work_fn_error() {
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let work_fn = move || -> std::result::Result<(), String> {
            let n = call_count_clone.fetch_add(1, Ordering::SeqCst);
            if n == 1 {
                // Fail on the 2nd call (0-indexed)
                Err("simulated failure on call 2".to_string())
            } else {
                work_10us()
            }
        };

        let config = HarnessConfig::default();
        let result = measure(config, work_fn);
        assert!(result.is_err(), "should propagate work_fn error");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("simulated failure"),
            "error should contain work_fn message, got: {err_msg}"
        );
    }

    #[test]
    fn harness_calibration_values_present() {
        let config = HarnessConfig {
            max_warmup_iterations: 5,
            max_steady_iterations: 10,
            target_ci_fraction: 0.50, // loose — stop quickly
            cusum_config: CusumConfig {
                min_samples: 2,
                threshold_sigma: 1.0,
                drift_fraction: 0.1,
            },
        };
        let result = measure(config, work_10us).expect("measure should succeed");

        // Calibration should be populated
        assert!(result.calibration.samples > 0);
        assert!(result.calibration.clock_resolution_ns > 0);
        // clock_overhead_ns may be 0 on very fast hardware, just check it's present
        let _ = result.calibration.clock_overhead_ns;
        let _ = result.calibration.fence_overhead_ns;
    }

    #[test]
    fn harness_cold_slower_than_warm() {
        // cold = 100μs, warm = 10μs
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let work_fn = move || -> std::result::Result<(), String> {
            let n = call_count_clone.fetch_add(1, Ordering::SeqCst);
            let target_ns = if n == 0 { 100_000u64 } else { 10_000u64 };
            let start = crate::clock::monotonic_ns().unwrap();
            loop {
                let now = crate::clock::monotonic_ns().unwrap();
                if now.saturating_sub(start) >= target_ns {
                    break;
                }
                std::hint::spin_loop();
            }
            Ok(())
        };

        let config = HarnessConfig {
            max_warmup_iterations: 20,
            max_steady_iterations: 20,
            target_ci_fraction: 0.10,
            cusum_config: CusumConfig::default(),
        };
        let result = measure(config, work_fn).expect("measure should succeed");

        // Cold should be significantly slower than any warm measurement
        let cold_ns = result.cold.duration_ns;
        if let Some(warm_sample) = result.warmup.first().or(result.steady_state.first()) {
            assert!(
                cold_ns > warm_sample.duration_ns,
                "cold ({cold_ns} ns) should be slower than warm ({} ns)",
                warm_sample.duration_ns
            );
        }
    }
}
