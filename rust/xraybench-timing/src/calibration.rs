use crate::clock::{monotonic_ns, timing_fence};
use xraybench_types::{CalibrationResult, Result};

const SAMPLES: usize = 10_000;

/// Returns the median of a sorted slice. Returns 0 for empty slices.
pub fn median_u64(sorted: &[u64]) -> u64 {
    match sorted.len() {
        0 => 0,
        n if n % 2 == 1 => sorted[n / 2],
        n => {
            let mid = n / 2;
            // Average of two middle elements, rounding down
            (sorted[mid - 1] / 2) + (sorted[mid] / 2)
                + ((sorted[mid - 1] % 2 + sorted[mid] % 2) / 2)
        }
    }
}

/// Calibrate the clock by measuring resolution and overhead.
pub fn calibrate() -> Result<CalibrationResult> {
    // --- clock_resolution: smallest non-zero delta from back-to-back reads ---
    let mut resolution_samples: Vec<u64> = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let t1 = monotonic_ns()?;
        let t2 = monotonic_ns()?;
        if t2 > t1 {
            resolution_samples.push(t2 - t1);
        }
    }
    resolution_samples.sort_unstable();

    let clock_resolution_ns = if resolution_samples.is_empty() {
        1 // fallback: at least 1 ns
    } else {
        resolution_samples[0] // smallest non-zero delta
    };

    // --- clock_overhead: median delta from back-to-back reads ---
    let mut overhead_samples: Vec<u64> = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let t1 = monotonic_ns()?;
        let t2 = monotonic_ns()?;
        let delta = t2.saturating_sub(t1);
        overhead_samples.push(delta);
    }
    overhead_samples.sort_unstable();
    let clock_overhead_ns = median_u64(&overhead_samples);

    // --- fence_overhead: median delta with two fences between reads ---
    let mut fence_samples: Vec<u64> = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let t1 = monotonic_ns()?;
        timing_fence();
        timing_fence();
        let t2 = monotonic_ns()?;
        let delta = t2.saturating_sub(t1);
        fence_samples.push(delta);
    }
    fence_samples.sort_unstable();
    let fence_overhead_ns = median_u64(&fence_samples);

    Ok(CalibrationResult {
        clock_resolution_ns,
        clock_overhead_ns,
        fence_overhead_ns,
        samples: SAMPLES as u32,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calibration_completes() {
        let result = calibrate().expect("calibrate should succeed");
        assert_eq!(result.samples, 10_000);
    }

    #[test]
    fn clock_resolution_is_reasonable() {
        let result = calibrate().expect("calibrate");
        // Resolution should be between 1 ns and 10 μs
        assert!(
            result.clock_resolution_ns >= 1,
            "resolution too small: {}",
            result.clock_resolution_ns
        );
        assert!(
            result.clock_resolution_ns <= 10_000,
            "resolution too large: {} ns",
            result.clock_resolution_ns
        );
    }

    #[test]
    fn clock_overhead_is_reasonable() {
        let result = calibrate().expect("calibrate");
        // Overhead should be less than 10 μs
        assert!(
            result.clock_overhead_ns < 10_000,
            "clock overhead too large: {} ns",
            result.clock_overhead_ns
        );
    }

    #[test]
    fn fence_overhead_is_reasonable() {
        let result = calibrate().expect("calibrate");
        // Fence overhead should be less than 10 μs
        assert!(
            result.fence_overhead_ns < 10_000,
            "fence overhead too large: {} ns",
            result.fence_overhead_ns
        );
    }

    #[test]
    fn calibration_serializes() {
        let result = calibrate().expect("calibrate");
        let json = serde_json::to_string(&result).expect("serialize");
        let result2: CalibrationResult = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(result.clock_resolution_ns, result2.clock_resolution_ns);
        assert_eq!(result.clock_overhead_ns, result2.clock_overhead_ns);
        assert_eq!(result.fence_overhead_ns, result2.fence_overhead_ns);
        assert_eq!(result.samples, result2.samples);
    }

    #[test]
    fn median_u64_works() {
        // Odd-length
        assert_eq!(median_u64(&[1, 2, 3]), 2);
        // Even-length
        assert_eq!(median_u64(&[1, 2, 3, 4]), 2);
        // Single element
        assert_eq!(median_u64(&[10]), 10);
        // Empty
        assert_eq!(median_u64(&[]), 0);
    }
}
