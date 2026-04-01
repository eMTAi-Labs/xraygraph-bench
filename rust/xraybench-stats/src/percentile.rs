use xraybench_types::{BenchError, Result};

// ── DescriptiveStats ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DescriptiveStats {
    pub count: usize,
    pub mean: f64,
    pub min: f64,
    pub max: f64,
    pub variance: f64,
    pub stddev: f64,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Compute the p-th percentile (p in [0,1]) using linear interpolation
/// between nearest ranks (NumPy-compatible `linear` method).
///
/// Sorts `values` in place.
pub fn exact_percentile(values: &mut [f64], p: f64) -> Result<f64> {
    if values.is_empty() {
        return Err(BenchError::InvalidData(
            "cannot compute percentile of empty slice".to_string(),
        ));
    }
    if !(0.0..=1.0).contains(&p) {
        return Err(BenchError::InvalidData(format!(
            "percentile p={p} out of range [0, 1]"
        )));
    }
    values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
    Ok(interpolate(values, p))
}

/// Compute multiple percentiles in a single sort pass.
///
/// Sorts `values` in place.
pub fn exact_percentiles(values: &mut [f64], percentiles: &[f64]) -> Result<Vec<f64>> {
    if values.is_empty() {
        return Err(BenchError::InvalidData(
            "cannot compute percentiles of empty slice".to_string(),
        ));
    }
    for &p in percentiles {
        if !(0.0..=1.0).contains(&p) {
            return Err(BenchError::InvalidData(format!(
                "percentile p={p} out of range [0, 1]"
            )));
        }
    }
    values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
    Ok(percentiles
        .iter()
        .map(|&p| interpolate(values, p))
        .collect())
}

/// Single-pass Welford's online algorithm for descriptive statistics.
pub fn descriptive(values: &[f64]) -> Result<DescriptiveStats> {
    if values.is_empty() {
        return Err(BenchError::InvalidData(
            "cannot compute descriptive stats of empty slice".to_string(),
        ));
    }
    let mut count = 0usize;
    let mut mean = 0.0f64;
    let mut m2 = 0.0f64;
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;

    for &x in values {
        count += 1;
        let delta = x - mean;
        mean += delta / count as f64;
        let delta2 = x - mean;
        m2 += delta * delta2;
        if x < min {
            min = x;
        }
        if x > max {
            max = x;
        }
    }

    let variance = if count < 2 {
        0.0
    } else {
        m2 / (count - 1) as f64
    };
    let stddev = variance.sqrt();

    Ok(DescriptiveStats {
        count,
        mean,
        min,
        max,
        variance,
        stddev,
    })
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// NumPy-compatible linear interpolation between nearest ranks.
/// Assumes `sorted` is already sorted and non-empty.
fn interpolate(sorted: &[f64], p: f64) -> f64 {
    let n = sorted.len();
    if n == 1 {
        return sorted[0];
    }
    // virtual index in [0, n-1]
    let idx = p * (n - 1) as f64;
    let lo = idx.floor() as usize;
    let hi = idx.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let frac = idx - lo as f64;
        sorted[lo] * (1.0 - frac) + sorted[hi] * frac
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_value() {
        let mut v = vec![42.0f64];
        assert_eq!(exact_percentile(&mut v, 0.5).unwrap(), 42.0);
    }

    #[test]
    fn sorted_1_to_100() {
        let mut v: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let p50 = exact_percentile(&mut v, 0.5).unwrap();
        // NumPy linear: index = 0.5 * 99 = 49.5 → 50.5
        assert!((p50 - 50.5).abs() < 1e-9);
        let p99 = exact_percentile(&mut v, 0.99).unwrap();
        // index = 0.99 * 99 = 98.01 → 99.01
        assert!((p99 - 99.01).abs() < 1e-9);
    }

    #[test]
    fn min_max() {
        let mut v: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        assert_eq!(exact_percentile(&mut v, 0.0).unwrap(), 1.0);
        assert_eq!(exact_percentile(&mut v, 1.0).unwrap(), 100.0);
    }

    #[test]
    fn empty_fails() {
        let mut v: Vec<f64> = vec![];
        assert!(exact_percentile(&mut v, 0.5).is_err());
    }

    #[test]
    fn invalid_p() {
        let mut v = vec![1.0, 2.0, 3.0];
        assert!(exact_percentile(&mut v, -0.1).is_err());
        assert!(exact_percentile(&mut v, 1.1).is_err());
    }

    #[test]
    fn batch() {
        let mut v: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let ps = exact_percentiles(&mut v, &[0.0, 0.5, 1.0]).unwrap();
        assert_eq!(ps.len(), 3);
        assert_eq!(ps[0], 1.0);
        assert!((ps[1] - 50.5).abs() < 1e-9);
        assert_eq!(ps[2], 100.0);
    }

    #[test]
    fn descriptive_basic() {
        // [2,4,4,4,5,5,7,9] → mean = 40/8 = 5.0, variance = (sample)
        let v = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let s = descriptive(&v).unwrap();
        assert_eq!(s.count, 8);
        assert!((s.mean - 5.0).abs() < 1e-9, "mean={}", s.mean);
        assert_eq!(s.min, 2.0);
        assert_eq!(s.max, 9.0);
        // sample variance = 4.571... stddev ≈ 2.138
        assert!(s.variance > 4.0 && s.variance < 5.0);
        assert!(s.stddev > 2.0 && s.stddev < 2.3);
    }

    #[test]
    fn descriptive_single() {
        let v = vec![7.0];
        let s = descriptive(&v).unwrap();
        assert_eq!(s.count, 1);
        assert_eq!(s.mean, 7.0);
        assert_eq!(s.variance, 0.0);
        assert_eq!(s.stddev, 0.0);
    }

    #[test]
    fn descriptive_empty() {
        assert!(descriptive(&[]).is_err());
    }
}
