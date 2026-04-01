use xraybench_types::{BenchError, Result};

// ── Constants ─────────────────────────────────────────────────────────────────

pub const DEFAULT_THRESHOLD: f64 = 3.5;

/// Scaling constant: 1 / Φ⁻¹(0.75) ≈ 1 / 0.6745
pub const MAD_SCALE: f64 = 0.6745;

// ── Result type ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OutlierResult {
    /// Indices into the original slice that are classified as outliers.
    pub outlier_indices: Vec<usize>,
    /// Modified Z-score for each element (same length as input).
    pub modified_z_scores: Vec<f64>,
    pub median: f64,
    pub mad: f64,
    pub threshold: f64,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Detect outliers using the Modified Z-Score method (Iglewicz & Hoaglin 1993).
///
/// When MAD = 0 (constant or near-constant data), falls back to mean absolute
/// deviation for the dispersion estimate.
pub fn detect_outliers(values: &[f64], threshold: f64) -> Result<OutlierResult> {
    if values.len() < 2 {
        return Err(BenchError::InsufficientSamples {
            needed: 2,
            got: values.len(),
        });
    }

    let median = compute_median(values);
    let mad_val = mad(values);

    let dispersion = if mad_val == 0.0 {
        // Fallback: mean absolute deviation from the median
        let mean_abs_dev = values.iter().map(|&v| (v - median).abs()).sum::<f64>()
            / values.len() as f64;
        mean_abs_dev
    } else {
        mad_val
    };

    let modified_z_scores: Vec<f64> = if dispersion == 0.0 {
        // All values identical: z-scores are all 0
        vec![0.0; values.len()]
    } else {
        values
            .iter()
            .map(|&v| MAD_SCALE * (v - median).abs() / dispersion)
            .collect()
    };

    let outlier_indices: Vec<usize> = modified_z_scores
        .iter()
        .enumerate()
        .filter(|(_, &z)| z > threshold)
        .map(|(i, _)| i)
        .collect();

    Ok(OutlierResult {
        outlier_indices,
        modified_z_scores,
        median,
        mad: mad_val,
        threshold,
    })
}

/// Median Absolute Deviation.
///
/// Returns the MAD of `values` (population estimate, not scaled).
pub fn mad(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let med = compute_median(values);
    let mut abs_devs: Vec<f64> = values.iter().map(|&v| (v - med).abs()).collect();
    compute_median_of_sorted_or_unsorted(&mut abs_devs)
}

/// Median of an unsorted slice (does not mutate the original).
pub fn compute_median(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    compute_median_of_sorted_or_unsorted(&mut sorted)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn compute_median_of_sorted_or_unsorted(v: &mut Vec<f64>) -> f64 {
    v.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_outliers_uniform() {
        let values: Vec<f64> = (1..=20).map(|i| i as f64).collect();
        let res = detect_outliers(&values, DEFAULT_THRESHOLD).unwrap();
        assert!(res.outlier_indices.is_empty());
    }

    #[test]
    fn detects_extreme_outlier() {
        let mut values: Vec<f64> = vec![10.0; 100];
        values.push(1_000_000.0); // one extreme outlier
        let res = detect_outliers(&values, DEFAULT_THRESHOLD).unwrap();
        assert!(!res.outlier_indices.is_empty(), "should detect the extreme outlier");
        assert!(res.outlier_indices.contains(&100));
    }

    #[test]
    fn mad_of_uniform_range() {
        // 1..=100: median = 50.5, abs deviations are symmetric around 49.5
        // MAD of 1..=100 ≈ 25.0
        let values: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let m = mad(&values);
        assert!((m - 25.0).abs() < 1.0, "mad={m}");
    }

    #[test]
    fn mad_of_constant() {
        let values = vec![5.0; 50];
        assert_eq!(mad(&values), 0.0);
    }

    #[test]
    fn handles_all_identical() {
        let values = vec![42.0; 10];
        let res = detect_outliers(&values, DEFAULT_THRESHOLD).unwrap();
        // All z-scores should be 0 when dispersion is 0
        assert!(res.outlier_indices.is_empty());
        for &z in &res.modified_z_scores {
            assert_eq!(z, 0.0);
        }
    }

    #[test]
    fn z_scores_correct_sign() {
        // Modified Z-scores should all be non-negative (we use abs deviation)
        let values: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 1000.0];
        let res = detect_outliers(&values, DEFAULT_THRESHOLD).unwrap();
        for &z in &res.modified_z_scores {
            assert!(z >= 0.0, "z-score should be non-negative, got {z}");
        }
    }

    #[test]
    fn too_few_samples() {
        assert!(detect_outliers(&[1.0], DEFAULT_THRESHOLD).is_err());
        assert!(detect_outliers(&[], DEFAULT_THRESHOLD).is_err());
    }
}
