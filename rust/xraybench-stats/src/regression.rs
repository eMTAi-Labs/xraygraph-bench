use crate::bootstrap::{bca_mean_ci, BootstrapConfig};
use xraybench_types::{BenchError, ChangeClass, MetricComparison, Result};

// ── Result types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct MannWhitneyResult {
    pub u_a: f64,
    pub u_b: f64,
    pub p_value: f64,
    pub significant: bool,
}

// ── Mann-Whitney U test ───────────────────────────────────────────────────────

/// Two-sided Mann-Whitney U test with tie handling and normal approximation.
pub fn mann_whitney_u(sample_a: &[f64], sample_b: &[f64]) -> Result<MannWhitneyResult> {
    if sample_a.len() < 2 {
        return Err(BenchError::InsufficientSamples {
            needed: 2,
            got: sample_a.len(),
        });
    }
    if sample_b.len() < 2 {
        return Err(BenchError::InsufficientSamples {
            needed: 2,
            got: sample_b.len(),
        });
    }

    let n_a = sample_a.len() as f64;
    let n_b = sample_b.len() as f64;

    // ── Rank all values together ──────────────────────────────────────────────
    let mut combined: Vec<(f64, usize)> = sample_a
        .iter()
        .map(|&v| (v, 0usize))
        .chain(sample_b.iter().map(|&v| (v, 1usize)))
        .collect();
    combined.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

    let total = combined.len();
    let mut ranks = vec![0.0f64; total];

    // Assign average ranks for ties
    let mut i = 0;
    while i < total {
        let mut j = i;
        while j < total && combined[j].0 == combined[i].0 {
            j += 1;
        }
        let avg_rank = (i + j + 1) as f64 / 2.0; // 1-based average
        for rank in ranks.iter_mut().take(j).skip(i) {
            *rank = avg_rank;
        }
        i = j;
    }

    // ── Sum of ranks for sample A ─────────────────────────────────────────────
    let r_a: f64 = combined
        .iter()
        .zip(ranks.iter())
        .filter(|((_, grp), _)| *grp == 0)
        .map(|(_, &r)| r)
        .sum();

    let u_a = r_a - n_a * (n_a + 1.0) / 2.0;
    let u_b = n_a * n_b - u_a;
    let u_min = u_a.min(u_b);

    // ── Tie correction for variance ───────────────────────────────────────────
    let n = total as f64;
    let tie_correction: f64 = {
        let mut tc = 0.0f64;
        let mut k = 0;
        while k < total {
            let mut m = k;
            while m < total && combined[m].0 == combined[k].0 {
                m += 1;
            }
            let t = (m - k) as f64;
            tc += t * t * t - t;
            k = m;
        }
        tc
    };

    let variance = n_a * n_b / 12.0 * (n + 1.0 - tie_correction / (n * (n - 1.0)));
    let variance = variance.max(1e-10); // guard against 0

    // Normal approximation: use U_A deviation from the expected mean
    let mean_u = n_a * n_b / 2.0;
    let z = (u_a - mean_u).abs() / variance.sqrt();
    let _ = u_min; // kept for API completeness
    let p_value = 2.0 * (1.0 - normal_cdf(z));

    Ok(MannWhitneyResult {
        u_a,
        u_b,
        p_value,
        significant: p_value < 0.05,
    })
}

// ── compare_metric ────────────────────────────────────────────────────────────

/// Compare a metric between two measurement vectors using BCa bootstrap CI on
/// differences and Mann-Whitney U for significance.
pub fn compare_metric(
    name: &str,
    values_a: &[f64],
    values_b: &[f64],
    significance_threshold: f64,
) -> Result<MetricComparison> {
    if values_a.is_empty() || values_b.is_empty() {
        return Err(BenchError::InvalidData(
            "empty sample passed to compare_metric".to_string(),
        ));
    }

    let mean_a = values_a.iter().sum::<f64>() / values_a.len() as f64;
    let mean_b = values_b.iter().sum::<f64>() / values_b.len() as f64;

    let absolute_delta = mean_b - mean_a;
    let percentage_change = if mean_a.abs() < 1e-15 {
        0.0
    } else {
        absolute_delta / mean_a * 100.0
    };

    // BCa CI on the difference distribution (bootstrap)
    // Build a vector of pairwise or unpaired differences
    let min_len = values_a.len().min(values_b.len());
    let config = BootstrapConfig::default();

    let (ci_lower, ci_upper, p_value, significant) = if min_len >= 3 {
        // Build paired differences where possible, then bootstrap CI on diffs
        let diffs: Vec<f64> = values_a[..min_len]
            .iter()
            .zip(values_b[..min_len].iter())
            .map(|(&a, &b)| b - a)
            .collect();

        let ci = bca_mean_ci(&diffs, config)?;

        // Mann-Whitney for significance
        let mw = mann_whitney_u(values_a, values_b)?;

        (
            ci.lower,
            ci.upper,
            mw.p_value,
            mw.p_value < significance_threshold,
        )
    } else {
        // Not enough data for bootstrap — use Mann-Whitney only
        let mw = mann_whitney_u(values_a, values_b)?;
        (
            absolute_delta,
            absolute_delta,
            mw.p_value,
            mw.p_value < significance_threshold,
        )
    };

    // ── Classify ──────────────────────────────────────────────────────────────
    let classification = if !significant {
        ChangeClass::NoChange
    } else if absolute_delta < 0.0 {
        // B < A → metric went down (improvement for latency/cost, regression for throughput)
        // Task specifies: 100→50 is Improvement, 50→100 is Regression
        ChangeClass::Improvement
    } else {
        ChangeClass::Regression
    };

    Ok(MetricComparison {
        metric_name: name.to_string(),
        value_a: mean_a,
        value_b: mean_b,
        absolute_delta,
        percentage_change,
        ci_lower,
        ci_upper,
        p_value,
        significant,
        classification,
    })
}

// ── Math helpers (local copies for no-import-cycle) ───────────────────────────

fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

fn erf(x: f64) -> f64 {
    const P: f64 = 0.327_591_1;
    const A1: f64 = 0.254_829_592;
    const A2: f64 = -0.284_496_736;
    const A3: f64 = 1.421_413_741;
    const A4: f64 = -1.453_152_027;
    const A5: f64 = 1.061_405_429;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + P * x);
    let poly = t * (A1 + t * (A2 + t * (A3 + t * (A4 + t * A5))));
    sign * (1.0 - poly * (-x * x).exp())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_not_significant() {
        let a: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let b = a.clone();
        let res = mann_whitney_u(&a, &b).unwrap();
        assert!(!res.significant, "p_value={}", res.p_value);
    }

    #[test]
    fn clearly_different() {
        let a: Vec<f64> = (1..=50).map(|i| i as f64).collect();
        let b: Vec<f64> = (1000..=1050).map(|i| i as f64).collect();
        let res = mann_whitney_u(&a, &b).unwrap();
        assert!(res.p_value < 0.001, "p_value={}", res.p_value);
        assert!(res.significant);
    }

    #[test]
    fn slightly_shifted() {
        // 10% shift detectable with enough samples (n=500 per group)
        let a: Vec<f64> = (1..=500).map(|i| i as f64).collect();
        let b: Vec<f64> = (1..=500).map(|i| i as f64 * 1.1).collect();
        let res = mann_whitney_u(&a, &b).unwrap();
        assert!(res.significant, "expected significant p={}", res.p_value);
    }

    #[test]
    fn u_statistic_symmetry() {
        let a: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let b: Vec<f64> = vec![6.0, 7.0, 8.0, 9.0, 10.0];
        let res = mann_whitney_u(&a, &b).unwrap();
        // U_A + U_B = n_A * n_B = 25
        assert!(
            (res.u_a + res.u_b - 25.0).abs() < 1e-9,
            "u_a={} u_b={}",
            res.u_a,
            res.u_b
        );
    }

    #[test]
    fn too_few_samples() {
        let a = vec![1.0];
        let b = vec![1.0, 2.0, 3.0];
        assert!(mann_whitney_u(&a, &b).is_err());
        assert!(mann_whitney_u(&b, &a).is_err());
    }

    #[test]
    fn compare_improvement() {
        // values_a = 100, values_b = 50 → delta negative → Improvement
        let a: Vec<f64> = vec![100.0; 50];
        let b: Vec<f64> = vec![50.0; 50];
        let res = compare_metric("latency_ns", &a, &b, 0.05).unwrap();
        assert_eq!(
            res.classification,
            ChangeClass::Improvement,
            "classification={:?} p={}",
            res.classification,
            res.p_value
        );
    }

    #[test]
    fn compare_regression() {
        // values_a = 50, values_b = 100 → delta positive → Regression
        let a: Vec<f64> = vec![50.0; 50];
        let b: Vec<f64> = vec![100.0; 50];
        let res = compare_metric("latency_ns", &a, &b, 0.05).unwrap();
        assert_eq!(
            res.classification,
            ChangeClass::Regression,
            "classification={:?} p={}",
            res.classification,
            res.p_value
        );
    }
}
