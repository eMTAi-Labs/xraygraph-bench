use crate::diff::diff_results;
use xraybench_types::{MetricComparison, Result};

/// A single engine's metrics: `(engine_name, &[(metric_name, values)])`.
pub type EngineMetrics<'a> = (&'a str, &'a [(&'a str, &'a [f64])]);

/// The result of a pairwise comparison between two named engines.
#[derive(Debug, Clone)]
pub struct PairwiseComparison {
    pub engine_a: String,
    pub engine_b: String,
    pub comparison: MetricComparison,
}

/// Compute all ordered pairs of engines and diff their metrics.
///
/// `engines` — slice of `(engine_name, &[(&str, &[f64])])` tuples where the
///             inner slice is the list of `(metric_name, values)` for that engine.
///
/// Returns one `PairwiseComparison` per (engine pair × matched metric).
pub fn pairwise_matrix(
    engines: &[EngineMetrics<'_>],
    significance_threshold: f64,
) -> Result<Vec<PairwiseComparison>> {
    let mut out = Vec::new();

    for i in 0..engines.len() {
        for j in (i + 1)..engines.len() {
            let (name_a, metrics_a) = &engines[i];
            let (name_b, metrics_b) = &engines[j];

            let comparisons = diff_results(metrics_a, metrics_b, significance_threshold)?;
            for cmp in comparisons {
                out.push(PairwiseComparison {
                    engine_a: name_a.to_string(),
                    engine_b: name_b.to_string(),
                    comparison: cmp,
                });
            }
        }
    }

    Ok(out)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_engine_matrix() {
        // 3 engines × 1 metric → C(3,2) = 3 pairwise comparisons
        let vals_a: Vec<f64> = vec![100.0; 30];
        let vals_b: Vec<f64> = vec![80.0; 30];
        let vals_c: Vec<f64> = vec![60.0; 30];

        let metrics_a: &[(&str, &[f64])] = &[("cold_ms", &vals_a)];
        let metrics_b: &[(&str, &[f64])] = &[("cold_ms", &vals_b)];
        let metrics_c: &[(&str, &[f64])] = &[("cold_ms", &vals_c)];

        let engines: &[EngineMetrics<'_>] = &[
            ("engine_x", metrics_a),
            ("engine_y", metrics_b),
            ("engine_z", metrics_c),
        ];

        let results = pairwise_matrix(engines, 0.05).unwrap();
        assert_eq!(
            results.len(),
            3,
            "C(3,2)=3 pairs expected, got {}",
            results.len()
        );

        // Verify each pair has correct engine names
        let pairs: Vec<(String, String)> = results
            .iter()
            .map(|r| (r.engine_a.clone(), r.engine_b.clone()))
            .collect();
        assert!(pairs.contains(&("engine_x".to_string(), "engine_y".to_string())));
        assert!(pairs.contains(&("engine_x".to_string(), "engine_z".to_string())));
        assert!(pairs.contains(&("engine_y".to_string(), "engine_z".to_string())));
    }
}
