use xraybench_types::{MetricComparison, Result};
use xraybench_stats::regression::compare_metric;

/// Compare two sets of named metrics.  Metrics are matched by name; unmatched
/// names are silently skipped.
///
/// `metrics_a` / `metrics_b` — slices of `(metric_name, &[f64])` tuples.
/// `significance_threshold`  — alpha level passed through to `compare_metric`.
pub fn diff_results(
    metrics_a: &[(&str, &[f64])],
    metrics_b: &[(&str, &[f64])],
    significance_threshold: f64,
) -> Result<Vec<MetricComparison>> {
    let mut results = Vec::new();

    for (name_a, values_a) in metrics_a {
        // Find the matching metric in b by name
        if let Some((_, values_b)) = metrics_b.iter().find(|(n, _)| n == name_a) {
            let cmp = compare_metric(name_a, values_a, values_b, significance_threshold)?;
            results.push(cmp);
        }
    }

    Ok(results)
}

/// Format a `MetricComparison` as a human-readable table row.
///
/// Example output:
///   latency_ms   | 100.00 → 50.00 | Δ -50.00 (-50.00%) | p=0.0010 | Improvement ↓
pub fn format_comparison(cmp: &MetricComparison) -> String {
    let arrow = match cmp.classification {
        xraybench_types::ChangeClass::Improvement => "↓",
        xraybench_types::ChangeClass::Regression  => "↑",
        xraybench_types::ChangeClass::NoChange     => "~",
        xraybench_types::ChangeClass::Inconclusive => "?",
    };
    let class_word = format!("{:?}", cmp.classification);

    format!(
        "{:<20} | {:>10.2} → {:>10.2} | Δ {:>+10.2} ({:>+7.2}%) | p={:.4} | {} {}",
        cmp.metric_name,
        cmp.value_a,
        cmp.value_b,
        cmp.absolute_delta,
        cmp.percentage_change,
        cmp.p_value,
        class_word,
        arrow,
    )
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a slice of identical values.
    fn same(v: f64, n: usize) -> Vec<f64> {
        vec![v; n]
    }

    #[test]
    fn diff_identical() {
        // Same values → not significant → NoChange
        let a_vals = same(100.0, 50);
        let b_vals = same(100.0, 50);
        let a: &[(&str, &[f64])] = &[("latency_ms", &a_vals)];
        let b: &[(&str, &[f64])] = &[("latency_ms", &b_vals)];
        let cmps = diff_results(a, b, 0.05).unwrap();
        assert_eq!(cmps.len(), 1);
        assert!(
            !cmps[0].significant,
            "identical samples should not be significant; p={}",
            cmps[0].p_value
        );
    }

    #[test]
    fn diff_different() {
        // A=100, B=50 → clear improvement, should be significant
        let a_vals = same(100.0, 50);
        let b_vals = same(50.0, 50);
        let a: &[(&str, &[f64])] = &[("latency_ms", &a_vals)];
        let b: &[(&str, &[f64])] = &[("latency_ms", &b_vals)];
        let cmps = diff_results(a, b, 0.05).unwrap();
        assert_eq!(cmps.len(), 1);
        assert!(
            cmps[0].significant,
            "clearly different samples should be significant; p={}",
            cmps[0].p_value
        );
        assert_eq!(cmps[0].classification, xraybench_types::ChangeClass::Improvement);
    }

    #[test]
    fn diff_multiple_metrics() {
        let lat_a = same(100.0, 50);
        let lat_b = same(50.0, 50);
        let thr_a = same(200.0, 50);
        let thr_b = same(200.0, 50);

        let a: &[(&str, &[f64])] = &[("latency_ms", &lat_a), ("throughput", &thr_a)];
        let b: &[(&str, &[f64])] = &[("latency_ms", &lat_b), ("throughput", &thr_b)];
        let cmps = diff_results(a, b, 0.05).unwrap();
        assert_eq!(cmps.len(), 2, "expected 2 metric comparisons");
    }

    #[test]
    fn format_produces_output() {
        let a_vals = same(100.0, 50);
        let b_vals = same(50.0, 50);
        let a: &[(&str, &[f64])] = &[("cold_ms", &a_vals)];
        let b: &[(&str, &[f64])] = &[("cold_ms", &b_vals)];
        let cmps = diff_results(a, b, 0.05).unwrap();
        let line = format_comparison(&cmps[0]);
        assert!(line.contains("cold_ms"), "output should contain metric name: {line}");
        // The classification word should appear (e.g. "Improvement" or "NoChange")
        let has_class = line.contains("Improvement")
            || line.contains("Regression")
            || line.contains("NoChange")
            || line.contains("Inconclusive");
        assert!(has_class, "output should contain classification word: {line}");
    }
}
