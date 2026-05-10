use xraybench_types::ChangeClass;

/// Classify a benchmark change given p-value, percentage change, significance
/// level alpha, and a minimum practical effect threshold.
///
/// Rules (lower-is-better for timing metrics):
/// - p > alpha                       → Inconclusive
/// - |percentage_change| < min_effect_pct → NoChange
/// - percentage_change < 0           → Improvement  (metric went down)
/// - percentage_change > 0           → Regression   (metric went up)
pub fn classify_change(
    p_value: f64,
    percentage_change: f64,
    alpha: f64,
    min_effect_pct: f64,
) -> ChangeClass {
    if p_value > alpha {
        return ChangeClass::Inconclusive;
    }
    if percentage_change.abs() < min_effect_pct {
        return ChangeClass::NoChange;
    }
    if percentage_change < 0.0 {
        ChangeClass::Improvement
    } else {
        ChangeClass::Regression
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn improvement() {
        // p significant, metric went down 10%
        let cls = classify_change(0.01, -10.0, 0.05, 1.0);
        assert_eq!(cls, ChangeClass::Improvement);
    }

    #[test]
    fn regression() {
        // p significant, metric went up 15%
        let cls = classify_change(0.02, 15.0, 0.05, 1.0);
        assert_eq!(cls, ChangeClass::Regression);
    }

    #[test]
    fn no_change_small_effect() {
        // p is significant but the practical effect is below threshold
        let cls = classify_change(0.03, 0.5, 0.05, 1.0);
        assert_eq!(cls, ChangeClass::NoChange);
    }

    #[test]
    fn inconclusive_high_p() {
        // p is above alpha regardless of how large the change looks
        let cls = classify_change(0.20, -50.0, 0.05, 1.0);
        assert_eq!(cls, ChangeClass::Inconclusive);
    }
}
