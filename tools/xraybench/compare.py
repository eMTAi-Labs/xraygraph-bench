"""Result comparison -- statistical analysis of two benchmark result sets."""

import json
from pathlib import Path
from typing import Any

import xraybench_core


def load_result(path: str | Path) -> dict[str, Any]:
    """Load a benchmark result JSON file."""
    with open(path) as f:
        return json.load(f)


def compare_results(
    result_a: dict[str, Any],
    result_b: dict[str, Any],
    confidence: float = 0.95,
) -> dict[str, Any]:
    """Compare two benchmark results on shared timing metrics.

    Compares cold_ms, warm_ms, and compile_ms (if both have it).
    Uses xraybench_core.compare.compare_metric for statistical analysis.

    Returns dict with:
    - benchmark: str
    - engine_a, engine_b: str
    - metrics: list of comparison dicts
    - summary: str (human-readable)
    """
    significance_threshold = 1.0 - confidence
    metrics = []

    for metric_name in ["cold_ms", "warm_ms", "compile_ms"]:
        val_a = result_a.get(metric_name)
        val_b = result_b.get(metric_name)
        if val_a is None or val_b is None:
            continue

        # For single-value results, create synthetic samples
        # (real multi-run results would have arrays)
        samples_a = result_a.get(f"{metric_name}_samples", [val_a] * 30)
        samples_b = result_b.get(f"{metric_name}_samples", [val_b] * 30)

        cmp = xraybench_core.compare.compare_metric(
            metric_name,
            [float(v) for v in samples_a],
            [float(v) for v in samples_b],
            significance_threshold,
        )
        metrics.append(cmp)

    # Generate summary
    arrow = {
        "Improvement": "down better",
        "Regression": "up worse",
        "NoChange": "= same",
        "Inconclusive": "? unclear",
    }
    lines = []
    for m in metrics:
        lines.append(
            f"  {m['metric_name']:<15} {m['value_a']:>10.2f} -> {m['value_b']:>10.2f}  "
            f"({m['percentage_change']:+.1f}%)  {arrow.get(m['classification'], '?')}"
        )

    return {
        "benchmark": result_a.get("benchmark", "unknown"),
        "engine_a": result_a.get("engine", "unknown"),
        "engine_b": result_b.get("engine", "unknown"),
        "metrics": metrics,
        "summary": "\n".join(lines),
    }


def format_comparison_table(comparison: dict[str, Any]) -> str:
    """Format comparison as a human-readable table."""
    header = (
        f"Comparing: {comparison['engine_a']} vs {comparison['engine_b']}\n"
        f"Benchmark: {comparison['benchmark']}\n"
        f"{'─' * 70}\n"
        f"{'Metric':<15} {'A':>10} {'B':>10} {'Change':>10} {'p-value':>10} {'Verdict':<12}\n"
        f"{'─' * 70}"
    )

    rows = []
    for m in comparison["metrics"]:
        verdict = m["classification"]
        rows.append(
            f"{m['metric_name']:<15} {m['value_a']:>10.2f} {m['value_b']:>10.2f} "
            f"{m['percentage_change']:>+9.1f}% {m['p_value']:>10.4f} {verdict:<12}"
        )

    return header + "\n" + "\n".join(rows)
