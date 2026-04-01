"""Adapter overhead calibration — measures no-op query latency."""

from __future__ import annotations

import statistics
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.xraybench.adapters.base import BaseAdapter

_PING_QUERY = "RETURN 1 AS _ping"


def measure_adapter_overhead(adapter: "BaseAdapter", iterations: int = 1000) -> dict:
    """Measure the round-trip overhead of an adapter by executing a no-op query.

    Executes ``RETURN 1 AS _ping`` *iterations* times and returns latency
    statistics derived from wall-clock timings.

    Args:
        adapter: An adapter instance whose ``execute`` method will be called.
        iterations: Number of times to execute the ping query.

    Returns:
        dict with keys: median_ms, p95_ms, p99_ms, min_ms, max_ms, mean_ms,
        iterations.  All float values are rounded to 4 decimal places.
    """
    samples: list[float] = []

    for _ in range(iterations):
        t0 = time.perf_counter()
        adapter.execute(_PING_QUERY)
        t1 = time.perf_counter()
        samples.append((t1 - t0) * 1000.0)

    sorted_samples = sorted(samples)
    n = len(sorted_samples)

    def percentile(p: float) -> float:
        idx = (p / 100.0) * (n - 1)
        lower = int(idx)
        upper = min(lower + 1, n - 1)
        frac = idx - lower
        return sorted_samples[lower] + frac * (sorted_samples[upper] - sorted_samples[lower])

    return {
        "median_ms": round(statistics.median(samples), 4),
        "p95_ms": round(percentile(95), 4),
        "p99_ms": round(percentile(99), 4),
        "min_ms": round(sorted_samples[0], 4),
        "max_ms": round(sorted_samples[-1], 4),
        "mean_ms": round(statistics.mean(samples), 4),
        "iterations": iterations,
    }
