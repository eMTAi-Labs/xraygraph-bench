"""TimeSeriesRunner — per-iteration latency capture for emergent edge benchmarks.

Captures individual latency samples across N iterations, supporting:
- Timeout detection and None-slot recording
- Per-iteration correctness checks
- Acceleration-point detection via sliding-window comparison
- Progressive bootstrap detection (early timeouts followed by real values)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from xraybench_core import timing as rust_timing
from xraybench_core import stats as rust_stats

from tools.xraybench.adapters.base import BaseAdapter


@dataclass
class TimeSeriesResult:
    """Per-iteration latency time series for a single query."""

    query: str
    iterations: int
    latencies_ms: list[float | None]
    timeout_count: int
    correctness_violations: int
    first_result: list[dict[str, Any]] | None
    metadata: dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary including computed statistics."""
        real_latencies: list[float] = [v for v in self.latencies_ms if v is not None]

        stats: dict[str, Any] = {}
        if real_latencies:
            desc = rust_stats.descriptive(real_latencies)
            stats["mean_ms"] = desc["mean"]
            pcts = rust_stats.percentiles(real_latencies, [0.50, 0.95, 0.99])
            stats["p50_ms"] = pcts[0]
            stats["p95_ms"] = pcts[1]
            stats["p99_ms"] = pcts[2]
            ci = rust_stats.bootstrap_ci(real_latencies)
            stats["ci_lower_ms"] = ci["lower"]
            stats["ci_upper_ms"] = ci["upper"]
            stats["ci_confidence"] = ci["confidence"]
            stats["sample_count"] = len(real_latencies)

        return {
            "query": self.query,
            "iterations": self.iterations,
            "latencies_ms": self.latencies_ms,
            "timeout_count": self.timeout_count,
            "correctness_violations": self.correctness_violations,
            "first_result": self.first_result,
            "metadata": self.metadata,
            "stats": stats,
        }

    # ------------------------------------------------------------------
    # Analysis helpers
    # ------------------------------------------------------------------

    def acceleration_point(
        self,
        window: int = 10,
        threshold: float = 0.5,
    ) -> int | None:
        """Return the iteration index where acceleration is first detected.

        Uses a sliding window comparison against the mean of the first window.
        Returns the first index *i* where the mean of
        ``latencies_ms[i : i + window]`` drops below
        ``first_window_mean * threshold``, or ``None`` if never detected.

        Args:
            window: Window size for the sliding comparison.
            threshold: Ratio threshold — windows below this fraction of the
                       first window's mean are considered accelerated.

        Returns:
            Integer iteration index or None.
        """
        real_slots: list[tuple[int, float]] = [
            (i, v) for i, v in enumerate(self.latencies_ms) if v is not None
        ]

        if len(real_slots) < window * 2:
            return None

        # Build a list of just the float values (preserving order)
        values = [v for _, v in real_slots]

        # Mean of the first window
        first_mean = sum(values[:window]) / window
        if first_mean <= 0:
            return None

        # Slide through the remainder
        for start in range(1, len(values) - window + 1):
            window_mean = sum(values[start : start + window]) / window
            if window_mean < first_mean * threshold:
                # Map back to the original iteration index
                return real_slots[start][0]

        return None

    def progressive_bootstrap_detected(self) -> bool:
        """Return True if early iterations timed out but later ones succeeded.

        Specifically, the first quarter of latencies must contain at least
        one ``None`` value and the last quarter must contain no ``None`` values.
        """
        n = len(self.latencies_ms)
        if n < 4:
            return False

        quarter = max(1, n // 4)
        early = self.latencies_ms[:quarter]
        late = self.latencies_ms[n - quarter :]

        has_early_none = any(v is None for v in early)
        all_late_real = all(v is not None for v in late)

        return has_early_none and all_late_real


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


class TimeSeriesRunner:
    """Executes a query repeatedly and captures per-iteration latencies."""

    def __init__(self, adapter: BaseAdapter) -> None:
        self.adapter = adapter

    def run_timeseries(
        self,
        query: str,
        iterations: int = 500,
        params: dict[str, Any] | None = None,
        timeout_ms: float | None = None,
        correctness_check: Callable[[list[dict[str, Any]]], bool] | None = None,
        clear_cache_before: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> TimeSeriesResult:
        """Execute *query* for *iterations* and record per-iteration latency.

        Args:
            query: Query string to execute each iteration.
            iterations: Number of iterations to run.
            params: Optional query parameters.
            timeout_ms: If set and elapsed exceeds this, record None for that
                        iteration and increment ``timeout_count``.
            correctness_check: Optional callable accepting ``rows`` (list of
                               dicts) and returning ``True`` for correct.
                               Violations are counted in the result.
            clear_cache_before: If True, call ``adapter.clear_caches()``
                                before the first iteration.
            metadata: Arbitrary dict stored verbatim in the result.

        Returns:
            TimeSeriesResult with per-iteration latency array and statistics.
        """
        if clear_cache_before:
            self.adapter.clear_caches()

        latencies_ms: list[float | None] = []
        timeout_count = 0
        correctness_violations = 0
        first_result: list[dict[str, Any]] | None = None

        for _ in range(iterations):
            t0 = rust_timing.monotonic_ns()
            try:
                execute_result = self.adapter.execute(query, params)
                t1 = rust_timing.monotonic_ns()
                elapsed_ms = (t1 - t0) / 1_000_000.0

                if timeout_ms is not None and elapsed_ms > timeout_ms:
                    latencies_ms.append(None)
                    timeout_count += 1
                else:
                    latencies_ms.append(elapsed_ms)

                    if first_result is None:
                        first_result = execute_result.rows

                    if correctness_check is not None:
                        if not correctness_check(execute_result.rows):
                            correctness_violations += 1

            except Exception:
                latencies_ms.append(None)
                timeout_count += 1

        return TimeSeriesResult(
            query=query,
            iterations=iterations,
            latencies_ms=latencies_ms,
            timeout_count=timeout_count,
            correctness_violations=correctness_violations,
            first_result=first_result,
            metadata=metadata or {},
        )
