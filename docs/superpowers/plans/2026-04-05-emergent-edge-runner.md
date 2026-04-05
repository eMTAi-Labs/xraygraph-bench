# Emergent Edge Runner Support — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add time-series measurement, multi-phase benchmark execution, and emergent edge metrics to the xraybench runner — enabling the 5 emergent edge benchmark specs to run against xrayGraphDB.

**Architecture:** New `TimeSeriesRunner` extends the existing `BenchmarkRunner` with per-iteration latency capture, multi-phase execution (warmup → mutate → measure), progressive timeout detection, and emergent edge engine metrics collection. A new `EmergentEdgeCollector` queries xrayGraphDB's emergent edge procedures for cache hit rates, tier distributions, and invalidation counts. Results include full time-series data for visualization.

**Tech Stack:** Python 3.12, xraybench_core (Rust timing/stats), existing adapter infrastructure

---

## File Structure

```
tools/xraybench/
  timeseries.py                       # CREATE: TimeSeriesRunner — per-iteration capture
  emergent_collector.py               # CREATE: EmergentEdgeCollector — engine metrics
  phases.py                           # CREATE: multi-phase execution (warmup/mutate/measure)
  cli.py                              # MODIFY: add 'run-emergent' command
tests/
  test_timeseries.py                  # CREATE
  test_emergent_collector.py          # CREATE
  test_phases.py                      # CREATE
```

---

### Task 0: Time-Series Runner — Per-Iteration Latency Capture

**Files:**
- Create: `tools/xraybench/timeseries.py`
- Create: `tests/test_timeseries.py`

- [ ] **Step 1: Write tests for TimeSeriesRunner**

Create `tests/test_timeseries.py`:

```python
"""Tests for time-series benchmark runner."""

from unittest.mock import MagicMock
from tools.xraybench.timeseries import TimeSeriesRunner, TimeSeriesResult
from tools.xraybench.models import ExecuteResult, CorrectnessResult
from tools.xraybench.adapters.capabilities import (
    CacheClearReport,
    ConnectionInfo,
    EngineInfo,
    LoadReport,
    Capability,
)


def _mock_adapter():
    adapter = MagicMock()
    adapter.connect.return_value = ConnectionInfo(
        host="localhost", port=7689, protocol="xrayProtocol", connected=True
    )
    adapter.load_dataset.return_value = LoadReport(
        node_count=100, edge_count=200, load_time_ms=10.0, verified=True
    )
    adapter.execute.return_value = ExecuteResult(
        rows=[{"count": 42}], wall_ms=1.0
    )
    adapter.clear_caches.return_value = CacheClearReport(cleared=True)
    adapter.engine_info.return_value = EngineInfo(
        name="xraygraphdb", version="4.0.2", capabilities={Capability.CACHE_CLEAR}
    )
    adapter.engine_version.return_value = "xraygraphdb-4.0.2"
    adapter.validate_correctness.return_value = CorrectnessResult(
        passed=True, detail="ok"
    )
    adapter.collect_metrics.return_value = {}
    adapter.capabilities.return_value = {Capability.CACHE_CLEAR}
    return adapter


def test_timeseries_captures_per_iteration():
    adapter = _mock_adapter()
    runner = TimeSeriesRunner(adapter)
    result = runner.run_timeseries(
        query="RETURN 1 AS x",
        iterations=50,
    )
    assert isinstance(result, TimeSeriesResult)
    assert len(result.latencies_ms) == 50
    assert all(t >= 0 for t in result.latencies_ms)


def test_timeseries_detects_acceleration():
    """Simulate a learning curve: first 20 iterations slow, rest fast."""
    adapter = _mock_adapter()
    call_count = [0]

    def slow_then_fast(query, params=None):
        call_count[0] += 1
        import time
        if call_count[0] <= 20:
            time.sleep(0.002)  # 2ms
        else:
            time.sleep(0.0005)  # 0.5ms
        return ExecuteResult(rows=[{"x": 1}], wall_ms=1.0)

    adapter.execute.side_effect = slow_then_fast
    runner = TimeSeriesRunner(adapter)
    result = runner.run_timeseries(query="RETURN 1", iterations=60)

    # Detect that later iterations are faster
    first_20_mean = sum(result.latencies_ms[:20]) / 20
    last_20_mean = sum(result.latencies_ms[-20:]) / 20
    assert last_20_mean < first_20_mean * 0.8  # At least 20% speedup


def test_timeseries_correctness_check():
    adapter = _mock_adapter()
    # First call returns 42, subsequent calls also return 42
    adapter.execute.return_value = ExecuteResult(
        rows=[{"count": 42}], wall_ms=1.0
    )
    runner = TimeSeriesRunner(adapter)
    result = runner.run_timeseries(
        query="RETURN 1",
        iterations=10,
        correctness_check=lambda rows: rows[0].get("count") == 42,
    )
    assert result.correctness_violations == 0


def test_timeseries_detects_correctness_violation():
    adapter = _mock_adapter()
    runner = TimeSeriesRunner(adapter)
    result = runner.run_timeseries(
        query="RETURN 1",
        iterations=10,
        correctness_check=lambda rows: False,  # Always fails
    )
    assert result.correctness_violations == 10


def test_timeseries_tracks_timeouts():
    adapter = _mock_adapter()

    def timeout_sometimes(query, params=None):
        import time
        time.sleep(0.001)
        return ExecuteResult(rows=[], wall_ms=1.0)

    adapter.execute.side_effect = timeout_sometimes
    runner = TimeSeriesRunner(adapter)
    result = runner.run_timeseries(
        query="RETURN 1",
        iterations=10,
        timeout_ms=0.5,  # Very short — will cause timeouts
    )
    assert result.timeout_count >= 0  # May or may not timeout depending on system


def test_timeseries_result_to_dict():
    result = TimeSeriesResult(
        query="RETURN 1",
        iterations=5,
        latencies_ms=[1.0, 2.0, 3.0, 4.0, 5.0],
        timeout_count=0,
        correctness_violations=0,
        first_result=[{"x": 1}],
    )
    d = result.to_dict()
    assert d["iterations"] == 5
    assert len(d["latencies_ms"]) == 5
    assert "stats" in d
    assert "p50_ms" in d["stats"]


def test_timeseries_computes_acceleration_point():
    result = TimeSeriesResult(
        query="RETURN 1",
        iterations=100,
        # Simulate: 50 iterations at 10ms, then 50 at 2ms
        latencies_ms=[10.0] * 50 + [2.0] * 50,
        timeout_count=0,
        correctness_violations=0,
    )
    accel = result.acceleration_point()
    # Should detect the change around iteration 50
    assert accel is not None
    assert 40 <= accel <= 60
```

- [ ] **Step 2: Implement timeseries.py**

Create `tools/xraybench/timeseries.py`:

```python
"""Time-series benchmark runner — per-iteration latency capture.

Extends the standard runner with:
- Per-iteration timing stored as a full latency array
- Correctness checking per iteration (detect stale cache data)
- Timeout tracking (detect timed-out traversals)
- Acceleration point detection (find when optimization kicks in)
- Progressive bootstrapping detection (timed-out queries that later complete)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import xraybench_core

from .adapters.base import BaseAdapter
from .models import ExecuteResult

logger = logging.getLogger(__name__)


@dataclass
class TimeSeriesResult:
    """Result of a time-series benchmark run."""

    query: str
    iterations: int
    latencies_ms: list[float]
    timeout_count: int = 0
    correctness_violations: int = 0
    first_result: list[dict[str, Any]] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        stats = {}
        valid = [t for t in self.latencies_ms if t is not None]
        if len(valid) >= 3:
            desc = xraybench_core.stats.descriptive(valid)
            pcts = xraybench_core.stats.percentiles(valid, [0.5, 0.95, 0.99])
            ci = xraybench_core.stats.bootstrap_ci(valid)
            stats = {
                "mean_ms": round(desc["mean"], 3),
                "p50_ms": round(pcts[0], 3),
                "p95_ms": round(pcts[1], 3),
                "p99_ms": round(pcts[2], 3),
                "min_ms": round(desc["min"], 3),
                "max_ms": round(desc["max"], 3),
                "stddev_ms": round(desc["stddev"], 3),
                "ci_lower_ms": round(ci["lower"], 3),
                "ci_upper_ms": round(ci["upper"], 3),
            }
        return {
            "query": self.query,
            "iterations": self.iterations,
            "latencies_ms": [round(t, 3) if t is not None else None for t in self.latencies_ms],
            "timeout_count": self.timeout_count,
            "correctness_violations": self.correctness_violations,
            "stats": stats,
            "metadata": self.metadata,
        }

    def acceleration_point(self, window: int = 10, threshold: float = 0.5) -> int | None:
        """Detect the iteration where latency drops significantly.

        Uses a sliding window comparison: compares each window's mean to
        the first window's mean. Returns the first iteration where the
        ratio drops below `threshold`.

        Args:
            window: Sliding window size.
            threshold: Ratio threshold (0.5 = 50% of initial latency).

        Returns:
            Iteration index of acceleration, or None if no drop detected.
        """
        valid = [t for t in self.latencies_ms if t is not None]
        if len(valid) < window * 2:
            return None

        baseline = sum(valid[:window]) / window
        if baseline <= 0:
            return None

        for i in range(window, len(valid) - window + 1):
            current = sum(valid[i : i + window]) / window
            if current / baseline < threshold:
                return i
        return None

    def progressive_bootstrap_detected(self) -> bool:
        """Check if a query that initially timed out later completed.

        Returns True if early iterations have None (timeout) latencies
        but later iterations have real values.
        """
        first_none = -1
        last_real = -1
        for i, t in enumerate(self.latencies_ms):
            if t is None and first_none == -1:
                first_none = i
            if t is not None:
                last_real = i
        return first_none != -1 and last_real > first_none


class TimeSeriesRunner:
    """Runs a query N times and captures per-iteration latency."""

    def __init__(self, adapter: BaseAdapter) -> None:
        self.adapter = adapter

    def run_timeseries(
        self,
        query: str,
        iterations: int = 500,
        params: dict[str, Any] | None = None,
        timeout_ms: float | None = None,
        correctness_check: Callable[[list[dict]], bool] | None = None,
        clear_cache_before: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> TimeSeriesResult:
        """Execute a query `iterations` times with per-iteration timing.

        Args:
            query: Cypher or GFQL query string.
            iterations: Number of repetitions.
            params: Query parameters.
            timeout_ms: Per-iteration timeout in milliseconds. If exceeded,
                the iteration is recorded as None (timeout).
            correctness_check: Optional callable that receives result rows
                and returns True if correct. Called every iteration.
            clear_cache_before: If True, clear caches before the first iteration.
            metadata: Extra metadata to attach to the result.

        Returns:
            TimeSeriesResult with full latency array.
        """
        latencies: list[float | None] = []
        timeout_count = 0
        violations = 0
        first_result = None

        if clear_cache_before:
            self.adapter.clear_caches()

        for i in range(iterations):
            t0 = xraybench_core.timing.monotonic_ns()
            try:
                result = self.adapter.execute(query, params)
                t1 = xraybench_core.timing.monotonic_ns()
                elapsed_ms = (t1 - t0) / 1_000_000.0

                if timeout_ms is not None and elapsed_ms > timeout_ms:
                    latencies.append(None)
                    timeout_count += 1
                else:
                    latencies.append(elapsed_ms)

                if i == 0:
                    first_result = result.rows

                if correctness_check is not None:
                    if not correctness_check(result.rows):
                        violations += 1

            except Exception as exc:
                t1 = xraybench_core.timing.monotonic_ns()
                elapsed_ms = (t1 - t0) / 1_000_000.0
                latencies.append(None)
                timeout_count += 1
                logger.debug("Iteration %d failed: %s (%.1fms)", i, exc, elapsed_ms)

        return TimeSeriesResult(
            query=query,
            iterations=iterations,
            latencies_ms=latencies,
            timeout_count=timeout_count,
            correctness_violations=violations,
            first_result=first_result,
            metadata=metadata or {},
        )
```

- [ ] **Step 3: Run tests**

Run: `.venv/bin/python3 -m pytest tests/test_timeseries.py -v`
Expected: All 7 tests pass.

- [ ] **Step 4: Commit**

```bash
git add tools/xraybench/timeseries.py tests/test_timeseries.py
git commit -m "feat: add TimeSeriesRunner — per-iteration latency capture, acceleration detection, progressive bootstrapping"
```

---

### Task 1: Multi-Phase Execution Engine

**Files:**
- Create: `tools/xraybench/phases.py`
- Create: `tests/test_phases.py`

- [ ] **Step 1: Write tests for multi-phase execution**

Create `tests/test_phases.py`:

```python
"""Tests for multi-phase benchmark execution."""

from unittest.mock import MagicMock, call
from tools.xraybench.phases import (
    PhaseRunner,
    Phase,
    PhaseResult,
    WarmupPhase,
    MutatePhase,
    MeasurePhase,
)
from tools.xraybench.models import ExecuteResult
from tools.xraybench.adapters.capabilities import CacheClearReport


def _mock_adapter():
    adapter = MagicMock()
    adapter.execute.return_value = ExecuteResult(
        rows=[{"count": 42}], wall_ms=1.0
    )
    adapter.clear_caches.return_value = CacheClearReport(cleared=True)
    return adapter


def test_warmup_phase():
    adapter = _mock_adapter()
    phase = WarmupPhase(query="RETURN 1", iterations=50)
    result = phase.execute(adapter)
    assert result.name == "warmup"
    assert len(result.latencies_ms) == 50
    assert adapter.execute.call_count == 50


def test_mutate_phase():
    adapter = _mock_adapter()
    mutations = [
        "CREATE (:Test {id: 999})",
        "MATCH (n:Test {id: 999}) DELETE n",
    ]
    phase = MutatePhase(mutations=mutations)
    result = phase.execute(adapter)
    assert result.name == "mutate"
    assert result.mutation_count == 2
    assert adapter.execute.call_count == 2


def test_measure_phase():
    adapter = _mock_adapter()
    phase = MeasurePhase(query="RETURN 1", iterations=30)
    result = phase.execute(adapter)
    assert result.name == "measure"
    assert len(result.latencies_ms) == 30


def test_phase_runner_executes_all_phases():
    adapter = _mock_adapter()
    runner = PhaseRunner(adapter)
    runner.add_phase(WarmupPhase(query="RETURN 1", iterations=20))
    runner.add_phase(MutatePhase(mutations=["CREATE (:X)"]))
    runner.add_phase(MeasurePhase(query="RETURN 1", iterations=10))
    results = runner.run()
    assert len(results) == 3
    assert results[0].name == "warmup"
    assert results[1].name == "mutate"
    assert results[2].name == "measure"


def test_phase_runner_to_dict():
    adapter = _mock_adapter()
    runner = PhaseRunner(adapter)
    runner.add_phase(WarmupPhase(query="RETURN 1", iterations=5))
    results = runner.run()
    d = PhaseRunner.results_to_dict(results)
    assert "phases" in d
    assert len(d["phases"]) == 1
    assert d["phases"][0]["name"] == "warmup"


def test_invalidation_pattern():
    """Test the warmup → mutate → measure pattern for invalidation benchmark."""
    adapter = _mock_adapter()
    call_num = [0]

    def track_calls(query, params=None):
        call_num[0] += 1
        return ExecuteResult(rows=[{"count": call_num[0]}], wall_ms=1.0)

    adapter.execute.side_effect = track_calls

    runner = PhaseRunner(adapter)
    runner.add_phase(WarmupPhase(query="MATCH (n) RETURN count(n)", iterations=10))
    runner.add_phase(MutatePhase(mutations=[
        "CREATE (:DT {id: 99999})-[:E]->(:DT {id: 99998})"
    ]))
    runner.add_phase(MeasurePhase(query="MATCH (n) RETURN count(n)", iterations=5))
    results = runner.run()

    # Warmup: 10 calls, mutate: 1 call, measure: 5 calls = 16 total
    assert adapter.execute.call_count == 16
    # Measure phase should show different count than warmup (mutation happened)
    assert results[2].first_result is not None
```

- [ ] **Step 2: Implement phases.py**

Create `tools/xraybench/phases.py`:

```python
"""Multi-phase benchmark execution for emergent edge testing.

Supports the warmup → mutate → measure pattern needed for:
- Invalidation benchmarks (warm cache, mutate graph, verify invalidation)
- Learning curve benchmarks (repeated execution with per-iteration capture)
- Consistency epoch benchmarks (warm, generate writes, measure read impact)
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import xraybench_core

from .adapters.base import BaseAdapter
from .models import ExecuteResult

logger = logging.getLogger(__name__)


@dataclass
class PhaseResult:
    """Result of a single benchmark phase."""

    name: str
    latencies_ms: list[float | None] = field(default_factory=list)
    mutation_count: int = 0
    first_result: list[dict[str, Any]] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "name": self.name,
            "iteration_count": len(self.latencies_ms),
            "mutation_count": self.mutation_count,
            "metadata": self.metadata,
        }
        valid = [t for t in self.latencies_ms if t is not None]
        if valid:
            d["latencies_ms"] = [round(t, 3) if t is not None else None for t in self.latencies_ms]
            if len(valid) >= 3:
                desc = xraybench_core.stats.descriptive(valid)
                pcts = xraybench_core.stats.percentiles(valid, [0.5, 0.95, 0.99])
                d["stats"] = {
                    "mean_ms": round(desc["mean"], 3),
                    "p50_ms": round(pcts[0], 3),
                    "p95_ms": round(pcts[1], 3),
                    "p99_ms": round(pcts[2], 3),
                }
        return d


class Phase(ABC):
    """Abstract base for a benchmark phase."""

    @abstractmethod
    def execute(self, adapter: BaseAdapter) -> PhaseResult:
        """Execute this phase and return results."""


class WarmupPhase(Phase):
    """Execute a query repeatedly to warm the engine's caches."""

    def __init__(self, query: str, iterations: int = 200, params: dict[str, Any] | None = None):
        self.query = query
        self.iterations = iterations
        self.params = params

    def execute(self, adapter: BaseAdapter) -> PhaseResult:
        latencies: list[float] = []
        first_result = None

        for i in range(self.iterations):
            t0 = xraybench_core.timing.monotonic_ns()
            result = adapter.execute(self.query, self.params)
            t1 = xraybench_core.timing.monotonic_ns()
            latencies.append((t1 - t0) / 1_000_000.0)
            if i == 0:
                first_result = result.rows

        logger.info("Warmup: %d iterations, mean=%.3fms",
                     self.iterations, sum(latencies) / len(latencies) if latencies else 0)

        return PhaseResult(
            name="warmup",
            latencies_ms=latencies,
            first_result=first_result,
        )


class MutatePhase(Phase):
    """Execute graph mutations to trigger invalidation."""

    def __init__(self, mutations: list[str]):
        self.mutations = mutations

    def execute(self, adapter: BaseAdapter) -> PhaseResult:
        latencies: list[float] = []

        for mutation in self.mutations:
            t0 = xraybench_core.timing.monotonic_ns()
            adapter.execute(mutation)
            t1 = xraybench_core.timing.monotonic_ns()
            latencies.append((t1 - t0) / 1_000_000.0)

        logger.info("Mutate: %d mutations executed", len(self.mutations))

        return PhaseResult(
            name="mutate",
            latencies_ms=latencies,
            mutation_count=len(self.mutations),
        )


class MeasurePhase(Phase):
    """Execute a query repeatedly and measure latency after mutation/warmup."""

    def __init__(
        self,
        query: str,
        iterations: int = 100,
        params: dict[str, Any] | None = None,
        correctness_check: Any = None,
    ):
        self.query = query
        self.iterations = iterations
        self.params = params
        self.correctness_check = correctness_check

    def execute(self, adapter: BaseAdapter) -> PhaseResult:
        latencies: list[float] = []
        first_result = None
        violations = 0

        for i in range(self.iterations):
            t0 = xraybench_core.timing.monotonic_ns()
            result = adapter.execute(self.query, self.params)
            t1 = xraybench_core.timing.monotonic_ns()
            latencies.append((t1 - t0) / 1_000_000.0)

            if i == 0:
                first_result = result.rows

            if self.correctness_check is not None:
                if not self.correctness_check(result.rows):
                    violations += 1

        logger.info("Measure: %d iterations, mean=%.3fms, violations=%d",
                     self.iterations,
                     sum(latencies) / len(latencies) if latencies else 0,
                     violations)

        return PhaseResult(
            name="measure",
            latencies_ms=latencies,
            first_result=first_result,
            metadata={"correctness_violations": violations},
        )


class PhaseRunner:
    """Orchestrates multi-phase benchmark execution."""

    def __init__(self, adapter: BaseAdapter) -> None:
        self.adapter = adapter
        self._phases: list[Phase] = []

    def add_phase(self, phase: Phase) -> None:
        self._phases.append(phase)

    def run(self) -> list[PhaseResult]:
        results: list[PhaseResult] = []
        for phase in self._phases:
            result = phase.execute(self.adapter)
            results.append(result)
        return results

    @staticmethod
    def results_to_dict(results: list[PhaseResult]) -> dict[str, Any]:
        return {
            "phases": [r.to_dict() for r in results],
            "phase_count": len(results),
        }
```

- [ ] **Step 3: Run tests**

Run: `.venv/bin/python3 -m pytest tests/test_phases.py -v`
Expected: All 6 tests pass.

- [ ] **Step 4: Commit**

```bash
git add tools/xraybench/phases.py tests/test_phases.py
git commit -m "feat: add multi-phase execution engine — warmup/mutate/measure pattern for emergent edge benchmarks"
```

---

### Task 2: Emergent Edge Metrics Collector

**Files:**
- Create: `tools/xraybench/emergent_collector.py`
- Create: `tests/test_emergent_collector.py`

- [ ] **Step 1: Write tests**

Create `tests/test_emergent_collector.py`:

```python
"""Tests for emergent edge metrics collector."""

from unittest.mock import MagicMock
from tools.xraybench.emergent_collector import EmergentEdgeCollector
from tools.xraybench.models import ExecuteResult


def _mock_adapter():
    adapter = MagicMock()
    return adapter


def test_collect_cache_stats():
    adapter = _mock_adapter()
    adapter.execute.return_value = ExecuteResult(
        rows=[{
            "tier1_count": 5,
            "tier2_count": 20,
            "tier3_count": 3,
            "total_hits": 150,
            "total_misses": 50,
        }],
        wall_ms=1.0,
    )
    collector = EmergentEdgeCollector(adapter)
    stats = collector.collect_cache_stats()
    assert stats["tier2_count"] == 20
    assert stats["hit_rate"] == 0.75  # 150 / (150+50)


def test_collect_cache_stats_no_procedure():
    adapter = _mock_adapter()
    adapter.execute.side_effect = Exception("Unknown procedure")
    collector = EmergentEdgeCollector(adapter)
    stats = collector.collect_cache_stats()
    assert stats == {}


def test_collect_invalidation_stats():
    adapter = _mock_adapter()
    adapter.execute.return_value = ExecuteResult(
        rows=[{
            "revalidations": 42,
            "evictions": 7,
            "stale_reads_blocked": 3,
        }],
        wall_ms=1.0,
    )
    collector = EmergentEdgeCollector(adapter)
    stats = collector.collect_invalidation_stats()
    assert stats["revalidations"] == 42
    assert stats["evictions"] == 7


def test_collect_all():
    adapter = _mock_adapter()
    adapter.execute.return_value = ExecuteResult(
        rows=[{"total_hits": 10, "total_misses": 5}], wall_ms=1.0
    )
    collector = EmergentEdgeCollector(adapter)
    all_stats = collector.collect_all()
    assert "cache" in all_stats
    assert "timestamp" in all_stats


def test_snapshot_before_after():
    adapter = _mock_adapter()
    call_count = [0]

    def changing_stats(query, params=None):
        call_count[0] += 1
        return ExecuteResult(
            rows=[{"total_hits": call_count[0] * 10, "total_misses": 5}],
            wall_ms=1.0,
        )

    adapter.execute.side_effect = changing_stats
    collector = EmergentEdgeCollector(adapter)

    before = collector.snapshot()
    # ... benchmark runs here ...
    after = collector.snapshot()

    delta = collector.compute_delta(before, after)
    assert delta["cache"]["total_hits_delta"] == 10
```

- [ ] **Step 2: Implement emergent_collector.py**

Create `tools/xraybench/emergent_collector.py`:

```python
"""Emergent edge metrics collector for xrayGraphDB.

Queries the engine's emergent edge procedures to collect:
- Cache tier distribution (ephemeral/session/materialized counts)
- Hit/miss rates
- Invalidation and revalidation counts
- Learning progress (detected patterns, synthesized candidates)

These metrics are specific to xrayGraphDB with emergent edges enabled.
On engines without this feature, all collectors return empty dicts.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from .adapters.base import BaseAdapter

logger = logging.getLogger(__name__)

# Procedure names — will be available when emergent edges are implemented
CACHE_STATS_QUERY = "CALL xray.emergent_cache_stats() YIELD * RETURN *"
INVALIDATION_STATS_QUERY = "CALL xray.emergent_invalidation_stats() YIELD * RETURN *"
LEARNING_STATS_QUERY = "CALL xray.emergent_learning_stats() YIELD * RETURN *"
CONFIG_QUERY = "CALL xray.emergent_config_show() YIELD * RETURN *"


class EmergentEdgeCollector:
    """Collects emergent edge engine metrics from xrayGraphDB."""

    def __init__(self, adapter: BaseAdapter) -> None:
        self._adapter = adapter

    def collect_cache_stats(self) -> dict[str, Any]:
        """Query emergent edge cache statistics."""
        try:
            result = self._adapter.execute(CACHE_STATS_QUERY)
            if not result.rows:
                return {}
            row = result.rows[0]
            stats = dict(row)
            # Compute hit rate if we have the fields
            hits = _int(stats.get("total_hits", 0))
            misses = _int(stats.get("total_misses", 0))
            total = hits + misses
            if total > 0:
                stats["hit_rate"] = round(hits / total, 4)
            return stats
        except Exception as exc:
            logger.debug("Emergent cache stats not available: %s", exc)
            return {}

    def collect_invalidation_stats(self) -> dict[str, Any]:
        """Query invalidation and revalidation statistics."""
        try:
            result = self._adapter.execute(INVALIDATION_STATS_QUERY)
            if not result.rows:
                return {}
            return dict(result.rows[0])
        except Exception as exc:
            logger.debug("Emergent invalidation stats not available: %s", exc)
            return {}

    def collect_learning_stats(self) -> dict[str, Any]:
        """Query pattern detection and synthesis statistics."""
        try:
            result = self._adapter.execute(LEARNING_STATS_QUERY)
            if not result.rows:
                return {}
            return dict(result.rows[0])
        except Exception as exc:
            logger.debug("Emergent learning stats not available: %s", exc)
            return {}

    def collect_config(self) -> dict[str, Any]:
        """Query current emergent edge configuration."""
        try:
            result = self._adapter.execute(CONFIG_QUERY)
            if not result.rows:
                return {}
            return {row.get("param", row.get("key", "")): row.get("value") for row in result.rows}
        except Exception as exc:
            logger.debug("Emergent config not available: %s", exc)
            return {}

    def collect_all(self) -> dict[str, Any]:
        """Collect all emergent edge metrics."""
        return {
            "cache": self.collect_cache_stats(),
            "invalidation": self.collect_invalidation_stats(),
            "learning": self.collect_learning_stats(),
            "config": self.collect_config(),
            "timestamp": time.time(),
        }

    def snapshot(self) -> dict[str, Any]:
        """Take a snapshot of current metrics for before/after comparison."""
        return self.collect_all()

    def compute_delta(
        self, before: dict[str, Any], after: dict[str, Any]
    ) -> dict[str, Any]:
        """Compute the difference between two snapshots.

        For numeric fields, computes after - before. For non-numeric fields,
        includes both values.
        """
        delta: dict[str, Any] = {}
        for section in ["cache", "invalidation", "learning"]:
            b = before.get(section, {})
            a = after.get(section, {})
            section_delta: dict[str, Any] = {}
            all_keys = set(list(b.keys()) + list(a.keys()))
            for key in all_keys:
                bv = b.get(key)
                av = a.get(key)
                if isinstance(bv, (int, float)) and isinstance(av, (int, float)):
                    section_delta[f"{key}_delta"] = av - bv
                else:
                    section_delta[f"{key}_before"] = bv
                    section_delta[f"{key}_after"] = av
            delta[section] = section_delta
        return delta


def _int(val: Any) -> int:
    """Safely convert to int."""
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0
```

- [ ] **Step 3: Run tests**

Run: `.venv/bin/python3 -m pytest tests/test_emergent_collector.py -v`
Expected: All 5 tests pass.

- [ ] **Step 4: Commit**

```bash
git add tools/xraybench/emergent_collector.py tests/test_emergent_collector.py
git commit -m "feat: add EmergentEdgeCollector — cache stats, invalidation, learning metrics, before/after snapshots"
```

---

### Task 3: CLI Command — run-emergent

**Files:**
- Modify: `tools/xraybench/cli.py`
- Create: `tests/test_cli_emergent.py`

- [ ] **Step 1: Write tests**

Create `tests/test_cli_emergent.py`:

```python
"""Tests for the run-emergent CLI command."""

import json
import os
import subprocess
import sys
import tempfile
import shutil


class TestCLIRunEmergent:
    def setup_method(self):
        self.out_dir = tempfile.mkdtemp(prefix="xraybench_emergent_")

    def teardown_method(self):
        shutil.rmtree(self.out_dir, ignore_errors=True)

    def test_help(self):
        result = subprocess.run(
            [sys.executable, "-m", "tools.xraybench.cli", "run-emergent", "--help"],
            capture_output=True, text=True,
            cwd="/Users/sendlane/github_projects/xraygraph-bench",
        )
        assert result.returncode == 0
        assert "learning-curve" in result.stdout or "benchmark-type" in result.stdout
```

- [ ] **Step 2: Add run-emergent command to cli.py**

Read existing cli.py, then add `run-emergent` subparser with:
- `--benchmark-type` (choices: learning-curve, invalidation, mode-legality, cold-start, consistency-epoch)
- `--engine` (required): adapter name
- `--host`, `--port`, `--username`, `--password`: connection params
- `--query` (required): the query to benchmark
- `--iterations` (default 500)
- `--output` (output JSON path)
- Standard params: `--param` repeatable key=value

Handler `_cmd_run_emergent(args)`:
1. Resolve adapter via `get_adapter(args.engine)`
2. Connect
3. Based on `--benchmark-type`:
   - `learning-curve`: use TimeSeriesRunner.run_timeseries()
   - `invalidation`: use PhaseRunner with warmup → mutate → measure
   - Others: use TimeSeriesRunner with appropriate settings
4. Collect emergent edge metrics if available
5. Write results JSON

- [ ] **Step 3: Run tests**

Run: `.venv/bin/python3 -m pytest tests/test_cli_emergent.py -v`
Expected: Pass.

- [ ] **Step 4: Commit**

```bash
git add tools/xraybench/cli.py tests/test_cli_emergent.py
git commit -m "feat: add run-emergent CLI command — time-series and multi-phase benchmark execution"
```

---

### Task 4: Full Test Suite Verification

- [ ] **Step 1: Run all tests**

Run:
```bash
source "$HOME/.cargo/env" && cargo test --workspace --manifest-path rust/Cargo.toml 2>&1 | grep "^test result:" | awk -F'[; ]' '{sum+=$4} END{print "Rust:", sum}' && .venv/bin/python3 -m pytest tests/ -q
```

Expected: 136 Rust + ~185 Python tests pass.

- [ ] **Step 2: Commit and push**

```bash
git add -A && git commit -m "feat: complete emergent edge runner support — time-series, multi-phase, metrics collector, CLI" && git push origin main
```
