"""Multi-phase execution engine for emergent edge benchmarks.

Provides warmup/mutate/measure patterns for xraygraph-bench.
"""

from __future__ import annotations

import statistics
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable

from xraybench_core import timing as rust_timing

from tools.xraybench.adapters.base import BaseAdapter
from tools.xraybench.models import ExecuteResult


# ---------------------------------------------------------------------------
# PhaseResult
# ---------------------------------------------------------------------------


@dataclass
class PhaseResult:
    """Result produced by a single benchmark phase."""

    name: str
    latencies_ms: list[float | None] = field(default_factory=list)
    mutation_count: int = 0
    first_result: list[dict[str, Any]] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary with computed stats when enough data exists."""
        valid: list[float] = [x for x in self.latencies_ms if x is not None]

        d: dict[str, Any] = {
            "name": self.name,
            "latencies_ms": self.latencies_ms,
            "mutation_count": self.mutation_count,
            "metadata": self.metadata,
        }

        if self.first_result is not None:
            d["first_result"] = self.first_result

        if len(valid) >= 2:
            d["stats"] = {
                "count": len(valid),
                "mean_ms": statistics.mean(valid),
                "median_ms": statistics.median(valid),
                "stdev_ms": statistics.stdev(valid),
                "min_ms": min(valid),
                "max_ms": max(valid),
            }
        elif len(valid) == 1:
            d["stats"] = {
                "count": 1,
                "mean_ms": valid[0],
                "median_ms": valid[0],
                "min_ms": valid[0],
                "max_ms": valid[0],
            }

        return d


# ---------------------------------------------------------------------------
# Phase ABC
# ---------------------------------------------------------------------------


class Phase(ABC):
    """Abstract base class for a single benchmark phase."""

    @abstractmethod
    def execute(self, adapter: BaseAdapter) -> PhaseResult:
        """Execute this phase against the given adapter.

        Args:
            adapter: The engine adapter to execute against.

        Returns:
            PhaseResult containing timing data and metadata.
        """


# ---------------------------------------------------------------------------
# Concrete phases
# ---------------------------------------------------------------------------


class WarmupPhase(Phase):
    """Runs a query N times to warm up engine caches.

    Collects per-iteration latency via the Rust timing core.
    """

    def __init__(
        self,
        query: str,
        iterations: int = 200,
        params: dict[str, Any] | None = None,
    ) -> None:
        self.query = query
        self.iterations = iterations
        self.params = params

    def execute(self, adapter: BaseAdapter) -> PhaseResult:
        latencies: list[float | None] = []
        first_result: list[dict[str, Any]] | None = None

        for i in range(self.iterations):
            t0 = rust_timing.monotonic_ns()
            result: ExecuteResult = adapter.execute(self.query, self.params)
            t1 = rust_timing.monotonic_ns()
            elapsed_ms = (t1 - t0) / 1_000_000.0
            latencies.append(elapsed_ms)
            if i == 0:
                first_result = list(result.rows)

        return PhaseResult(
            name="warmup",
            latencies_ms=latencies,
            first_result=first_result,
            metadata={"iterations": self.iterations},
        )


class MutatePhase(Phase):
    """Executes a list of mutation queries (e.g. CREATE, DELETE, UPDATE).

    Tracks how many mutations were applied.
    """

    def __init__(self, mutations: list[str]) -> None:
        self.mutations = mutations

    def execute(self, adapter: BaseAdapter) -> PhaseResult:
        latencies: list[float | None] = []
        first_result: list[dict[str, Any]] | None = None

        for i, mutation in enumerate(self.mutations):
            t0 = rust_timing.monotonic_ns()
            result: ExecuteResult = adapter.execute(mutation)
            t1 = rust_timing.monotonic_ns()
            elapsed_ms = (t1 - t0) / 1_000_000.0
            latencies.append(elapsed_ms)
            if i == 0:
                first_result = list(result.rows)

        return PhaseResult(
            name="mutate",
            latencies_ms=latencies,
            mutation_count=len(self.mutations),
            first_result=first_result,
            metadata={"mutation_count": len(self.mutations)},
        )


class MeasurePhase(Phase):
    """Runs a query N times with fenced timing and optional correctness check.

    This is the primary measurement phase — results are used for
    performance reporting.
    """

    def __init__(
        self,
        query: str,
        iterations: int = 100,
        params: dict[str, Any] | None = None,
        correctness_check: Callable[[ExecuteResult], bool] | None = None,
    ) -> None:
        self.query = query
        self.iterations = iterations
        self.params = params
        self.correctness_check = correctness_check

    def execute(self, adapter: BaseAdapter) -> PhaseResult:
        latencies: list[float | None] = []
        correctness_failures = 0
        first_result: list[dict[str, Any]] | None = None

        for i in range(self.iterations):
            t0 = rust_timing.monotonic_ns()
            result: ExecuteResult = adapter.execute(self.query, self.params)
            t1 = rust_timing.monotonic_ns()
            elapsed_ms = (t1 - t0) / 1_000_000.0
            latencies.append(elapsed_ms)

            if i == 0:
                first_result = list(result.rows)

            if self.correctness_check is not None:
                if not self.correctness_check(result):
                    correctness_failures += 1

        meta: dict[str, Any] = {"iterations": self.iterations}
        if self.correctness_check is not None:
            meta["correctness_failures"] = correctness_failures

        return PhaseResult(
            name="measure",
            latencies_ms=latencies,
            first_result=first_result,
            metadata=meta,
        )


# ---------------------------------------------------------------------------
# PhaseRunner orchestrator
# ---------------------------------------------------------------------------


class PhaseRunner:
    """Orchestrates an ordered sequence of phases against a single adapter."""

    def __init__(self, adapter: BaseAdapter) -> None:
        self.adapter = adapter
        self._phases: list[Phase] = []

    def add_phase(self, phase: Phase) -> None:
        """Append a phase to the execution sequence."""
        self._phases.append(phase)

    def run(self) -> list[PhaseResult]:
        """Execute all phases in order and return their results."""
        results: list[PhaseResult] = []
        for phase in self._phases:
            result = phase.execute(self.adapter)
            results.append(result)
        return results

    @staticmethod
    def results_to_dict(results: list[PhaseResult]) -> dict[str, Any]:
        """Serialize a list of PhaseResults to a dictionary.

        Returns a dict with a ``phases`` key containing the serialized
        list.
        """
        return {"phases": [r.to_dict() for r in results]}
