"""Tests for the multi-phase execution engine (warmup/mutate/measure).

Uses a mock adapter — no live engine required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from tools.xraybench.adapters.base import BaseAdapter
from tools.xraybench.adapters.capabilities import CacheClearReport
from tools.xraybench.models import ExecuteResult
from tools.xraybench.phases import (
    MeasurePhase,
    MutatePhase,
    PhaseResult,
    PhaseRunner,
    WarmupPhase,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_adapter() -> MagicMock:
    """Return a mock adapter whose execute() yields a minimal ExecuteResult."""
    adapter = MagicMock(spec=BaseAdapter)
    adapter.execute.return_value = ExecuteResult(
        rows=[{"count": 42}],
        wall_ms=1.0,
    )
    adapter.clear_caches.return_value = CacheClearReport(cleared=True)
    return adapter


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestWarmupPhase:
    """test_warmup_phase: 50 iterations, correct name and call count."""

    def test_warmup_phase(self) -> None:
        adapter = _make_mock_adapter()
        phase = WarmupPhase(query="MATCH (n) RETURN count(n)", iterations=50)
        result = phase.execute(adapter)

        assert result.name == "warmup"
        assert len(result.latencies_ms) == 50
        assert all(lat is not None and lat >= 0.0 for lat in result.latencies_ms)
        assert adapter.execute.call_count == 50


class TestMutatePhase:
    """test_mutate_phase: 2 mutations, correct name, mutation_count, call count."""

    def test_mutate_phase(self) -> None:
        adapter = _make_mock_adapter()
        mutations = [
            "CREATE (:Node {id: 1})",
            "CREATE (:Node {id: 2})",
        ]
        phase = MutatePhase(mutations=mutations)
        result = phase.execute(adapter)

        assert result.name == "mutate"
        assert result.mutation_count == 2
        assert adapter.execute.call_count == 2


class TestMeasurePhase:
    """test_measure_phase: 30 iterations, correct name and latency count."""

    def test_measure_phase(self) -> None:
        adapter = _make_mock_adapter()
        phase = MeasurePhase(query="MATCH (n) RETURN count(n)", iterations=30)
        result = phase.execute(adapter)

        assert result.name == "measure"
        assert len(result.latencies_ms) == 30
        assert all(lat is not None and lat >= 0.0 for lat in result.latencies_ms)


class TestPhaseRunnerExecutesAllPhases:
    """test_phase_runner_executes_all_phases: warmup+mutate+measure in order."""

    def test_phase_runner_executes_all_phases(self) -> None:
        adapter = _make_mock_adapter()
        runner = PhaseRunner(adapter)
        runner.add_phase(WarmupPhase(query="MATCH (n) RETURN count(n)", iterations=20))
        runner.add_phase(MutatePhase(mutations=["CREATE (:Node {id: 99})"]))
        runner.add_phase(MeasurePhase(query="MATCH (n) RETURN count(n)", iterations=10))

        results = runner.run()

        assert len(results) == 3
        assert results[0].name == "warmup"
        assert results[1].name == "mutate"
        assert results[2].name == "measure"


class TestPhaseRunnerToDict:
    """test_phase_runner_to_dict: dict has 'phases' list with correct structure."""

    def test_phase_runner_to_dict(self) -> None:
        adapter = _make_mock_adapter()
        runner = PhaseRunner(adapter)
        runner.add_phase(WarmupPhase(query="MATCH (n) RETURN 1", iterations=5))
        runner.add_phase(MutatePhase(mutations=["CREATE (:X)"]))
        runner.add_phase(MeasurePhase(query="MATCH (n) RETURN 1", iterations=3))

        results = runner.run()
        d = PhaseRunner.results_to_dict(results)

        assert "phases" in d
        assert isinstance(d["phases"], list)
        assert len(d["phases"]) == 3

        names = [p["name"] for p in d["phases"]]
        assert names == ["warmup", "mutate", "measure"]

        # mutate entry must carry mutation_count
        mutate_entry = d["phases"][1]
        assert mutate_entry["mutation_count"] == 1


class TestInvalidationPattern:
    """test_invalidation_pattern: warmup(10)+mutate(1)+measure(5) == 16 execute calls."""

    def test_invalidation_pattern(self) -> None:
        adapter = _make_mock_adapter()
        runner = PhaseRunner(adapter)
        runner.add_phase(WarmupPhase(query="MATCH (n) RETURN count(n)", iterations=10))
        runner.add_phase(MutatePhase(mutations=["CREATE (:Node {id: 999})"]))
        runner.add_phase(MeasurePhase(query="MATCH (n) RETURN count(n)", iterations=5))

        runner.run()

        # 10 (warmup) + 1 (mutate) + 5 (measure) = 16
        assert adapter.execute.call_count == 16
