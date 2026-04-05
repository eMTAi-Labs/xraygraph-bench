"""Tests for TimeSeriesRunner — per-iteration latency capture.

Run with: .venv/bin/python3 -m pytest tests/test_timeseries.py -v
"""

from __future__ import annotations

from unittest.mock import MagicMock

from tools.xraybench.adapters.base import BaseAdapter
from tools.xraybench.adapters.capabilities import CacheClearReport
from tools.xraybench.models import ExecuteResult
from tools.xraybench.timeseries import TimeSeriesResult, TimeSeriesRunner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_adapter(wall_ms: float = 1.0) -> MagicMock:
    """Return a mock BaseAdapter whose execute() returns a fixed ExecuteResult."""
    adapter = MagicMock(spec=BaseAdapter)
    adapter.execute.return_value = ExecuteResult(rows=[{"x": 1}], wall_ms=wall_ms)
    adapter.clear_caches.return_value = CacheClearReport(cleared=True)
    return adapter


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestTimeSeriesCapturesPerIteration:
    """test_timeseries_captures_per_iteration: all 50 latencies >= 0."""

    def test_timeseries_captures_per_iteration(self):
        adapter = _make_adapter()
        runner = TimeSeriesRunner(adapter)
        result = runner.run_timeseries("MATCH (n) RETURN n", iterations=50)

        assert isinstance(result, TimeSeriesResult)
        assert result.iterations == 50
        assert len(result.latencies_ms) == 50
        # No timeouts expected; all latencies should be non-None and >= 0
        for lat in result.latencies_ms:
            assert lat is not None
            assert lat >= 0.0


class TestTimeSeriesDetectsAcceleration:
    """test_timeseries_detects_acceleration: last-20 mean < first-20 mean * 0.8."""

    def test_timeseries_detects_acceleration(self):
        # Slow for first 20 calls, fast for remaining 80
        call_count = 0
        slow_result = ExecuteResult(rows=[{"x": 1}], wall_ms=100.0)
        fast_result = ExecuteResult(rows=[{"x": 1}], wall_ms=1.0)

        adapter = MagicMock(spec=BaseAdapter)
        adapter.clear_caches.return_value = CacheClearReport(cleared=True)

        def side_effect(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 20:
                return slow_result
            return fast_result

        adapter.execute.side_effect = side_effect

        runner = TimeSeriesRunner(adapter)
        result = runner.run_timeseries("MATCH (n) RETURN n", iterations=100)

        real = [v for v in result.latencies_ms if v is not None]
        assert len(real) == 100

        first_20_mean = sum(real[:20]) / 20
        last_20_mean = sum(real[-20:]) / 20

        assert last_20_mean < first_20_mean * 0.8


class TestTimeSeriesCorrectnessPasses:
    """test_timeseries_correctness_check: check always True → violations == 0."""

    def test_timeseries_correctness_check(self):
        adapter = _make_adapter()
        runner = TimeSeriesRunner(adapter)
        result = runner.run_timeseries(
            "MATCH (n) RETURN n",
            iterations=20,
            correctness_check=lambda rows: True,
        )

        assert result.correctness_violations == 0


class TestTimeSeriesDetectsCorrectnessViolations:
    """test_timeseries_detects_correctness_violation: check always False → violations == iterations."""

    def test_timeseries_detects_correctness_violation(self):
        adapter = _make_adapter()
        runner = TimeSeriesRunner(adapter)
        iterations = 30
        result = runner.run_timeseries(
            "MATCH (n) RETURN n",
            iterations=iterations,
            correctness_check=lambda rows: False,
        )

        assert result.correctness_violations == iterations


class TestTimeSeriesTracksTimeouts:
    """test_timeseries_tracks_timeouts: very short timeout_ms causes timeouts."""

    def test_timeseries_tracks_timeouts(self):
        adapter = _make_adapter()
        runner = TimeSeriesRunner(adapter)

        # Use an extremely small timeout that real execution will exceed
        result = runner.run_timeseries(
            "MATCH (n) RETURN n",
            iterations=20,
            timeout_ms=0.0001,  # 0.1 microsecond — impossible to meet
        )

        # Every iteration should have timed out
        assert result.timeout_count > 0
        timed_out_slots = [v for v in result.latencies_ms if v is None]
        assert len(timed_out_slots) == result.timeout_count


class TestTimeSeriesResultToDict:
    """test_timeseries_result_to_dict: dict has iterations, latencies_ms, stats.p50_ms."""

    def test_timeseries_result_to_dict(self):
        adapter = _make_adapter()
        runner = TimeSeriesRunner(adapter)
        result = runner.run_timeseries("MATCH (n) RETURN n", iterations=50)

        d = result.to_dict()

        assert "iterations" in d
        assert d["iterations"] == 50
        assert "latencies_ms" in d
        assert len(d["latencies_ms"]) == 50
        assert "stats" in d
        assert "p50_ms" in d["stats"]
        assert d["stats"]["p50_ms"] >= 0.0


class TestTimeSeriesComputesAccelerationPoint:
    """test_timeseries_computes_acceleration_point: [10.0]*50+[2.0]*50 → point in 40-60."""

    def test_timeseries_computes_acceleration_point(self):
        # Hand-craft the result directly without running an adapter
        latencies: list[float | None] = [10.0] * 50 + [2.0] * 50
        result = TimeSeriesResult(
            query="MATCH (n) RETURN n",
            iterations=100,
            latencies_ms=latencies,
            timeout_count=0,
            correctness_violations=0,
            first_result=None,
        )

        point = result.acceleration_point(window=10, threshold=0.5)

        assert point is not None, "acceleration_point should detect a step change"
        assert 40 <= point <= 60, (
            f"Expected acceleration point between 40 and 60, got {point}"
        )
