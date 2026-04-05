"""Tests for EmergentEdgeCollector.

All tests use a mock adapter — no running xrayGraphDB instance required.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from tools.xraybench.adapters.base import BaseAdapter
from tools.xraybench.emergent_collector import EmergentEdgeCollector
from tools.xraybench.models import ExecuteResult


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _make_adapter(rows: list[dict] | None = None, raise_exc: bool = False) -> MagicMock:
    """Return a mock adapter whose execute() either raises or returns rows."""
    adapter = MagicMock(spec=BaseAdapter)
    if raise_exc:
        adapter.execute.side_effect = Exception("procedure does not exist")
    else:
        adapter.execute.return_value = ExecuteResult(
            rows=rows or [],
            wall_ms=0.5,
        )
    return adapter


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCollectCacheStats:
    """test_collect_cache_stats — verify hit_rate is computed correctly."""

    def test_collect_cache_stats(self) -> None:
        rows = [
            {
                "tier1_count": 5,
                "tier2_count": 20,
                "tier3_count": 3,
                "total_hits": 150,
                "total_misses": 50,
            }
        ]
        adapter = _make_adapter(rows=rows)
        collector = EmergentEdgeCollector(adapter)
        stats = collector.collect_cache_stats()

        assert stats["tier1_count"] == 5
        assert stats["tier2_count"] == 20
        assert stats["tier3_count"] == 3
        assert stats["total_hits"] == 150
        assert stats["total_misses"] == 50
        assert stats["hit_rate"] == 0.75


class TestCollectCacheStatsNoProcedure:
    """test_collect_cache_stats_no_procedure — returns {} when execute raises."""

    def test_collect_cache_stats_no_procedure(self) -> None:
        adapter = _make_adapter(raise_exc=True)
        collector = EmergentEdgeCollector(adapter)
        stats = collector.collect_cache_stats()

        assert stats == {}


class TestCollectInvalidationStats:
    """test_collect_invalidation_stats — verify returned values match mock."""

    def test_collect_invalidation_stats(self) -> None:
        rows = [
            {
                "revalidations": 42,
                "evictions": 7,
                "stale_reads_blocked": 3,
            }
        ]
        adapter = _make_adapter(rows=rows)
        collector = EmergentEdgeCollector(adapter)
        stats = collector.collect_invalidation_stats()

        assert stats["revalidations"] == 42
        assert stats["evictions"] == 7
        assert stats["stale_reads_blocked"] == 3


class TestCollectAll:
    """test_collect_all — verify snapshot has cache and timestamp keys."""

    def test_collect_all(self) -> None:
        rows = [{"total_hits": 10, "total_misses": 0}]
        adapter = _make_adapter(rows=rows)
        collector = EmergentEdgeCollector(adapter)
        result = collector.collect_all()

        assert "cache" in result
        assert "timestamp" in result
        assert isinstance(result["timestamp"], float)
        assert result["timestamp"] > 0


class TestSnapshotBeforeAfter:
    """test_snapshot_before_after — verify compute_delta reports correct deltas."""

    def test_snapshot_before_after(self) -> None:
        adapter = MagicMock(spec=BaseAdapter)

        # First snapshot: total_hits=10
        first_result = ExecuteResult(
            rows=[{"total_hits": 10, "total_misses": 5}],
            wall_ms=0.5,
        )
        # Second snapshot: total_hits=20
        second_result = ExecuteResult(
            rows=[{"total_hits": 20, "total_misses": 5}],
            wall_ms=0.5,
        )

        collector = EmergentEdgeCollector(adapter)

        # Patch execute to return first_result for all queries in before snapshot
        adapter.execute.return_value = first_result
        before = collector.snapshot()

        # Patch execute to return second_result for all queries in after snapshot
        adapter.execute.return_value = second_result
        after = collector.snapshot()

        delta = collector.compute_delta(before, after)

        assert "cache" in delta
        assert delta["cache"]["total_hits_delta"] == 10.0
        assert delta["cache"]["total_misses_delta"] == 0.0
