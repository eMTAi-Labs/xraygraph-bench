"""Tests for the ingestion benchmark runner.

Uses mock adapters so no running engine is required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, call
from typing import Any

from tools.xraybench.ingest_runner import IngestionResult, IngestionRunner
from tools.xraybench.models import ExecuteResult
from tools.xraybench.adapters.base import BaseAdapter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_adapter(
    verify_count: int = 100,
) -> MagicMock:
    """Create a mock adapter that returns an ExecuteResult from execute()."""
    adapter = MagicMock(spec=BaseAdapter)

    # Default execute() returns a count result matching verify_count
    adapter.execute.return_value = ExecuteResult(
        rows=[{"c": verify_count}],
        wall_ms=1.0,
    )
    return adapter


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestNodeCreationReturnsResult:
    """Verify benchmark_node_creation returns a properly populated IngestionResult."""

    def test_node_creation_returns_result(self) -> None:
        adapter = _make_mock_adapter(verify_count=100)
        runner = IngestionRunner(adapter)

        result = runner.benchmark_node_creation(
            node_count=100, batch_size=50, property_count=3, seed=1
        )

        assert isinstance(result, IngestionResult)
        assert result.benchmark == "bulk-node-create"
        assert result.node_count == 100
        assert result.edge_count == 0
        assert result.property_count == 3
        assert result.batch_size == 50
        assert result.total_ms >= 0
        assert result.nodes_per_sec >= 0
        assert result.edges_per_sec == 0
        assert result.metadata["expected_nodes"] == 100
        assert result.metadata["verified"] is True


class TestNodeCreationCallsExecute:
    """Verify batched CREATE calls are issued to the adapter."""

    def test_node_creation_calls_execute(self) -> None:
        adapter = _make_mock_adapter(verify_count=10)
        runner = IngestionRunner(adapter)

        runner.benchmark_node_creation(
            node_count=10, batch_size=5, property_count=2, seed=0
        )

        # 1 DELETE + 2 CREATE batches (5+5) + 1 COUNT verify = 4 calls
        assert adapter.execute.call_count == 4

        # First call is the cleanup DELETE
        first_call_query = adapter.execute.call_args_list[0][0][0]
        assert "DETACH DELETE" in first_call_query

        # Second and third calls are CREATE batches
        second_call_query = adapter.execute.call_args_list[1][0][0]
        assert second_call_query.startswith("CREATE")

        third_call_query = adapter.execute.call_args_list[2][0][0]
        assert third_call_query.startswith("CREATE")

        # Last call is the verification COUNT
        last_call_query = adapter.execute.call_args_list[3][0][0]
        assert "count(n)" in last_call_query


class TestIndexBuildReturnsMs:
    """Verify benchmark_index_build returns a non-negative float."""

    def test_index_build_returns_ms(self) -> None:
        adapter = _make_mock_adapter()
        runner = IngestionRunner(adapter)

        ms = runner.benchmark_index_build(label="TestLabel", property_name="pk")

        assert isinstance(ms, float)
        assert ms >= 0

        # Verify the correct CREATE INDEX query was issued
        adapter.execute.assert_called_once()
        query = adapter.execute.call_args[0][0]
        assert "CREATE INDEX" in query
        assert "TestLabel" in query
        assert "pk" in query


class TestMixedIngestCapturesReads:
    """Verify mixed_ingest captures read latencies at the configured interval."""

    def test_mixed_ingest_captures_reads(self) -> None:
        adapter = _make_mock_adapter()
        runner = IngestionRunner(adapter)

        # 5 batches of 2 nodes each, read_interval=1 means read after every batch
        result = runner.benchmark_mixed_ingest(
            node_count=10, batch_size=2, read_interval=1
        )

        assert isinstance(result, IngestionResult)
        assert result.benchmark == "mixed-ingest"
        assert result.node_count == 10
        assert len(result.read_latencies_ms) == 5
        for lat in result.read_latencies_ms:
            assert isinstance(lat, float)
            assert lat >= 0


class TestResultToDict:
    """Verify IngestionResult.to_dict() serialization."""

    def test_result_to_dict(self) -> None:
        result = IngestionResult(
            benchmark="test-bench",
            node_count=500,
            edge_count=100,
            property_count=4,
            batch_size=50,
            total_ms=1234.567,
            nodes_per_sec=405.2,
            edges_per_sec=81.04,
            index_build_ms=23.456,
            read_latencies_ms=[1.0, 2.0, 3.0, 4.0, 5.0],
            metadata={"key": "value"},
        )

        d = result.to_dict()

        assert d["benchmark"] == "test-bench"
        assert d["node_count"] == 500
        assert d["edge_count"] == 100
        assert d["property_count"] == 4
        assert d["batch_size"] == 50
        assert d["total_ms"] == 1234.57
        assert d["nodes_per_sec"] == 405.2
        assert d["edges_per_sec"] == 81.0
        assert d["index_build_ms"] == 23.46

        # read_latency stats should be present (5 values >= 3)
        assert "read_latency" in d
        rl = d["read_latency"]
        assert "p50_ms" in rl
        assert "p95_ms" in rl
        assert "p99_ms" in rl
        assert "mean_ms" in rl

        assert d["metadata"] == {"key": "value"}

    def test_result_to_dict_minimal(self) -> None:
        """Minimal result without optional fields."""
        result = IngestionResult(
            benchmark="minimal",
            node_count=10,
            edge_count=0,
            property_count=1,
            batch_size=10,
            total_ms=5.0,
            nodes_per_sec=2000.0,
            edges_per_sec=0.0,
        )

        d = result.to_dict()

        assert d["benchmark"] == "minimal"
        assert "index_build_ms" not in d
        assert "read_latency" not in d
        assert "metadata" not in d
