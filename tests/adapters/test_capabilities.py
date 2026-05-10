"""Tests for tools.xraybench.adapters.capabilities."""

from __future__ import annotations

import pytest

from tools.xraybench.adapters.capabilities import (
    CacheClearReport,
    Capability,
    ConnectionInfo,
    EngineInfo,
    EngineState,
    HealthStatus,
    LoadReport,
    Outcome,
    ProfileResult,
    QueryPlan,
)


# ---------------------------------------------------------------------------
# Test 1: Capability enum values
# ---------------------------------------------------------------------------

def test_capability_enum_values() -> None:
    assert Capability.COMPILE_TIME_REPORTING.value == "compile_time"
    assert Capability.PLAN_PROFILING.value == "plan_profile"
    assert Capability.CACHE_CLEAR.value == "cache_clear"
    assert Capability.VECTORIZED_METRICS.value == "vectorized_metrics"
    assert Capability.STREAMING_RESULTS.value == "streaming_results"
    assert Capability.NATIVE_PROTOCOL.value == "native_protocol"
    assert Capability.EXPLAIN_ANALYZE.value == "explain_analyze"
    assert Capability.MEMORY_REPORTING.value == "memory_reporting"
    assert Capability.GFQL.value == "gfql"
    assert len(Capability) == 9


# ---------------------------------------------------------------------------
# Test 2: Outcome enum values
# ---------------------------------------------------------------------------

def test_outcome_enum_values() -> None:
    assert Outcome.SUCCESS.value == "success"
    assert Outcome.CORRECTNESS_MISMATCH.value == "correctness_mismatch"
    assert Outcome.ENGINE_ERROR.value == "engine_error"
    assert Outcome.TIMEOUT.value == "timeout"
    assert Outcome.UNSUPPORTED.value == "unsupported"
    assert Outcome.DATASET_VERIFICATION_FAILED.value == "dataset_verification_failed"
    assert Outcome.HARNESS_FAILURE.value == "harness_failure"
    assert Outcome.CONNECTION_FAILURE.value == "connection_failure"
    assert Outcome.OUT_OF_MEMORY.value == "out_of_memory"
    assert len(Outcome) == 9


# ---------------------------------------------------------------------------
# Test 3: ConnectionInfo creation and to_dict
# ---------------------------------------------------------------------------

def test_connection_info_creation_and_to_dict() -> None:
    info = ConnectionInfo(
        host="localhost",
        port=7687,
        protocol="bolt",
        connected=True,
        database="neo4j",
    )
    assert info.host == "localhost"
    assert info.port == 7687
    assert info.protocol == "bolt"
    assert info.connected is True
    assert info.database == "neo4j"

    d = info.to_dict()
    assert d == {
        "host": "localhost",
        "port": 7687,
        "protocol": "bolt",
        "connected": True,
        "database": "neo4j",
    }


def test_connection_info_to_dict_omits_none_database() -> None:
    info = ConnectionInfo(host="db", port=7474, protocol="http", connected=False)
    d = info.to_dict()
    assert "database" not in d


# ---------------------------------------------------------------------------
# Test 4: HealthStatus
# ---------------------------------------------------------------------------

def test_health_status() -> None:
    hs = HealthStatus(healthy=True, latency_ms=12.5)
    assert hs.healthy is True
    assert hs.latency_ms == pytest.approx(12.5)
    assert hs.detail == ""

    hs2 = HealthStatus(healthy=False, latency_ms=0.0, detail="connection refused")
    assert hs2.healthy is False
    assert hs2.detail == "connection refused"


# ---------------------------------------------------------------------------
# Test 5: LoadReport creation and to_dict
# ---------------------------------------------------------------------------

def test_load_report_creation_and_to_dict() -> None:
    report = LoadReport(
        node_count=1000,
        edge_count=5000,
        load_time_ms=750.0,
        verified=True,
        expected_nodes=1000,
        expected_edges=5000,
        errors=[],
    )
    assert report.node_count == 1000
    assert report.verified is True

    d = report.to_dict()
    assert d["node_count"] == 1000
    assert d["edge_count"] == 5000
    assert d["load_time_ms"] == pytest.approx(750.0)
    assert d["verified"] is True
    assert d["expected_nodes"] == 1000
    assert d["expected_edges"] == 5000
    assert d["errors"] == []


def test_load_report_to_dict_omits_none_optionals() -> None:
    report = LoadReport(node_count=10, edge_count=20, load_time_ms=1.0, verified=False)
    d = report.to_dict()
    assert "expected_nodes" not in d
    assert "expected_edges" not in d


def test_load_report_with_errors() -> None:
    report = LoadReport(
        node_count=0,
        edge_count=0,
        load_time_ms=0.0,
        verified=False,
        errors=["parse error on line 42"],
    )
    assert report.errors == ["parse error on line 42"]
    assert report.to_dict()["errors"] == ["parse error on line 42"]


# ---------------------------------------------------------------------------
# Test 6: EngineInfo with capabilities and to_dict
# ---------------------------------------------------------------------------

def test_engine_info_with_capabilities_and_to_dict() -> None:
    caps = {Capability.CACHE_CLEAR, Capability.GFQL, Capability.PLAN_PROFILING}
    info = EngineInfo(
        name="XRayGraphDB",
        version="2.0.0",
        build="release",
        capabilities=caps,
    )
    assert info.name == "XRayGraphDB"
    assert Capability.GFQL in info.capabilities

    d = info.to_dict()
    assert d["name"] == "XRayGraphDB"
    assert d["version"] == "2.0.0"
    assert d["build"] == "release"
    # Capabilities should be sorted strings
    assert d["capabilities"] == sorted(c.value for c in caps)
    assert "config_hash" not in d


def test_engine_info_to_dict_includes_config_hash() -> None:
    info = EngineInfo(
        name="Neo4j",
        version="5.0",
        build="community",
        capabilities=set(),
        config_hash="abc123",
    )
    d = info.to_dict()
    assert d["config_hash"] == "abc123"


# ---------------------------------------------------------------------------
# Test 7: CacheClearReport
# ---------------------------------------------------------------------------

def test_cache_clear_report() -> None:
    report = CacheClearReport(cleared=True)
    assert report.cleared is True
    assert report.detail == ""

    report2 = CacheClearReport(cleared=False, detail="permission denied")
    assert report2.cleared is False
    assert report2.detail == "permission denied"


# ---------------------------------------------------------------------------
# Test 8: EngineState
# ---------------------------------------------------------------------------

def test_engine_state() -> None:
    state = EngineState()
    assert state.memory_used_mb is None
    assert state.memory_available_mb is None
    assert state.active_queries is None
    assert state.cache_size_mb is None

    state2 = EngineState(
        memory_used_mb=512.0,
        memory_available_mb=1024.0,
        active_queries=3,
        cache_size_mb=64.0,
    )
    assert state2.memory_used_mb == pytest.approx(512.0)
    assert state2.active_queries == 3


# ---------------------------------------------------------------------------
# Test 9: QueryPlan
# ---------------------------------------------------------------------------

def test_query_plan() -> None:
    ops = [{"name": "NodeByLabelScan", "rows": 1000}]
    plan = QueryPlan(operators=ops, raw={"plan": ops})
    assert plan.operators == ops
    assert plan.estimated_cost is None
    assert plan.raw == {"plan": ops}

    plan2 = QueryPlan(operators=ops, raw=ops, estimated_cost=42.7)
    assert plan2.estimated_cost == pytest.approx(42.7)


# ---------------------------------------------------------------------------
# Test 10: ProfileResult
# ---------------------------------------------------------------------------

def test_profile_result() -> None:
    operators = [
        {"name": "NodeByLabelScan", "db_hits": 1000, "rows": 500},
        {"name": "Filter", "db_hits": 0, "rows": 100},
    ]
    result = ProfileResult(
        operators=operators,
        total_db_hits=1000,
        total_rows=600,
        raw={"profile": operators},
    )
    assert result.total_db_hits == 1000
    assert result.total_rows == 600
    assert len(result.operators) == 2
    assert result.raw == {"profile": operators}
