"""Tests for xrayGraphDB Bolt adapter — no live engine required."""

from __future__ import annotations

from tools.xraybench.adapters.xraygraphdb import XrayGraphDBAdapter
from tools.xraybench.adapters.capabilities import Capability


def test_capabilities():
    adapter = XrayGraphDBAdapter()
    caps = adapter.capabilities()
    assert Capability.COMPILE_TIME_REPORTING in caps
    assert Capability.PLAN_PROFILING in caps
    assert Capability.CACHE_CLEAR in caps
    assert Capability.EXPLAIN_ANALYZE in caps
    assert Capability.GFQL in caps


def test_engine_info_before_connect():
    adapter = XrayGraphDBAdapter()
    info = adapter.engine_info()
    assert info.name == "xraygraphdb"
    assert Capability.GFQL in info.capabilities


def test_gfql_context_format():
    adapter = XrayGraphDBAdapter()
    stmt = adapter._gfql_context_statement("tenant1", "repo1")
    assert "SET GFQL_CONTEXT" in stmt
    assert "tenant1" in stmt
    assert "repo1" in stmt


def test_breaker_detection():
    adapter = XrayGraphDBAdapter()
    names = adapter._breaker_operator_names()
    assert "Sort" in names
    assert "Aggregate" in names
    assert "Distinct" in names
