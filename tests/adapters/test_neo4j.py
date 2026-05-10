"""Tests for the Neo4j adapter — offline capability and metadata checks."""

from __future__ import annotations

from tools.xraybench.adapters.neo4j import Neo4jAdapter
from tools.xraybench.adapters.capabilities import Capability


def test_capabilities() -> None:
    """Neo4j adapter exposes CACHE_CLEAR and PLAN_PROFILING; not GFQL."""
    adapter = Neo4jAdapter()
    caps = adapter.capabilities()
    assert Capability.CACHE_CLEAR in caps
    assert Capability.PLAN_PROFILING in caps
    assert Capability.GFQL not in caps


def test_engine_info() -> None:
    """engine_info() returns an EngineInfo with name 'neo4j' and no GFQL."""
    adapter = Neo4jAdapter()
    info = adapter.engine_info()
    assert info.name == "neo4j"
    assert Capability.GFQL not in info.capabilities
