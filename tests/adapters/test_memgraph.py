"""Tests for the Memgraph adapter — offline capability and metadata checks."""

from __future__ import annotations

from tools.xraybench.adapters.memgraph import MemgraphAdapter
from tools.xraybench.adapters.capabilities import Capability


def test_capabilities() -> None:
    """Memgraph adapter exposes CACHE_CLEAR; not GFQL or NATIVE_PROTOCOL."""
    adapter = MemgraphAdapter()
    caps = adapter.capabilities()
    assert Capability.CACHE_CLEAR in caps
    assert Capability.GFQL not in caps
    assert Capability.NATIVE_PROTOCOL not in caps


def test_engine_info() -> None:
    """engine_info() returns an EngineInfo with name 'memgraph'."""
    adapter = MemgraphAdapter()
    info = adapter.engine_info()
    assert info.name == "memgraph"
