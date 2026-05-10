"""Unit tests for the xrayGraphDB native protocol adapter.

Tests adapter construction, capabilities, and engine info without
requiring a live server connection.
"""

from __future__ import annotations

from tools.xraybench.adapters.capabilities import Capability, EngineInfo
from tools.xraybench.adapters.xraygraphdb_native import XrayGraphDBNativeAdapter


class TestCapabilities:
    def test_capabilities_include_native_protocol(self) -> None:
        """Adapter must report NATIVE_PROTOCOL capability."""
        adapter = XrayGraphDBNativeAdapter()
        caps = adapter.capabilities()
        assert Capability.NATIVE_PROTOCOL in caps

    def test_capabilities_include_gfql(self) -> None:
        """Adapter must report GFQL capability."""
        adapter = XrayGraphDBNativeAdapter()
        caps = adapter.capabilities()
        assert Capability.GFQL in caps

    def test_capabilities_include_streaming(self) -> None:
        """Adapter must report STREAMING_RESULTS capability."""
        adapter = XrayGraphDBNativeAdapter()
        caps = adapter.capabilities()
        assert Capability.STREAMING_RESULTS in caps

    def test_capabilities_include_compile_time(self) -> None:
        """Adapter must report COMPILE_TIME_REPORTING capability."""
        adapter = XrayGraphDBNativeAdapter()
        caps = adapter.capabilities()
        assert Capability.COMPILE_TIME_REPORTING in caps

    def test_capabilities_include_plan_profiling(self) -> None:
        """Adapter must report PLAN_PROFILING capability."""
        adapter = XrayGraphDBNativeAdapter()
        caps = adapter.capabilities()
        assert Capability.PLAN_PROFILING in caps

    def test_capabilities_include_cache_clear(self) -> None:
        """Adapter must report CACHE_CLEAR capability."""
        adapter = XrayGraphDBNativeAdapter()
        caps = adapter.capabilities()
        assert Capability.CACHE_CLEAR in caps

    def test_capabilities_include_explain_analyze(self) -> None:
        """Adapter must report EXPLAIN_ANALYZE capability."""
        adapter = XrayGraphDBNativeAdapter()
        caps = adapter.capabilities()
        assert Capability.EXPLAIN_ANALYZE in caps

    def test_all_expected_capabilities(self) -> None:
        """All 7 expected capabilities must be present."""
        adapter = XrayGraphDBNativeAdapter()
        caps = adapter.capabilities()
        expected = {
            Capability.COMPILE_TIME_REPORTING,
            Capability.PLAN_PROFILING,
            Capability.CACHE_CLEAR,
            Capability.EXPLAIN_ANALYZE,
            Capability.GFQL,
            Capability.NATIVE_PROTOCOL,
            Capability.STREAMING_RESULTS,
        }
        assert caps == expected


class TestEngineInfo:
    def test_engine_info_name(self) -> None:
        """Engine info name must be 'xraygraphdb-native'."""
        adapter = XrayGraphDBNativeAdapter()
        info = adapter.engine_info()
        assert info.name == "xraygraphdb-native"

    def test_engine_info_build(self) -> None:
        """Engine info build must indicate native protocol."""
        adapter = XrayGraphDBNativeAdapter()
        info = adapter.engine_info()
        assert info.build == "native-protocol"

    def test_engine_info_default_version(self) -> None:
        """Before connecting, version should be empty or 'unknown'."""
        adapter = XrayGraphDBNativeAdapter()
        info = adapter.engine_info()
        # server_info is "" before connection, so version is ""
        assert info.version == "" or info.version == "unknown"

    def test_engine_info_capabilities(self) -> None:
        """Engine info must include all adapter capabilities."""
        adapter = XrayGraphDBNativeAdapter()
        info = adapter.engine_info()
        assert Capability.NATIVE_PROTOCOL in info.capabilities
        assert Capability.GFQL in info.capabilities


class TestAdapterConstruction:
    def test_initial_state(self) -> None:
        """Adapter starts with no connection."""
        adapter = XrayGraphDBNativeAdapter()
        assert adapter._client is None
        assert adapter._host == "localhost"
        assert adapter._port == 7689

    def test_close_without_connect(self) -> None:
        """Closing without connecting should not raise."""
        adapter = XrayGraphDBNativeAdapter()
        adapter.close()  # Should not raise

    def test_protocol_overhead_default(self) -> None:
        """Protocol overhead is 0 before connection."""
        adapter = XrayGraphDBNativeAdapter()
        assert adapter.protocol_overhead_ms() == 0.0

    def test_collect_metrics_default(self) -> None:
        """Metrics before connection should have defaults."""
        adapter = XrayGraphDBNativeAdapter()
        metrics = adapter.collect_metrics()
        assert metrics["protocol"] == "xray-native"
        assert metrics["handshake_overhead_ms"] == 0.0
        assert metrics["server_info"] == ""
        assert metrics["server_protocol_version"] == 0

    def test_execute_without_connect_raises(self) -> None:
        """Executing without connecting must raise RuntimeError."""
        adapter = XrayGraphDBNativeAdapter()
        import pytest

        with pytest.raises(RuntimeError, match="Not connected"):
            adapter.execute("RETURN 1")

    def test_clear_caches_without_connect(self) -> None:
        """clear_caches without connection should return not cleared."""
        adapter = XrayGraphDBNativeAdapter()
        report = adapter.clear_caches()
        assert report.cleared is False
        assert "Not connected" in report.detail


class TestAdapterRegistry:
    def test_registry_includes_native(self) -> None:
        """The adapter registry must include 'xraygraphdb-native'."""
        from tools.xraybench.adapters import _ADAPTER_REGISTRY

        assert "xraygraphdb-native" in _ADAPTER_REGISTRY

    def test_get_adapter_native(self) -> None:
        """get_adapter('xraygraphdb-native') must return the correct class."""
        from tools.xraybench.adapters import get_adapter

        cls = get_adapter("xraygraphdb-native")
        assert cls is XrayGraphDBNativeAdapter
