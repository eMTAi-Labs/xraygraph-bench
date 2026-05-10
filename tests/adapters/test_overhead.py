"""Tests for adapter overhead calibration."""

from unittest.mock import MagicMock

from tools.xraybench.adapters.overhead import measure_adapter_overhead
from tools.xraybench.models import ExecuteResult


def test_overhead_returns_positive():
    adapter = MagicMock()
    adapter.execute.return_value = ExecuteResult(rows=[{"health": 1}], wall_ms=0.5)
    result = measure_adapter_overhead(adapter, iterations=100)
    assert result["median_ms"] >= 0
    assert result["iterations"] == 100
    assert "p95_ms" in result
    assert "p99_ms" in result


def test_overhead_calls_execute():
    adapter = MagicMock()
    adapter.execute.return_value = ExecuteResult(rows=[{"health": 1}], wall_ms=0.5)
    measure_adapter_overhead(adapter, iterations=50)
    assert adapter.execute.call_count == 50


def test_overhead_uses_return_1():
    adapter = MagicMock()
    adapter.execute.return_value = ExecuteResult(rows=[{"health": 1}], wall_ms=0.5)
    measure_adapter_overhead(adapter, iterations=10)
    for call in adapter.execute.call_args_list:
        assert call[0][0] == "RETURN 1 AS _ping"
