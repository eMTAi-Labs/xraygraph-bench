import json
import os
import tempfile
from tools.xraybench.compare import compare_results, format_comparison_table, load_result


def _make_result(benchmark="test", engine="engine-a", cold_ms=100.0, warm_ms=10.0):
    return {
        "benchmark": benchmark,
        "engine": engine,
        "cold_ms": cold_ms,
        "warm_ms": warm_ms,
        "rows_out": 1000,
        "correctness": {"passed": True},
        "timestamp": "2026-04-01T00:00:00Z",
    }


def test_compare_identical():
    a = _make_result(cold_ms=100.0, warm_ms=10.0)
    b = _make_result(engine="engine-b", cold_ms=100.0, warm_ms=10.0)
    result = compare_results(a, b)
    assert result["benchmark"] == "test"
    assert len(result["metrics"]) == 2  # cold_ms and warm_ms
    for m in result["metrics"]:
        assert not m["significant"]


def test_compare_improvement():
    a = _make_result(cold_ms=100.0, warm_ms=10.0)
    b = _make_result(engine="engine-b", cold_ms=50.0, warm_ms=5.0)
    result = compare_results(a, b)
    cold = next(m for m in result["metrics"] if m["metric_name"] == "cold_ms")
    assert cold["classification"] == "Improvement"
    assert cold["percentage_change"] < -40.0


def test_format_table():
    a = _make_result(cold_ms=100.0)
    b = _make_result(engine="engine-b", cold_ms=50.0)
    result = compare_results(a, b)
    table = format_comparison_table(result)
    assert "engine-a" in table
    assert "engine-b" in table
    assert "cold_ms" in table


def test_load_result():
    tmpdir = tempfile.mkdtemp()
    path = os.path.join(tmpdir, "result.json")
    data = _make_result()
    with open(path, "w") as f:
        json.dump(data, f)
    loaded = load_result(path)
    assert loaded["benchmark"] == "test"
    os.unlink(path)
    os.rmdir(tmpdir)
