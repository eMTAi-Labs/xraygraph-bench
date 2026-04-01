import json
import os
import csv
import tempfile
import shutil
from tools.xraybench.export import load_results, flatten_result, export_csv


def _write_result(results_dir, name, data):
    path = os.path.join(results_dir, f"{name}.json")
    with open(path, "w") as f:
        json.dump(data, f)
    return path


def _make_result(benchmark="test", engine="eng", cold_ms=100.0, warm_ms=10.0):
    return {
        "benchmark": benchmark,
        "engine": engine,
        "cold_ms": cold_ms,
        "warm_ms": warm_ms,
        "rows_out": 1000,
        "correctness": {"passed": True, "detail": "ok"},
        "host": {"os": "Linux", "cpu": "x86", "cores": 4},
        "timestamp": "2026-04-01T00:00:00Z",
    }


def test_load_results():
    tmpdir = tempfile.mkdtemp()
    try:
        _write_result(tmpdir, "a", _make_result(engine="a"))
        _write_result(tmpdir, "b", _make_result(engine="b"))
        results = load_results(tmpdir)
        assert len(results) == 2
    finally:
        shutil.rmtree(tmpdir)


def test_flatten_result():
    r = _make_result()
    flat = flatten_result(r)
    assert flat["benchmark"] == "test"
    assert flat["correctness.passed"] is True
    assert flat["host.os"] == "Linux"
    assert flat["host.cores"] == 4


def test_export_csv():
    tmpdir = tempfile.mkdtemp()
    try:
        results = [_make_result(engine="a"), _make_result(engine="b")]
        out_path = os.path.join(tmpdir, "results.csv")
        count = export_csv(results, out_path)
        assert count == 2
        with open(out_path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 2
        assert rows[0]["benchmark"] == "test"
        assert "host.os" in rows[0]
    finally:
        shutil.rmtree(tmpdir)


def test_export_csv_empty():
    tmpdir = tempfile.mkdtemp()
    try:
        out_path = os.path.join(tmpdir, "empty.csv")
        count = export_csv([], out_path)
        assert count == 0
    finally:
        shutil.rmtree(tmpdir)


def test_flatten_nested_list():
    r = {"benchmark": "test", "breakers": ["Sort", "Agg"]}
    flat = flatten_result(r)
    assert flat["breakers"] == '["Sort", "Agg"]'
