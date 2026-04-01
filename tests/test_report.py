import json
import os
import tempfile
import shutil
from tools.xraybench.report import generate_report


def _make_result(benchmark="test", engine="eng-a", cold_ms=100.0, warm_ms=10.0):
    return {
        "benchmark": benchmark, "engine": engine,
        "cold_ms": cold_ms, "warm_ms": warm_ms,
        "rows_out": 1000, "correctness": {"passed": True},
        "host": {"os": "Linux", "cpu": "x86", "cores": 4, "memory_gb": 16},
        "timestamp": "2026-04-01T00:00:00Z",
    }


def test_generate_report_with_results():
    tmpdir = tempfile.mkdtemp()
    try:
        for i, engine in enumerate(["xraygraphdb", "neo4j", "memgraph"]):
            path = os.path.join(tmpdir, f"result_{i}.json")
            with open(path, "w") as f:
                json.dump(_make_result(engine=engine, cold_ms=100-i*20), f)

        out = os.path.join(tmpdir, "report.html")
        count = generate_report(tmpdir, out)
        assert count == 3
        assert os.path.isfile(out)
        html = open(out).read()
        assert "plotly" in html.lower()
        assert "xraygraphdb" in html
        assert "neo4j" in html
    finally:
        shutil.rmtree(tmpdir)


def test_generate_empty_report():
    tmpdir = tempfile.mkdtemp()
    try:
        out = os.path.join(tmpdir, "report.html")
        count = generate_report(tmpdir, out)
        assert count == 0
        assert os.path.isfile(out)
        html = open(out).read()
        assert "No results" in html
    finally:
        shutil.rmtree(tmpdir)


def test_report_contains_table():
    tmpdir = tempfile.mkdtemp()
    try:
        path = os.path.join(tmpdir, "r.json")
        with open(path, "w") as f:
            json.dump(_make_result(), f)
        out = os.path.join(tmpdir, "report.html")
        generate_report(tmpdir, out)
        html = open(out).read()
        assert "<table>" in html
        assert "PASS" in html
    finally:
        shutil.rmtree(tmpdir)


def test_report_contains_environment():
    tmpdir = tempfile.mkdtemp()
    try:
        path = os.path.join(tmpdir, "r.json")
        with open(path, "w") as f:
            json.dump(_make_result(), f)
        out = os.path.join(tmpdir, "report.html")
        generate_report(tmpdir, out)
        html = open(out).read()
        assert "Linux" in html
        assert "Environment" in html
    finally:
        shutil.rmtree(tmpdir)
