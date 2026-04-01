import json
import os
import shutil
import tempfile


def _make_result(
    benchmark: str = "test",
    engine: str = "eng",
    cold_ms: float = 100.0,
    warm_ms: float = 10.0,
) -> dict:
    return {
        "benchmark": benchmark,
        "engine": engine,
        "cold_ms": cold_ms,
        "warm_ms": warm_ms,
        "rows_out": 1000,
        "correctness": {"passed": True},
        "host": {"os": "Linux", "cpu": "x86", "cores": 4, "memory_gb": 16},
        "timestamp": "2026-04-01T00:00:00Z",
    }


def test_create_app() -> None:
    """Test that the app can be created."""
    tmpdir = tempfile.mkdtemp()
    try:
        from tools.xraybench.dashboard import create_app

        app = create_app(tmpdir)
        assert app is not None
        assert app.title == "xraygraph-bench Dashboard"
    except ImportError:
        import pytest

        pytest.skip("FastAPI not installed")
    finally:
        shutil.rmtree(tmpdir)


def test_index_page() -> None:
    """Test the index page returns HTML with results."""
    tmpdir = tempfile.mkdtemp()
    try:
        # Write a result
        with open(os.path.join(tmpdir, "r1.json"), "w") as f:
            json.dump(_make_result(engine="xraygraphdb"), f)

        from fastapi.testclient import TestClient

        from tools.xraybench.dashboard import create_app

        app = create_app(tmpdir)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "xraygraphdb" in resp.text
        assert "htmx" in resp.text.lower()
    except ImportError:
        import pytest

        pytest.skip("FastAPI not installed")
    finally:
        shutil.rmtree(tmpdir)


def test_api_results_filter() -> None:
    """Test the HTMX results endpoint with filtering."""
    tmpdir = tempfile.mkdtemp()
    try:
        with open(os.path.join(tmpdir, "r1.json"), "w") as f:
            json.dump(_make_result(engine="xraygraphdb"), f)
        with open(os.path.join(tmpdir, "r2.json"), "w") as f:
            json.dump(_make_result(engine="neo4j"), f)

        from fastapi.testclient import TestClient

        from tools.xraybench.dashboard import create_app

        app = create_app(tmpdir)
        client = TestClient(app)

        # Unfiltered
        resp = client.get("/api/results")
        assert resp.status_code == 200
        assert "xraygraphdb" in resp.text
        assert "neo4j" in resp.text

        # Filtered
        resp = client.get("/api/results?engine=xraygraphdb")
        assert resp.status_code == 200
        assert "xraygraphdb" in resp.text
        assert "neo4j" not in resp.text
    except ImportError:
        import pytest

        pytest.skip("FastAPI not installed")
    finally:
        shutil.rmtree(tmpdir)
