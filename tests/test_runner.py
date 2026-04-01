"""Tests for the benchmark runner with Rust timing core integration.

Uses mock adapters so no running engine is required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, PropertyMock
from typing import Any

import pytest

from tools.xraybench.runner import BenchmarkRunner
from tools.xraybench.models import (
    BenchmarkResult,
    CorrectnessResult,
    ExecuteResult,
    HostInfo,
    ResourceControl,
    RunnerCalibration,
)
from tools.xraybench.adapters.capabilities import (
    CacheClearReport,
    ConnectionInfo,
    Capability,
    EngineInfo,
    LoadReport,
)
from tools.xraybench.adapters.base import BaseAdapter


# ---------------------------------------------------------------------------
# Helpers — mock adapter and spec
# ---------------------------------------------------------------------------


def _make_mock_adapter() -> MagicMock:
    """Create a fully-configured mock adapter for testing."""
    adapter = MagicMock(spec=BaseAdapter)

    adapter.connect.return_value = ConnectionInfo(
        host="localhost", port=7687, protocol="bolt", connected=True
    )
    adapter.close.return_value = None

    adapter.engine_info.return_value = EngineInfo(
        name="mock-engine",
        version="0.0.1-test",
        build="test-build",
        capabilities={Capability.CACHE_CLEAR},
    )
    adapter.engine_version.return_value = "0.0.1-test"

    adapter.load_dataset.return_value = LoadReport(
        node_count=100,
        edge_count=200,
        load_time_ms=5.0,
        verified=True,
    )

    adapter.clear_caches.return_value = CacheClearReport(cleared=True)

    # execute() returns an ExecuteResult with 10 rows
    sample_rows: list[dict[str, Any]] = [{"id": i, "val": i * 10} for i in range(10)]
    adapter.execute.return_value = ExecuteResult(
        rows=sample_rows,
        wall_ms=1.5,
        compile_ms=0.3,
    )

    adapter.collect_metrics.return_value = {}

    adapter.validate_correctness.return_value = CorrectnessResult(
        passed=True, detail="row_count=10 matches expected=10"
    )

    adapter.capabilities.return_value = {Capability.CACHE_CLEAR}

    return adapter


def _make_mock_spec():
    """Return a mock BenchmarkSpec with all required attributes."""
    from tools.xraybench.models import (
        BenchmarkSpec,
        CorrectnessOracle,
        DatasetSpec,
        ParameterSpec,
    )

    return BenchmarkSpec(
        name="test-benchmark",
        family="test",
        version="1.0.0",
        description="A test benchmark",
        dataset=DatasetSpec(
            name="test-dataset",
            type="generated",
            generator="simple",
            generator_params={"seed": "42", "nodes": 100, "edges": 200},
        ),
        query_template="MATCH (n) RETURN n LIMIT $limit",
        parameters={
            "limit": ParameterSpec(type="int", default=10, description="Row limit"),
        },
        correctness_oracle=CorrectnessOracle(
            type="row_count",
            expected_row_count=10,
        ),
        metrics=["wall_ms", "compile_ms"],
        warm_runs=5,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRunnerProducesResult:
    """test_runner_produces_result: verify result has expected fields."""

    def test_runner_produces_result(self, tmp_path: Any):
        """Run the full pipeline with a mock adapter and check the result."""
        adapter = _make_mock_adapter()
        runner = BenchmarkRunner(adapter, config={"engine": "mock-engine"})
        spec = _make_mock_spec()

        with patch(
            "tools.xraybench.runner.load_benchmark_spec", return_value=spec
        ), patch("tools.xraybench.runner.validate", return_value=[]):
            result = runner.run(str(tmp_path / "fake.yaml"))

        assert isinstance(result, BenchmarkResult)
        assert result.benchmark == "test-benchmark"
        assert result.engine == "mock-engine"
        assert result.engine_version == "0.0.1-test"
        assert result.dataset == "test-dataset"
        assert result.rows_out == 10
        assert result.cold_ms > 0
        assert result.warm_ms > 0
        assert result.outcome == "success"
        assert result.correctness.passed is True


class TestRunnerUsesRustTiming:
    """test_runner_uses_rust_timing: verify calibration is populated."""

    def test_runner_uses_rust_timing(self, tmp_path: Any):
        """Verify calibration data comes from the Rust core."""
        adapter = _make_mock_adapter()
        runner = BenchmarkRunner(adapter)
        spec = _make_mock_spec()

        with patch(
            "tools.xraybench.runner.load_benchmark_spec", return_value=spec
        ), patch("tools.xraybench.runner.validate", return_value=[]):
            result = runner.run(str(tmp_path / "fake.yaml"))

        assert result.calibration is not None
        assert isinstance(result.calibration, RunnerCalibration)
        assert result.calibration.clock_resolution_ns > 0
        # adapter_overhead_ms is populated from fence measurement
        assert result.calibration.adapter_overhead_ms is not None
        assert result.calibration.adapter_overhead_ms >= 0.0

        # Verify to_dict includes calibration
        d = result.to_dict()
        assert "calibration" in d
        assert "clock_resolution_ns" in d["calibration"]


class TestRunnerComputesStatistics:
    """test_runner_computes_statistics: verify CI, warmup, steady samples."""

    def test_runner_computes_statistics(self, tmp_path: Any):
        """Verify statistical fields are populated from warm run data."""
        adapter = _make_mock_adapter()
        runner = BenchmarkRunner(adapter)
        spec = _make_mock_spec()

        with patch(
            "tools.xraybench.runner.load_benchmark_spec", return_value=spec
        ), patch("tools.xraybench.runner.validate", return_value=[]):
            result = runner.run(str(tmp_path / "fake.yaml"))

        # warmup_iterations should match spec.warm_runs
        assert result.warmup_iterations == 5
        # steady_state_samples <= warmup_iterations (outliers removed)
        assert result.steady_state_samples is not None
        assert 0 < result.steady_state_samples <= 5

        # CI bounds should be present
        assert result.ci_lower_ms is not None
        assert result.ci_upper_ms is not None
        assert result.ci_lower_ms <= result.ci_upper_ms

        # Percentiles
        assert result.latency_p50 is not None
        assert result.latency_p95 is not None
        assert result.latency_p99 is not None


class TestRunnerCapturesEnvironment:
    """test_runner_captures_environment: verify host info fields."""

    def test_runner_captures_environment(self, tmp_path: Any):
        """Verify the enhanced HostInfo is populated."""
        adapter = _make_mock_adapter()
        runner = BenchmarkRunner(adapter)
        spec = _make_mock_spec()

        with patch(
            "tools.xraybench.runner.load_benchmark_spec", return_value=spec
        ), patch("tools.xraybench.runner.validate", return_value=[]):
            result = runner.run(str(tmp_path / "fake.yaml"))

        assert result.host is not None
        host = result.host
        assert isinstance(host, HostInfo)
        assert host.os  # non-empty string
        assert host.cpu  # non-empty string
        assert host.cores > 0
        assert host.memory_gb >= 0.0
        # threads is populated via os.cpu_count()
        assert host.threads is not None
        assert host.threads > 0

        # to_dict includes the new fields
        d = host.to_dict()
        assert "os" in d
        assert "cpu" in d
        assert "cores" in d
        assert "threads" in d

        # Resource control should be present
        assert result.resource_control is not None
        assert isinstance(result.resource_control, ResourceControl)


class TestRunnerValidatesCorrectness:
    """test_runner_validates_correctness: verify correctness result."""

    def test_runner_validates_correctness_pass(self, tmp_path: Any):
        """Verify correctness result is populated when validation passes."""
        adapter = _make_mock_adapter()
        runner = BenchmarkRunner(adapter)
        spec = _make_mock_spec()

        with patch(
            "tools.xraybench.runner.load_benchmark_spec", return_value=spec
        ), patch("tools.xraybench.runner.validate", return_value=[]):
            result = runner.run(str(tmp_path / "fake.yaml"))

        assert result.correctness is not None
        assert result.correctness.passed is True
        assert result.correctness.detail != ""
        assert result.outcome == "success"

    def test_runner_validates_correctness_fail(self, tmp_path: Any):
        """Verify outcome changes when correctness validation fails."""
        adapter = _make_mock_adapter()
        adapter.validate_correctness.return_value = CorrectnessResult(
            passed=False,
            detail="row_count mismatch: got 10, expected 20",
        )
        runner = BenchmarkRunner(adapter)
        spec = _make_mock_spec()

        with patch(
            "tools.xraybench.runner.load_benchmark_spec", return_value=spec
        ), patch("tools.xraybench.runner.validate", return_value=[]):
            result = runner.run(str(tmp_path / "fake.yaml"))

        assert result.correctness is not None
        assert result.correctness.passed is False
        assert result.outcome == "correctness_mismatch"
        assert result.outcome_detail is not None
        assert "mismatch" in result.outcome_detail
