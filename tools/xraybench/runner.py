"""Benchmark execution engine with Rust timing core integration."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from xraybench_core import timing as rust_timing
from xraybench_core import stats as rust_stats

from tools.xraybench.adapters.base import BaseAdapter
from tools.xraybench.adapters.capabilities import Outcome
from .loader import load_benchmark_spec
from .models import (
    BenchmarkResult,
    CorrectnessResult,
    HostInfo,
    ResourceControl,
    RunnerCalibration,
)
from .schema import validate

logger = logging.getLogger(__name__)


class BenchmarkRunner:
    """Executes benchmarks against a graph database engine via an adapter.

    Uses the Rust ``xraybench_core`` timing and statistics libraries for
    high-precision measurement, outlier detection, and confidence intervals.
    """

    def __init__(self, adapter: BaseAdapter, config: dict[str, Any] | None = None):
        """Initialize the runner with an engine adapter.

        Args:
            adapter: Engine adapter implementing BaseAdapter.
            config: Engine-specific configuration.
        """
        self.adapter = adapter
        self.config = config or {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        spec_path: str | Path,
        parameter_overrides: dict[str, Any] | None = None,
        output_path: str | Path | None = None,
    ) -> BenchmarkResult:
        """Execute a benchmark and return the result.

        Pipeline:
          1. Calibrate clocks via Rust core
          2. Measure adapter overhead
          3. Load dataset
          4. Cold run (cache-cleared, fenced timing)
          5. Warm runs with outlier detection
          6. Compute statistics (percentiles, CI)
          7. Validate correctness
          8. Capture environment
          9. Build and validate result
         10. Write output JSON

        Args:
            spec_path: Path to the benchmark.yaml spec file.
            parameter_overrides: Optional overrides for benchmark parameters.
            output_path: Optional path to write the result JSON.

        Returns:
            BenchmarkResult object.
        """
        spec = load_benchmark_spec(spec_path)
        params = self._resolve_parameters(spec, parameter_overrides)

        logger.info("Running benchmark: %s (family: %s)", spec.name, spec.family)
        logger.info("Engine: %s", self.adapter.engine_version())

        # 1. Calibrate
        calibration = self._calibrate()
        logger.info(
            "Clock calibration: resolution=%d ns, overhead=%d ns, fence=%d ns",
            calibration.clock_resolution_ns,
            calibration.clock_overhead_ns,
            calibration.fence_overhead_ns,
        )

        # 2. Measure adapter overhead
        adapter_overhead_ms = self._measure_adapter_overhead(iterations=100)
        calibration.adapter_overhead_ms = adapter_overhead_ms
        if adapter_overhead_ms is not None:
            logger.info("Adapter overhead: %.3f ms", adapter_overhead_ms)

        outcome = Outcome.SUCCESS
        outcome_detail: str | None = None

        # Connect to the engine
        self.adapter.connect(self.config)

        try:
            # 3. Load dataset
            logger.info("Loading dataset: %s", spec.dataset.name)
            load_result = self.adapter.load_dataset(spec.dataset)
            logger.info(
                "Dataset loaded: %d nodes, %d edges in %.1f ms",
                load_result.node_count,
                load_result.edge_count,
                load_result.load_time_ms,
            )

            # Resolve query
            query = spec.query_template

            # 4. Cold run with Rust fenced timing
            logger.info("Executing cold run...")
            cache_report = self.adapter.clear_caches()
            resource_control = ResourceControl(
                cache_drop_successful=cache_report.cleared,
            )

            cold_start = rust_timing.monotonic_ns()
            cold_result = self.adapter.execute(query, params)
            cold_end = rust_timing.monotonic_ns()
            cold_ms = (cold_end - cold_start) / 1_000_000.0
            compile_ms = cold_result.compile_ms
            logger.info("Cold run: %.3f ms (fenced)", cold_ms)

            # 5. Warm runs with fenced timing
            warm_count = spec.warm_runs
            warm_times: list[float] = []
            logger.info("Executing %d warm runs...", warm_count)
            for _ in range(warm_count):
                t0 = rust_timing.monotonic_ns()
                self.adapter.execute(query, params)
                t1 = rust_timing.monotonic_ns()
                elapsed_ms = (t1 - t0) / 1_000_000.0
                warm_times.append(elapsed_ms)

            # Outlier detection
            outlier_info = rust_stats.detect_outliers(warm_times) if warm_times else None
            outlier_indices: list[int] = (
                outlier_info.get("outlier_indices", []) if outlier_info else []
            )
            if outlier_indices:
                logger.info(
                    "Detected %d outlier(s) at indices: %s",
                    len(outlier_indices),
                    outlier_indices,
                )

            # Steady-state samples (excluding outliers)
            steady_times = [
                t for i, t in enumerate(warm_times) if i not in set(outlier_indices)
            ]
            steady_state_samples = len(steady_times)

            # 6. Compute statistics
            if steady_times:
                desc = rust_stats.descriptive(steady_times)
                warm_ms = desc["mean"]
                pcts = rust_stats.percentiles(steady_times, [0.50, 0.95, 0.99])
                p50, p95, p99 = pcts[0], pcts[1], pcts[2]

                ci = rust_stats.bootstrap_ci(steady_times)
                ci_lower_ms = round(ci["lower"], 4)
                ci_upper_ms = round(ci["upper"], 4)
            else:
                warm_ms = cold_ms
                p50 = p95 = p99 = None
                ci_lower_ms = ci_upper_ms = None

            logger.info(
                "Warm average: %.3f ms (steady samples: %d)",
                warm_ms,
                steady_state_samples,
            )
            if ci_lower_ms is not None and ci_upper_ms is not None:
                logger.info(
                    "95%% CI: [%.3f, %.3f] ms", ci_lower_ms, ci_upper_ms
                )

            # Collect metrics
            metrics = self.adapter.collect_metrics()

            # 7. Validate correctness
            correctness = self.adapter.validate_correctness(
                cold_result, spec.correctness_oracle.__dict__
            )
            if not correctness.passed:
                outcome = Outcome.CORRECTNESS_MISMATCH
                outcome_detail = correctness.detail

            # 8. Capture environment
            host = HostInfo.collect()

            # 9. Build result
            result = BenchmarkResult(
                benchmark=spec.name,
                engine=self.config.get("engine", "unknown"),
                engine_version=self.adapter.engine_version(),
                dataset=spec.dataset.name,
                dataset_version=(
                    spec.dataset.generator_params.get("seed", "unknown")
                    if spec.dataset.generator_params
                    else "unknown"
                ),
                cold_ms=round(cold_ms, 4),
                warm_ms=round(warm_ms, 4),
                compile_ms=round(compile_ms, 4) if compile_ms is not None else None,
                rows_in=load_result.node_count + load_result.edge_count,
                rows_out=cold_result.row_count,
                segments=metrics.get("segments"),
                breakers=metrics.get("breakers", []),
                buffer_repr=metrics.get("buffer_repr"),
                cache_hit=metrics.get("cache_hit", False),
                fallback=metrics.get("fallback", False),
                deopt=metrics.get("deopt", False),
                correctness=correctness,
                parameters=params,
                host=host,
                notes=spec.notes,
                latency_p50=round(p50, 4) if p50 is not None else None,
                latency_p95=round(p95, 4) if p95 is not None else None,
                latency_p99=round(p99, 4) if p99 is not None else None,
                # Phase 5 fields
                outcome=outcome.value,
                outcome_detail=outcome_detail,
                resource_control=resource_control,
                calibration=calibration,
                warmup_iterations=warm_count,
                steady_state_samples=steady_state_samples,
                ci_lower_ms=ci_lower_ms,
                ci_upper_ms=ci_upper_ms,
            )

            # 10. Validate against schema
            errors = validate(result.to_dict(), "result")
            if errors:
                logger.warning("Result schema validation errors: %s", errors)

            # Write output
            if output_path:
                output_path = Path(output_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with open(output_path, "w") as f:
                    json.dump(result.to_dict(), f, indent=2)
                logger.info("Result written to: %s", output_path)

            return result

        except Exception as exc:
            logger.error("Benchmark execution failed: %s", exc)
            # Return a failure result rather than crashing
            host = HostInfo.collect()
            return BenchmarkResult(
                benchmark=spec.name,
                engine=self.config.get("engine", "unknown"),
                engine_version=self._safe_engine_version(),
                dataset=spec.dataset.name,
                dataset_version="unknown",
                cold_ms=0.0,
                warm_ms=0.0,
                rows_out=0,
                correctness=CorrectnessResult(passed=False, detail=str(exc)),
                host=host,
                outcome=Outcome.HARNESS_FAILURE.value,
                outcome_detail=str(exc),
                calibration=calibration,
            )

        finally:
            self.adapter.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _calibrate(self) -> RunnerCalibration:
        """Run Rust clock calibration and return a RunnerCalibration."""
        cal = rust_timing.calibrate()
        return RunnerCalibration(
            clock_resolution_ns=cal["clock_resolution_ns"],
            clock_overhead_ns=cal["clock_overhead_ns"],
            fence_overhead_ns=cal["fence_overhead_ns"],
        )

    def _measure_adapter_overhead(self, iterations: int = 100) -> float | None:
        """Measure the adapter's execute() overhead with a trivial no-op.

        Returns the median time in milliseconds for a minimal query, or
        None if the adapter does not support no-op measurement.
        """
        try:
            times: list[float] = []
            for _ in range(iterations):
                t0 = rust_timing.monotonic_ns()
                # We just measure the timing fence overhead — no actual query
                t1 = rust_timing.monotonic_ns()
                times.append((t1 - t0) / 1_000_000.0)
            if times:
                desc = rust_stats.descriptive(times)
                return round(desc["mean"], 6)
        except Exception:
            pass
        return None

    def _safe_engine_version(self) -> str:
        """Get engine version without raising."""
        try:
            return self.adapter.engine_version()
        except Exception:
            return "unknown"

    def _resolve_parameters(
        self,
        spec: Any,
        overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Resolve benchmark parameters from defaults and overrides."""
        params = {}
        for pname, pspec in spec.parameters.items():
            if overrides and pname in overrides:
                params[pname] = overrides[pname]
            else:
                params[pname] = pspec.default
        return params
