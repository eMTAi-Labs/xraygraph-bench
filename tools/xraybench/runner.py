"""Benchmark execution engine."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from tools.xraybench.adapters.base import BaseAdapter
from .loader import load_benchmark_spec
from .models import BenchmarkResult, HostInfo
from .schema import validate

logger = logging.getLogger(__name__)


class BenchmarkRunner:
    """Executes benchmarks against a graph database engine via an adapter."""

    def __init__(self, adapter: BaseAdapter, config: dict[str, Any] | None = None):
        """Initialize the runner with an engine adapter.

        Args:
            adapter: Engine adapter implementing BaseAdapter.
            config: Engine-specific configuration.
        """
        self.adapter = adapter
        self.config = config or {}

    def run(
        self,
        spec_path: str | Path,
        parameter_overrides: dict[str, Any] | None = None,
        output_path: str | Path | None = None,
    ) -> BenchmarkResult:
        """Execute a benchmark and return the result.

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

        # Connect to the engine
        self.adapter.connect(self.config)

        try:
            # Load dataset
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

            # Cold run
            logger.info("Executing cold run...")
            self.adapter.clear_caches()
            cold_result = self.adapter.execute(query, params)
            cold_ms = cold_result.wall_ms
            compile_ms = cold_result.compile_ms
            logger.info("Cold run: %.1f ms", cold_ms)

            # Warm runs
            warm_times: list[float] = []
            logger.info("Executing %d warm runs...", spec.warm_runs)
            for _ in range(spec.warm_runs):
                warm_result = self.adapter.execute(query, params)
                warm_times.append(warm_result.wall_ms)

            warm_ms = sum(warm_times) / len(warm_times) if warm_times else cold_ms
            logger.info("Warm average: %.1f ms (stddev: %.1f ms)", warm_ms, _stddev(warm_times))

            # Collect metrics
            metrics = self.adapter.collect_metrics()

            # Validate correctness
            correctness = self.adapter.validate_correctness(
                cold_result, spec.correctness_oracle.__dict__
            )

            # Build result
            result = BenchmarkResult(
                benchmark=spec.name,
                engine=self.config.get("engine", "unknown"),
                engine_version=self.adapter.engine_version(),
                dataset=spec.dataset.name,
                dataset_version=spec.dataset.generator_params.get("seed", "unknown")
                if spec.dataset.generator_params
                else "unknown",
                cold_ms=round(cold_ms, 2),
                warm_ms=round(warm_ms, 2),
                compile_ms=round(compile_ms, 2) if compile_ms is not None else None,
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
                host=HostInfo.collect(),
                notes=spec.notes,
            )

            # Validate against schema
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

        finally:
            self.adapter.close()

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


def _stddev(values: list[float]) -> float:
    """Compute sample standard deviation."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
    return variance**0.5
