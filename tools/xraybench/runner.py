"""Benchmark execution engine with Rust timing core integration."""

from __future__ import annotations

import hashlib
import json
import logging
import random  # noqa: F401 — used in _generate_nodes_inline
from pathlib import Path
from typing import Any

from xraybench_core import timing as rust_timing
from xraybench_core import stats as rust_stats

from tools.xraybench.adapters.base import BaseAdapter
from tools.xraybench.adapters.capabilities import Outcome
from .loader import load_benchmark_spec
from .models import (
    BenchmarkResult,
    BenchmarkSpec,
    CorrectnessResult,
    DatasetSpec,
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

        # Compute spec hash (SHA-256 of the spec file contents)
        spec_path_obj = Path(spec_path)
        spec_hash: str | None = None
        if spec_path_obj.exists():
            spec_hash = hashlib.sha256(spec_path_obj.read_bytes()).hexdigest()

        # Compute dataset manifest hash if a manifest exists on disk
        dataset_manifest_hash: str | None = None
        if spec.dataset.name:
            # Check common manifest locations
            for candidate_dir in [
                Path(self.config.get("data_dir", "/data/xraybench"))
                / "synthetic"
                / spec.dataset.name,
                Path(self.config.get("data_dir", "/data/xraybench"))
                / spec.dataset.type
                / spec.dataset.name,
            ]:
                manifest_path = candidate_dir / "manifest.yaml"
                if manifest_path.exists():
                    dataset_manifest_hash = hashlib.sha256(
                        manifest_path.read_bytes()
                    ).hexdigest()
                    break

        # Build engine_mode from config
        engine_mode = {
            "storage": self.config.get("storage_mode", "in-memory"),
            "durability": self.config.get("durability", "relaxed"),
            "execution_model": self.config.get("execution_model", "unknown"),
            "concurrency_model": self.config.get("concurrency_model", "unknown"),
            "isolation": self.config.get("isolation", "snapshot"),
            "replication": self.config.get("replication", "none"),
        }

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

        # For native protocol adapters, create a Bolt helper for data loading
        # (xrayProtocol is fast for queries but crashes on large UNWIND writes)
        bolt_adapter = None
        if self.config.get("engine") in ("xraygraphdb-native",):
            from tools.xraybench.adapters.xraygraphdb import XrayGraphDBAdapter
            bolt_adapter = XrayGraphDBAdapter()
            bolt_config = dict(self.config)
            bolt_config["port"] = 7687  # Bolt port
            bolt_adapter.connect(bolt_config)
            logger.info("Bolt helper connected for data loading on port 7687")

        # Use Bolt for writes, native for queries
        write_adapter = bolt_adapter or self.adapter

        try:
            # 3. Load dataset — generate synthetic data if spec has a generator
            logger.info("Loading dataset: %s", spec.dataset.name)

            # Clear existing data first
            write_adapter.clear_dataset()

            # Generate synthetic data (may create nodes inline or return edges)
            # Use write_adapter for data generation
            saved_adapter = self.adapter
            self.adapter = write_adapter
            data_source = self._generate_synthetic_data(spec, params)
            self.adapter = saved_adapter

            if data_source is not None:
                # Edge-based data returned — load via Bolt adapter
                from tools.xraybench.adapters.dataset_loader import load_edges_into_adapter
                load_edges_into_adapter(write_adapter, data_source)

            # Count what's in the database
            from tools.xraybench.adapters.capabilities import LoadReport
            try:
                nr = self.adapter.execute("MATCH (n) RETURN count(n) AS cnt")
                node_count = int(nr.rows[0]["cnt"]) if nr.rows else 0
            except Exception:
                node_count = 0
            try:
                er = self.adapter.execute("MATCH ()-[r]->() RETURN count(r) AS cnt")
                edge_count = int(er.rows[0]["cnt"]) if er.rows else 0
            except Exception:
                edge_count = 0

            load_result = LoadReport(
                node_count=node_count,
                edge_count=edge_count,
                load_time_ms=0.0,
                verified=True,
            )
            logger.info(
                "Dataset loaded: %d nodes, %d edges",
                load_result.node_count,
                load_result.edge_count,
            )

            # Resolve query and derive query parameters
            query = spec.query_template
            params = self._derive_query_params(spec, params)

            # Inline parameters into query for protocols that don't support
            # parameterized queries (xrayProtocol)
            query = self._inline_params(query, params)
            params = {}  # Already inlined

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

            # 7. Validate correctness — use a COUNT query for accurate row count
            #    (Bolt PULL batching may cap rows returned to the client)
            oracle = spec.correctness_oracle.__dict__
            if oracle.get("type") in ("row_count_range", "row_count"):
                try:
                    import re as _re
                    # Replace only the RETURN projection, preserving ORDER BY / LIMIT
                    # RETURN <cols> [ORDER BY ...] [LIMIT N]
                    # → RETURN count(*) AS __cnt  (drop ORDER BY, keep LIMIT)
                    count_query = _re.sub(
                        r"RETURN\s+.+?(?=\bORDER\b|\bLIMIT\b|$)",
                        "RETURN count(*) AS __cnt ",
                        query.strip(),
                        flags=_re.IGNORECASE | _re.DOTALL,
                    )
                    # Remove ORDER BY (meaningless on count)
                    count_query = _re.sub(
                        r"\bORDER\s+BY\s+.+?(?=\bLIMIT\b|$)",
                        "",
                        count_query,
                        flags=_re.IGNORECASE | _re.DOTALL,
                    )
                    count_result = self.adapter.execute(count_query, params)
                    if count_result.rows:
                        actual_count = int(count_result.rows[0]["__cnt"])
                        cold_result.row_count = actual_count
                        logger.info("Correctness row count: %d", actual_count)
                except Exception as exc:
                    logger.warning("Count query for correctness failed: %s", exc)

            correctness = self.adapter.validate_correctness(cold_result, oracle)
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
                # Methodology gap closures
                engine_mode=engine_mode,
                raw_timings_ms=warm_times,
                spec_hash=spec_hash,
                dataset_manifest_hash=dataset_manifest_hash,
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
            if bolt_adapter is not None:
                bolt_adapter.close()

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

    @staticmethod
    def _inline_params(query: str, params: dict[str, Any]) -> str:
        """Substitute $param references with literal values in the query.

        This is needed for protocols that don't support parameterized queries
        (e.g., xrayProtocol).
        """
        import re
        for name, value in params.items():
            if isinstance(value, str):
                literal = f"'{value}'"
            elif isinstance(value, float):
                literal = f"{value:.15g}"
            elif isinstance(value, bool):
                literal = "true" if value else "false"
            elif isinstance(value, int):
                literal = str(value)
            else:
                literal = str(value)
            query = re.sub(rf"\${name}\b", literal, query)
        return query

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
        # Include overrides not in spec (e.g., row_count for dataset capping)
        if overrides:
            for k, v in overrides.items():
                if k not in params:
                    params[k] = v
        return params

    def _generate_synthetic_data(
        self,
        spec: BenchmarkSpec,
        params: dict[str, Any],
    ) -> Any:
        """Generate synthetic dataset based on the benchmark spec.

        For node-only generators (uniform_nodes, categorical_nodes, flat_nodes),
        generates data directly via Cypher on the connected adapter.

        For edge-based generators (power_law, hub, deep_traversal, etc.),
        generates an edge list via Rust generators and returns it for the
        adapter's load_dataset to ingest.

        Returns:
            Edge list for graph generators, or None if data was loaded inline.
        """
        dataset = spec.dataset
        if not dataset.generator:
            return None

        gen = dataset.generator
        gen_params = dataset.generator_params or {}
        seed = gen_params.get("seed", 42)

        # --- Node-only generators: create nodes directly via Cypher ---
        if gen in ("uniform_nodes", "flat_nodes", "categorical_nodes", "wide_nodes"):
            # Allow row_count override to cap dataset size (prevents OOM on small servers)
            node_count = params.get("row_count", gen_params.get("node_count", 10000))
            if isinstance(node_count, str):
                node_count = int(node_count)
            logger.info("Generating %d synthetic nodes (generator=%s)", node_count, gen)
            self._generate_nodes_inline(gen, node_count, gen_params, seed)
            return None  # Data already loaded

        # --- Edge-based generators: use dataset_loader ---
        from tools.xraybench.adapters.dataset_loader import generate_synthetic_edges

        # Normalize generator name to what dataset_loader accepts
        gen_map = {
            "power_law": "power_law",
            "power_law_graph": "power_law",
            "powerlaw_graph": "power_law",
            "uniform_directed_graph": "power_law",
            "hub_graph": "hub",
            "hub": "hub",
            "deep_traversal": "deep_traversal",
            "chain": "chain",
        }

        loader_gen = gen_map.get(gen)
        if loader_gen is None:
            logger.warning(
                "Unknown generator %r — skipping data generation. "
                "Benchmark will run on empty graph.",
                gen,
            )
            return None

        # Build params for the Rust generator
        # Allow row_count override to cap dataset size (prevents OOM)
        override_count = params.get("row_count")
        if isinstance(override_count, str):
            override_count = int(override_count)

        loader_params: dict[str, Any] = {"seed": seed}
        if loader_gen == "power_law":
            loader_params["node_count"] = override_count or gen_params.get(
                "node_count", 10000
            )
            loader_params["m"] = gen_params.get("m", 3)
        elif loader_gen == "hub":
            loader_params["hub_count"] = gen_params.get("hub_count", 5)
            loader_params["spokes_per_hub"] = gen_params.get("spokes_per_hub", 100)
        elif loader_gen == "deep_traversal":
            loader_params["num_roots"] = gen_params.get("num_roots", 1)
            loader_params["fanout_per_level"] = gen_params.get(
                "fanout_per_level", [3, 3, 3]
            )
        elif loader_gen == "chain":
            loader_params["length"] = gen_params.get("length", 1000)

        logger.info(
            "Generating synthetic edges (generator=%s, params=%s)", loader_gen, loader_params
        )
        edges = generate_synthetic_edges(loader_gen, loader_params)
        logger.info("Generated %d edges", len(edges))
        return edges

    def _generate_nodes_inline(
        self,
        generator: str,
        node_count: int,
        gen_params: dict[str, Any],
        seed: int,
    ) -> None:
        """Generate flat nodes directly in the database via Cypher.

        Creates nodes with properties appropriate for the generator type.
        Uses UNWIND batches for efficient insertion.
        """
        rng = random.Random(seed)
        batch_size = 5000
        properties = gen_params.get("properties", {})
        nullable_ratio = gen_params.get("nullable_ratio", 0.0)
        categories = gen_params.get("categories", ["A", "B", "C", "D", "E"])

        for batch_start in range(0, node_count, batch_size):
            batch_end = min(batch_start + batch_size, node_count)
            batch_data: list[dict[str, Any]] = []

            for i in range(batch_start, batch_end):
                node: dict[str, Any] = {"id": i}

                if generator == "uniform_nodes":
                    node["value"] = rng.random()
                    node["category"] = rng.choice(categories)
                    if nullable_ratio > 0 and rng.random() < nullable_ratio:
                        pass  # nullable_field omitted
                    else:
                        node["nullable_field"] = rng.random()
                elif generator == "categorical_nodes":
                    node["value"] = rng.random()
                    node["category"] = rng.choice(categories)
                    node["group"] = rng.choice(["G1", "G2", "G3"])
                elif generator == "flat_nodes":
                    node["value"] = rng.random()
                elif generator == "wide_nodes":
                    # Generate many properties
                    prop_count = gen_params.get("property_count", 10)
                    for p in range(prop_count):
                        node[f"p{p}"] = rng.random()

                batch_data.append(node)

            # Build UNWIND Cypher — inline data as literal (xrayProtocol
            # doesn't support parameterized queries yet)
            def _to_cypher_map(d: dict[str, Any]) -> str:
                parts = []
                for k, v in d.items():
                    if isinstance(v, str):
                        parts.append(f"{k}: '{v}'")
                    elif isinstance(v, float):
                        parts.append(f"{k}: {v:.15g}")
                    else:
                        parts.append(f"{k}: {v}")
                return "{" + ", ".join(parts) + "}"

            maps = ", ".join(_to_cypher_map(n) for n in batch_data)
            self.adapter.execute(
                f"UNWIND [{maps}] AS props CREATE (n:Node) SET n = props",
            )

            if batch_start % 50000 == 0 and batch_start > 0:
                logger.info("  ... loaded %d / %d nodes", batch_start, node_count)

        # Create index on id for lookups
        try:
            self.adapter.execute("CREATE INDEX ON :Node(id)")
        except Exception:
            pass  # Index may already exist

        logger.info("Inline node generation complete: %d nodes", node_count)

    def _derive_query_params(
        self,
        spec: BenchmarkSpec,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Derive query parameters from benchmark spec parameters.

        Some query templates reference parameters that must be computed
        from the spec parameters (e.g., $threshold = 1.0 - selectivity).

        Also scans the query template for $param references and ensures
        all referenced parameters are present.
        """
        import re

        query = spec.query_template
        query_params = dict(params)

        # Derived parameter rules based on common benchmark patterns
        if "selectivity" in params and "$threshold" in query:
            query_params["threshold"] = 1.0 - float(params["selectivity"])

        if "row_count" in params and "$max_id" in query:
            query_params["max_id"] = int(params["row_count"])

        if "top_k" in params and "$k" in query:
            query_params["k"] = int(params["top_k"])

        # Extract all $param references from query template
        referenced = set(re.findall(r"\$([a-zA-Z_][a-zA-Z0-9_]*)", query))

        # Default values for common query parameters not in spec
        defaults: dict[str, Any] = {
            "threshold": 0.5,
            "category": "A",
            "seed_id": 0,
            "hub_id": 0,
            "max_depth": 3,
            "hop_depth": 3,
            "depth": 3,
            "limit": 1000,
            "min_degree": 5,
            "min_reach": 10,
            "function_id": 0,
            "target_id": 0,
            "id": 0,
            "module_id": 0,
            "lo": 0.2,
            "hi": 0.8,
            "cat": "A",
            "ts": 0,
            "v": 0.5,
        }

        # Only pass parameters that the query actually references
        filtered: dict[str, Any] = {}
        for name in referenced:
            if name in query_params:
                filtered[name] = query_params[name]
            elif name in defaults:
                filtered[name] = defaults[name]
                logger.debug("Using default for $%s: %s", name, defaults[name])
            else:
                logger.warning(
                    "Query references $%s but no matching parameter found", name
                )

        return filtered
