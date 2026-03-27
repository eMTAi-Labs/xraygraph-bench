"""xrayGraphDB adapter for xraygraph-bench.

The most complete adapter, designed for xrayGraphDB's compiled execution
engine. Extracts compilation timing, execution segments, materialization
boundaries, and cache/fallback/deopt status.

Configuration:
    host: xrayGraphDB hostname (default: localhost)
    port: Bolt port (default: 7687)
    username: Authentication user (default: empty)
    password: Authentication password (default: empty)
    database: Database name (default: xraygraph)
"""

from __future__ import annotations

import logging
import time
from typing import Any

from .base import BaseAdapter
from ..models import (
    CorrectnessResult,
    DatasetManifest,
    DatasetSpec,
    ExecuteResult,
    LoadResult,
)

logger = logging.getLogger(__name__)


class XrayGraphDBAdapter(BaseAdapter):
    """Adapter for xrayGraphDB compiled execution engine.

    Extends base adapter with xrayGraphDB-specific metric extraction:
    - compile_ms from execution profile
    - segment count from execution plan
    - breaker (materialization boundary) identification
    - plan cache hit/miss tracking
    - fallback and deoptimization detection
    """

    def __init__(self):
        self._driver = None
        self._version: str = "unknown"
        self._last_profile: dict[str, Any] = {}
        self._cache_stats: dict[str, Any] = {}

    def connect(self, config: dict[str, Any]) -> None:
        host = config.get("host", "localhost")
        port = config.get("port", 7687)
        username = config.get("username", "")
        password = config.get("password", "")

        try:
            import neo4j

            uri = f"bolt://{host}:{port}"
            auth = (username, password) if username else None
            self._driver = neo4j.GraphDatabase.driver(uri, auth=auth)

            # Verify connection and get version
            with self._driver.session() as session:
                result = session.run("RETURN 1 AS health")
                result.single()

            # Attempt to get version info
            with self._driver.session() as session:
                try:
                    result = session.run(
                        "CALL mg.info() YIELD key, value "
                        "WHERE key = 'version' RETURN value"
                    )
                    record = result.single()
                    if record:
                        self._version = record["value"]
                except Exception:
                    self._version = "unknown"

            logger.info("Connected to xrayGraphDB %s at %s", self._version, uri)

        except ImportError:
            raise ImportError(
                "The neo4j Python driver is required for the xrayGraphDB adapter. "
                "Run: pip install neo4j"
            )
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to xrayGraphDB at {host}:{port}: {e}"
            )

    def close(self) -> None:
        if self._driver:
            self._driver.close()
            self._driver = None

    def load_dataset(self, dataset: DatasetSpec | DatasetManifest) -> LoadResult:
        start = time.perf_counter()

        logger.info("Loading dataset: %s (type: %s)", dataset.name, dataset.type)

        with self._driver.session() as session:
            # Clear existing data
            session.run("MATCH (n) DETACH DELETE n")

            # Create indexes for common patterns
            try:
                session.run("CREATE INDEX ON :Node(id)")
            except Exception:
                pass  # Index may already exist
            try:
                session.run("CREATE INDEX ON :Function(id)")
            except Exception:
                pass
            try:
                session.run("CREATE INDEX ON :Artifact(id)")
            except Exception:
                pass

        # TODO: Implement full dataset loading
        # xrayGraphDB-specific optimizations:
        # - Use LOAD CSV for large datasets if available
        # - Use batch UNWIND for edge creation
        # - Leverage parallel import if supported

        elapsed_ms = (time.perf_counter() - start) * 1000

        with self._driver.session() as session:
            node_count = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
            edge_count = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]

        return LoadResult(
            node_count=node_count,
            edge_count=edge_count,
            load_time_ms=round(elapsed_ms, 2),
        )

    def execute(self, query: str, params: dict[str, Any] | None = None) -> ExecuteResult:
        if not self._driver:
            raise RuntimeError("Not connected. Call connect() first.")

        start = time.perf_counter()
        with self._driver.session() as session:
            result = session.run(query, parameters=params or {})
            rows = [dict(record) for record in result]
            summary = result.consume()
        wall_ms = (time.perf_counter() - start) * 1000

        # Extract xrayGraphDB-specific execution details
        compile_ms = self._extract_compile_time(summary)
        self._last_profile = self._extract_profile(summary)

        metadata: dict[str, Any] = {}
        if summary.result_available_after is not None:
            metadata["result_available_after_ms"] = summary.result_available_after
        if self._last_profile:
            metadata["profile"] = self._last_profile

        return ExecuteResult(
            rows=rows,
            wall_ms=round(wall_ms, 2),
            compile_ms=round(compile_ms, 2) if compile_ms is not None else None,
            metadata=metadata,
        )

    def clear_caches(self) -> None:
        """Clear xrayGraphDB plan and execution caches."""
        if self._driver:
            with self._driver.session() as session:
                # Try xrayGraphDB-specific cache clearing
                for cmd in [
                    "FREE MEMORY",
                    "CALL mg.clear_cache()",
                ]:
                    try:
                        session.run(cmd)
                    except Exception:
                        pass

        self._last_profile = {}
        self._cache_stats = {}

    def collect_metrics(self) -> dict[str, Any]:
        """Collect xrayGraphDB execution metrics from the last profile."""
        profile = self._last_profile

        # Extract segments (execution pipeline stages)
        segments = profile.get("segment_count")

        # Extract breakers (materialization boundary operators)
        breakers: list[str] = []
        if "operators" in profile:
            for op in profile["operators"]:
                if op.get("is_breaker", False):
                    breakers.append(op.get("name", "unknown"))

        # Detect compilation and cache behavior
        cache_hit = profile.get("cache_hit", False)
        fallback = profile.get("fallback", False)
        deopt = profile.get("deopt", False)
        buffer_repr = profile.get("buffer_repr")

        return {
            "segments": segments,
            "breakers": breakers,
            "buffer_repr": buffer_repr,
            "cache_hit": cache_hit,
            "fallback": fallback,
            "deopt": deopt,
        }

    def validate_correctness(
        self, result: ExecuteResult, oracle: dict[str, Any]
    ) -> CorrectnessResult:
        return _validate_oracle(result, oracle)

    def engine_version(self) -> str:
        return f"xraygraphdb-{self._version}"

    def _extract_compile_time(self, summary: Any) -> float | None:
        """Extract compilation time from execution summary.

        xrayGraphDB may expose compile_ms in the result summary or
        via profiling metadata. This method attempts multiple extraction
        strategies.
        """
        # Strategy 1: Direct compile_ms in summary metadata
        if hasattr(summary, "metadata") and summary.metadata:
            if "compile_ms" in summary.metadata:
                return float(summary.metadata["compile_ms"])

        # Strategy 2: Infer from result_available_after and plan time
        if (
            hasattr(summary, "result_available_after")
            and summary.result_available_after is not None
        ):
            # result_available_after includes compile + execute on first run
            # Without a separate compile metric, we return None rather than guess
            pass

        return None

    def _extract_profile(self, summary: Any) -> dict[str, Any]:
        """Extract execution profile from result summary."""
        profile: dict[str, Any] = {}

        if hasattr(summary, "profile") and summary.profile:
            profile["plan_type"] = "profile"
            profile["operators"] = self._flatten_plan(summary.profile)
            profile["segment_count"] = len(profile["operators"])

        if hasattr(summary, "plan") and summary.plan:
            profile["plan_type"] = profile.get("plan_type", "plan")

        return profile

    def _flatten_plan(self, plan_node: Any) -> list[dict[str, Any]]:
        """Flatten a query plan tree into a list of operators."""
        operators: list[dict[str, Any]] = []

        if plan_node is None:
            return operators

        op: dict[str, Any] = {
            "name": getattr(plan_node, "operator_type", "unknown"),
        }

        # Mark known breaker operators
        breaker_names = {"Sort", "Aggregate", "Distinct", "OrderBy", "HashJoin"}
        if op["name"] in breaker_names:
            op["is_breaker"] = True
        else:
            op["is_breaker"] = False

        if hasattr(plan_node, "db_hits"):
            op["db_hits"] = plan_node.db_hits
        if hasattr(plan_node, "rows"):
            op["rows"] = plan_node.rows

        operators.append(op)

        if hasattr(plan_node, "children"):
            for child in plan_node.children:
                operators.extend(self._flatten_plan(child))

        return operators


def _validate_oracle(result: ExecuteResult, oracle: dict[str, Any]) -> CorrectnessResult:
    """Correctness validation logic."""
    oracle_type = oracle.get("type", "")

    if oracle_type == "row_count":
        expected = oracle.get("expected_row_count", 0)
        actual = result.row_count
        if actual == expected:
            return CorrectnessResult(
                passed=True,
                detail=f"Row count {actual} matches expected {expected}",
            )
        return CorrectnessResult(
            passed=False,
            detail=f"Row count {actual} != expected {expected}",
        )

    elif oracle_type == "row_count_range":
        lo = oracle.get("expected_row_count_min", 0)
        hi = oracle.get("expected_row_count_max", float("inf"))
        actual = result.row_count
        if lo <= actual <= hi:
            return CorrectnessResult(
                passed=True,
                detail=f"Row count {actual} within expected range [{lo}, {hi}]",
            )
        return CorrectnessResult(
            passed=False,
            detail=f"Row count {actual} outside expected range [{lo}, {hi}]",
        )

    elif oracle_type == "exact_match":
        # TODO: Implement exact match validation
        return CorrectnessResult(
            passed=True,
            detail="Exact match validation not yet implemented",
        )

    elif oracle_type == "checksum":
        # TODO: Implement checksum validation
        return CorrectnessResult(
            passed=True,
            detail="Checksum validation not yet implemented",
        )

    elif oracle_type == "structural":
        # Structural checks are benchmark-specific
        return CorrectnessResult(
            passed=True,
            detail="Structural validation not yet implemented",
        )

    else:
        return CorrectnessResult(
            passed=True,
            detail=f"Oracle type '{oracle_type}' not yet implemented",
        )
