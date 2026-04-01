"""xrayGraphDB adapter for xraygraph-bench.

The most complete adapter, designed for xrayGraphDB's compiled execution
engine.  Extracts compilation timing, execution segments, materialization
boundaries, and cache/fallback/deopt status.

Configuration:
    host: xrayGraphDB hostname (default: localhost)
    port: Bolt port (default: 7688)
    username: Authentication user (default: empty)
    password: Authentication password (default: empty)
    database: Database name (default: xraygraph)
"""

from __future__ import annotations

import logging
from time import perf_counter
from typing import Any

from .base import BaseAdapter
from .capabilities import (
    CacheClearReport,
    Capability,
    ConnectionInfo,
    EngineInfo,
    EngineState,
    LoadReport,
    ProfileResult,
    QueryPlan,
)
from .validation import validate_oracle
from ..models import (
    CorrectnessResult,
    DatasetManifest,
    DatasetSpec,
    ExecuteResult,
)

logger = logging.getLogger(__name__)

_CAPABILITIES: frozenset[Capability] = frozenset(
    {
        Capability.COMPILE_TIME_REPORTING,
        Capability.PLAN_PROFILING,
        Capability.CACHE_CLEAR,
        Capability.EXPLAIN_ANALYZE,
        Capability.GFQL,
    }
)


class XrayGraphDBAdapter(BaseAdapter):
    """Adapter for xrayGraphDB compiled execution engine.

    Extends base adapter with xrayGraphDB-specific metric extraction:
    - compile_ms from execution profile
    - segment count from execution plan
    - breaker (materialization boundary) identification
    - plan cache hit/miss tracking
    - fallback and deoptimization detection
    """

    def __init__(self) -> None:
        self._driver: Any = None
        self._version: str = "unknown"
        self._build: str = "unknown"
        self._last_profile: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Abstract method implementations
    # ------------------------------------------------------------------

    def connect(self, config: dict[str, Any]) -> ConnectionInfo:
        """Establish a Bolt connection to xrayGraphDB.

        Args:
            config: Keys: host, port (default 7688), username, password, database.

        Returns:
            ConnectionInfo describing the established connection.
        """
        host: str = config.get("host", "localhost")
        port: int = int(config.get("port", 7688))
        username: str = config.get("username", "")
        password: str = config.get("password", "")

        try:
            import neo4j  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "The neo4j Python driver is required for the xrayGraphDB adapter. "
                "Run: pip install neo4j"
            ) from exc

        try:
            uri = f"bolt://{host}:{port}"
            auth = (username, password) if username else None
            self._driver = neo4j.GraphDatabase.driver(uri, auth=auth)

            # Verify liveness
            with self._driver.session() as session:
                session.run("RETURN 1 AS health").single()

            # Attempt to get version / build from mg.info()
            with self._driver.session() as session:
                try:
                    result = session.run("CALL mg.info()")
                    info: dict[str, str] = {}
                    for record in result:
                        key = record.get("key", "")
                        val = record.get("value", "")
                        if key:
                            info[key] = val
                    self._version = info.get("version", "unknown")
                    self._build = info.get("build", "unknown")
                except Exception:
                    self._version = "unknown"
                    self._build = "unknown"

            logger.info("Connected to xrayGraphDB %s at %s", self._version, uri)
            return ConnectionInfo(
                host=host,
                port=port,
                protocol="bolt",
                connected=True,
            )

        except Exception as exc:
            raise ConnectionError(
                f"Failed to connect to xrayGraphDB at {host}:{port}: {exc}"
            ) from exc

    def close(self) -> None:
        """Close the Bolt driver and release resources."""
        if self._driver is not None:
            self._driver.close()
            self._driver = None

    def load_dataset(
        self,
        dataset: DatasetSpec | DatasetManifest,
        data_source: Any | None = None,
    ) -> LoadReport:
        """Clear the graph, load via dataset_loader if data_source provided, verify counts.

        Args:
            dataset: Dataset specification or manifest.
            data_source: Optional edge list or generator config for dataset_loader.

        Returns:
            LoadReport with node/edge counts and timing.
        """
        if self._driver is None:
            raise RuntimeError("Not connected. Call connect() first.")

        start = perf_counter()
        errors: list[str] = []

        # Clear existing data
        self.clear_dataset()

        if data_source is not None:
            try:
                from .dataset_loader import load_edges_into_adapter

                load_edges_into_adapter(self, data_source)
            except Exception as exc:
                errors.append(f"dataset_loader error: {exc}")

        elapsed_ms = (perf_counter() - start) * 1000.0

        # Query actual counts
        try:
            node_res = self.execute("MATCH (n) RETURN count(n) AS cnt")
            node_count = int(node_res.rows[0]["cnt"]) if node_res.rows else 0
        except Exception as exc:
            errors.append(f"node count query failed: {exc}")
            node_count = 0

        try:
            edge_res = self.execute("MATCH ()-[r]->() RETURN count(r) AS cnt")
            edge_count = int(edge_res.rows[0]["cnt"]) if edge_res.rows else 0
        except Exception as exc:
            errors.append(f"edge count query failed: {exc}")
            edge_count = 0

        # Verify against manifest if available
        verified = False
        expected_nodes: int | None = None
        expected_edges: int | None = None
        if isinstance(dataset, DatasetManifest):
            expected_nodes = dataset.node_count
            expected_edges = dataset.edge_count
            verified = (node_count == expected_nodes and edge_count == expected_edges)
        else:
            verified = len(errors) == 0

        return LoadReport(
            node_count=node_count,
            edge_count=edge_count,
            load_time_ms=round(elapsed_ms, 2),
            verified=verified,
            expected_nodes=expected_nodes,
            expected_edges=expected_edges,
            errors=errors,
        )

    def execute(self, query: str, params: dict[str, Any] | None = None) -> ExecuteResult:
        """Execute a Cypher query and return results with timing.

        Args:
            query: Cypher query string.
            params: Optional query parameters.

        Returns:
            ExecuteResult with rows, wall_ms, compile_ms, and metadata.
        """
        if self._driver is None:
            raise RuntimeError("Not connected. Call connect() first.")

        start = perf_counter()
        with self._driver.session() as session:
            result = session.run(query, parameters=params or {})
            rows = [dict(record) for record in result]
            summary = result.consume()
        wall_ms = (perf_counter() - start) * 1000.0

        compile_ms = self._extract_compile_time(summary)
        self._last_profile = self._extract_profile(summary)

        metadata: dict[str, Any] = {}
        if hasattr(summary, "result_available_after") and summary.result_available_after is not None:
            metadata["result_available_after_ms"] = summary.result_available_after
        if self._last_profile:
            metadata["profile"] = self._last_profile

        return ExecuteResult(
            rows=rows,
            wall_ms=round(wall_ms, 2),
            compile_ms=round(compile_ms, 2) if compile_ms is not None else None,
            metadata=metadata,
        )

    def clear_caches(self) -> CacheClearReport:
        """Clear xrayGraphDB plan and execution caches.

        Tries "FREE MEMORY" and "CALL mg.clear_cache()" in sequence.

        Returns:
            CacheClearReport indicating success/failure.
        """
        if self._driver is None:
            return CacheClearReport(cleared=False, detail="Not connected")

        cleared = False
        messages: list[str] = []

        with self._driver.session() as session:
            for cmd in ("FREE MEMORY", "CALL mg.clear_cache()"):
                try:
                    session.run(cmd)
                    cleared = True
                    messages.append(f"{cmd}: ok")
                except Exception as exc:
                    messages.append(f"{cmd}: {exc}")

        self._last_profile = {}
        return CacheClearReport(cleared=cleared, detail="; ".join(messages))

    def engine_info(self) -> EngineInfo:
        """Return static metadata about xrayGraphDB.

        Returns:
            EngineInfo with name, version, build, and capabilities.
        """
        return EngineInfo(
            name="xraygraphdb",
            version=self._version,
            build=self._build,
            capabilities=set(_CAPABILITIES),
        )

    def capabilities(self) -> set[Capability]:
        """Return the full set of capabilities this adapter supports.

        Returns:
            Set of Capability enum members.
        """
        return set(_CAPABILITIES)

    def validate_correctness(
        self,
        result: ExecuteResult,
        oracle: CorrectnessResult,
    ) -> CorrectnessResult:
        """Delegate correctness checking to the shared validate_oracle helper.

        Args:
            result: The execution result to validate.
            oracle: Correctness oracle specification (dict or CorrectnessResult).

        Returns:
            CorrectnessResult with passed status and detail message.
        """
        # oracle may be passed as a dict from the runner framework
        if isinstance(oracle, dict):
            return validate_oracle(result, oracle)
        # If it's already a CorrectnessResult (e.g. from test code), return as-is
        return oracle

    # ------------------------------------------------------------------
    # Overridden default methods
    # ------------------------------------------------------------------

    def explain(
        self, query: str, params: dict[str, Any] | None = None
    ) -> QueryPlan:
        """Return the execution plan for *query* without executing it.

        Args:
            query: Cypher query string.
            params: Optional query parameters.

        Returns:
            QueryPlan with operators list and raw plan.
        """
        plan_query = f"EXPLAIN {query}"
        result = self.execute(plan_query, params)
        profile = result.metadata.get("profile", {})
        operators = profile.get("operators", result.rows)
        return QueryPlan(operators=operators, raw=profile or result.rows)

    def profile(
        self, query: str, params: dict[str, Any] | None = None
    ) -> ProfileResult:
        """Execute *query* and return detailed profiling data.

        Args:
            query: Cypher query string.
            params: Optional query parameters.

        Returns:
            ProfileResult with operators, db_hits, and row counts.
        """
        profile_query = f"PROFILE {query}"
        result = self.execute(profile_query, params)
        profile = result.metadata.get("profile", {})
        operators: list[dict[str, Any]] = profile.get("operators", [])
        total_db_hits = sum(int(op.get("db_hits", 0)) for op in operators)
        total_rows = sum(int(op.get("rows", 0)) for op in operators)
        return ProfileResult(
            operators=operators,
            total_db_hits=total_db_hits,
            total_rows=total_rows,
            raw=profile or result.rows,
        )

    def engine_state(self) -> EngineState:
        """Return a runtime snapshot of xrayGraphDB resource usage.

        Queries mg.info() for memory metrics.

        Returns:
            EngineState with memory fields populated when available.
        """
        if self._driver is None:
            return EngineState()

        memory_used_mb: float | None = None
        memory_available_mb: float | None = None

        try:
            with self._driver.session() as session:
                result = session.run("CALL mg.info()")
                for record in result:
                    key = record.get("key", "")
                    val = record.get("value", "")
                    if key == "memory_used":
                        try:
                            memory_used_mb = float(val) / (1024 * 1024)
                        except (TypeError, ValueError):
                            pass
                    elif key == "memory_available":
                        try:
                            memory_available_mb = float(val) / (1024 * 1024)
                        except (TypeError, ValueError):
                            pass
        except Exception:
            pass

        return EngineState(
            memory_used_mb=memory_used_mb,
            memory_available_mb=memory_available_mb,
        )

    def collect_metrics(self) -> dict[str, Any]:
        """Gather xrayGraphDB execution metrics from the last profile.

        Returns:
            Dict with keys: segments, breakers, cache_hit, fallback, deopt.
        """
        profile = self._last_profile

        segments: int | None = profile.get("segment_count")

        breakers: list[str] = []
        for op in profile.get("operators", []):
            if op.get("is_breaker", False):
                breakers.append(op.get("name", "unknown"))

        cache_hit: bool = bool(profile.get("cache_hit", False))
        fallback: bool = bool(profile.get("fallback", False))
        deopt: bool = bool(profile.get("deopt", False))

        return {
            "segments": segments,
            "breakers": breakers,
            "cache_hit": cache_hit,
            "fallback": fallback,
            "deopt": deopt,
        }

    # ------------------------------------------------------------------
    # GFQL support
    # ------------------------------------------------------------------

    def execute_gfql(
        self,
        gfql_query: str,
        tenant_id: str,
        repo_id: str,
        params: dict[str, Any] | None = None,
    ) -> ExecuteResult:
        """Execute a GFQL query with tenant/repo context.

        Sets GFQL_CONTEXT for tenant_id and repo_id before running the query.

        Args:
            gfql_query: GFQL query string.
            tenant_id: Tenant identifier for the GFQL context.
            repo_id: Repository identifier for the GFQL context.
            params: Optional query parameters.

        Returns:
            ExecuteResult with rows, wall_ms, compile_ms, and metadata.
        """
        if self._driver is None:
            raise RuntimeError("Not connected. Call connect() first.")

        ctx_stmt = self._gfql_context_statement(tenant_id, repo_id)

        with self._driver.session() as session:
            # Set GFQL context
            session.run(ctx_stmt)

        # Execute the GFQL query via normal execute path
        return self.execute(gfql_query, params)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _gfql_context_statement(self, tenant_id: str, repo_id: str) -> str:
        """Build the SET GFQL_CONTEXT statement for tenant/repo scoping.

        Args:
            tenant_id: Tenant identifier.
            repo_id: Repository identifier.

        Returns:
            Cypher-like statement string to set the GFQL context.
        """
        return (
            f"SET GFQL_CONTEXT tenant_id = '{tenant_id}', repo_id = '{repo_id}'"
        )

    def _breaker_operator_names(self) -> frozenset[str]:
        """Return the set of operator names that are materialization breakers.

        Breakers are pipeline boundaries that force full materialisation of
        their input before producing output.

        Returns:
            Frozenset of operator name strings.
        """
        return frozenset(
            {"Sort", "Aggregate", "Distinct", "OrderBy", "HashJoin", "Accumulate", "Unwind"}
        )

    def _extract_compile_time(self, summary: Any) -> float | None:
        """Extract compilation time from execution summary metadata.

        Args:
            summary: neo4j ResultSummary object.

        Returns:
            Compile time in milliseconds, or None if not available.
        """
        if hasattr(summary, "metadata") and isinstance(summary.metadata, dict):
            raw = summary.metadata.get("compile_ms")
            if raw is not None:
                try:
                    return float(raw)
                except (TypeError, ValueError):
                    pass
        return None

    def _extract_profile(self, summary: Any) -> dict[str, Any]:
        """Extract execution profile from result summary.

        Args:
            summary: neo4j ResultSummary object.

        Returns:
            Dict with plan_type, operators list, and segment_count.
        """
        profile: dict[str, Any] = {}

        if hasattr(summary, "profile") and summary.profile is not None:
            profile["plan_type"] = "profile"
            profile["operators"] = self._flatten_plan(summary.profile)
            profile["segment_count"] = len(profile["operators"])

        if hasattr(summary, "plan") and summary.plan is not None:
            if "plan_type" not in profile:
                profile["plan_type"] = "plan"

        return profile

    def _flatten_plan(self, plan_node: Any) -> list[dict[str, Any]]:
        """Recursively flatten a query plan tree into an ordered operator list.

        Marks breaker operators (materialization boundaries) with is_breaker=True.

        Args:
            plan_node: A neo4j plan/profile node with optional .children attribute.

        Returns:
            Flat list of operator dicts.
        """
        operators: list[dict[str, Any]] = []

        if plan_node is None:
            return operators

        name: str = getattr(plan_node, "operator_type", "unknown")
        op: dict[str, Any] = {
            "name": name,
            "is_breaker": name in self._breaker_operator_names(),
        }

        if hasattr(plan_node, "db_hits"):
            op["db_hits"] = plan_node.db_hits
        if hasattr(plan_node, "rows"):
            op["rows"] = plan_node.rows
        if hasattr(plan_node, "arguments") and plan_node.arguments:
            op["arguments"] = dict(plan_node.arguments)

        operators.append(op)

        for child in getattr(plan_node, "children", []):
            operators.extend(self._flatten_plan(child))

        return operators
