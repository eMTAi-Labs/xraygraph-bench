"""Abstract base adapter for graph database engines."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Any

from .capabilities import (
    CacheClearReport,
    Capability,
    ConnectionInfo,
    EngineInfo,
    EngineState,
    HealthStatus,
    LoadReport,
    ProfileResult,
    QueryPlan,
)
from ..models import (
    CorrectnessResult,
    DatasetManifest,
    DatasetSpec,
    ExecuteResult,
)


class BaseAdapter(ABC):
    """Abstract interface for graph database engine adapters.

    Every adapter must implement the abstract methods to participate in
    xraygraph-bench benchmark execution.
    """

    # ------------------------------------------------------------------
    # Abstract methods — must be implemented by every adapter
    # ------------------------------------------------------------------

    @abstractmethod
    def connect(self, config: dict[str, Any]) -> ConnectionInfo:
        """Establish a connection to the engine.

        Args:
            config: Engine-specific connection parameters.

        Returns:
            ConnectionInfo describing the established connection.
        """

    @abstractmethod
    def close(self) -> None:
        """Clean up connections and resources."""

    @abstractmethod
    def load_dataset(
        self,
        dataset: DatasetSpec | DatasetManifest,
        data_source: Any | None = None,
    ) -> LoadReport:
        """Ingest a dataset into the engine.

        Args:
            dataset: Dataset specification or manifest.
            data_source: Optional engine-specific data source reference.

        Returns:
            LoadReport describing the ingestion result.
        """

    @abstractmethod
    def execute(
        self, query: str, params: dict[str, Any] | None = None
    ) -> ExecuteResult:
        """Execute a query and return results with timing.

        Args:
            query: Query string (typically Cypher or GFQL).
            params: Optional query parameters.

        Returns:
            ExecuteResult with rows, wall_ms, compile_ms, and metadata.
        """

    @abstractmethod
    def clear_caches(self) -> CacheClearReport:
        """Clear query plan caches and execution caches.

        Returns:
            CacheClearReport indicating success and optional detail.
        """

    @abstractmethod
    def engine_info(self) -> EngineInfo:
        """Return static metadata about the engine.

        Returns:
            EngineInfo with name, version, build, and capabilities.
        """

    @abstractmethod
    def capabilities(self) -> set[Capability]:
        """Return the set of capabilities this adapter supports.

        Returns:
            Set of Capability enum members.
        """

    @abstractmethod
    def validate_correctness(
        self,
        result: ExecuteResult,
        oracle: CorrectnessResult,
    ) -> CorrectnessResult:
        """Check query results against the benchmark's correctness oracle.

        Args:
            result: The execution result to validate.
            oracle: Correctness oracle specification from the benchmark spec.

        Returns:
            CorrectnessResult with passed status and detail message.
        """

    # ------------------------------------------------------------------
    # Default methods — override for engine-specific behaviour
    # ------------------------------------------------------------------

    def health_check(self) -> HealthStatus:
        """Run a trivial query to verify the engine is responsive.

        Default implementation executes ``RETURN 1`` and measures latency.
        Override for engines that do not support Cypher.
        """
        start = time.monotonic()
        try:
            self.execute("RETURN 1")
            latency_ms = (time.monotonic() - start) * 1000.0
            return HealthStatus(healthy=True, latency_ms=latency_ms)
        except Exception as exc:
            latency_ms = (time.monotonic() - start) * 1000.0
            return HealthStatus(
                healthy=False,
                latency_ms=latency_ms,
                detail=str(exc),
            )

    def verify_dataset(self, manifest: DatasetManifest) -> bool:
        """Verify the loaded dataset matches the manifest counts.

        Queries the engine for node and edge counts and compares them
        against the manifest expectations.

        Args:
            manifest: Dataset manifest containing expected counts.

        Returns:
            True if counts match, False otherwise.
        """
        try:
            node_result = self.execute("MATCH (n) RETURN count(n) AS cnt")
            edge_result = self.execute("MATCH ()-[r]->() RETURN count(r) AS cnt")

            node_count = node_result.rows[0]["cnt"] if node_result.rows else 0
            edge_count = edge_result.rows[0]["cnt"] if edge_result.rows else 0

            return (
                int(node_count) == manifest.node_count
                and int(edge_count) == manifest.edge_count
            )
        except Exception:
            return False

    def clear_dataset(self) -> None:
        """Remove all nodes and relationships from the graph.

        Uses ``MATCH (n) DETACH DELETE n`` which is the standard Cypher
        idiom. Override for engines that require a different approach.
        """
        self.execute("MATCH (n) DETACH DELETE n")

    def explain(
        self, query: str, params: dict[str, Any] | None = None
    ) -> QueryPlan:
        """Return the execution plan for *query* without executing it.

        Default implementation wraps the query with ``EXPLAIN``.
        Override for engines that expose richer plan APIs.
        """
        plan_query = f"EXPLAIN {query}"
        result = self.execute(plan_query, params)
        return QueryPlan(operators=result.rows, raw=result.rows)

    def profile(
        self, query: str, params: dict[str, Any] | None = None
    ) -> ProfileResult:
        """Execute *query* and return detailed profiling data.

        Default implementation wraps the query with ``PROFILE``.
        Override for engines that expose richer profiling APIs.
        """
        profile_query = f"PROFILE {query}"
        result = self.execute(profile_query, params)
        operators = [dict(row) for row in result.rows]
        total_db_hits = sum(int(op.get("db_hits", 0)) for op in operators)
        total_rows = sum(int(op.get("rows", 0)) for op in operators)
        return ProfileResult(
            operators=operators,
            total_db_hits=total_db_hits,
            total_rows=total_rows,
            raw=result.rows,
        )

    def engine_state(self) -> EngineState:
        """Return a runtime snapshot of engine resource usage.

        Default implementation returns an empty EngineState.
        Override to query engine-specific metrics.
        """
        return EngineState()

    def engine_version(self) -> str:
        """Return the engine version string.

        Delegates to :meth:`engine_info` so adapters only need to
        implement one method.
        """
        return self.engine_info().version

    def collect_metrics(self) -> dict[str, Any]:
        """Gather engine-specific metrics after execution.

        Returns an empty dict by default. This method is retained for
        legacy compatibility; prefer :meth:`engine_state` for new code.

        Returns:
            Dictionary with available metrics.
        """
        return {}

