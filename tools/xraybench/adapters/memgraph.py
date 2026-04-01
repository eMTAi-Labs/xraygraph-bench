"""Memgraph adapter for xraygraph-bench.

Connects to Memgraph via the Bolt protocol using the neo4j Python driver
(which Memgraph is wire-compatible with). Translates benchmark operations
into Memgraph-compatible Cypher queries.

Configuration:
    host: Memgraph hostname (default: localhost)
    port: Bolt port (default: 7687)
    username: Authentication user (default: empty — no auth)
    password: Authentication password (default: empty)
    database: Database name (default: memgraph)

Capabilities: CACHE_CLEAR, PLAN_PROFILING, MEMORY_REPORTING
Not supported: GFQL, NATIVE_PROTOCOL
"""

from __future__ import annotations

import logging
import time
from typing import Any

from .base import BaseAdapter
from .capabilities import (
    CacheClearReport,
    Capability,
    ConnectionInfo,
    EngineInfo,
    EngineState,
    LoadReport,
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
        Capability.CACHE_CLEAR,
        Capability.PLAN_PROFILING,
        Capability.MEMORY_REPORTING,
    }
)


class MemgraphAdapter(BaseAdapter):
    """Adapter for Memgraph in-memory graph database.

    Supports plan profiling via PROFILE/EXPLAIN prefixes, memory cache
    freeing via ``FREE MEMORY``, and memory usage reporting via
    ``CALL mg.info()``. Does not support GFQL or the native binary protocol.
    """

    def __init__(self) -> None:
        self._driver: Any = None
        self._version: str = "unknown"
        self._host: str = "localhost"
        self._port: int = 7687

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self, config: dict[str, Any]) -> ConnectionInfo:
        """Establish a Bolt connection to Memgraph.

        Args:
            config: Mapping with optional keys host, port, username, password.
                    Unlike the Neo4j adapter, password is not required.
                    Auth is omitted entirely when username is empty.

        Returns:
            ConnectionInfo describing the established connection.

        Raises:
            ImportError: If the neo4j Python driver is not installed.
            ConnectionError: If the connection attempt fails.
        """
        self._host = config.get("host", "localhost")
        self._port = int(config.get("port", 7687))
        username: str = config.get("username", "")
        password: str = config.get("password", "")

        try:
            import neo4j  # type: ignore[import-untyped]

            uri = f"bolt://{self._host}:{self._port}"
            auth = (username, password) if username else None
            self._driver = neo4j.GraphDatabase.driver(uri, auth=auth)

            with self._driver.session() as session:
                result = session.run("CALL mg.info() YIELD key, value RETURN *")
                info = {record["key"]: record["value"] for record in result}
                self._version = info.get("version", "unknown")

            logger.info("Connected to Memgraph %s at %s", self._version, uri)

        except ImportError:
            raise ImportError(
                "The neo4j Python driver is required for the Memgraph adapter. "
                "Run: pip install neo4j"
            )
        except Exception as exc:
            raise ConnectionError(
                f"Failed to connect to Memgraph at {self._host}:{self._port}: {exc}"
            ) from exc

        return ConnectionInfo(
            host=self._host,
            port=self._port,
            protocol="bolt",
            connected=True,
            database=None,  # Memgraph does not use named databases in the same way
        )

    def close(self) -> None:
        """Close the Memgraph driver and release resources."""
        if self._driver is not None:
            self._driver.close()
            self._driver = None

    # ------------------------------------------------------------------
    # Dataset management
    # ------------------------------------------------------------------

    def load_dataset(
        self,
        dataset: DatasetSpec | DatasetManifest,
        data_source: Any | None = None,
    ) -> LoadReport:
        """Ingest a dataset into Memgraph.

        Clears existing data, then optionally loads edges from *data_source*
        using the shared dataset loader. If *data_source* is None the graph
        is left empty.

        Args:
            dataset: Dataset specification or manifest.
            data_source: Optional edge list ``list[tuple[int, int]]`` produced
                         by :func:`~tools.xraybench.adapters.dataset_loader.generate_synthetic_edges`.

        Returns:
            LoadReport with node/edge counts and elapsed load time.
        """
        if self._driver is None:
            raise RuntimeError("Not connected. Call connect() first.")

        start = time.perf_counter()

        logger.info(
            "Loading dataset '%s' (type=%s) into Memgraph",
            dataset.name,
            dataset.type,
        )

        # Clear existing graph data
        with self._driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

        if data_source is not None:
            from . import dataset_loader

            node_count, edge_count = dataset_loader.load_edges_into_adapter(
                self, data_source
            )
        else:
            with self._driver.session() as session:
                node_count = session.run(
                    "MATCH (n) RETURN count(n) AS c"
                ).single()["c"]
                edge_count = session.run(
                    "MATCH ()-[r]->() RETURN count(r) AS c"
                ).single()["c"]

        elapsed_ms = (time.perf_counter() - start) * 1000

        expected_nodes: int | None = None
        expected_edges: int | None = None
        if isinstance(dataset, DatasetManifest):
            expected_nodes = dataset.node_count
            expected_edges = dataset.edge_count

        return LoadReport(
            node_count=int(node_count),
            edge_count=int(edge_count),
            load_time_ms=round(elapsed_ms, 2),
            verified=True,
            expected_nodes=expected_nodes,
            expected_edges=expected_edges,
        )

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def execute(
        self, query: str, params: dict[str, Any] | None = None
    ) -> ExecuteResult:
        """Execute a Cypher query against Memgraph.

        Args:
            query: Cypher query string. May be prefixed with PROFILE or
                   EXPLAIN for plan information.
            params: Optional query parameters.

        Returns:
            ExecuteResult with rows and wall_ms. compile_ms is always None
            because Memgraph does not expose compilation time separately.
        """
        if self._driver is None:
            raise RuntimeError("Not connected. Call connect() first.")

        start = time.perf_counter()
        with self._driver.session() as session:
            result = session.run(query, parameters=params or {})
            rows = [dict(record) for record in result]
        wall_ms = (time.perf_counter() - start) * 1000

        return ExecuteResult(
            rows=rows,
            wall_ms=round(wall_ms, 2),
            compile_ms=None,  # Memgraph does not expose compile time separately
        )

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def clear_caches(self) -> CacheClearReport:
        """Free Memgraph memory caches via ``FREE MEMORY``.

        Returns:
            CacheClearReport indicating whether the operation succeeded.
        """
        if self._driver is None:
            return CacheClearReport(cleared=False, detail="driver not connected")

        try:
            with self._driver.session() as session:
                session.run("FREE MEMORY")
            return CacheClearReport(cleared=True)
        except Exception as exc:
            logger.debug("Memory cache clearing failed: %s", exc)
            return CacheClearReport(cleared=False, detail=str(exc))

    # ------------------------------------------------------------------
    # Capability and engine metadata
    # ------------------------------------------------------------------

    def capabilities(self) -> set[Capability]:
        """Return the capabilities this adapter supports.

        Memgraph supports CACHE_CLEAR, PLAN_PROFILING, and MEMORY_REPORTING.
        GFQL and NATIVE_PROTOCOL are not supported.

        Returns:
            Set of supported Capability enum members.
        """
        return set(_CAPABILITIES)

    def engine_info(self) -> EngineInfo:
        """Return static metadata about the Memgraph engine.

        Returns:
            EngineInfo with name 'memgraph', current version, and capabilities.
        """
        return EngineInfo(
            name="memgraph",
            version=self._version,
            build="release",
            capabilities=set(_CAPABILITIES),
        )

    def engine_state(self) -> EngineState:
        """Query Memgraph memory usage via ``CALL mg.info()``.

        Returns:
            EngineState with memory_used_mb populated if available,
            or an empty EngineState if the driver is not connected or
            the query fails.
        """
        if self._driver is None:
            return EngineState()

        try:
            with self._driver.session() as session:
                result = session.run("CALL mg.info() YIELD key, value RETURN *")
                info = {record["key"]: record["value"] for record in result}

            memory_usage = info.get("memory_usage")
            memory_used_mb: float | None = None
            if memory_usage is not None:
                try:
                    # Memgraph may return bytes as int or a string with units
                    if isinstance(memory_usage, (int, float)):
                        memory_used_mb = float(memory_usage) / (1024 * 1024)
                    elif isinstance(memory_usage, str):
                        # Strip trailing unit suffix if present (e.g. "134217728 B")
                        numeric_part = memory_usage.split()[0]
                        memory_used_mb = float(numeric_part) / (1024 * 1024)
                except (ValueError, IndexError):
                    memory_used_mb = None

            return EngineState(memory_used_mb=memory_used_mb)
        except Exception as exc:
            logger.debug("engine_state query failed: %s", exc)
            return EngineState()

    # ------------------------------------------------------------------
    # Correctness validation
    # ------------------------------------------------------------------

    def validate_correctness(
        self,
        result: ExecuteResult,
        oracle: CorrectnessResult,
    ) -> CorrectnessResult:
        """Validate execution results against the correctness oracle.

        Delegates to the shared :func:`~tools.xraybench.adapters.validation.validate_oracle`
        implementation.

        Args:
            result: Execution result to validate.
            oracle: Correctness oracle specification (passed as dict).

        Returns:
            CorrectnessResult with passed flag and detail message.
        """
        if isinstance(oracle, dict):
            return validate_oracle(result, oracle)
        return validate_oracle(result, vars(oracle))

    # ------------------------------------------------------------------
    # Metrics collection
    # ------------------------------------------------------------------

    def collect_metrics(self) -> dict[str, Any]:
        """Return engine metrics (all None/empty for Memgraph).

        Returns:
            Dict with segments, breakers, buffer_repr, cache_hit,
            fallback, and deopt keys — all at their zero/None defaults.
        """
        return {
            "segments": None,
            "breakers": [],
            "buffer_repr": None,
            "cache_hit": False,
            "fallback": False,
            "deopt": False,
        }
