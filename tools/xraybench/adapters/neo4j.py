"""Neo4j adapter for xraygraph-bench.

Connects to Neo4j via the Bolt protocol using the official neo4j Python
driver. Translates benchmark operations into Neo4j-compatible Cypher queries.

Configuration:
    host: Neo4j hostname (default: localhost)
    port: Bolt port (default: 7687)
    username: Authentication user (default: neo4j)
    password: Authentication password (required — raises ValueError if missing)
    database: Database name (default: neo4j)

Capabilities: CACHE_CLEAR, PLAN_PROFILING
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
    {Capability.CACHE_CLEAR, Capability.PLAN_PROFILING}
)


class Neo4jAdapter(BaseAdapter):
    """Adapter for Neo4j graph database.

    Supports plan profiling via PROFILE/EXPLAIN prefixes and query cache
    clearing via ``CALL db.clearQueryCaches()``. Does not support GFQL or
    the native binary protocol.
    """

    def __init__(self) -> None:
        self._driver: Any = None
        self._database: str = "neo4j"
        self._version: str = "unknown"
        self._host: str = "localhost"
        self._port: int = 7687

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self, config: dict[str, Any]) -> ConnectionInfo:
        """Establish a Bolt connection to Neo4j.

        Args:
            config: Mapping with keys host, port, username, password, database.
                    ``password`` is required; a ValueError is raised if absent.

        Returns:
            ConnectionInfo describing the established connection.

        Raises:
            ValueError: If ``password`` is not provided in config.
            ImportError: If the neo4j Python driver is not installed.
            ConnectionError: If the connection attempt fails.
        """
        self._host = config.get("host", "localhost")
        self._port = int(config.get("port", 7687))
        username: str = config.get("username", "neo4j")
        password: str = config.get("password", "")
        self._database = config.get("database", "neo4j")

        if not password:
            raise ValueError("Neo4j adapter requires a password in config")

        try:
            import neo4j  # type: ignore[import-untyped]

            uri = f"bolt://{self._host}:{self._port}"
            self._driver = neo4j.GraphDatabase.driver(uri, auth=(username, password))

            with self._driver.session(database=self._database) as session:
                result = session.run(
                    "CALL dbms.components() YIELD name, versions "
                    "RETURN versions[0] AS version"
                )
                record = result.single()
                if record:
                    self._version = record["version"]

            logger.info("Connected to Neo4j %s at %s", self._version, uri)

        except ImportError:
            raise ImportError(
                "The neo4j Python driver is required for the Neo4j adapter. "
                "Run: pip install neo4j"
            )
        except Exception as exc:
            raise ConnectionError(
                f"Failed to connect to Neo4j at {self._host}:{self._port}: {exc}"
            ) from exc

        return ConnectionInfo(
            host=self._host,
            port=self._port,
            protocol="bolt",
            connected=True,
            database=self._database,
        )

    def close(self) -> None:
        """Close the Neo4j driver and release resources."""
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
        """Ingest a dataset into Neo4j.

        Clears existing data, then optionally loads edges from *data_source*
        using the shared dataset loader. If *data_source* is None the graph
        is left empty (useful for manually constructed benchmarks).

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
            "Loading dataset '%s' (type=%s) into Neo4j database '%s'",
            dataset.name,
            dataset.type,
            self._database,
        )

        # Clear existing graph data
        with self._driver.session(database=self._database) as session:
            session.run("MATCH (n) DETACH DELETE n")

        if data_source is not None:
            from . import dataset_loader

            node_count, edge_count = dataset_loader.load_edges_into_adapter(
                self, data_source
            )
        else:
            with self._driver.session(database=self._database) as session:
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
        """Execute a Cypher query against Neo4j.

        Args:
            query: Cypher query string. May be prefixed with PROFILE or
                   EXPLAIN for plan information.
            params: Optional query parameters.

        Returns:
            ExecuteResult with rows, wall_ms, and Neo4j summary metadata.
        """
        if self._driver is None:
            raise RuntimeError("Not connected. Call connect() first.")

        start = time.perf_counter()
        with self._driver.session(database=self._database) as session:
            result = session.run(query, parameters=params or {})
            rows = [dict(record) for record in result]
            summary = result.consume()
        wall_ms = (time.perf_counter() - start) * 1000

        metadata: dict[str, Any] = {}
        if summary.result_available_after is not None:
            metadata["result_available_after_ms"] = summary.result_available_after
        if summary.result_consumed_after is not None:
            metadata["result_consumed_after_ms"] = summary.result_consumed_after
        if summary.plan:
            metadata["plan"] = str(summary.plan)

        return ExecuteResult(
            rows=rows,
            wall_ms=round(wall_ms, 2),
            compile_ms=None,  # Neo4j does not expose compile time separately
            metadata=metadata,
        )

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def clear_caches(self) -> CacheClearReport:
        """Clear Neo4j query plan caches via ``CALL db.clearQueryCaches()``.

        Returns:
            CacheClearReport indicating whether the operation succeeded.
        """
        if self._driver is None:
            return CacheClearReport(cleared=False, detail="driver not connected")

        try:
            with self._driver.session(database=self._database) as session:
                session.run("CALL db.clearQueryCaches()")
            return CacheClearReport(cleared=True)
        except Exception as exc:
            logger.debug("Query cache clearing failed: %s", exc)
            return CacheClearReport(cleared=False, detail=str(exc))

    # ------------------------------------------------------------------
    # Capability and engine metadata
    # ------------------------------------------------------------------

    def capabilities(self) -> set[Capability]:
        """Return the capabilities this adapter supports.

        Neo4j supports CACHE_CLEAR and PLAN_PROFILING.
        GFQL and NATIVE_PROTOCOL are not supported.

        Returns:
            Set of supported Capability enum members.
        """
        return set(_CAPABILITIES)

    def engine_info(self) -> EngineInfo:
        """Return static metadata about the Neo4j engine.

        Returns:
            EngineInfo with name 'neo4j', current version, and capabilities.
        """
        return EngineInfo(
            name="neo4j",
            version=self._version,
            build="community",
            capabilities=set(_CAPABILITIES),
        )

    def engine_state(self) -> EngineState:
        """Return an empty EngineState (Neo4j does not expose memory via Bolt).

        Returns:
            EngineState with all fields set to None.
        """
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
        # oracle may arrive as a CorrectnessResult or a plain dict from the harness
        if isinstance(oracle, dict):
            return validate_oracle(result, oracle)
        return validate_oracle(result, vars(oracle))

    # ------------------------------------------------------------------
    # Metrics collection
    # ------------------------------------------------------------------

    def collect_metrics(self) -> dict[str, Any]:
        """Return engine metrics (all None/empty for Neo4j).

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
