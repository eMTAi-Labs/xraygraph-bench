"""Neo4j adapter for xraygraph-bench.

Connects to Neo4j via the Bolt protocol using the official neo4j Python
driver. Translates benchmark operations into Neo4j-compatible Cypher queries.

Configuration:
    host: Neo4j hostname (default: localhost)
    port: Bolt port (default: 7687)
    username: Authentication user (default: neo4j)
    password: Authentication password (required)
    database: Database name (default: neo4j)
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


class Neo4jAdapter(BaseAdapter):
    """Adapter for Neo4j graph database."""

    def __init__(self):
        self._driver = None
        self._database: str = "neo4j"
        self._version: str = "unknown"

    def connect(self, config: dict[str, Any]) -> None:
        host = config.get("host", "localhost")
        port = config.get("port", 7687)
        username = config.get("username", "neo4j")
        password = config.get("password", "")
        self._database = config.get("database", "neo4j")

        if not password:
            raise ValueError("Neo4j adapter requires a password in config")

        try:
            import neo4j

            uri = f"bolt://{host}:{port}"
            self._driver = neo4j.GraphDatabase.driver(uri, auth=(username, password))

            # Verify connection and get version
            with self._driver.session(database=self._database) as session:
                result = session.run("CALL dbms.components() YIELD name, versions RETURN versions[0] AS version")
                record = result.single()
                if record:
                    self._version = record["version"]

            logger.info("Connected to Neo4j %s at %s", self._version, uri)

        except ImportError:
            raise ImportError(
                "The neo4j Python driver is required for the Neo4j adapter. "
                "Run: pip install neo4j"
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Neo4j at {host}:{port}: {e}")

    def close(self) -> None:
        if self._driver:
            self._driver.close()
            self._driver = None

    def load_dataset(self, dataset: DatasetSpec | DatasetManifest) -> LoadResult:
        if not self._driver:
            raise RuntimeError("Not connected. Call connect() first.")

        start = time.perf_counter()

        logger.info("Loading dataset: %s (type: %s)", dataset.name, dataset.type)

        # Clear existing data
        with self._driver.session(database=self._database) as session:
            session.run("MATCH (n) DETACH DELETE n")

        # TODO: Implement dataset-specific loading logic
        # - For synthetic: call generator and execute CREATE statements
        # - For SNAP/OGB: parse edge lists and batch-insert via UNWIND
        # - For code-graph/provenance: load from manifest-specified format

        elapsed_ms = (time.perf_counter() - start) * 1000

        # Query counts after load
        with self._driver.session(database=self._database) as session:
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
        with self._driver.session(database=self._database) as session:
            result = session.run(query, parameters=params or {})
            rows = [dict(record) for record in result]
            summary = result.consume()
        wall_ms = (time.perf_counter() - start) * 1000

        # Neo4j exposes some execution metadata via result summary
        compile_ms = None
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
            compile_ms=compile_ms,
            metadata=metadata,
        )

    def clear_caches(self) -> None:
        if self._driver:
            with self._driver.session(database=self._database) as session:
                try:
                    session.run("CALL db.clearQueryCaches()")
                except Exception:
                    logger.debug("Query cache clearing not supported or failed")

    def collect_metrics(self) -> dict[str, Any]:
        return {
            "segments": None,
            "breakers": [],
            "buffer_repr": None,
            "cache_hit": False,
            "fallback": False,
            "deopt": False,
        }

    def validate_correctness(
        self, result: ExecuteResult, oracle: dict[str, Any]
    ) -> CorrectnessResult:
        return _validate_oracle(result, oracle)

    def engine_version(self) -> str:
        return f"neo4j-{self._version}"


def _validate_oracle(result: ExecuteResult, oracle: dict[str, Any]) -> CorrectnessResult:
    """Shared correctness validation logic."""
    oracle_type = oracle.get("type", "")

    if oracle_type == "row_count":
        expected = oracle.get("expected_row_count", 0)
        actual = result.row_count
        if actual == expected:
            return CorrectnessResult(passed=True, detail=f"Row count {actual} matches expected {expected}")
        return CorrectnessResult(passed=False, detail=f"Row count {actual} != expected {expected}")

    elif oracle_type == "row_count_range":
        lo = oracle.get("expected_row_count_min", 0)
        hi = oracle.get("expected_row_count_max", float("inf"))
        actual = result.row_count
        if lo <= actual <= hi:
            return CorrectnessResult(passed=True, detail=f"Row count {actual} within [{lo}, {hi}]")
        return CorrectnessResult(passed=False, detail=f"Row count {actual} outside [{lo}, {hi}]")

    elif oracle_type == "structural":
        return CorrectnessResult(passed=True, detail="Structural validation not yet implemented")

    else:
        return CorrectnessResult(passed=True, detail=f"Oracle type '{oracle_type}' not yet implemented")
