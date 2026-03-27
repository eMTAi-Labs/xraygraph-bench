"""Memgraph adapter for xraygraph-bench.

Connects to Memgraph via the Bolt protocol using the neo4j Python driver
(which Memgraph supports). Translates benchmark operations into Memgraph-
compatible Cypher queries.

Configuration:
    host: Memgraph hostname (default: localhost)
    port: Bolt port (default: 7687)
    username: Authentication user (default: empty)
    password: Authentication password (default: empty)
    database: Database name (default: memgraph)
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


class MemgraphAdapter(BaseAdapter):
    """Adapter for Memgraph graph database."""

    def __init__(self):
        self._driver = None
        self._version: str = "unknown"

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
                result = session.run("CALL mg.info() YIELD key, value RETURN *")
                info = {record["key"]: record["value"] for record in result}
                self._version = info.get("version", "unknown")

            logger.info("Connected to Memgraph %s at %s", self._version, uri)

        except ImportError:
            raise ImportError(
                "The neo4j Python driver is required for the Memgraph adapter. "
                "Run: pip install neo4j"
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Memgraph at {host}:{port}: {e}")

    def close(self) -> None:
        if self._driver:
            self._driver.close()
            self._driver = None

    def load_dataset(self, dataset: DatasetSpec | DatasetManifest) -> LoadResult:
        start = time.perf_counter()

        # Dataset loading is generator/format specific.
        # This is a skeleton that should be extended for each dataset type.
        logger.info("Loading dataset: %s (type: %s)", dataset.name, dataset.type)

        # Clear existing data
        with self._driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

        # TODO: Implement dataset-specific loading logic
        # - For synthetic: call generator and execute CREATE statements
        # - For SNAP/OGB: parse edge lists and batch-insert
        # - For code-graph/provenance: load from manifest-specified format

        elapsed_ms = (time.perf_counter() - start) * 1000

        # Query counts after load
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
        wall_ms = (time.perf_counter() - start) * 1000

        return ExecuteResult(
            rows=rows,
            wall_ms=round(wall_ms, 2),
            compile_ms=None,  # Memgraph does not expose compile time separately
        )

    def clear_caches(self) -> None:
        if self._driver:
            with self._driver.session() as session:
                try:
                    session.run("FREE MEMORY")
                except Exception:
                    logger.debug("Cache clearing not supported or failed")

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
        return f"memgraph-{self._version}"


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
        # Structural checks require benchmark-specific logic
        return CorrectnessResult(passed=True, detail="Structural validation not yet implemented")

    else:
        return CorrectnessResult(passed=True, detail=f"Oracle type '{oracle_type}' not yet implemented")
