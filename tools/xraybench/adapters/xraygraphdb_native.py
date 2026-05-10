"""xrayGraphDB native protocol adapter for xraygraph-bench.

Uses the xrayProtocol binary columnar wire protocol (port 7689) instead of
the Bolt driver.  This adapter can request PROFILE/EXPLAIN via the options
bitmask and tracks protocol-level overhead separately.

Configuration:
    host: xrayGraphDB hostname (default: localhost)
    port: xrayProtocol port (default: 7689)
    username: Authentication user (default: admin)
    password: Authentication password (default: admin)
    capabilities: Requested capability bitmask (default: 0)
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
from .xray_protocol import (
    LANG_CYPHER,
    LANG_GFQL,
    OPT_EXPLAIN,
    OPT_PROFILE,
    OPT_READ_ONLY,
    XrayProtocolClient,
    XrayProtocolError,
)
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
        Capability.NATIVE_PROTOCOL,
        Capability.STREAMING_RESULTS,
    }
)


class XrayGraphDBNativeAdapter(BaseAdapter):
    """Adapter for xrayGraphDB using the native binary columnar protocol.

    Uses XrayProtocolClient internally for all wire communication.  Supports
    the full xraygraph-bench adapter interface including PROFILE/EXPLAIN via
    native options bitmask, GFQL execution, and streaming results.
    """

    def __init__(self) -> None:
        self._client: XrayProtocolClient | None = None
        self._host: str = "localhost"
        self._port: int = 7689
        self._server_info: str = ""
        self._server_version_num: int = 0
        self._server_caps: int = 0
        self._protocol_overhead_ms: float = 0.0

    # ------------------------------------------------------------------
    # Abstract method implementations
    # ------------------------------------------------------------------

    def connect(self, config: dict[str, Any]) -> ConnectionInfo:
        """Establish a native protocol connection to xrayGraphDB.

        Args:
            config: Keys: host, port (default 7689), username, password,
                    capabilities (default 0).

        Returns:
            ConnectionInfo describing the established connection.
        """
        self._host = config.get("host", "localhost")
        self._port = int(config.get("port", 7689))
        username: str = config.get("username", "admin")
        password: str = config.get("password", "admin")
        capabilities: int = int(config.get("capabilities", 0))

        try:
            self._client = XrayProtocolClient(
                self._host, self._port, timeout=config.get("timeout", 30.0)
            )

            start = perf_counter()
            version, caps, info = self._client.connect(
                username=username,
                password=password,
                capabilities=capabilities,
            )
            self._protocol_overhead_ms = (perf_counter() - start) * 1000.0

            self._server_version_num = version
            self._server_caps = caps
            self._server_info = info

            logger.info(
                "Connected to xrayGraphDB (native) — %s at %s:%d",
                info,
                self._host,
                self._port,
            )

            return ConnectionInfo(
                host=self._host,
                port=self._port,
                protocol="xray-native",
                connected=True,
            )

        except Exception as exc:
            raise ConnectionError(
                f"Failed to connect to xrayGraphDB native at "
                f"{self._host}:{self._port}: {exc}"
            ) from exc

    def close(self) -> None:
        """Close the native protocol connection."""
        if self._client is not None:
            self._client.close()
            self._client = None

    def load_dataset(
        self,
        dataset: DatasetSpec | DatasetManifest,
        data_source: Any | None = None,
    ) -> LoadReport:
        """Clear the graph, load via dataset_loader, verify counts.

        Args:
            dataset: Dataset specification or manifest.
            data_source: Optional edge list or generator config.

        Returns:
            LoadReport with node/edge counts and timing.
        """
        if self._client is None:
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

        verified = False
        expected_nodes: int | None = None
        expected_edges: int | None = None
        if isinstance(dataset, DatasetManifest):
            expected_nodes = dataset.node_count
            expected_edges = dataset.edge_count
            verified = node_count == expected_nodes and edge_count == expected_edges
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

    def execute(
        self, query: str, params: dict[str, Any] | None = None
    ) -> ExecuteResult:
        """Execute a Cypher query via the native protocol.

        Args:
            query: Cypher query string.
            params: Optional query parameters.

        Returns:
            ExecuteResult with rows, wall_ms, compile_ms, and metadata.
        """
        if self._client is None:
            raise RuntimeError("Not connected. Call connect() first.")

        start = perf_counter()
        columns, rows, flags = self._client.execute(
            query, language=LANG_CYPHER, params=params
        )
        wall_ms = (perf_counter() - start) * 1000.0

        metadata: dict[str, Any] = {
            "protocol": "xray-native",
            "complete_flags": flags,
            "column_count": len(columns),
        }

        return ExecuteResult(
            rows=rows,
            wall_ms=round(wall_ms, 2),
            compile_ms=None,
            metadata=metadata,
        )

    def clear_caches(self) -> CacheClearReport:
        """Clear xrayGraphDB plan and execution caches via native protocol.

        Returns:
            CacheClearReport indicating success/failure.
        """
        if self._client is None:
            return CacheClearReport(cleared=False, detail="Not connected")

        cleared = False
        messages: list[str] = []

        for cmd in ("FREE MEMORY", "CALL mg.clear_cache()"):
            try:
                self._client.execute(cmd)
                cleared = True
                messages.append(f"{cmd}: ok")
            except XrayProtocolError as exc:
                messages.append(f"{cmd}: {exc}")

        return CacheClearReport(cleared=cleared, detail="; ".join(messages))

    def engine_info(self) -> EngineInfo:
        """Return static metadata about xrayGraphDB (native).

        Returns:
            EngineInfo with name, version, build, and capabilities.
        """
        # Parse version from server_info string if available
        version = self._server_info or "unknown"
        return EngineInfo(
            name="xraygraphdb-native",
            version=version,
            build="native-protocol",
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
            oracle: Correctness oracle specification.

        Returns:
            CorrectnessResult with passed status and detail message.
        """
        if isinstance(oracle, dict):
            return validate_oracle(result, oracle)
        return oracle

    # ------------------------------------------------------------------
    # Overridden default methods
    # ------------------------------------------------------------------

    def explain(
        self, query: str, params: dict[str, Any] | None = None
    ) -> QueryPlan:
        """Return the execution plan using the native EXPLAIN option bit.

        Args:
            query: Cypher query string.
            params: Optional query parameters.

        Returns:
            QueryPlan with operators and raw plan data.
        """
        if self._client is None:
            raise RuntimeError("Not connected. Call connect() first.")

        columns, rows, flags = self._client.execute(
            query, language=LANG_CYPHER, params=params, options=OPT_EXPLAIN
        )
        return QueryPlan(operators=rows, raw=rows)

    def profile(
        self, query: str, params: dict[str, Any] | None = None
    ) -> ProfileResult:
        """Execute with PROFILE option bit and return profiling data.

        Args:
            query: Cypher query string.
            params: Optional query parameters.

        Returns:
            ProfileResult with operators, db_hits, and row counts.
        """
        if self._client is None:
            raise RuntimeError("Not connected. Call connect() first.")

        columns, rows, flags = self._client.execute(
            query, language=LANG_CYPHER, params=params, options=OPT_PROFILE
        )
        operators = [dict(row) for row in rows]
        total_db_hits = sum(int(op.get("db_hits", 0)) for op in operators)
        total_rows = sum(int(op.get("rows", 0)) for op in operators)
        return ProfileResult(
            operators=operators,
            total_db_hits=total_db_hits,
            total_rows=total_rows,
            raw=rows,
        )

    def health_check(self) -> Any:
        """Verify connectivity via PING/PONG.

        Falls back to RETURN 1 if PING is not supported.
        """
        from .capabilities import HealthStatus

        start = perf_counter()
        try:
            if self._client is not None:
                self._client.ping()
                latency_ms = (perf_counter() - start) * 1000.0
                return HealthStatus(healthy=True, latency_ms=latency_ms)
            else:
                return HealthStatus(
                    healthy=False,
                    latency_ms=0.0,
                    detail="Not connected",
                )
        except Exception as exc:
            latency_ms = (perf_counter() - start) * 1000.0
            # Fall back to query-based health check
            try:
                self.execute("RETURN 1")
                latency_ms = (perf_counter() - start) * 1000.0
                return HealthStatus(healthy=True, latency_ms=latency_ms)
            except Exception as exc2:
                return HealthStatus(
                    healthy=False,
                    latency_ms=latency_ms,
                    detail=str(exc2),
                )

    # ------------------------------------------------------------------
    # GFQL support
    # ------------------------------------------------------------------

    def execute_gfql(
        self,
        gfql_query: str,
        params: dict[str, Any] | None = None,
    ) -> ExecuteResult:
        """Execute a GFQL query via the native protocol.

        Args:
            gfql_query: GFQL query string.
            params: Optional query parameters.

        Returns:
            ExecuteResult with rows, wall_ms, and metadata.
        """
        if self._client is None:
            raise RuntimeError("Not connected. Call connect() first.")

        start = perf_counter()
        columns, rows, flags = self._client.execute(
            gfql_query, language=LANG_GFQL, params=params
        )
        wall_ms = (perf_counter() - start) * 1000.0

        metadata: dict[str, Any] = {
            "protocol": "xray-native",
            "language": "gfql",
            "complete_flags": flags,
            "column_count": len(columns),
        }

        return ExecuteResult(
            rows=rows,
            wall_ms=round(wall_ms, 2),
            metadata=metadata,
        )

    # ------------------------------------------------------------------
    # Protocol-specific helpers
    # ------------------------------------------------------------------

    def protocol_overhead_ms(self) -> float:
        """Return the handshake overhead in milliseconds."""
        return self._protocol_overhead_ms

    def collect_metrics(self) -> dict[str, Any]:
        """Gather protocol-specific metrics.

        Returns:
            Dict with protocol overhead and connection details.
        """
        return {
            "protocol": "xray-native",
            "handshake_overhead_ms": round(self._protocol_overhead_ms, 2),
            "server_info": self._server_info,
            "server_protocol_version": self._server_version_num,
            "server_capabilities": self._server_caps,
        }
