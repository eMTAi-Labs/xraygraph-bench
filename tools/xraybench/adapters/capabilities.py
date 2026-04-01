"""Capability system and adapter-related types for xraygraph-bench."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Capability(Enum):
    COMPILE_TIME_REPORTING = "compile_time"
    PLAN_PROFILING = "plan_profile"
    CACHE_CLEAR = "cache_clear"
    VECTORIZED_METRICS = "vectorized_metrics"
    STREAMING_RESULTS = "streaming_results"
    NATIVE_PROTOCOL = "native_protocol"
    EXPLAIN_ANALYZE = "explain_analyze"
    MEMORY_REPORTING = "memory_reporting"
    GFQL = "gfql"


class Outcome(Enum):
    SUCCESS = "success"
    CORRECTNESS_MISMATCH = "correctness_mismatch"
    ENGINE_ERROR = "engine_error"
    TIMEOUT = "timeout"
    UNSUPPORTED = "unsupported"
    DATASET_VERIFICATION_FAILED = "dataset_verification_failed"
    HARNESS_FAILURE = "harness_failure"
    CONNECTION_FAILURE = "connection_failure"
    OUT_OF_MEMORY = "out_of_memory"


@dataclass
class ConnectionInfo:
    """Connection details for an engine adapter."""

    host: str
    port: int
    protocol: str
    connected: bool
    database: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "protocol": self.protocol,
            "connected": self.connected,
        }
        if self.database is not None:
            result["database"] = self.database
        return result


@dataclass
class HealthStatus:
    """Health check result for an engine."""

    healthy: bool
    latency_ms: float
    detail: str = ""


@dataclass
class LoadReport:
    """Report produced after loading a dataset into an engine."""

    node_count: int
    edge_count: int
    load_time_ms: float
    verified: bool
    expected_nodes: int | None = None
    expected_edges: int | None = None
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "load_time_ms": self.load_time_ms,
            "verified": self.verified,
            "errors": self.errors,
        }
        if self.expected_nodes is not None:
            result["expected_nodes"] = self.expected_nodes
        if self.expected_edges is not None:
            result["expected_edges"] = self.expected_edges
        return result


@dataclass
class CacheClearReport:
    """Report produced after clearing engine caches."""

    cleared: bool
    detail: str = ""


@dataclass
class EngineInfo:
    """Static metadata about a graph database engine."""

    name: str
    version: str
    build: str
    capabilities: set[Capability] = field(default_factory=set)
    config_hash: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "name": self.name,
            "version": self.version,
            "build": self.build,
            "capabilities": sorted(c.value for c in self.capabilities),
        }
        if self.config_hash is not None:
            result["config_hash"] = self.config_hash
        return result


@dataclass
class EngineState:
    """Runtime state snapshot of an engine."""

    memory_used_mb: float | None = None
    memory_available_mb: float | None = None
    active_queries: int | None = None
    cache_size_mb: float | None = None


@dataclass
class QueryPlan:
    """Execution plan returned by EXPLAIN."""

    operators: list[Any]
    raw: Any
    estimated_cost: float | None = None


@dataclass
class ProfileResult:
    """Profiling data returned by PROFILE / EXPLAIN ANALYZE."""

    operators: list[dict[str, Any]]
    total_db_hits: int
    total_rows: int
    raw: Any
