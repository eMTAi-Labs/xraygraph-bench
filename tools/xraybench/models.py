"""Data models for benchmark specs, results, and dataset manifests."""

from __future__ import annotations

import datetime
import platform
import os
from dataclasses import dataclass, field
from typing import Any


@dataclass
class DatasetSpec:
    """Dataset specification within a benchmark."""

    name: str
    type: str
    generator: str | None = None
    generator_params: dict[str, Any] | None = None
    source_url: str | None = None


@dataclass
class CorrectnessOracle:
    """Correctness validation specification."""

    type: str
    expected_row_count: int | None = None
    expected_row_count_min: int | None = None
    expected_row_count_max: int | None = None
    expected_columns: list[str] | None = None
    checksum_algorithm: str | None = None
    expected_checksum: str | None = None
    description: str | None = None


@dataclass
class ParameterSpec:
    """Specification for a tunable benchmark parameter."""

    type: str
    default: Any
    description: str
    min: Any | None = None
    max: Any | None = None
    enum: list[Any] | None = None


@dataclass
class BenchmarkSpec:
    """Complete benchmark specification loaded from a YAML file."""

    name: str
    family: str
    version: str
    description: str
    dataset: DatasetSpec
    query_template: str
    parameters: dict[str, ParameterSpec]
    correctness_oracle: CorrectnessOracle
    metrics: list[str]
    tags: list[str] = field(default_factory=list)
    warm_runs: int = 10
    timeout_seconds: int = 300
    notes: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BenchmarkSpec:
        """Parse a benchmark spec from a dictionary (loaded from YAML)."""
        dataset = DatasetSpec(**data["dataset"])

        oracle_data = data["correctness_oracle"]
        oracle = CorrectnessOracle(**oracle_data)

        parameters = {}
        for pname, pdata in data.get("parameters", {}).items():
            parameters[pname] = ParameterSpec(**pdata)

        return cls(
            name=data["name"],
            family=data["family"],
            version=data["version"],
            description=data["description"],
            dataset=dataset,
            query_template=data["query_template"],
            parameters=parameters,
            correctness_oracle=oracle,
            metrics=data["metrics"],
            tags=data.get("tags", []),
            warm_runs=data.get("warm_runs", 10),
            timeout_seconds=data.get("timeout_seconds", 300),
            notes=data.get("notes"),
        )


@dataclass
class HostInfo:
    """Host environment information."""

    os: str
    cpu: str
    cores: int
    memory_gb: float

    @classmethod
    def collect(cls) -> HostInfo:
        """Collect host information from the current environment."""
        return cls(
            os=f"{platform.system()} {platform.release()}",
            cpu=platform.processor() or "unknown",
            cores=os.cpu_count() or 1,
            memory_gb=_get_memory_gb(),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "os": self.os,
            "cpu": self.cpu,
            "cores": self.cores,
            "memory_gb": self.memory_gb,
        }


@dataclass
class CorrectnessResult:
    """Result of correctness validation."""

    passed: bool
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {"passed": self.passed, "detail": self.detail}


@dataclass
class ExecuteResult:
    """Raw result from executing a query."""

    rows: list[dict[str, Any]]
    wall_ms: float
    compile_ms: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def row_count(self) -> int:
        return len(self.rows)


@dataclass
class LoadResult:
    """Result from loading a dataset."""

    node_count: int
    edge_count: int
    load_time_ms: float


@dataclass
class BenchmarkResult:
    """Complete benchmark result conforming to the result schema."""

    benchmark: str
    engine: str
    engine_version: str
    dataset: str
    dataset_version: str
    cold_ms: float
    warm_ms: float
    rows_out: int
    correctness: CorrectnessResult
    timestamp: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )
    query_shape: str | None = None
    compile_ms: float | None = None
    rows_in: int | None = None
    segments: int | None = None
    breakers: list[str] = field(default_factory=list)
    buffer_repr: str | None = None
    cache_hit: bool = False
    fallback: bool = False
    deopt: bool = False
    concurrency: int | None = None
    qps: float | None = None
    latency_p50: float | None = None
    latency_p95: float | None = None
    latency_p99: float | None = None
    error_rate: float | None = None
    parameters: dict[str, Any] = field(default_factory=dict)
    host: HostInfo | None = None
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary matching the result schema."""
        result: dict[str, Any] = {
            "benchmark": self.benchmark,
            "engine": self.engine,
            "engine_version": self.engine_version,
            "dataset": self.dataset,
            "dataset_version": self.dataset_version,
            "cold_ms": self.cold_ms,
            "warm_ms": self.warm_ms,
            "rows_out": self.rows_out,
            "correctness": self.correctness.to_dict(),
            "timestamp": self.timestamp,
        }

        if self.query_shape is not None:
            result["query_shape"] = self.query_shape
        if self.compile_ms is not None:
            result["compile_ms"] = self.compile_ms
        if self.rows_in is not None:
            result["rows_in"] = self.rows_in
        if self.segments is not None:
            result["segments"] = self.segments
        if self.breakers:
            result["breakers"] = self.breakers
        if self.buffer_repr is not None:
            result["buffer_repr"] = self.buffer_repr

        result["cache_hit"] = self.cache_hit
        result["fallback"] = self.fallback
        result["deopt"] = self.deopt

        if self.concurrency is not None:
            result["concurrency"] = self.concurrency
        if self.qps is not None:
            result["qps"] = self.qps
        if self.latency_p50 is not None:
            result["latency_p50"] = self.latency_p50
        if self.latency_p95 is not None:
            result["latency_p95"] = self.latency_p95
        if self.latency_p99 is not None:
            result["latency_p99"] = self.latency_p99
        if self.error_rate is not None:
            result["error_rate"] = self.error_rate

        if self.parameters:
            result["parameters"] = self.parameters
        if self.host is not None:
            result["host"] = self.host.to_dict()
        if self.notes is not None:
            result["notes"] = self.notes

        return result


@dataclass
class DatasetManifest:
    """Dataset manifest loaded from a YAML file."""

    name: str
    version: str
    type: str
    description: str
    node_count: int
    edge_count: int
    source: dict[str, Any] | None = None
    generator: dict[str, Any] | None = None
    format: str | None = None
    labels: list[str] = field(default_factory=list)
    edge_types: list[str] = field(default_factory=list)
    properties: dict[str, Any] = field(default_factory=dict)
    size_bytes: int | None = None
    notes: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DatasetManifest:
        return cls(**data)


def _get_memory_gb() -> float:
    """Get system memory in GB. Cross-platform."""
    try:
        if platform.system() == "Linux":
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        kb = int(line.split()[1])
                        return round(kb / (1024 * 1024), 1)
        elif platform.system() == "Darwin":
            import subprocess

            result = subprocess.run(
                ["sysctl", "-n", "hw.memsize"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return round(int(result.stdout.strip()) / (1024**3), 1)
    except Exception:
        pass
    return 0.0
