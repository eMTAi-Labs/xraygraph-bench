"""Data models for benchmark specs, results, and dataset manifests."""

from __future__ import annotations

import datetime
import platform
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
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


# ---------------------------------------------------------------------------
# Enhanced HostInfo with full environment capture
# ---------------------------------------------------------------------------


@dataclass
class HostInfo:
    """Host environment information with comprehensive system details."""

    os: str
    cpu: str
    cores: int
    threads: int | None = None
    memory_gb: float = 0.0
    memory_available_gb: float | None = None
    numa_nodes: int | None = None
    cpu_governor: str | None = None
    swap_gb: float | None = None
    container: bool = False
    cgroup_memory_limit_gb: float | None = None

    @classmethod
    def collect(cls) -> HostInfo:
        """Collect host information from the current environment."""
        system = platform.system()
        cores = os.cpu_count() or 1
        return cls(
            os=f"{system} {platform.release()}",
            cpu=platform.processor() or "unknown",
            cores=_get_physical_cores(system, cores),
            threads=cores,
            memory_gb=_get_memory_gb(),
            memory_available_gb=_get_memory_available_gb(system),
            numa_nodes=_get_numa_nodes(system),
            cpu_governor=_get_cpu_governor(system),
            swap_gb=_get_swap_gb(system),
            container=_detect_container(),
            cgroup_memory_limit_gb=_get_cgroup_memory_limit_gb(),
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "os": self.os,
            "cpu": self.cpu,
            "cores": self.cores,
            "memory_gb": self.memory_gb,
        }
        if self.threads is not None:
            result["threads"] = self.threads
        if self.memory_available_gb is not None:
            result["memory_available_gb"] = self.memory_available_gb
        if self.numa_nodes is not None:
            result["numa_nodes"] = self.numa_nodes
        if self.cpu_governor is not None:
            result["cpu_governor"] = self.cpu_governor
        if self.swap_gb is not None:
            result["swap_gb"] = self.swap_gb
        if self.container:
            result["container"] = self.container
        if self.cgroup_memory_limit_gb is not None:
            result["cgroup_memory_limit_gb"] = self.cgroup_memory_limit_gb
        return result


# ---------------------------------------------------------------------------
# ResourceControl — records what environment controls were applied
# ---------------------------------------------------------------------------


@dataclass
class ResourceControl:
    """Records which resource controls were applied during the benchmark run."""

    cpu_governor: str | None = None
    turbo_boost: bool | None = None
    swap_enabled: bool | None = None
    core_pinning: str | None = None
    numa_policy: str | None = None
    engine_memory_limit_gb: float | None = None
    cache_drop_successful: bool | None = None
    engine_restarted_for_cold: bool = False

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if self.cpu_governor is not None:
            result["cpu_governor"] = self.cpu_governor
        if self.turbo_boost is not None:
            result["turbo_boost"] = self.turbo_boost
        if self.swap_enabled is not None:
            result["swap_enabled"] = self.swap_enabled
        if self.core_pinning is not None:
            result["core_pinning"] = self.core_pinning
        if self.numa_policy is not None:
            result["numa_policy"] = self.numa_policy
        if self.engine_memory_limit_gb is not None:
            result["engine_memory_limit_gb"] = self.engine_memory_limit_gb
        if self.cache_drop_successful is not None:
            result["cache_drop_successful"] = self.cache_drop_successful
        if self.engine_restarted_for_cold:
            result["engine_restarted_for_cold"] = self.engine_restarted_for_cold
        return result


# ---------------------------------------------------------------------------
# RunnerCalibration — clock and adapter overhead measurements
# ---------------------------------------------------------------------------


@dataclass
class RunnerCalibration:
    """Clock and adapter overhead calibration from the Rust timing core."""

    clock_resolution_ns: int
    clock_overhead_ns: int
    fence_overhead_ns: int
    adapter_overhead_ms: float | None = None

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "clock_resolution_ns": self.clock_resolution_ns,
            "clock_overhead_ns": self.clock_overhead_ns,
            "fence_overhead_ns": self.fence_overhead_ns,
        }
        if self.adapter_overhead_ms is not None:
            result["adapter_overhead_ms"] = self.adapter_overhead_ms
        return result


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
    # --- Phase 5 additions ---
    outcome: str | None = None
    outcome_detail: str | None = None
    tier: str | None = None
    resource_control: ResourceControl | None = None
    calibration: RunnerCalibration | None = None
    warmup_iterations: int | None = None
    steady_state_samples: int | None = None
    ci_lower_ms: float | None = None
    ci_upper_ms: float | None = None

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

        # Phase 5 fields
        if self.outcome is not None:
            result["outcome"] = self.outcome
        if self.outcome_detail is not None:
            result["outcome_detail"] = self.outcome_detail
        if self.tier is not None:
            result["tier"] = self.tier
        if self.resource_control is not None:
            rc_dict = self.resource_control.to_dict()
            if rc_dict:
                result["resource_control"] = rc_dict
        if self.calibration is not None:
            result["calibration"] = self.calibration.to_dict()
        if self.warmup_iterations is not None:
            result["warmup_iterations"] = self.warmup_iterations
        if self.steady_state_samples is not None:
            result["steady_state_samples"] = self.steady_state_samples
        if self.ci_lower_ms is not None:
            result["ci_lower_ms"] = self.ci_lower_ms
        if self.ci_upper_ms is not None:
            result["ci_upper_ms"] = self.ci_upper_ms

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


# ---------------------------------------------------------------------------
# Environment detection helpers
# ---------------------------------------------------------------------------


def _get_memory_gb() -> float:
    """Get total system memory in GB. Cross-platform."""
    try:
        if platform.system() == "Linux":
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        kb = int(line.split()[1])
                        return round(kb / (1024 * 1024), 1)
        elif platform.system() == "Darwin":
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


def _get_memory_available_gb(system: str) -> float | None:
    """Get available memory in GB."""
    try:
        if system == "Linux":
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemAvailable:"):
                        kb = int(line.split()[1])
                        return round(kb / (1024 * 1024), 1)
        elif system == "Darwin":
            # Use vm_stat to estimate available memory
            result = subprocess.run(
                ["vm_stat"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                free_pages = 0
                inactive_pages = 0
                page_size = 16384  # default on Apple Silicon
                for line in result.stdout.splitlines():
                    if "page size of" in line:
                        try:
                            page_size = int(line.split()[-2])
                        except (ValueError, IndexError):
                            pass
                    elif "Pages free:" in line:
                        free_pages = int(line.split()[-1].rstrip("."))
                    elif "Pages inactive:" in line:
                        inactive_pages = int(line.split()[-1].rstrip("."))
                avail_bytes = (free_pages + inactive_pages) * page_size
                return round(avail_bytes / (1024**3), 1)
    except Exception:
        pass
    return None


def _get_physical_cores(system: str, logical_count: int) -> int:
    """Get physical core count (as opposed to logical/thread count)."""
    try:
        if system == "Darwin":
            result = subprocess.run(
                ["sysctl", "-n", "hw.physicalcpu"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return int(result.stdout.strip())
        elif system == "Linux":
            # Count unique physical core ids
            result = subprocess.run(
                ["lscpu"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                cores_per = None
                sockets = None
                for line in result.stdout.splitlines():
                    if line.startswith("Core(s) per socket:"):
                        cores_per = int(line.split(":")[-1].strip())
                    elif line.startswith("Socket(s):"):
                        sockets = int(line.split(":")[-1].strip())
                if cores_per is not None and sockets is not None:
                    return cores_per * sockets
    except Exception:
        pass
    return logical_count


def _get_numa_nodes(system: str) -> int | None:
    """Detect NUMA node count (Linux only)."""
    if system != "Linux":
        return None
    try:
        node_dir = Path("/sys/devices/system/node")
        if node_dir.exists():
            nodes = [d for d in node_dir.iterdir() if d.name.startswith("node")]
            if nodes:
                return len(nodes)
    except Exception:
        pass
    return None


def _get_cpu_governor(system: str) -> str | None:
    """Read CPU frequency governor (Linux only)."""
    if system != "Linux":
        return None
    try:
        gov_path = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
        if gov_path.exists():
            return gov_path.read_text().strip()
    except Exception:
        pass
    return None


def _get_swap_gb(system: str) -> float | None:
    """Get swap size in GB."""
    try:
        if system == "Linux":
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("SwapTotal:"):
                        kb = int(line.split()[1])
                        return round(kb / (1024 * 1024), 1)
        elif system == "Darwin":
            result = subprocess.run(
                ["sysctl", "-n", "vm.swapusage"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                # Output: "total = 1024.00M  used = 0.00M  free = 1024.00M ..."
                for part in result.stdout.split():
                    if part.endswith("M") and "=" not in part:
                        # First numeric value after "total =" is total swap
                        try:
                            return round(float(part.rstrip("M")) / 1024, 1)
                        except ValueError:
                            continue
                # Fallback: parse "total = X.XXM"
                text = result.stdout
                if "total" in text:
                    idx = text.index("total")
                    segment = text[idx:idx + 40]
                    for token in segment.split():
                        if token.endswith("M"):
                            try:
                                return round(float(token.rstrip("M")) / 1024, 1)
                            except ValueError:
                                continue
    except Exception:
        pass
    return None


def _detect_container() -> bool:
    """Detect if running inside a container (Docker/Podman/LXC)."""
    # Check for /.dockerenv
    if Path("/.dockerenv").exists():
        return True
    # Check cgroup for docker/lxc markers
    try:
        cgroup_path = Path("/proc/1/cgroup")
        if cgroup_path.exists():
            text = cgroup_path.read_text()
            if "docker" in text or "lxc" in text or "containerd" in text:
                return True
    except Exception:
        pass
    # Check for container env variable
    if os.environ.get("container"):
        return True
    return False


def _get_cgroup_memory_limit_gb() -> float | None:
    """Get cgroup memory limit in GB (Linux containers)."""
    try:
        # cgroup v2
        limit_path = Path("/sys/fs/cgroup/memory.max")
        if limit_path.exists():
            text = limit_path.read_text().strip()
            if text != "max":
                return round(int(text) / (1024**3), 1)
        # cgroup v1
        limit_path = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        if limit_path.exists():
            value = int(limit_path.read_text().strip())
            # Values near maxint mean unlimited
            if value < 2**62:
                return round(value / (1024**3), 1)
    except Exception:
        pass
    return None
