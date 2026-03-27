"""Concurrent load testing module.

Supports four test profiles:
- throughput: fixed concurrency, measure QPS and latency
- saturation: ramp concurrency to find throughput plateau
- mixed: weighted query mix simulating real workloads
- stability: long-running test to detect drift and degradation
"""

from __future__ import annotations

import logging
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from .adapters.base import BaseAdapter
from .models import BenchmarkResult, CorrectnessResult, HostInfo

logger = logging.getLogger(__name__)


@dataclass
class LoadTestConfig:
    """Configuration for a load test."""

    profile: str  # throughput, saturation, mixed, stability
    clients: int = 8
    duration_seconds: int = 60
    query: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    ramp_step: int = 4  # saturation: clients added per step
    ramp_step_duration: int = 30  # saturation: seconds per step
    max_clients: int = 128  # saturation: upper bound
    query_mix: list[dict[str, Any]] = field(default_factory=list)
    engine_config: dict[str, Any] = field(default_factory=dict)


@dataclass
class LatencyStats:
    """Latency statistics from a load test."""

    p50: float = 0.0
    p95: float = 0.0
    p99: float = 0.0
    mean: float = 0.0
    min: float = 0.0
    max: float = 0.0

    @classmethod
    def from_latencies(cls, latencies: list[float]) -> LatencyStats:
        if not latencies:
            return cls()
        sorted_lat = sorted(latencies)
        n = len(sorted_lat)
        return cls(
            p50=sorted_lat[int(n * 0.50)],
            p95=sorted_lat[int(n * 0.95)] if n >= 20 else sorted_lat[-1],
            p99=sorted_lat[int(n * 0.99)] if n >= 100 else sorted_lat[-1],
            mean=statistics.mean(sorted_lat),
            min=sorted_lat[0],
            max=sorted_lat[-1],
        )


@dataclass
class LoadTestResult:
    """Result from a load test run."""

    profile: str
    clients: int
    duration_seconds: float
    total_queries: int
    successful_queries: int
    failed_queries: int
    qps: float
    latency: LatencyStats
    error_rate: float
    time_series: list[dict[str, Any]] = field(default_factory=list)

    def to_benchmark_result(
        self,
        benchmark_name: str,
        engine: str,
        engine_version: str,
        dataset: str,
    ) -> BenchmarkResult:
        """Convert to a BenchmarkResult for schema-compatible output."""
        return BenchmarkResult(
            benchmark=benchmark_name,
            engine=engine,
            engine_version=engine_version,
            dataset=dataset,
            dataset_version="load-test",
            cold_ms=self.latency.max,
            warm_ms=self.latency.mean,
            rows_out=self.total_queries,
            correctness=CorrectnessResult(
                passed=self.error_rate < 0.01,
                detail=f"Error rate: {self.error_rate:.4f}",
            ),
            concurrency=self.clients,
            qps=round(self.qps, 2),
            latency_p50=round(self.latency.p50, 2),
            latency_p95=round(self.latency.p95, 2),
            latency_p99=round(self.latency.p99, 2),
            error_rate=round(self.error_rate, 6),
            host=HostInfo.collect(),
            notes=f"Load test profile: {self.profile}",
        )


class LoadTester:
    """Concurrent load testing engine."""

    def __init__(self, adapter: BaseAdapter, config: dict[str, Any] | None = None):
        self.adapter = adapter
        self.config = config or {}

    def run(self, test_config: LoadTestConfig) -> LoadTestResult:
        """Execute a load test based on the given configuration."""
        self.adapter.connect(self.config)

        try:
            if test_config.profile == "throughput":
                return self._run_throughput(test_config)
            elif test_config.profile == "saturation":
                return self._run_saturation(test_config)
            elif test_config.profile == "mixed":
                return self._run_mixed(test_config)
            elif test_config.profile == "stability":
                return self._run_stability(test_config)
            else:
                raise ValueError(f"Unknown load test profile: {test_config.profile}")
        finally:
            self.adapter.close()

    def _run_throughput(self, config: LoadTestConfig) -> LoadTestResult:
        """Fixed concurrency throughput test."""
        logger.info(
            "Starting throughput test: %d clients, %d seconds",
            config.clients,
            config.duration_seconds,
        )

        latencies: list[float] = []
        errors = 0
        lock = threading.Lock()
        stop_event = threading.Event()

        def worker():
            nonlocal errors
            local_latencies: list[float] = []
            local_errors = 0
            while not stop_event.is_set():
                start = time.perf_counter()
                try:
                    self.adapter.execute(config.query, config.params)
                    elapsed_ms = (time.perf_counter() - start) * 1000
                    local_latencies.append(elapsed_ms)
                except Exception:
                    local_errors += 1
            with lock:
                latencies.extend(local_latencies)
                errors += local_errors

        start_time = time.monotonic()
        with ThreadPoolExecutor(max_workers=config.clients) as pool:
            futures = [pool.submit(worker) for _ in range(config.clients)]
            time.sleep(config.duration_seconds)
            stop_event.set()
            for f in futures:
                f.result()

        elapsed = time.monotonic() - start_time
        total = len(latencies) + errors
        qps = total / elapsed if elapsed > 0 else 0

        return LoadTestResult(
            profile="throughput",
            clients=config.clients,
            duration_seconds=round(elapsed, 2),
            total_queries=total,
            successful_queries=len(latencies),
            failed_queries=errors,
            qps=round(qps, 2),
            latency=LatencyStats.from_latencies(latencies),
            error_rate=errors / total if total > 0 else 0.0,
        )

    def _run_saturation(self, config: LoadTestConfig) -> LoadTestResult:
        """Gradually increase concurrency to find throughput plateau."""
        logger.info(
            "Starting saturation test: %d to %d clients, step %d",
            config.ramp_step,
            config.max_clients,
            config.ramp_step,
        )

        all_latencies: list[float] = []
        time_series: list[dict[str, Any]] = []
        total_errors = 0
        total_queries = 0

        current_clients = config.ramp_step
        while current_clients <= config.max_clients:
            step_latencies: list[float] = []
            step_errors = 0
            lock = threading.Lock()
            stop_event = threading.Event()

            def worker():
                nonlocal step_errors
                local_lat: list[float] = []
                local_err = 0
                while not stop_event.is_set():
                    start = time.perf_counter()
                    try:
                        self.adapter.execute(config.query, config.params)
                        local_lat.append((time.perf_counter() - start) * 1000)
                    except Exception:
                        local_err += 1
                with lock:
                    step_latencies.extend(local_lat)
                    step_errors += local_err

            with ThreadPoolExecutor(max_workers=current_clients) as pool:
                futures = [pool.submit(worker) for _ in range(current_clients)]
                time.sleep(config.ramp_step_duration)
                stop_event.set()
                for f in futures:
                    f.result()

            step_total = len(step_latencies) + step_errors
            step_qps = step_total / config.ramp_step_duration if config.ramp_step_duration > 0 else 0
            stats = LatencyStats.from_latencies(step_latencies)

            time_series.append({
                "clients": current_clients,
                "qps": round(step_qps, 2),
                "p50": round(stats.p50, 2),
                "p95": round(stats.p95, 2),
                "p99": round(stats.p99, 2),
                "errors": step_errors,
            })

            logger.info(
                "Clients=%d QPS=%.1f p50=%.1f p95=%.1f p99=%.1f errors=%d",
                current_clients, step_qps, stats.p50, stats.p95, stats.p99, step_errors,
            )

            all_latencies.extend(step_latencies)
            total_errors += step_errors
            total_queries += step_total
            current_clients += config.ramp_step

        overall_qps = total_queries / (len(time_series) * config.ramp_step_duration) if time_series else 0

        return LoadTestResult(
            profile="saturation",
            clients=config.max_clients,
            duration_seconds=len(time_series) * config.ramp_step_duration,
            total_queries=total_queries,
            successful_queries=len(all_latencies),
            failed_queries=total_errors,
            qps=round(overall_qps, 2),
            latency=LatencyStats.from_latencies(all_latencies),
            error_rate=total_errors / total_queries if total_queries > 0 else 0.0,
            time_series=time_series,
        )

    def _run_mixed(self, config: LoadTestConfig) -> LoadTestResult:
        """Mixed workload test with weighted query distribution."""
        if not config.query_mix:
            # Default mix: 70% read, 20% analytical, 10% write
            config.query_mix = [
                {"query": config.query, "weight": 0.7, "label": "read"},
                {"query": config.query, "weight": 0.2, "label": "analytical"},
                {"query": config.query, "weight": 0.1, "label": "write"},
            ]

        logger.info(
            "Starting mixed workload test: %d clients, %d seconds, %d query types",
            config.clients,
            config.duration_seconds,
            len(config.query_mix),
        )

        import random

        weights = [q["weight"] for q in config.query_mix]
        queries = [q["query"] for q in config.query_mix]

        latencies: list[float] = []
        errors = 0
        lock = threading.Lock()
        stop_event = threading.Event()

        def worker():
            nonlocal errors
            local_latencies: list[float] = []
            local_errors = 0
            rng = random.Random()
            while not stop_event.is_set():
                query = rng.choices(queries, weights=weights, k=1)[0]
                start = time.perf_counter()
                try:
                    self.adapter.execute(query, config.params)
                    local_latencies.append((time.perf_counter() - start) * 1000)
                except Exception:
                    local_errors += 1
            with lock:
                latencies.extend(local_latencies)
                errors += local_errors

        start_time = time.monotonic()
        with ThreadPoolExecutor(max_workers=config.clients) as pool:
            futures = [pool.submit(worker) for _ in range(config.clients)]
            time.sleep(config.duration_seconds)
            stop_event.set()
            for f in futures:
                f.result()

        elapsed = time.monotonic() - start_time
        total = len(latencies) + errors
        qps = total / elapsed if elapsed > 0 else 0

        return LoadTestResult(
            profile="mixed",
            clients=config.clients,
            duration_seconds=round(elapsed, 2),
            total_queries=total,
            successful_queries=len(latencies),
            failed_queries=errors,
            qps=round(qps, 2),
            latency=LatencyStats.from_latencies(latencies),
            error_rate=errors / total if total > 0 else 0.0,
        )

    def _run_stability(self, config: LoadTestConfig) -> LoadTestResult:
        """Long-running stability test with periodic sampling."""
        logger.info(
            "Starting stability test: %d clients, %d seconds",
            config.clients,
            config.duration_seconds,
        )

        sample_interval = 60  # sample every 60 seconds
        time_series: list[dict[str, Any]] = []
        all_latencies: list[float] = []
        total_errors = 0
        total_queries = 0

        elapsed_total = 0.0
        while elapsed_total < config.duration_seconds:
            remaining = config.duration_seconds - elapsed_total
            sample_duration = min(sample_interval, remaining)
            if sample_duration <= 0:
                break

            sample_latencies: list[float] = []
            sample_errors = 0
            lock = threading.Lock()
            stop_event = threading.Event()

            def worker():
                nonlocal sample_errors
                local_lat: list[float] = []
                local_err = 0
                while not stop_event.is_set():
                    start = time.perf_counter()
                    try:
                        self.adapter.execute(config.query, config.params)
                        local_lat.append((time.perf_counter() - start) * 1000)
                    except Exception:
                        local_err += 1
                with lock:
                    sample_latencies.extend(local_lat)
                    sample_errors += local_err

            with ThreadPoolExecutor(max_workers=config.clients) as pool:
                futures = [pool.submit(worker) for _ in range(config.clients)]
                time.sleep(sample_duration)
                stop_event.set()
                for f in futures:
                    f.result()

            sample_total = len(sample_latencies) + sample_errors
            stats = LatencyStats.from_latencies(sample_latencies)

            time_series.append({
                "elapsed_seconds": round(elapsed_total + sample_duration, 1),
                "qps": round(sample_total / sample_duration, 2) if sample_duration > 0 else 0,
                "p50": round(stats.p50, 2),
                "p95": round(stats.p95, 2),
                "p99": round(stats.p99, 2),
                "errors": sample_errors,
            })

            all_latencies.extend(sample_latencies)
            total_errors += sample_errors
            total_queries += sample_total
            elapsed_total += sample_duration

        overall_qps = total_queries / elapsed_total if elapsed_total > 0 else 0

        return LoadTestResult(
            profile="stability",
            clients=config.clients,
            duration_seconds=round(elapsed_total, 2),
            total_queries=total_queries,
            successful_queries=len(all_latencies),
            failed_queries=total_errors,
            qps=round(overall_qps, 2),
            latency=LatencyStats.from_latencies(all_latencies),
            error_rate=total_errors / total_queries if total_queries > 0 else 0.0,
            time_series=time_series,
        )
