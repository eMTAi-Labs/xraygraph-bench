"""Ingestion benchmark runner -- measures data loading performance."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from xraybench_core import stats as rust_stats
from xraybench_core import timing as rust_timing

from tools.xraybench.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


@dataclass
class IngestionResult:
    """Result of an ingestion benchmark."""

    benchmark: str
    node_count: int
    edge_count: int
    property_count: int
    batch_size: int
    total_ms: float
    nodes_per_sec: float
    edges_per_sec: float
    index_build_ms: float | None = None
    read_latencies_ms: list[float] = field(default_factory=list)  # for mixed-ingest
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "benchmark": self.benchmark,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "property_count": self.property_count,
            "batch_size": self.batch_size,
            "total_ms": round(self.total_ms, 2),
            "nodes_per_sec": round(self.nodes_per_sec, 1),
            "edges_per_sec": round(self.edges_per_sec, 1),
        }
        if self.index_build_ms is not None:
            d["index_build_ms"] = round(self.index_build_ms, 2)
        if self.read_latencies_ms:
            valid = self.read_latencies_ms
            if len(valid) >= 3:
                desc = rust_stats.descriptive(valid)
                pcts = rust_stats.percentiles(valid, [0.5, 0.95, 0.99])
                d["read_latency"] = {
                    "p50_ms": round(pcts[0], 3),
                    "p95_ms": round(pcts[1], 3),
                    "p99_ms": round(pcts[2], 3),
                    "mean_ms": round(desc["mean"], 3),
                }
        if self.metadata:
            d["metadata"] = self.metadata
        return d


class IngestionRunner:
    """Runs ingestion benchmarks against a connected adapter."""

    def __init__(self, adapter: BaseAdapter) -> None:
        self.adapter = adapter

    def benchmark_node_creation(
        self,
        node_count: int = 100000,
        batch_size: int = 1000,
        property_count: int = 5,
        seed: int = 42,
    ) -> IngestionResult:
        """Benchmark bulk node creation rate."""
        # Clear existing test data
        self.adapter.execute("MATCH (n:IngestBench) DETACH DELETE n")

        t0 = rust_timing.monotonic_ns()

        for batch_start in range(0, node_count, batch_size):
            batch_end = min(batch_start + batch_size, node_count)

            def _node_clause(i: int) -> str:
                props = ", ".join(
                    f"prop_{p}: {(i * 17 + p * 31) % 10000}"
                    for p in range(property_count)
                )
                return f"(:IngestBench {{id: {i}, {props}}})"

            creates = ", ".join(
                _node_clause(i) for i in range(batch_start, batch_end)
            )
            self.adapter.execute(f"CREATE {creates}")

        t1 = rust_timing.monotonic_ns()
        total_ms = (t1 - t0) / 1e6
        nodes_per_sec = (node_count / total_ms) * 1000 if total_ms > 0 else 0

        # Verify
        result = self.adapter.execute(
            "MATCH (n:IngestBench) RETURN count(n) AS c"
        )
        actual = int(result.rows[0]["c"]) if result.rows else 0

        return IngestionResult(
            benchmark="bulk-node-create",
            node_count=actual,
            edge_count=0,
            property_count=property_count,
            batch_size=batch_size,
            total_ms=total_ms,
            nodes_per_sec=nodes_per_sec,
            edges_per_sec=0,
            metadata={
                "expected_nodes": node_count,
                "verified": actual == node_count,
            },
        )

    def benchmark_index_build(
        self, label: str = "IngestBench", property_name: str = "id"
    ) -> float:
        """Benchmark index creation time. Returns milliseconds."""
        t0 = rust_timing.monotonic_ns()
        try:
            self.adapter.execute(
                f"CREATE INDEX ON :{label}({property_name})"
            )
        except Exception:
            pass  # Index may already exist
        t1 = rust_timing.monotonic_ns()
        return (t1 - t0) / 1e6

    def benchmark_mixed_ingest(
        self,
        node_count: int = 100000,
        batch_size: int = 1000,
        read_interval: int = 100,
        read_query: str = "MATCH (n:IngestBench) RETURN count(n) AS c",
    ) -> IngestionResult:
        """Benchmark concurrent read+write -- interleave reads during ingestion."""
        self.adapter.execute("MATCH (n:IngestBench) DETACH DELETE n")
        read_latencies: list[float] = []
        writes_done = 0

        t0 = rust_timing.monotonic_ns()

        for batch_start in range(0, node_count, batch_size):
            batch_end = min(batch_start + batch_size, node_count)
            creates = ", ".join(
                f"(:IngestBench {{id: {i}}})"
                for i in range(batch_start, batch_end)
            )
            self.adapter.execute(f"CREATE {creates}")
            writes_done += 1

            if writes_done % read_interval == 0:
                rt0 = rust_timing.monotonic_ns()
                self.adapter.execute(read_query)
                rt1 = rust_timing.monotonic_ns()
                read_latencies.append((rt1 - rt0) / 1e6)

        t1 = rust_timing.monotonic_ns()
        total_ms = (t1 - t0) / 1e6
        nodes_per_sec = (node_count / total_ms) * 1000 if total_ms > 0 else 0

        return IngestionResult(
            benchmark="mixed-ingest",
            node_count=node_count,
            edge_count=0,
            property_count=1,
            batch_size=batch_size,
            total_ms=total_ms,
            nodes_per_sec=nodes_per_sec,
            edges_per_sec=0,
            read_latencies_ms=read_latencies,
        )

    def cleanup(self) -> None:
        """Remove all ingestion test data."""
        self.adapter.execute("MATCH (n:IngestBench) DETACH DELETE n")
