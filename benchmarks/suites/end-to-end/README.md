# end-to-end

Benchmarks using realistic graph workloads drawn from production use cases.

## Purpose

These benchmarks combine traversal, filtering, and aggregation in patterns
that real applications require. They use graph shapes and query patterns
from domains like code intelligence, data lineage, and dependency management.

## What this exposes

- Performance on mixed operator pipelines (traverse + filter + aggregate)
- Query planning quality on complex multi-pattern queries
- Real-world graph shape handling (code graphs, provenance graphs)
- Application-level latency characteristics
- How engine behavior changes with realistic data distributions

## Benchmarks

| Benchmark | Description |
|-----------|-------------|
| `lineage-basic` | Provenance graph traversal: upstream and downstream lineage |
| `code-dependency-analysis` | Function-to-dependency traversal in a code graph |
| `impact-analysis` | Reverse dependency traversal ("what breaks if X changes") |

## Why this matters

Synthetic benchmarks with uniform degree distributions and simple query
patterns are necessary for isolating specific engine behaviors, but they do
not predict production performance. End-to-end benchmarks use graph structures
and access patterns drawn from real workloads to test how engines perform
on the queries users actually run.

These benchmarks are particularly relevant for evaluating engines intended
for code intelligence, data governance, and operational analytics.
