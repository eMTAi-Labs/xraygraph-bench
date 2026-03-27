# core-executor

Benchmarks that measure pure execution engine overhead on non-traversal
workloads.

## Purpose

These benchmarks isolate the execution engine from graph-specific operations.
They measure how efficiently an engine can process flat or nearly-flat data
through scan, filter, projection, aggregation, and sort operators.

## What this exposes

- Storage read-path efficiency
- Filter evaluation cost at varying selectivity
- Projection width impact on throughput
- Aggregation cost vs group cardinality
- Sort and top-k memory behavior
- Covered vs non-covered index access paths

## Benchmarks

| Benchmark | Description |
|-----------|-------------|
| `scan-filter-project` | Scan with predicate filter and column projection at varying selectivity |
| `aggregate-groupby` | Group-by aggregation measuring cost vs group cardinality |
| `sort-topk` | ORDER BY with LIMIT, measuring sort cost and memory pressure |

## Why this matters

Many graph databases are evaluated only on traversal performance. But real
workloads include significant non-traversal operations: filtering large
result sets, aggregating grouped data, sorting for presentation. Engines
with strong traversal but weak executor overhead will underperform on
mixed workloads.

These benchmarks provide a baseline for executor efficiency that is
independent of graph structure.
