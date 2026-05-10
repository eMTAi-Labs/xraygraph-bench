# graph-breakers

Benchmarks that stress irregular graph work: expansion, traversal, and
operations that break streaming execution.

## Purpose

Graph traversal introduces non-linear cardinality changes that stress
execution engines differently than flat relational work. A single
high-fanout node can multiply the working set by orders of magnitude.
BFS frontier growth can be exponential. Multi-hop traversal can produce
intermediate results far larger than the final output.

These benchmarks expose how engines handle the irregularity inherent in
real graph workloads.

## What this exposes

- Edge expansion cost under high fan-out
- Materialization boundary behavior when intermediate cardinality explodes
- Traversal amplification (intermediate result size vs final output size)
- BFS frontier growth and associated memory pressure
- Post-expand projection and aggregation cost
- Variable-length path enumeration scaling with hop count

## Benchmarks

| Benchmark | Description |
|-----------|-------------|
| `expand-high-fanout` | 1-hop expansion on synthetic hub nodes with 10K-1M edges |
| `bfs-frontier` | Breadth-first traversal measuring frontier growth and iteration cost |
| `multi-hop-traversal` | Variable-length traversal (1-5 hops) measuring exponential fan-out |

## Why this matters

Graph-breaker behavior is the primary differentiator between graph database
execution engines. An engine that handles regular fan-out efficiently may
degrade severely on skewed degree distributions. Engines that materialize
intermediate results pay memory cost proportional to the traversal frontier;
engines that stream may avoid this but sacrifice other optimizations.

These benchmarks are designed to make these trade-offs measurable.
