# compile

Benchmarks that measure compilation, caching, and deoptimization behavior.

## Purpose

Engines that compile queries to native code or bytecode pay upfront cost
that amortizes over repeated execution. This family explicitly separates
compilation cost from execution cost and measures how plan caches,
recompilation, and fallback behavior affect real-world performance.

## What this exposes

- First-run compilation overhead vs warm execution
- Plan cache hit rates under different access patterns
- Execution time variance across identical repeated queries
- Cache churn when query diversity exceeds cache capacity
- Deoptimization and fallback frequency under mixed workloads

## Benchmarks

| Benchmark | Description |
|-----------|-------------|
| `cold-vs-warm` | Explicit comparison of first-run vs cached execution |
| `repeated-query-stability` | Variance tracking over N identical query runs |
| `mixed-query-workload` | Cache behavior under rotating query patterns |

## Why this matters

Compilation behavior is one of the most significant differentiators between
graph database execution engines. An engine that compiles queries can be
10-100x faster on warm runs but 2-5x slower on cold runs compared to an
interpreted engine. Whether compilation is beneficial depends entirely on
the workload:

- **High query reuse (OLTP):** Compilation amortizes well. Warm performance
  dominates.
- **Low query reuse (ad-hoc analytics):** Compilation cost may dominate.
  Cold performance matters.
- **Mixed workloads:** Cache pressure determines whether compilation helps
  or hurts.

These benchmarks make the trade-off measurable rather than theoretical.
