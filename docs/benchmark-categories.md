# Benchmark categories

xraygraph-bench organizes benchmarks into five families. Each family targets
a distinct aspect of graph database execution engine behavior.

## core-executor

**Purpose:** Measure pure executor overhead on non-traversal workloads.

These benchmarks isolate the execution engine from graph-specific operations.
They measure how efficiently an engine can scan, filter, project, aggregate,
and sort data.

**What this exposes:**
- Storage read-path efficiency
- Filter evaluation cost at varying selectivity
- Projection width impact on throughput
- Aggregation cost vs group cardinality
- Sort and top-k memory behavior
- Covered vs non-covered index access paths

**Benchmarks:**
- `scan-filter-project` -- scan with predicate filter and column projection
- `aggregate-groupby` -- group-by aggregation at varying cardinality
- `sort-topk` -- ORDER BY with LIMIT, measuring sort cost and memory

## graph-breakers

**Purpose:** Measure irregular graph work that breaks streaming execution.

Graph traversal introduces non-linear cardinality changes that stress
execution engines differently than flat relational work. A single
high-fanout node can multiply the working set by orders of magnitude.

**What this exposes:**
- Edge expansion cost under high fan-out
- Materialization boundary behavior
- Traversal amplification (intermediate cardinality explosion)
- BFS frontier growth and memory pressure
- Post-expand projection and aggregation cost
- Variable-length path enumeration scaling

**Benchmarks:**
- `expand-high-fanout` -- 1-hop expansion on synthetic hub nodes
- `bfs-frontier` -- breadth-first traversal measuring frontier growth
- `multi-hop-traversal` -- variable-length traversal (1-5 hops)

## end-to-end

**Purpose:** Measure performance on realistic graph workloads.

These benchmarks use query patterns and graph shapes drawn from production
use cases. They combine traversal, filtering, and aggregation in ways that
real applications require.

**What this exposes:**
- Performance on mixed operator pipelines
- Query planning quality on complex patterns
- Real-world graph shape handling (not just synthetic regularity)
- Application-level latency characteristics

**Benchmarks:**
- `lineage-basic` -- provenance graph traversal (upstream/downstream)
- `code-dependency-analysis` -- function-to-dependency traversal
- `impact-analysis` -- reverse dependency traversal ("what breaks if X changes")

## public-compare

**Purpose:** Provide portable workloads for fair cross-engine comparison.

These benchmarks use conservative query patterns that map cleanly to multiple
query languages and execution models. They are designed for public sharing
and comparison.

**What this exposes:**
- Baseline traversal performance across engines
- Reachability query scaling
- Simple pattern matching efficiency

**Benchmarks:**
- `portable-reachability` -- bounded reachability from seed nodes

## compile

**Purpose:** Measure compilation and caching behavior.

Engines that compile queries pay upfront cost that amortizes over repeated
execution. This family explicitly separates that cost and measures how
caching and recompilation behave under different access patterns.

**What this exposes:**
- First-run compilation overhead
- Plan cache effectiveness
- Variance across repeated identical queries
- Cache churn under mixed workloads
- Deoptimization and fallback frequency

**Benchmarks:**
- `cold-vs-warm` -- explicit separation of first-run and cached execution
- `repeated-query-stability` -- variance tracking over N identical runs
- `mixed-query-workload` -- cache behavior under rotating query patterns
