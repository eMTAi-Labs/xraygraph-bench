# xraygraph-bench Methodology

A benchmark is only as credible as its methodology. This document defines the
rules, constraints, and disclosure requirements that every xraygraph-bench
result must satisfy. It is the public contract that makes results trustworthy
to outsiders.

## 1. Fairness Contract

Every engine participating in xraygraph-bench operates under identical
conditions. No engine receives an advantage that is not available to all.

### 1.1 Hardware and Environment

- **Same machine.** All engines in a comparison run on the same physical
  hardware in the same run session. Cross-machine comparisons are labeled
  as such and never presented as direct comparisons.
- **Dedicated resources.** No other workloads run during benchmark execution.
  The benchmark runner and the engine under test are the only active processes.
- **Environment captured.** Every result includes: CPU model, core count,
  thread count, NUMA topology, memory total/available, swap status, CPU
  governor, turbo boost status, container detection, and OS kernel version.

### 1.2 Dataset Rules

- **Same dataset.** All engines in a comparison ingest the identical dataset.
  Dataset identity is verified by SHA-256 manifest hash.
- **Same ingestion method.** Tier A benchmarks use batched Cypher `CREATE`
  for all engines. No engine-specific bulk loaders.
- **Same indexes.** Tier A benchmark specs declare exact index creation
  statements. All engines create the same indexes. No undeclared indexes.
- **Dataset verified.** After ingestion, node and edge counts are verified
  against the manifest. Mismatch = no result.

### 1.3 Execution Rules

- **Same query.** All engines execute the identical query string (Cypher).
  No engine-specific hints, pragmas, or query rewrites.
- **Same parameter values.** Benchmark parameters are fixed per run and
  applied identically to all engines.
- **Same run order.** Cold run first, then warm runs. The cold run follows
  a cache clear. Warm runs execute sequentially with no cache clearing.
- **Same warmup protocol.** Adaptive warmup detection (CUSUM) with the same
  configuration parameters for all engines.
- **Same timeout.** Per-query timeout is declared in the benchmark spec and
  applied equally. Timeout = no result for that query, not zero.
- **Same concurrency.** Single-client benchmarks use one connection.
  Concurrent benchmarks use the same client count for all engines.

### 1.4 What Engines May Choose

- **Configuration tuning.** Engines may use any configuration not prohibited
  by the benchmark spec. All configuration is disclosed in the result.
- **Storage mode.** Must be declared: in-memory or disk-backed. Comparisons
  only between engines in the same storage mode.
- **Durability mode.** Must be declared: full ACID durability, relaxed
  durability, or benchmark mode. Comparisons only between engines in the
  same durability class.

### 1.5 What Engines May NOT Do

- Add undeclared indexes in Tier A benchmarks.
- Use engine-specific query hints or extensions in Tier A benchmarks.
- Pre-warm caches outside the benchmark protocol.
- Modify the dataset between cold and warm runs.
- Use a different Bolt protocol version than what the driver negotiates.

---

## 2. Benchmark Classes

Results are never presented as a single "winner" score. Performance is
reported across distinct classes, each measuring a different aspect of
engine behavior.

### 2.1 Single-Query Latency

Measures how fast a single query executes.

- **Cold latency:** Time for the first execution after cache clear.
  Includes compilation, planning, and execution.
- **Warm latency:** Median of steady-state executions after warmup.
  Excludes compilation cost on engines with plan caches.
- **Compile latency:** Cold minus warm (where engine reports it), or
  directly from engine metadata. Reported separately, never hidden.

Reported as: p50, p95, p99, with 95% BCa confidence intervals.

### 2.2 Throughput / QPS

Measures sustained query rate under load.

- **Single-client QPS:** Sequential query execution on one connection.
- **Multi-client QPS:** Concurrent execution at 1, 4, 16, 64, 128 clients.
- **Saturation point:** Client count where throughput plateaus.

Reported as: QPS at each concurrency level, with throughput curve.

### 2.3 Compilation-Aware Performance

Measures the cost of query compilation and caching.

- **Cold vs warm delta:** Percentage difference between cold and warm latency.
- **Plan cache effectiveness:** Warm latency after 1, 10, 100, 1000 repetitions.
- **Mixed workload cache pressure:** Alternating different query shapes.
- **Deoptimization frequency:** How often the engine falls back from compiled
  to interpreted execution.

### 2.4 Mixed-Workload Stability

Measures performance under realistic, non-uniform workloads.

- **Query mix:** Weighted combination of scan, filter, traversal, and
  aggregation queries executing concurrently.
- **Stability over time:** Latency variance over sustained execution periods.
- **Tail latency under load:** p99 latency when the system is at 80% of
  saturation throughput.

### 2.5 Hybrid Graph+Vector (Planned)

Measures the handoff between vector similarity search and graph traversal.

- **Vector → Graph expansion:** Vector top-k results seeding graph neighborhood
  traversal. Measures the transition cost.
- **Graph-constrained ANN:** Approximate nearest neighbor search filtered by
  graph relationship constraints. Measures filter overhead.
- **Multi-hop from vector seeds:** Vector similarity selects start nodes,
  then graph traversal expands N hops. Measures pipeline latency.
- **Rerank by graph context:** Vector candidates reranked using graph path
  or neighborhood features. Measures reranking cost.

This family is distinct because it isolates the **execution break** between
the vector stage and the graph stage — a cost most benchmarks hide.

---

## 3. Engine Mode Declarations

Every result must declare the engine's operating mode. Results are only
comparable within the same mode class.

### 3.1 Required Declarations

| Declaration | Options | Why It Matters |
|-------------|---------|----------------|
| **Storage mode** | in-memory, disk-backed, hybrid | In-memory engines have no I/O cost |
| **Durability** | full-ACID, relaxed, benchmark-mode | Relaxed durability is faster but unsafe |
| **Execution model** | interpreted, vectorized, compiled, JIT | Fundamentally different performance profiles |
| **Concurrency model** | single-threaded, multi-threaded, morsel-parallel | Affects scaling behavior |
| **Transaction isolation** | serializable, snapshot, read-committed | Higher isolation = more overhead |
| **Replication** | none, sync, async | Sync replication adds latency |

### 3.2 Mode in Results

Every result JSON includes:

```json
{
  "engine_mode": {
    "storage": "in-memory",
    "durability": "relaxed",
    "execution_model": "vectorized",
    "concurrency_model": "morsel-parallel",
    "isolation": "snapshot",
    "replication": "none"
  }
}
```

### 3.3 Comparison Rules

- In-memory engines are only compared to other in-memory engines.
- Full-ACID results are compared separately from relaxed-durability results.
- If an engine runs in "benchmark mode" (disabled WAL, no fsync), this must
  be disclosed prominently and results are in a separate class.

---

## 4. Dataset Scaling Tiers

Benchmarks run at multiple dataset sizes to expose scaling behavior.

| Tier | Nodes | Edges | Degree | Use Case |
|------|-------|-------|--------|----------|
| **Small** | 10K | 50K | uniform | Baseline, overhead measurement |
| **Medium** | 100K | 1M | power-law | Typical application scale |
| **Large** | 1M | 10M | power-law | Production-adjacent scale |
| **Skewed** | 100K | 10M | hub (100:100K) | Irregular fan-out stress test |
| **Deep** | 50K | 55K | tree (10 levels) | Hop-depth stress test |

### 4.1 Scaling Reports

For each benchmark, results at all applicable tiers are reported together.
The scaling curve (latency vs dataset size) is a first-class output.

### 4.2 Skewed Degree Distribution

The **skewed** tier is a headline dimension. Uniform-degree benchmarks hide
the differences that matter in production. Real graphs have power-law degree
distributions where a small number of hub nodes connect to millions of edges.
Engines that perform well on uniform graphs may collapse on skewed graphs.

---

## 5. Reproducibility Packets

Every official result is published with a reproducibility packet containing
everything needed to independently verify the numbers.

### 5.1 Packet Contents

| Artifact | Description |
|----------|-------------|
| `result.json` | Full result with all metrics and metadata |
| `benchmark.yaml` | Exact benchmark spec used (with SHA-256) |
| `dataset-manifest.yaml` | Dataset manifest with file hashes |
| `environment.json` | Full hardware/OS/engine capture |
| `engine-config.json` | Complete engine configuration dump |
| `runner-version` | xraybench version and Rust core version |
| `raw-timings.json` | Per-iteration timing array (not just summary) |

### 5.2 Verification Protocol

An independent party can reproduce a result by:

1. Obtaining the same hardware class (or equivalent)
2. Installing the same engine version
3. Applying the same engine configuration
4. Loading the dataset (verified by manifest hash)
5. Running the benchmark spec
6. Comparing results within stated confidence intervals

Results are considered **reproduced** if the independent measurement falls
within the 95% confidence interval of the original result.

---

## 6. Correctness Before Performance

> A fast wrong answer is not a result.

This is not a guideline. It is enforced by the benchmark runner.

### 6.1 Enforcement

- Every benchmark spec defines a correctness oracle.
- The oracle is evaluated on every cold run.
- If the oracle fails, the result is classified as `CORRECTNESS_MISMATCH`.
- `CORRECTNESS_MISMATCH` results are **never** included in performance
  comparisons. They appear in a separate "failures" view.
- No score is published for a benchmark where correctness was not verified.

### 6.2 Oracle Types

| Oracle | What It Checks |
|--------|---------------|
| **exact_match** | BLAKE3 hash of canonical sorted rows |
| **row_count** | Exact expected row count |
| **row_count_range** | Row count within [min, max] |
| **structural** | Path topology (lengths, connectivity, seed nodes) |
| **invariant** | Per-row predicates (all paths ≤ max_depth, etc.) |
| **checksum** | Float-tolerant hash with ULP tolerance |

---

## 7. Result Presentation

### 7.1 No Single Winner

Results are presented as a **matrix**, not a ranking:

| Engine | Cold Latency | Warm Latency | Throughput | Tail p99 | Compile Cost | Stability |
|--------|-------------|-------------|-----------|---------|-------------|----------|
| Engine A | ... | ... | ... | ... | ... | ... |
| Engine B | ... | ... | ... | ... | ... | ... |

Each cell has a value and confidence interval. No cell is highlighted as
"best" — readers draw their own conclusions.

### 7.2 Mandatory Context

Every comparison includes:
- Dataset tier and characteristics
- Engine mode declarations
- Benchmark tier (A = portable, B = engine-native)
- Confidence intervals on every metric
- Sample size (number of iterations)
- Outlier count and handling

### 7.3 Tier Separation

Tier A (portable, cross-engine) and Tier B (engine-native) results are
**never mixed** in the same comparison chart. Tier B results demonstrate
what an engine can do with its native features. Tier A results are the
apples-to-apples comparison.

---

## 8. Benchmark Governance

### 8.1 Spec Lifecycle

```
DRAFT → CANDIDATE → PUBLISHED → DEPRECATED → RETIRED
```

Published specs are immutable. Changes require a new version number.

### 8.2 Version Policy

- **PATCH:** Documentation only. No measurement impact.
- **MINOR:** Parameter range changes. Results comparable within same MAJOR.
- **MAJOR:** Query or dataset changes. Results NOT comparable across MAJOR.

### 8.3 Dispute Resolution

If an engine vendor disputes a result:
1. The vendor provides specific technical objections (not "we disagree").
2. The benchmark team reviews and either corrects the methodology or
   explains why the result stands.
3. All disputes and resolutions are published alongside the results.

---

## 9. Known Limitations

A benchmark suite gains credibility by documenting what it cannot guarantee.

- **OS cache dropping** may require root privileges. Without it, cold-run
  measurements may include warm OS page cache. Reported in results.
- **Clock resolution** varies by hardware. Self-calibration reports actual
  resolution. Measurements below clock resolution are flagged.
- **Adapter overhead** differs between protocols. Calibrated and reported,
  not hidden.
- **Synthetic graphs** approximate but do not replicate real-world topology.
  Results on synthetic data generalize to similar graph structures, not all.
- **Cross-engine Cypher compatibility** is imperfect. Queries tested as
  "portable" may trigger different optimization paths in different engines.
- **Network overhead** for remote engines adds to all measurements equally
  but may dominate for fast queries. Local execution recommended for
  sub-millisecond benchmarks.

---

*xraygraph-bench Methodology v1.0 — Apache License 2.0*
