# xraygraph-bench Full Implementation Design

**Date:** 2026-03-31
**Status:** Approved
**Scope:** Complete implementation of the world's first industry-standard graph database benchmark suite

## 1. Overview

xraygraph-bench is a reproducible benchmark suite for evaluating graph database
execution engines. It targets the dimensions that matter in production: execution
model differences (vectorized vs iterator vs compiled), deep traversal under
combinatorial explosion, cold vs warm execution, compilation overhead, and
fallback/deoptimization behavior.

### Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Target engines | xrayGraphDB (Bolt + xrayProtocol), Neo4j, Memgraph | Primary engine + two major comparables |
| Deployment | Environment-agnostic, environment-aware | Works anywhere, captures rich metadata |
| Language | Python orchestration + Rust hot paths | Python for flexibility, Rust for measurement credibility |
| Rust architecture | Layered workspace with shared foundation | Independent crates, single PyO3 boundary |
| Benchmark specs | Engine-informed, research-validated | Novel + defensible, cites published work |
| Visualization | CLI compare + static reports + dashboard | Full stack from terminal to browser |
| Priority | Measurement credibility first | If timing isn't trustworthy, nothing is |
| Data storage | /data/xraybench/{dataset}/ | Outside project tree, persistent across runs |

### Guiding Principles

1. A fast wrong answer is not a result.
2. Measurement overhead must be quantified, not hidden.
3. Every result must be reproducible by an independent party.
4. No engine-specific advantages baked into benchmark design.
5. Deep traversal (10+ hops) is the defining challenge, not a footnote.

## 2. Measurement Foundation (Rust Core)

### 2.1 Timing Harness (`xraybench-timing`)

**Clock source:** `clock_gettime(CLOCK_MONOTONIC_RAW)` on Linux,
`mach_absolute_time()` on macOS. Not `std::time::Instant` — we need monotonic,
non-NTP-adjusted, nanosecond resolution with known error bounds.

**Self-calibration on startup:**
- Measures clock resolution (smallest observable delta)
- Measures clock read overhead (cost of the syscall itself)
- Measures memory fence overhead
- All three values reported alongside every result

**Measurement protocol:**

1. **Fence before** — `std::sync::atomic::fence(SeqCst)` + compiler barrier.
   Prevents out-of-order execution from polluting timing.
2. **Cold run isolation** — adapter clears caches, harness requests OS cache drop
   (if privileged), waits for quiescence, then measures single execution.
3. **Warm-up detection** — CUSUM (cumulative sum) change-point detection identifies
   when execution time stabilizes. Reports warm-up iteration count as metadata.
   Not a fixed N — adapts to the engine's actual behavior.
4. **Steady-state measurement** — after warm-up, collects samples until either:
   (a) target confidence interval width achieved, or (b) max iteration count
   reached. Adaptive sample sizing.
5. **Fence after** — ensures all work completed before reading stop clock.

**Per-measurement record:**

```rust
struct Measurement {
    timestamp_ns: u64,        // wall clock at start
    duration_ns: u64,         // elapsed
    clock_resolution_ns: u64, // measured granularity
    clock_overhead_ns: u64,   // cost of reading clock
    fence_overhead_ns: u64,   // cost of memory fences
    iteration: u32,           // which iteration
    phase: Phase,             // Cold | WarmUp | SteadyState
}
```

### 2.2 Statistical Engine (`xraybench-stats`)

- **Percentiles:** Exact (sorted array) for N < 10,000; t-digest for streaming/large N
- **Confidence intervals:** Bootstrapped BCa (bias-corrected and accelerated) — no
  normality assumption
- **Outlier detection:** Modified Z-score using MAD (median absolute deviation)
- **Variance decomposition:** Separates run-to-run variance from within-run variance
- **Regression detection:** Mann-Whitney U test for comparing two result sets
  (non-parametric)
- **Warm-up detection:** CUSUM change-point detection on time series

### 2.3 Graph Generators (`xraybench-generators`)

Compiled replacements for the current Python generators. Output formats:
edge-list files (binary + text), or streaming Cypher via Python callback.

- **Uniform nodes** — flat property distribution, configurable cardinality
- **Power-law graph** — preferential attachment (Barabási-Albert model),
  configurable exponent. Produces the skewed degree distribution that breaks
  naive traversal.
- **Hub graph** — explicit hub-and-spoke for controlled fanout testing
- **Community graph** — Stochastic Block Model with tunable intra/inter density
- **Chain graph** — linear chain for sequential traversal baseline
- **Deep traversal graph** — NEW. Controlled fanout at each depth level.
  Parameters: `fanout_per_level: [50, 50, 30, 20, 10, 5, 3, 2, 2, 2]` produces
  a graph where hop 1 has fanout 50 but hop 10 has fanout 2, modeling realistic
  social/dependency graphs where connectivity attenuates with distance.

All generators are deterministic (seeded PRNG). Same seed → same graph, byte-identical.

### 2.4 Correctness Validators (`xraybench-checksum`)

**Oracle types:**

1. **Exact match** — BLAKE3 hash of canonical row representation. Row-order-independent
   (sorted). IEEE 754 epsilon-aware float comparison. UTF-8 NFC normalization.
   Streaming — never materializes full result set.
2. **Row count range** — `[min, max]` for queries with engine-dependent cardinality.
3. **Structural validation** — topology checks for graph-shaped results (path lengths,
   degree distributions, connectivity).
4. **Checksum with tolerance** — ULP (units in last place) tolerance for
   floating-point aggregations.
5. **Invariant validation** — predicate-based: "every path has length <= max_depth",
   "every node has property X > threshold". Python callables validated per row.

**Audit trail per validation:**

```json
{
  "oracle_type": "exact_match",
  "passed": true,
  "reference_hash": "blake3:a1b2c3...",
  "computed_hash": "blake3:a1b2c3...",
  "row_count_expected": 42731,
  "row_count_actual": 42731,
  "float_tolerance_ulp": 4,
  "validation_duration_ms": 23.4,
  "validator_version": "0.1.0"
}
```

### 2.5 Result Comparison Engine (`xraybench-compare`)

Compares two or more result sets with statistical rigor:

- Per-metric comparison: absolute delta, percentage change, confidence interval
- Mann-Whitney U test for statistical significance
- Regression/improvement classification with configurable threshold
- Multi-engine comparison matrix
- Output: structured JSON, human-readable table, or report section

## 3. Adapter Architecture

### 3.1 Revised Interface

```python
class BenchmarkAdapter(ABC):
    # Lifecycle
    def connect(self, config: AdapterConfig) -> ConnectionInfo
    def close(self) -> None
    def health_check(self) -> HealthStatus

    # Dataset
    def load_dataset(self, manifest: DatasetManifest, source: DataSource) -> LoadReport
    def verify_dataset(self, manifest: DatasetManifest) -> DatasetVerification
    def clear_dataset(self) -> None

    # Execution
    def execute(self, query: str, params: dict) -> ExecutionResult
    def explain(self, query: str, params: dict) -> QueryPlan
    def profile(self, query: str, params: dict) -> ProfileResult

    # Cache & State
    def clear_caches(self) -> CacheClearReport
    def engine_state(self) -> EngineState

    # Metadata
    def engine_info(self) -> EngineInfo
    def capabilities(self) -> set[Capability]
```

### 3.2 Capability Declaration

```python
class Capability(Enum):
    COMPILE_TIME_REPORTING = "compile_time"
    PLAN_PROFILING = "plan_profile"
    CACHE_CLEAR = "cache_clear"
    VECTORIZED_METRICS = "vectorized_metrics"
    STREAMING_RESULTS = "streaming_results"
    NATIVE_PROTOCOL = "native_protocol"
    EXPLAIN_ANALYZE = "explain_analyze"
    MEMORY_REPORTING = "memory_reporting"
```

Runner checks capabilities before measurement. Missing capability → result field
is `null` with `"reason": "engine_does_not_report"`, not a silent zero.

### 3.3 Adapter Overhead Measurement

At adapter startup: execute `RETURN 1` 1000 times, measure round-trip.
Report `adapter_overhead_ms` (median no-op latency) in every result.
This is metadata — not subtracted — but tells auditors the adapter's noise floor.

### 3.4 xrayGraphDB Adapters

**Bolt path (`XrayGraphDBBoltAdapter`):**
- `neo4j` Python driver against port 7688
- Extracts `result_available_after`, `result_consumed_after` from Bolt metadata
- `EXPLAIN ANALYZE VERBOSE` for plan introspection
- Comparable measurement path to Neo4j/Memgraph

**xrayProtocol path (`XrayGraphDBNativeAdapter`):**
- Custom Python client for binary frame protocol (port 7689)
- Frame header: `[4B payload_len][1B msg_type][1B flags][2B query_id][payload]`
- Parses SCHEMA/BATCH/COMPLETE messages for columnar results
- Extracts per-batch timing, morsel counts, kernel selection
- Captures vectorized execution metrics
- Reports protocol overhead separately from query execution time

### 3.5 GFQL Support (xrayGraphDB Only)

xrayGraphDB supports GFQL (Graph Frame Query Language) as a first-class
query language alongside Cypher. GFQL uses dataframe-style chaining:

```
chain(n({type: "User"}), e_forward(hops: 3), n(), order_by("name"), limit(100))
```

**Adapter requirements for GFQL:**
- Session setup: `SET GFQL_CONTEXT tenant_id = '...', repo_id = '...'` before any GFQL query
- Query language auto-detected by engine (2-char lookahead, sub-50ns)
- Works over both Bolt and xrayProtocol
- Results identical to Cypher — same correctness oracles apply
- EXPLAIN returns transpiled Cypher in `gfql_plan` column

**Benchmark implications:**
- GFQL benchmarks are always Tier B (engine-native)
- Benchmark specs with `query_language: gfql` include a `gfql_template` field
- Key measurement: transpilation overhead (GFQL→Cypher parse/transpile cost)
- Correctness cross-check: GFQL and Cypher on the same query must produce
  equivalent results. This is a built-in validation for each GFQL benchmark.

**GFQL-specific benchmarks (Tier B):**
- GFQL variants of core-executor and graph-breaker benchmarks
- Transpilation overhead measurement (cold GFQL vs cold Cypher on same query)
- GFQL graph algorithm benchmarks (BFS, PageRank, shortest path via GFQL builtins)

### 3.6 Neo4j Adapter

- Bolt via `neo4j` Python driver
- `CALL db.clearQueryCaches()` for cache clearing
- `EXPLAIN` / `PROFILE` for plan introspection
- JMX or Prometheus endpoint for memory/cache metrics
- Dataset loading via batched Cypher `CREATE` or `LOAD CSV`

### 3.7 Memgraph Adapter

- Bolt via `neo4j` Python driver (wire-compatible)
- `FREE MEMORY` for cache clearing
- `EXPLAIN` / `PROFILE` for plan introspection
- HTTP `/metrics` for memory reporting
- Dataset loading via batched Cypher `CREATE`

### 3.8 Dataset Loading Protocol

1. Adapter receives `DatasetManifest` + `DataSource` (generator iterator or file path)
2. Batched ingestion: 1000 statements per transaction (configurable)
3. Progress reporting with row counts
4. Post-load verification: `MATCH (n) RETURN count(n)` and
   `MATCH ()-[r]->() RETURN count(r)` compared against manifest
5. Reports: load duration, rows ingested, verification pass/fail

## 4. Dataset Pipeline

### 4.1 Storage Layout

```
/data/xraybench/
  synthetic/
    uniform-1m/           # 1M uniform nodes
    power-law-1m/         # 1M power-law graph
    hub-100x100k/         # 100 hubs × 100K spokes
    community-50x20k/     # 50 communities × 20K nodes
    deep-traversal-1m/    # 1M nodes with depth-controlled fanout
  snap/
    soc-LiveJournal1/     # 4.8M nodes, 69M edges
    web-Google/           # 875K nodes, 5.1M edges
  ogb/
    ogbn-products/        # 2.4M nodes, 61.9M edges
    ogbn-papers100M/      # 111M nodes, 1.6B edges
  manifests/              # YAML manifests with hashes
```

### 4.2 Synthetic Generation

Rust generators write binary edge-list files to `/data/xraybench/synthetic/`.
Each dataset directory contains:
- `edges.bin` — binary edge list (u64 source, u64 target pairs)
- `edges.csv` — text edge list for engines that prefer CSV import
- `nodes.bin` / `nodes.csv` — node properties
- `manifest.yaml` — dataset metadata, SHA-256 of all data files, generation params

### 4.3 External Datasets (SNAP, OGB)

Download pipeline:
1. Fetch from source URL
2. Verify SHA-256 against manifest
3. Decompress (gzip/bz2)
4. Convert to canonical edge-list format
5. Generate manifest with actual counts and hash
6. Store in `/data/xraybench/{source}/{name}/`

### 4.4 Dataset Verification

Before any benchmark run:
1. Check manifest exists and data files present
2. Verify SHA-256 of data files against manifest
3. If verification fails, refuse to run — do not silently use stale data

## 5. Benchmark Specifications

### 5.1 Spec Format

Immutable once versioned. Changes require new version number. Each spec YAML contains:

```yaml
name: benchmark-name
family: core-executor | graph-breakers | end-to-end | public-compare | compile
version: "1.0.0"
status: draft | candidate | published | deprecated | retired
tier: A | B                 # A = portable cross-engine, B = engine-native advantage
description: ...
citation: ...               # research basis
dataset: ...
query_language: cypher | gfql  # default: cypher. GFQL = Tier B only.
query_template: ...
gfql_template: ...          # optional GFQL equivalent (Tier B benchmarks)
parameters: ...
indexes: ...                # exact index creation statements (Tier A)
resource_limits:            # standardized for Tier A
  memory_gb: 8
  threads: null             # null = use host core count
correctness_oracle: ...
  duplicates: allowed | deduplicated | unspecified  # for path results
metrics: ...
tags: [...]
```

### 5.2 Missing Specs — Design Summary

**core-executor/aggregate-groupby** (v1.0.0)
- GROUP BY with varying cardinality (10, 1K, 100K, 1M groups)
- Exposes hash-aggregate vs sort-aggregate engine decisions
- Research basis: TPC-H Q1 aggregation, LDBC SNB BI workloads

**core-executor/sort-topk** (v1.0.0)
- ORDER BY + LIMIT with varying K and sort key distributions
- Exposes heap-based vs full-sort engine decisions
- Research basis: Top-K query processing literature (Ilyas et al.)

**graph-breakers/bfs-frontier** (v1.0.0)
- BFS from seed node, measure frontier size at each depth
- Progressive depth: 1, 3, 5, 7, 10 hops
- Dataset: power-law and deep-traversal graphs
- Exposes: memory pressure, frontier materialization, SIMD BFS vs scalar
- Research basis: Graph500 BFS benchmark, Beamer et al. direction-optimizing BFS

**graph-breakers/multi-hop-traversal** (v1.0.0)
- Variable-length path queries with increasing depth
- Parameterized fanout control per depth level
- Exposes: combinatorial explosion handling, pruning, backpressure
- Research basis: LDBC SNB interactive workloads, Dann et al. path query evaluation

**end-to-end/code-dependency-analysis** (v1.0.0)
- Find all transitive dependencies of a module
- Dataset: code-graph (function call graph)
- Exposes: real-world traversal with heterogeneous edge types
- Research basis: Software dependency analysis (Bavota et al.)

**end-to-end/impact-analysis** (v1.0.0)
- Given a changed node, find all downstream affected nodes
- Reverse dependency traversal with depth limit
- Exposes: reverse adjacency performance, fan-in analysis
- Research basis: Change impact analysis (Lehnert et al.)

**public-compare/portable-reachability** (v1.0.0)
- Simple reachability query portable across all graph databases
- Designed for fair cross-engine comparison on neutral ground
- Research basis: LDBC Graphalytics reachability

**compile/cold-vs-warm** (v1.0.0)
- Same query executed cold (first time) then warm (repeated)
- Measures compilation cost as the delta
- Exposes: JIT warmup, plan caching, recompilation triggers
- Research basis: Neumann "Efficiently Compiling Efficient Query Plans"

**compile/repeated-query-stability** (v1.0.0)
- Same query 10,000 times — measure variance, detect degradation
- Exposes: GC pauses, memory leaks, cache eviction under repetition
- Research basis: Database performance stability (Raasveldt & Mühleisen)

**compile/mixed-query-workload** (v1.0.0)
- Alternating different query shapes — measures plan cache contention
- Exposes: cache thrashing, recompilation frequency, working set behavior
- Research basis: Real-world mixed workload analysis (Pavlo et al.)

## 6. Visualization Stack

### 6.1 CLI Comparison (`xraybench compare`)

```bash
xraybench compare result-a.json result-b.json --confidence 0.95
```

Output: structured table with per-metric delta, percentage change,
confidence interval, and significance flag. Machine-readable JSON output
with `--format json`.

### 6.2 Static Report (`xraybench report`)

```bash
xraybench report results/ --output report.html
```

Self-contained HTML file with embedded Plotly.js charts:
- Latency distributions (cold and warm, overlaid)
- Cold vs warm scatter plot per benchmark
- Throughput curves (from load test results)
- Scaling plots (concurrency vs QPS)
- Engine comparison matrices
- Full environment metadata sidebar

### 6.3 Data Export

```bash
xraybench export results/ --format parquet --output results.parquet
xraybench export results/ --format csv --output results.csv
```

### 6.4 Interactive Dashboard (`xraybench dashboard`)

```bash
xraybench dashboard --results-dir results/ --port 8080
```

FastAPI + HTMX. No heavy JS framework — server-rendered with progressive
enhancement.

- Filter by engine, family, benchmark, date range
- Drill into individual runs
- Overlay multiple engines on same chart
- Time-series view for regression tracking
- Live refresh when new results appear

## 7. Environment Capture

Automatic, embedded in every result file:

```json
{
  "host": {
    "os": "Linux 6.8.0",
    "cpu": "AMD EPYC 9654",
    "cores": 96,
    "threads": 192,
    "numa_nodes": 2,
    "cpu_governor": "performance",
    "memory_gb": 768,
    "memory_available_gb": 742,
    "hugepages_total": 0,
    "swap_gb": 0,
    "container": false,
    "cgroup_memory_limit_gb": null
  },
  "engine": {
    "name": "xraygraphdb",
    "version": "4.0.2",
    "build": "release",
    "config_hash": "sha256:...",
    "capabilities": ["compile_time", "plan_profile", "vectorized_metrics"]
  },
  "benchmark_runner": {
    "version": "0.1.0",
    "rust_core_version": "0.1.0",
    "python_version": "3.12.3",
    "timestamp": "2026-03-31T14:22:00Z"
  },
  "resource_control": {
    "cpu_governor": "performance",
    "turbo_boost": false,
    "swap_enabled": false,
    "core_pinning": null,
    "numa_policy": "local",
    "engine_memory_limit_gb": 8,
    "cache_drop_successful": true,
    "engine_restarted_for_cold": true
  },
  "outcome": "success",
  "outcome_detail": null,
  "tier": "A"
}
```

## 8. Rust Workspace Structure

```
rust/
  Cargo.toml                    # workspace root
  xraybench-types/
    src/lib.rs                  # shared types, errors, constants
  xraybench-timing/
    src/lib.rs                  # clock, measurement, fences
    src/clock.rs                # platform-specific clock access
    src/calibration.rs          # self-calibration
    src/warmup.rs               # CUSUM change-point detection
  xraybench-stats/
    src/lib.rs                  # statistical computations
    src/percentile.rs           # exact + t-digest
    src/bootstrap.rs            # BCa confidence intervals
    src/outlier.rs              # MAD-based detection
    src/regression.rs           # Mann-Whitney U test
  xraybench-generators/
    src/lib.rs                  # graph generators
    src/uniform.rs
    src/power_law.rs
    src/hub.rs
    src/community.rs
    src/chain.rs
    src/deep_traversal.rs
    src/io.rs                   # edge-list file I/O
  xraybench-checksum/
    src/lib.rs                  # correctness validation
    src/blake3.rs               # streaming BLAKE3
    src/canonical.rs            # deterministic row serialization
    src/structural.rs           # topology validation
  xraybench-compare/
    src/lib.rs                  # result comparison engine
    src/diff.rs                 # per-metric delta + CI
    src/significance.rs         # statistical tests
    src/matrix.rs               # multi-engine comparison
  xraybench-py/
    src/lib.rs                  # PyO3 bindings, re-exports all crates
    Cargo.toml                  # depends on all workspace crates
```

## 9. Fairness Policy

An industry benchmark is only as credible as its fairness guarantees. This
section defines the rules that prevent any engine from gaining an unfair
advantage.

### 9.1 Benchmark Tiers

All benchmarks are classified into one of two tiers:

**Tier A — Portable Cross-Engine Comparison**
- All engines use the **Bolt protocol** (or OpenCypher-compatible wire protocol)
- Queries use **portable Cypher** — no engine-specific extensions, hints, or procedures
- Dataset loading uses **batched Cypher CREATE** — no engine-native bulk loaders
- Index creation is **standardized per spec** — each benchmark spec declares exactly
  which indexes are created, using portable syntax. No undeclared indexes.
- Engine configuration uses **default settings** unless the spec explicitly overrides
  (e.g., memory limit). Overrides are declared in the spec and applied to all engines.
- Results from Tier A benchmarks are directly comparable across engines.

**Tier B — Engine-Native Advantage**
- Engines may use their **best native protocol** (xrayProtocol for xrayGraphDB)
- Engine-specific **bulk loaders, indexes, hints, and configuration tuning** are allowed
- Engine-specific **metrics** (vectorized profile, morsel counts, kernel selection) are captured
- Results from Tier B benchmarks are **not directly comparable** across engines —
  they show what each engine can do when unconstrained.
- Tier B results are always displayed separately from Tier A in reports and dashboards.

**Classification rule:** Every benchmark spec declares `tier: A` or `tier: B`.
The `public-compare` family is always Tier A. The `compile` family is always Tier B
(compile behavior is inherently engine-specific). Other families may have both tiers.

### 9.2 Index Policy

- Tier A: benchmark spec declares exact index creation statements. All engines
  create the same indexes using portable Cypher. No additional indexes allowed.
- Tier B: engines may create any indexes they choose. Index creation is logged
  in the result metadata.

### 9.3 Configuration Standardization

- Tier A: engines run with default configuration except for:
  - Memory allocation (standardized per spec, e.g., "8 GB heap")
  - Thread count (standardized per spec or set to host core count)
  - Storage mode (in-memory for all engines, or on-disk for all — never mixed)
- Tier B: engines may use any configuration. Full config dump captured in result.

### 9.4 Dataset Loading Normalization

- Tier A: all engines ingest via batched Cypher CREATE in identical transactions.
  Loading time is measured but reported separately from query execution.
- Tier B: engines may use native bulk loaders (e.g., `LOAD CSV`, binary import).
  Loading method and time are documented in the result.

### 9.5 Protocol Comparison Within xrayGraphDB

xrayGraphDB Bolt results are compared against Neo4j/Memgraph Bolt results (Tier A).
xrayGraphDB xrayProtocol results are Tier B — they demonstrate native capability
but are never placed in the same comparison chart as Bolt results from other engines.
The report explicitly separates these: "Cross-engine (Bolt)" vs "Native protocol."

## 10. Failure Classification

Every benchmark execution produces a machine-readable outcome status.
Results are never silently missing — every run has an explicit classification.

```python
class Outcome(Enum):
    SUCCESS = "success"                      # completed, correctness passed
    CORRECTNESS_MISMATCH = "correctness_mismatch"  # completed, wrong answer
    ENGINE_ERROR = "engine_error"            # engine returned an error
    TIMEOUT = "timeout"                      # exceeded time limit
    UNSUPPORTED = "unsupported"              # engine lacks required capability
    DATASET_VERIFICATION_FAILED = "dataset_verification_failed"
    HARNESS_FAILURE = "harness_failure"      # benchmark runner internal error
    CONNECTION_FAILURE = "connection_failure" # could not reach engine
    OOM = "out_of_memory"                    # engine or host OOM
```

Every result file includes:

```json
{
  "outcome": "success",
  "outcome_detail": null,
  "outcome_timestamp": "2026-03-31T14:22:00Z"
}
```

On failure:

```json
{
  "outcome": "engine_error",
  "outcome_detail": "RuntimeError: query exceeded memory limit (8589934592 bytes)",
  "outcome_timestamp": "2026-03-31T14:22:03Z",
  "partial_metrics": {
    "duration_before_failure_ms": 4231.7
  }
}
```

Dashboards and comparison tools filter by outcome. Only `SUCCESS` results are
used in performance comparisons. Other outcomes are displayed in a separate
"failures" view with classification breakdowns.

## 11. Resource Control Policy

The difference between an "interesting" benchmark and a publishable one is
whether the execution environment is controlled. These policies apply to
all Tier A benchmarks. Tier B benchmarks document deviations.

### 11.1 CPU Policy

- **Governor:** Must be set to `performance` (no dynamic frequency scaling).
  Runner verifies at startup and warns if not set.
- **Turbo boost:** Must be disabled for Tier A. Frequency variance is a
  confound. Runner checks `/sys/devices/system/cpu/intel_pstate/no_turbo`
  or equivalent.
- **Core pinning:** Not required by default. If used, must be declared in
  result metadata. Benchmark specs may require it for specific workloads.
- **NUMA policy:** For multi-socket systems, engine and benchmark runner should
  be on the same NUMA node. Runner captures NUMA topology in environment metadata.

### 11.2 Memory Policy

- **Swap:** Must be disabled (`swapoff -a`). Swap activity is a confound that
  makes timing measurements meaningless. Runner verifies.
- **Memory limit:** Tier A specs declare a memory budget. All engines get the
  same budget. Runner monitors RSS and reports peak memory in results.
- **Huge pages:** Optional. If enabled, declared in environment metadata.

### 11.3 Engine Lifecycle

- **Cold runs:** Engine is restarted between cold-run measurements unless the
  benchmark spec explicitly says otherwise. "Clear caches" within a running
  engine is not equivalent to a fresh start — plan caches, JIT artifacts,
  and internal state may persist.
- **Warm runs:** Engine stays running. Only query-level cache is cleared
  between warm iterations (via adapter's `clear_caches()`).
- **Between benchmarks:** Engine is restarted and dataset is reloaded. No
  cross-benchmark contamination.

### 11.4 Runner Isolation

- Benchmark runner should not compete with the engine for resources.
- Recommended: runner on separate cores or separate machine.
- If co-located, runner's CPU/memory overhead is captured in environment metadata.

## 12. Benchmark Governance

### 12.1 Spec Lifecycle

```
DRAFT → CANDIDATE → PUBLISHED → DEPRECATED → RETIRED
```

- **DRAFT:** Under development. May change freely. Not used in official results.
- **CANDIDATE:** Feature-complete, seeking review. May change based on feedback.
- **PUBLISHED:** Immutable. Any change requires a new version number.
- **DEPRECATED:** Superseded by a newer version. Still valid for historical comparison.
  Results generated against deprecated specs are still legitimate.
- **RETIRED:** No longer recommended. Results still valid but not included in
  new comparison reports.

### 12.2 Version Policy

- Spec versions use semver: `MAJOR.MINOR.PATCH`
- **PATCH:** Documentation/description clarification only. No measurement impact.
- **MINOR:** Parameter range change, oracle tolerance adjustment, or new optional metric.
  Results from MINOR versions within the same MAJOR are comparable.
- **MAJOR:** Query change, dataset change, or correctness oracle change.
  Results across MAJOR versions are NOT comparable.

### 12.3 Oracle Regeneration

Reference answers (BLAKE3 hashes, expected row counts) are generated by:
1. Running the benchmark against a reference engine with known-correct behavior
2. Manually verifying a sample of results
3. Committing the reference data alongside the spec
4. Reference data carries the spec version and generation timestamp

When a spec gets a new MAJOR version, reference data is regenerated.

### 12.4 Approval Process

- New specs require review against the fairness policy before reaching CANDIDATE
- PUBLISHED status requires: (a) passing validation, (b) reference oracle generated,
  (c) at least one successful run against a real engine

## 13. Known Limitations and Threats to Validity

A benchmark suite gains credibility by documenting what it cannot guarantee.

### 13.1 Measurement Limitations

- **OS cache dropping** requires root privileges. Without privileges, cold-run
  measurements may include warm OS page cache. The runner reports whether
  cache drop was successful.
- **Clock resolution** varies by hardware. On some VMs, `CLOCK_MONOTONIC_RAW`
  may have microsecond (not nanosecond) resolution. The self-calibration
  reports actual resolution so consumers can assess measurement granularity.
- **Warm-up detection** is statistical (CUSUM change-point). It may misidentify
  the transition point in pathological cases (e.g., bimodal execution time).
  The full time series is preserved so this can be audited.

### 13.2 Adapter Limitations

- Different adapters introduce different overhead. The calibration measurement
  quantifies this, but it cannot be perfectly eliminated.
- Bolt protocol serialization overhead differs from xrayProtocol. Tier separation
  addresses this, but within Tier A, Bolt overhead is present for all engines.
- Some engines expose richer profiling than others. The capability system handles
  this gracefully, but engines with more capabilities produce richer results.

### 13.3 Dataset Limitations

- Synthetic graphs are approximations of real-world topology. Power-law generators
  produce degree distributions that match the exponent but not the clustering
  coefficient or community structure of real social/dependency networks.
- SNAP and OGB datasets have known biases (collection methodology, domain-specific
  structure). Results on these datasets generalize to similar domains, not all graphs.
- Dataset loading performance is engine-dependent and may warm internal structures
  differently, affecting subsequent query performance.

### 13.4 Cross-Engine Comparison Limitations

- Cypher dialects vary. Queries tested as "portable" may trigger different
  optimization paths in different engines due to parser/planner differences.
- "Default configuration" means different things for different engines. A Neo4j
  default heap of 1 GB vs a Memgraph default of "all available memory" is not
  a level playing field. Tier A specs must specify explicit resource limits.
- Engine maturity differs. A benchmark that reveals one engine's weakness may
  simply reflect that engine's development priorities, not a fundamental
  architectural limitation.

### 13.5 Path Result Semantics

Variable-length path queries require special treatment for correctness validation:

- **Path enumeration order** differs between engines. Row-order-independent
  comparison is necessary but not sufficient.
- **Path representation** differs. Some engines return full path objects; others
  return node/relationship ID lists. Canonical form for comparison: sorted list
  of (source_id, target_id, hop_index) tuples extracted from each path.
- **Duplicate path semantics** may differ. Some engines deduplicate by default;
  others enumerate all paths including duplicates. Benchmark specs must declare
  expected duplicate handling: `duplicates: allowed | deduplicated | unspecified`.
  When `unspecified`, correctness checks use invariant validation (path validity)
  rather than exact match.
- **Equivalent but different paths.** Between nodes A and Z, paths
  A→B→C→Z and A→D→E→Z are different paths. But A→B→C→Z returned as
  `[A,B,C,Z]` vs `[Z,C,B,A]` (reversed) is the same path. Canonical form
  uses source-to-target ordering.

## 14. Implementation Priority

Measurement credibility first. Each phase builds on the previous.

**Phase 1: Rust Core + Unit Tests**
1. xraybench-types (shared types)
2. xraybench-timing (clock, calibration, fences, CUSUM)
3. xraybench-stats (percentiles, bootstrap CI, outlier, Mann-Whitney)
4. xraybench-checksum (BLAKE3, canonical serialization)
5. xraybench-generators (all 6 generators)
6. xraybench-compare (diff engine)
7. xraybench-py (PyO3 bindings)
8. Unit tests for every crate — timing accuracy, statistical correctness,
   generator determinism, hash collision resistance

**Phase 2: Adapter Implementations**
1. Revised adapter base class with capabilities
2. xrayGraphDB Bolt adapter (fully working)
3. xrayGraphDB xrayProtocol adapter (native client)
4. Neo4j adapter (fully working)
5. Memgraph adapter (fully working)
6. Adapter overhead calibration
7. Integration tests against real engines

**Phase 3: Dataset Pipeline**
1. Synthetic dataset generation (Rust generators → /data/xraybench/)
2. SNAP download + ingestion pipeline
3. OGB download + ingestion pipeline
4. Dataset verification framework
5. Manifest generation and validation

**Phase 4: Benchmark Specs**
1. Write all 10 missing benchmark specs with research citations
2. Correctness oracle reference data generation
3. Spec validation in CI

**Phase 5: Runner Integration**
1. Update runner to use Rust timing harness
2. Integrate adaptive warm-up detection
3. Integrate correctness validation framework
4. Environment capture (full host/engine metadata)
5. End-to-end integration tests

**Phase 6: Visualization**
1. CLI comparison tool
2. Static HTML report generator
3. CSV/Parquet export
4. FastAPI + HTMX interactive dashboard

**Phase 7: CI & Reproducibility**
1. Rust crate tests in CI
2. Python integration tests in CI
3. Schema validation (existing, extend)
4. Reproducibility proof runs
