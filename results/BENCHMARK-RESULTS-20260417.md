# xrayGraphDB v4.9.2 Official Benchmark Results

**Date:** 2026-04-17
**Server:** Intel Xeon E3-1265L v3 (4C/8T @ 2.5GHz), 32GB RAM, NVMe SSD
**OS:** Ubuntu 24.04 LTS (kernel 6.8, GLIBC 2.39)
**Database:** xrayGraphDB v4.9.2 "1986 DELAYED ENTRY"
**Protocol:** xrayProtocol (port 7689) — native vectorized columnar transport
**Data loading:** Bolt (port 7687) for stability
**Dataset:** 100K nodes per benchmark
**Timing:** Rust fenced monotonic clock (10-14ns resolution)
**Methodology:** 1 cold run + 10-50 warm runs, outlier detection, 95% BCa bootstrap CI

---

## xrayProtocol Results (23 benchmarks, final run)

### Sub-100µs Tier — Microsecond-Scale Query Latencies

| Benchmark | Warm | P50 | P95 | Description |
|-----------|------|-----|-----|-------------|
| coordinate-conversion | **30µs** | 30µs | 40µs | GIS coordinate transforms |
| string-function-overhead | **40µs** | 40µs | 50µs | Vectorized string ops |
| segmentation-simple | **50µs** | 40µs | 80µs | Graph segmentation with filter |
| math-function-overhead | **60µs** | 50µs | 90µs | Vectorized math on 100K nodes |
| segmentation-medium | **60µs** | 60µs | 70µs | Multi-step graph segmentation |
| segmentation-complex | **70µs** | 60µs | 100µs | Complex graph segmentation with reach |

### Sub-millisecond Tier

| Benchmark | Warm | P50 | P95 | Description |
|-----------|------|-----|-----|-------------|
| expand-high-fanout | **210µs** | 210µs | 230µs | Hub expansion on high-degree nodes |

### Low Latency (1-5ms)

| Benchmark | Warm (ms) | P50 (ms) | P95 (ms) | Description |
|-----------|----------|---------|---------|-------------|
| single-client | **1.00** | 1.01 | 1.04 | Single-client graph query |
| medium-concurrency | **1.27** | 1.26 | 1.38 | Graph query under concurrent load |
| list-function-overhead | **2.14** | 2.15 | 2.20 | List operations |
| military-computation | **4.03** | 4.02 | 4.07 | Domain-specific military computation |
| oceanographic-computation | **4.18** | 4.16 | 4.34 | Domain-specific oceanography |
| physics-computation | **4.19** | 4.20 | 4.23 | Domain-specific physics |
| bearing-navigation | **4.37** | 4.18 | 4.68 | GIS bearing navigation |
| nearest-neighbor-geo | **4.47** | 4.47 | 4.53 | GIS nearest neighbor search |
| destination-projection | **4.52** | 4.51 | 4.63 | GIS destination projection |
| orbital-mechanics | **4.60** | 4.57 | 5.97 | Domain-specific orbital mechanics |

### Analytical Queries (10-344ms)

| Benchmark | Warm (ms) | P50 (ms) | P95 (ms) | Description |
|-----------|----------|---------|---------|-------------|
| scan-filter-project | **10.92** | 10.88 | 11.08 | Full scan + filter on 100K nodes |
| repeated-query-stability | **83.55** | 83.52 | 83.91 | Repeated execution consistency |
| cold-vs-warm | **223.92** | 223.85 | 225.70 | Filter + sort + limit |
| aggregate-groupby | **342.96** | 342.41 | 345.65 | GROUP BY aggregation |

### Heavy Queries (2-5 seconds)

| Benchmark | Warm (ms) | P50 (ms) | P95 (ms) | Description |
|-----------|----------|---------|---------|-------------|
| sort-topk | **2,174** | 2,173 | 2,209 | ORDER BY + LIMIT on 100K nodes |
| bfs-frontier | **5,058** | 5,045 | 5,114 | BFS on 100K-node power-law graph |

---

## LDBC SNB Status

LDBC SF1 (3.2M nodes / 10M+ relationships) requires more RAM than this 32GB server.
Partial load achieved: 925K nodes + 439K relationships before OOM at 28GB limit.
xrayGraphDB recommends 64+ GB RAM for production graph workloads.

**LDBC SF1 will be run on a production-spec server (64GB+ RAM).**

A separate issue was discovered: xrayGraphDB v4.9.2 SIGABRT crashes during
Bolt UNWIND ingestion at approximately 900K-1M total nodes. This has been
documented for the database team.

---

## Protocol Comparison: xrayProtocol vs Bolt

| Benchmark | Bolt (ms) | xrayProtocol (ms) | Speedup |
|-----------|----------|-------------------|---------|
| coordinate-conversion | 0.6 | **0.03** | **20x** |
| string-function-overhead | 0.7 | **0.04** | **18x** |
| segmentation-simple | 0.7 | **0.05** | **14x** |
| math-function-overhead | 0.6 | **0.06** | **10x** |
| segmentation-medium | 0.7 | **0.06** | **12x** |
| segmentation-complex | 0.8 | **0.07** | **11x** |
| expand-high-fanout | 1.1 | **0.21** | **5.2x** |
| single-client | 2.1 | **1.00** | **2.1x** |
| medium-concurrency | 1.8 | **1.27** | **1.4x** |
| list-function-overhead | 3.3 | **2.14** | **1.5x** |
| scan-filter-project | 28.1 | **10.92** | **2.6x** |
| domain-science (avg) | 5.6 | **4.25** | **1.3x** |
| cold-vs-warm | 257 | **224** | **1.15x** |
| aggregate-groupby | 341 | **343** | ~same |

**Summary:** xrayProtocol delivers **10-20x speedup** on sub-millisecond queries (protocol overhead dominates on Bolt), **1.3-2.6x** on medium queries, and converges to ~same on heavy analytical queries where compute dominates.

---

## Cross-Database Comparison

### xrayGraphDB vs Memgraph 2.22.0 (LiveJournal 4.8M nodes/69M edges, Bolt)

| Query | xrayGraphDB | Memgraph | Speedup |
|-------|------------|----------|---------|
| COUNT nodes | 1.94ms | 19.13ms | **9.9x** |
| Scan LIMIT 10K | 28.61ms | 449.02ms | **15.7x** |
| 1-hop traversal | 1.29ms | 42.96ms | **33.3x** |
| 2-hop traversal | 1.43ms | 49.66ms | **34.7x** |
| COUNT edges | 1.53ms | 84.06ms | **54.9x** |
| Load 69M edges | 1.4s | 9,668s | **6,906x** |

*Source: xrayGraphDB benchmark-numbers-update-20260413.md, same binary v4.9.2*

---

## Available Engine Capabilities

xrayGraphDB v4.9.2 provides:
- **385+ vectorized built-in functions** across 30 categories (Aggregation, Aviation, GIS, GraphAnalytics, Math, ParticlePhysics, Physics, RAG_LLM, ReactiveEngine, etc.)
- **10 native xray.* procedures** (edge_aggregate, impact_analysis, query_budget, find_path_budgeted, semantic_search, frontier_profile, neighborhood_stats, topk_reachable, health_report, live_aggregate)
- **Dual protocol:** Bolt (7687) + xrayProtocol (7689)
- **GFQL** (Graph Frame Query Language) via xrayProtocol

---

## Test Coverage

| Category | Total | Ran | JSON | Skip | Skip Reason |
|----------|-------|-----|------|------|-------------|
| compile | 3 | 3 | 2 | 0 | |
| concurrency | 3 | 3 | 2 | 0 | |
| core-executor | 6 | 6 | 6 | 0 | |
| domain-science | 4 | 4 | 4 | 0 | |
| function-eval | 3 | 3 | 3 | 0 | |
| geometric | 8 | 8 | 4 | 0 | |
| graph-breakers | 3 | 3 | 2 | 0 | |
| public-compare | 1 | 1 | 0 | 0 | |
| analytical | 9 | 0 | 0 | 9 | xray.* procs (available, need spec fix) |
| code-intelligence | 5 | 0 | 0 | 5 | code_graph generator needed |
| end-to-end | 3 | 0 | 0 | 3 | code_graph/provenance generators |
| gfql | 6 | 0 | 0 | 6 | GFQL query syntax via xrayProtocol |
| hybrid-vector | 3 | 0 | 0 | 3 | Vector index infrastructure |
| ingestion | 6 | 0 | 0 | 6 | Ingestion measurement methodology |
| **TOTAL** | **68** | **36** | **23** | **32** | |

---

## Reproducibility

```bash
# On Ubuntu 24.04 with xrayGraphDB v4.9.2
source /opt/xraybench-env/bin/activate
./run_all_benchmarks.sh localhost 7689 xraygraphdb-native
```

Results: `/opt/xraybench-results/20260417_010752/`
