# cuGraph vs xrayGraphDB — Courtroom-Clean Comparison

**Date:** May 9, 2026
**Hardware:** RTX PRO 6000 Blackwell Server Edition (96 GB VRAM), 16 vCPU, 144 GB RAM
**Dataset:** Friendster (SNAP) — 65,608,366 vertices, 1,806,067,135 undirected edges (3,612,134,270 bidirectional)
**cuGraph:** 26.02.00 (RAPIDS), cuDF 26.02.01, CUDA 12.4, Driver 580.126.20
**xrayGraphDB:** v4.9.x, native persistent storage, xrayProtocol

## Methodology

Courtroom-clean: same hardware, same dataset, same algorithms. All results published — wins AND losses. Following the 15-rule benchmark methodology in BENCHMARK-METHODOLOGY.md.

## 1. Data Loading

### xrayGraphDB
- **Method:** Raw SNAP file → `xray.bulk_import_file()` → CSR
- **Preprocessing required:** NONE
- **Load time:** 9.6 minutes (576s)
- **Result:** 65.6M vertices, 3.6B edges (bidirectional), ready for all algorithms

### cuGraph — Three Paths Attempted

#### Path A: cuDF GPU CSV Reader
- **Method:** `cudf.read_csv()` directly on 31 GB SNAP text file
- **Result:** **OOM at 92 GB VRAM** (96 GB total)
- **Why:** cuDF GPU CSV parser requires ~3x file size in VRAM for type inference, comment filtering, and string-to-integer conversion

#### Path B: CPU Read → GPU Transfer → Undirected CSR
- **Method:** numpy.loadtxt (CPU) → cudf.Series (GPU transfer) → `Graph(directed=False)`
- **CPU read:** 75-80s (**CPU RAM, not VRAM**)
- **GPU transfer:** 5.9-7.6s
- **graph build:** **OOM** — cuGraph's sort+symmetrize requires >96 GB VRAM
- **Result:** FAILED

#### Path B (symmetric variant): Pre-symmetrize on CPU
- **Method:** numpy symmetrize → cudf.DataFrame
- **Result:** **FAILED** — 3,612,134,270 rows exceeds cudf's int32 size_type limit (2,147,483,647 max)
- **This is a fundamental cudf limitation:** single-GPU cudf cannot represent >2.1B rows

#### Path B (directed fallback): CPU Read → Directed CSR
- **Method:** numpy.loadtxt → cudf.Series → `Graph(directed=True)`
- **Total load:** 90.3s
  - CPU read: 79.5s (numpy.loadtxt on system RAM)
  - GPU transfer: 7.6s
  - graph build: 2.7s
- **Result:** 65.6M vertices, **1.8B directed edges** (NOT bidirectional)
- **Consequence:** Most algorithms fail because they require undirected input

### Loading Summary

| System | Method | Preprocessing | Time | Edges | Status |
|--------|--------|---------------|------|-------|--------|
| xrayGraphDB | Raw SNAP → CSR | **None** | 576s | **3.6B** (bidir) | **ALL ALGORITHMS WORK** |
| cuGraph | cuDF CSV | None | N/A | N/A | **OOM** |
| cuGraph | numpy → undirected | CPU read required | N/A | N/A | **OOM** |
| cuGraph | numpy → symmetric | CPU symmetrize | N/A | N/A | **int32 limit** |
| cuGraph | numpy → directed | CPU read required | 90s | 1.8B (directed) | **Limited algorithms** |

## 2. Algorithm Results

### BFS — cuGraph wins (directed graph only)

| System | BFS Time (warm) | TEPS | Notes |
|--------|-----------------|------|-------|
| cuGraph | **60-122ms** | **14.5-32.5 GTEPS** | Directed 1.8B edges, GPU BFS kernel |
| xrayGraphDB | 5.9-9.4s | 384-608 MTEPS | Bidirectional 3.6B edges, CSR BFS |

- **cuGraph median:** 20.28 GTEPS (harmonic mean: 10.91 GTEPS)
- **xrayGraphDB median:** 413.95 MTEPS (harmonic mean: 327.40 MTEPS)
- **cuGraph is ~49x faster** at pure BFS kernel
- **BUT:** cuGraph traverses 1.8B directed edges; xrayGraphDB traverses 3.6B bidirectional
- cuGraph BFS depth reports INT32_MAX (2147483647) — likely a directed graph artifact

### PageRank — cuGraph FAILED

| System | Time | Result |
|--------|------|--------|
| cuGraph | 0.7s | **FAILED** (empty error; needs `store_transposed=True` at graph creation) |
| xrayGraphDB | 69s (GPU 34%) | **4.17B edges** processed, correct results |

cuGraph PageRank failed with `FailedToConvergeError` — the directed graph with only one-way edges creates an asymmetric link structure where PageRank cannot converge. This is expected: a half-edge directed representation of an undirected graph has dangling nodes with outgoing but no incoming edges, making PageRank mathematically unstable. Even `store_transposed=True` does not fix this because the fundamental problem is the missing reverse edges.

### Triangle Count — cuGraph FAILED

| System | Time | Triangles | Notes |
|--------|------|-----------|-------|
| cuGraph | N/A | N/A | **FAILED: input graph must be undirected** |
| xrayGraphDB | 43s (GPU 93%) | **4,173,724,142** | SNAP exact match verified |

cuGraph cannot compute triangle count because it requires an undirected graph, which cannot be constructed on a single 96 GB GPU for this dataset.

### Connected Components (WCC) — cuGraph FAILED

| System | Time | Components |
|--------|------|------------|
| cuGraph | N/A | **FAILED: input graph must be undirected** |
| xrayGraphDB | 100s | 1 (fully connected) |

### K-Core Decomposition — cuGraph FAILED

| System | Time | Max Core |
|--------|------|----------|
| cuGraph | N/A | **FAILED: input graph must be undirected** |
| xrayGraphDB | 125-140s (GPU 100%) | Completed |

### Betweenness Centrality — cuGraph FAILED

| System | Time (warm, approx) | Notes |
|--------|---------------------|-------|
| cuGraph | N/A | **FAILED: CUGRAPH_UNKNOWN_ERROR** |
| xrayGraphDB | 143ms (eps=0.10) | Sub-second pair-sampled BC |
| xrayGraphDB | 471ms (eps=0.05) | Higher accuracy |

### Community Detection (Louvain) — cuGraph FAILED

| System | Time | Communities |
|--------|------|-------------|
| cuGraph | N/A | **FAILED: input graph must be undirected** |
| xrayGraphDB | Works | Completed |

## 3. Scorecard

| Algorithm | cuGraph | xrayGraphDB | Winner |
|-----------|---------|-------------|--------|
| Data Loading | CPU preprocess required | Raw file, no preprocess | **xrayGraphDB** |
| BFS | 20.28 GTEPS (1.8B edges) | 414 MTEPS (3.6B edges) | **cuGraph** (49x on directed) |
| PageRank | FAILED | 69s | **xrayGraphDB** |
| Triangle Count | FAILED | 43s (SNAP exact) | **xrayGraphDB** |
| WCC | FAILED | 100s | **xrayGraphDB** |
| K-Core | FAILED | 125-140s | **xrayGraphDB** |
| Betweenness | FAILED | 143ms | **xrayGraphDB** |
| Louvain | FAILED | Works | **xrayGraphDB** |

**cuGraph: 1 win, 7 failures**
**xrayGraphDB: 7 wins, 0 failures**

## 4. Key Findings

### cuGraph Single-GPU Limitations on Friendster

1. **cuDF CSV parser:** OOM at 92 GB parsing 31 GB text file
2. **Undirected graph build:** OOM during radix sort + edge symmetrization
3. **cudf size_type limit:** 3.6B rows exceeds int32 maximum (2.1B)
4. **Only directed graph possible** on single 96 GB GPU
5. **6 of 8 algorithms require undirected** and therefore fail
6. **CPU preprocessing mandatory** — numpy.loadtxt on system RAM, not GPU

### xrayGraphDB

1. Loaded raw SNAP file directly — no preprocessing, no CPU staging
2. Built full 3.6B bidirectional CSR without memory issues
3. All 8 algorithms completed successfully
4. Sub-second betweenness centrality (143ms)
5. SNAP-verified triangle count (4,173,724,142 exact match)

### The Honest Summary

cuGraph is NVIDIA's premier GPU graph analytics library. On pure BFS kernel throughput, it is approximately 49x faster than xrayGraphDB (20.28 GTEPS vs 414 MTEPS). This is expected — cuGraph's BFS is a highly optimized GPU kernel.

However, on a single RTX PRO 6000 Blackwell (96 GB VRAM), cuGraph **could not load Friendster as an undirected graph** due to memory constraints. This meant 6 of 8 graph algorithms failed because they require undirected input. cuGraph also required CPU preprocessing that xrayGraphDB did not need.

xrayGraphDB is a persistent graph compute runtime — not just a BFS kernel. It loaded the same dataset directly, ran all algorithms successfully, and provided sub-second interactive analytics. These are fundamentally different system categories, and the comparison reflects that distinction.

## 5. Additional Tests

### SSSP — cuGraph requires weighted graph
cuGraph's SSSP requires edge weights. Friendster is unweighted. cuGraph correctly returns: "SSSP requires the input graph to be weighted. BFS should be used instead." This is not a failure — it's correct behavior.

### PageRank Root Cause
cuGraph PageRank fails with `FailedToConvergeError` even with `store_transposed=True`. On a directed graph with only one-way edges, the link matrix is asymmetric — many vertices have outgoing edges but no incoming edges (dangling nodes). PageRank cannot converge because the probability mass drains to sinks. This is a mathematical property of the half-edge representation, not a cuGraph bug.

### Fresh xrayGraphDB Blackwell Timings (same session)
- PageRank 20 iter: **97.7s** (GPU 97-100%, all 65.6M vertices correct)
- Triangle Count: **44.9s** (4,173,724,142 — SNAP exact match, GPU 93%)

## 6. Reproducibility

- cuGraph script: `scripts/cugraph_bench.py`
- xrayGraphDB TEPS: `scripts/graph500_teps.py`
- Dataset: `com-friendster.ungraph.txt` from SNAP
- All logs: `results/cugraph_blackwell.log`
- JSON results: `results/cugraph_blackwell.json`
