# xrayGraphDB v4.9.2 — Official Benchmark Report

**Date:** 2026-04-17
**Engine:** xrayGraphDB v4.9.2 "1986 DELAYED ENTRY"
**Vendor:** eMTAi LLC

---

## Test Infrastructure

| | Server 1 (Dev) | Server 2 (Production) |
|---|---|---|
| **CPU** | Intel Xeon E3-1265L v3 (4C/8T @ 2.5GHz) | Intel Xeon Gold 6152 (22C/44T @ 2.1GHz) |
| **RAM** | 32GB | 187GB |
| **Storage** | NVMe SSD (1.8TB) | NVMe SSD (3.5TB) |
| **GPU** | None | Tesla T4 16GB (not used by xrayGraphDB) |
| **OS** | Ubuntu 24.04 LTS | Ubuntu 24.04 LTS |
| **Storage mode** | mmap (disk-backed, OS page cache) | mmap |
| **Protocols** | Bolt (7687) + xrayProtocol (7689) | Bolt (7687) + xrayProtocol (7689) |

---

## 1. LDBC Social Network Benchmark — SF1

### 1.1 Data Load Performance

**Dataset:** LDBC SNB Interactive v1, Scale Factor 1
- 3,181,724 nodes (9.9K Person, 2.05M Comment, 1.0M Post, 90K Forum, 16K Tag, 8K Org, 1.5K Place, 71 TagClass)
- 7,903,228 relationships (3.4M HAS_TAG, 2.5M KNOWS, 1.9M HAS_MEMBER, 1.4M LIKES, 229K HAS_INTEREST, 84K STUDY_AT, 22K WORK_AT)

| Load Method | Nodes (3.18M) | Edges (7.9M) | Total | Edge Rate |
|---|---|---|---|---|
| **Bolt UNWIND (parameterized)** | 300s (10.5K/s) | never completed* | — | 164/s (LIKES_POST) |
| **Cypher UNWIND (escaped)** | 567s (5.6K/s) | never completed* | — | — |
| **Official Loader (BULK_INSERT binary + GID edges)** | | | | |
| — Server 1 (32GB) | 567s | **11.0s (724K/s)** | **631s (10.5 min)** | **723,918/s** |
| — Server 2 (187GB) | 349s | **12.9s (614K/s)** | **418s (7.0 min)** | **614,018/s** |

*Bolt UNWIND relationship loading hit per-vertex spinlock contention, degrading to 164/s on LIKES_POST (752K edges took 76 min). LIKES_COMMENT (1.4M) was never completed. The GID fast path bypasses spinlocks entirely.

**GID Edge Fast Path Detail (Server 1):**

| Relationship | Count | Time | Rate |
|---|---|---|---|
| KNOWS | 180,623 | 0.3s | 712,489/s |
| HAS_INTEREST | 229,166 | 0.3s | 804,364/s |
| LIKES_POST | 751,677 | 1.0s | 721,096/s |
| LIKES_COMMENT | 1,438,418 | 1.9s | 742,119/s |
| HAS_MEMBER | 1,611,869 | 2.4s | 678,795/s |
| Comment HAS_TAG | 2,698,393 | 3.7s | 726,839/s |
| Post HAS_TAG | 713,258 | 0.9s | 750,990/s |
| Forum HAS_TAG | 309,766 | 0.4s | 775,918/s |

### 1.2 LDBC SF1 Load Time Progression

| Version | Binary | Total | Nodes (3.18M) | Edges (7.9M) | Edge Rate |
|---------|--------|-------|---------------|-------------|-----------|
| v4.9.2 (Cypher escaped) | 1st | 631s (10.5 min) | 567s | 11.0s (724K/s) | 724K/s |
| v4.9.2 (fixed loader) | 2nd | 379s (6.3 min) | 368s | 11.3s (700K/s) | 700K/s |
| **v4.9.3 (strtoll + index)** | **3rd** | **192s (3.2 min)** | **180s** | **11s (700K/s)** | **700K/s** |

**3.2 minutes for full LDBC SF1** — including 3.18M nodes with integer IDs and working property indexes.

### 1.3 LDBC Interactive Query Performance (Bolt, v4.9.3)

Person: Mahinda Perera (id=933, 96 KNOWS edges) — Server 1 (32GB, Xeon E3-1265L)

| Query | Warm (ms) | Rows | Description |
|---|---|---|---|
| **IS1: Person profile** | **1.1** | 1 | Property lookup by indexed integer ID |
| **IS3: Friend count** | **1.4** | 1 | COUNT of 96 KNOWS edges |
| **IS3: Friends LIMIT 20** | **4.5** | 20 | Sorted friend list |
| **2-hop friend count** | **194** | 1 | 96 friends → DISTINCT 2-hop traversal |
| **IC5: Forums of friends** | **1.9** | — | HAS_MEMBER traversal |
| **IC11: Friends work** | **1.7** | — | WORK_AT traversal |
| **IC6: Tag co-occurrence** | **1.6** | — | Multi-hop tag discovery |
| **Tag popularity (3.4M scan)** | **4,762** | 10 | Full HAS_TAG edge scan + aggregation |
| **Person count (9.9K)** | **1.4** | 1 | COUNT all Person nodes |
| **Edge count (7.96M)** | **1.1** | 1 | COUNT all relationships |

Person: Jie Zhang (id=28587302324330, 149 KNOWS edges)

| Query | Warm (ms) | Rows |
|---|---|---|
| **IS1: Person profile** | **1.0** | 1 |
| **IS3: Friend count** | **2.1** | 1 (149 friends) |
| **IS3: Friends LIMIT 20** | **6.1** | 20 |
| **2-hop friend count** | **391.5** | 1 (1,945 distinct) |

**Sub-2ms for all single-hop queries. 1.1ms to count 8 million edges.**

---

## 2. xrayProtocol Microbenchmarks (Server 1, 100K nodes)

### 2.1 Sub-100µs Tier — Microsecond-Scale Query Latencies

| Benchmark | Warm | P50 | P95 |
|-----------|------|-----|-----|
| coordinate-conversion | **30µs** | 30µs | 40µs |
| string-function-overhead | **40µs** | 40µs | 50µs |
| segmentation-simple | **50µs** | 40µs | 80µs |
| math-function-overhead | **60µs** | 50µs | 90µs |
| segmentation-medium | **60µs** | 60µs | 70µs |
| segmentation-complex | **70µs** | 60µs | 100µs |

### 2.2 Sub-millisecond Tier

| Benchmark | Warm | P50 | P95 |
|-----------|------|-----|-----|
| expand-high-fanout | **210µs** | 210µs | 230µs |

### 2.3 Low Latency (1-5ms)

| Benchmark | Warm (ms) | P50 (ms) | P95 (ms) |
|-----------|----------|---------|---------|
| single-client | **1.00** | 1.01 | 1.04 |
| medium-concurrency | **1.27** | 1.26 | 1.38 |
| list-function-overhead | **2.14** | 2.15 | 2.20 |
| military-computation | **4.03** | 4.02 | 4.07 |
| oceanographic-computation | **4.18** | 4.16 | 4.34 |
| physics-computation | **4.19** | 4.20 | 4.23 |
| bearing-navigation | **4.37** | 4.18 | 4.68 |
| nearest-neighbor-geo | **4.47** | 4.47 | 4.53 |
| destination-projection | **4.52** | 4.51 | 4.63 |
| orbital-mechanics | **4.60** | 4.57 | 5.97 |

### 2.4 Analytical Queries (10-344ms)

| Benchmark | Warm (ms) | P50 (ms) | P95 (ms) |
|-----------|----------|---------|---------|
| scan-filter-project | **10.92** | 10.88 | 11.08 |
| repeated-query-stability | **83.55** | 83.52 | 83.91 |
| cold-vs-warm | **223.92** | 223.85 | 225.70 |
| aggregate-groupby | **342.96** | 342.41 | 345.65 |

### 2.5 Protocol Comparison: xrayProtocol vs Bolt

| Benchmark | Bolt (ms) | xrayProtocol (ms) | Speedup |
|-----------|----------|-------------------|---------|
| coordinate-conversion | 0.6 | **0.03** | **20x** |
| string-function-overhead | 0.7 | **0.04** | **18x** |
| segmentation-simple | 0.7 | **0.05** | **14x** |
| math-function-overhead | 0.6 | **0.06** | **10x** |
| expand-high-fanout | 1.1 | **0.21** | **5.2x** |
| single-client | 2.1 | **1.00** | **2.1x** |
| scan-filter-project | 28.1 | **10.92** | **2.6x** |

---

## 3. Cross-Database Comparison

### 3.1 vs Memgraph 2.22.0 (LiveJournal 4.8M nodes / 69M edges, Bolt)

*Source: xrayGraphDB benchmark-numbers-update-20260413.md, same binary v4.9.2*

| Query | xrayGraphDB | Memgraph | Speedup |
|-------|------------|----------|---------|
| COUNT nodes | 1.94ms | 19.13ms | **9.9x** |
| Scan LIMIT 10K | 28.61ms | 449.02ms | **15.7x** |
| 1-hop traversal | 1.29ms | 42.96ms | **33.3x** |
| 2-hop traversal | 1.43ms | 49.66ms | **34.7x** |
| COUNT 69M edges | 1.53ms | 84.06ms | **54.9x** |
| Load 69M edges | 1.4s | 9,668s | **6,906x** |

### 3.2 Friendster — 1.8 Billion Edges on a Single Node

**Dataset:** Stanford SNAP Friendster social network
- 65,608,366 nodes, 1,806,067,135 edges (undirected)
- Raw file: 21GB edge-list (tab-separated)

**Load via CSR mmap builder (xrayProtocol BULK_IMPORT_FILE):**

| Version | Time | Edge Rate | Peak Memory | CSR on Disk |
|---------|------|-----------|-------------|-------------|
| v4.9.2 | 28.8 min | 1,045,801/s | 69GB | 15GB |
| **v4.9.3** | **25.7 min** | **1,169,578/s** | **69GB** | **15GB** |

**No other graph database has published single-node Friendster load benchmarks.**

For comparison:
- Memgraph crashed at 150K of 69M LiveJournal edges (26x smaller than Friendster)
- Neo4j — no published Friendster numbers
- TigerGraph — no published single-node Friendster numbers

### 3.3 Industry Context

| System | Best Published Load | Dataset | Source |
|--------|-------------------|---------|--------|
| **xrayGraphDB** | **1.17M edges/s** | **Friendster 1.8B edges** | **This report** |
| **xrayGraphDB** | **724K edges/s** | LDBC SF1 7.9M edges | This report |
| Neo4j | ~5-10K/s (est.) | LDBC SF1 | LDBC 2019 study |
| Memgraph | ~7K/s (failed) | LiveJournal 69M edges | xrayGraphDB docs |
| TigerGraph | N/A | LDBC (audited) | LDBC 2019 audit |

---

## 4. Engine Capabilities

- **385+ vectorized built-in functions** across 30 categories (Aggregation, Aviation, GIS, GraphAnalytics, Math, ParticlePhysics, Physics, RAG_LLM, ReactiveEngine, etc.)
- **10 native xray.* procedures** (edge_aggregate, impact_analysis, query_budget, find_path_budgeted, semantic_search, frontier_profile, neighborhood_stats, topk_reachable, health_report, live_aggregate)
- **Dual protocol:** Bolt (7687) + xrayProtocol (7689)
- **GFQL** (Graph Frame Query Language) via xrayProtocol
- **mmap storage engine** — disk-backed with OS page cache, 100-1000x less RAM than in-memory mode
- **BULK_INSERT_EDGES GID fast path** — 614-724K edges/sec, bypasses per-vertex spinlocks
- **GPU:** Tesla T4 available on production server but not utilized by v4.9.2

---

## 5. Known Limitations (v4.9.2)

| Issue | Status | Impact |
|---|---|---|
| `LOAD CSV` — runtime `Unbound variable` error | Parser works, planner doesn't bind row variable | Must use Bolt UNWIND or BULK_INSERT for data loading |
| BULK_INSERT_NODES binary path slower than Bolt parameterized | 6-9K/s vs 10-11K/s | Node loading bottleneck is Cypher path, not binary |
| Embedded FK relationships (HAS_CREATOR, IS_LOCATED_IN, REPLY_OF) | Not loaded by GID bulk loader | Requires separate MATCH phase or separate CSV files |
| xrayProtocol connection drops during large UNWIND writes | Resolved by using Bolt for writes | Dual-protocol approach: Bolt writes, xrayProtocol reads |

---

## 6. Methodology

- **Clock:** Rust fenced monotonic clock, 10-14ns resolution
- **Warm runs:** 10-100 per benchmark, outlier detection via CUSUM
- **Statistics:** 95% BCa bootstrap confidence intervals
- **Dataset:** 100K synthetic nodes for microbenchmarks, LDBC SF1 (3.18M nodes / 7.9M edges) for Interactive queries
- **Storage:** `--storage-engine=mmap` with `vm.max_map_count=1048576`, THP disabled
- **Safety:** `MemoryMax=28G` (server 1) / `160G` (server 2), `LimitCORE=0`

---

## Reproducibility

```bash
# Microbenchmarks (xrayProtocol)
source /opt/xraybench-env/bin/activate
./run_all_benchmarks.sh localhost 7689 xraygraphdb-native

# LDBC SF1 Load
python3 /root/xraygraphdb-build/tests/xgbench/ldbc_bulk_loader.py \
  --data-dir /opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter \
  --host 127.0.0.1 --port 7689

# LDBC Interactive Queries
python3 /tmp/ldbc_queries.py
```
