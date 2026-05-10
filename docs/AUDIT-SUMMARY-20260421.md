# Benchmark Audit Summary — 2026-04-18 to 2026-04-21

## What Was Accomplished

### Competitor Benchmarks (Complete)
Tested 7 databases natively on Server 2 (187GB RAM, Tesla T4 GPU):
- **Memgraph 2.22** — DIED at BFS 5-hop (454 seconds)
- **Neo4j 2026.03** — BFS plateaus at 42ms
- **NebulaGraph 3.8** — BFS degrades linearly to 1,240ms at 10-hop
- **DuckDB 1.5** — Fastest analytics (TPC-H Q1: 23ms), BFS via CTE: 21.7s at 10-hop
- **PostgreSQL 16 + AGE** — BFS via CTE: 1,551ms at 10-hop
- **MySQL 8.0** — DIED at BFS 5-hop, TPC-H Q5: 57 seconds
- **TPC-H SF1** results for DuckDB, PostgreSQL, MySQL

### Databases That Could Not Be Installed
- **TigerGraph** — Registration wall (403 Forbidden)
- **ArangoDB** — Expired GPG keys
- **FalkorDB** — Requires Redis 8.0+ (Ubuntu has 7.0)

### xrayGraphDB Achievements
- **BFS planner fix**: 390,000ms → 35ms (11,000x improvement)
- **VectorizedBFS routing**: 51ms → 35ms → 33ms
- **Hop threshold**: BFS 2-hop back to 1.2ms
- **Friendster loaded**: 65.6M vertices, 1.8B edges via bulk_import_file (834K edges/sec)
- **13 analytics procedures tested on Friendster** (largest public social graph)

### Friendster Results (What Worked)

| Procedure | Time | Status |
|-----------|------|--------|
| Health Report | 113ms | PASS |
| Graph Stats | 54.7s | PASS |
| Degree Distribution | 1.6s | PASS |
| Frontier Profile 5-hop | 21s | PASS |
| Frontier Profile 10-hop | 21s | PASS |
| Shortest Path | 273ms | PASS |
| Find Path Budgeted | 25ms | PASS |
| TopK Reachable | 2.8s | PASS |
| Common Neighbors | 2ms | PASS |
| Jaccard Similarity | 2ms | PASS |
| Link Prediction | 23.7s | PASS (was crashing, fixed) |
| Connected Components | 19.6s | PASS |
| PageRank (5 iter) | 173s | PASS |
| Triangle Count | 30.9s | PASS (483M triangles!) |
| Betweenness (100) | 174s | PASS |
| K-Core | 107s | PASS |
| Community Detection (3 iter) | 4ms | PASS (was crashing, fixed) |
| HITS (3 iter) | 133s | PASS (was crashing, fixed) |
| PersonalizedPR (20 iter) | 1,100s | PASS (was crashing, fixed) |
| Similarity | RUNNING | Was taking >10 min, not completed |
| Clustering Coefficient | NOT TESTED | Blocked by Similarity |

## What the DB Team Needs to Fix

### Critical (Blocks Publishing)

1. **Procedure stability at scale**: Multiple procedures crashed repeatedly on Friendster before being fixed. Each fix introduced new regressions (Link Prediction crash from Community fix, schema-mismatch crashes). The fix-deploy-test cycle went through 8+ binary deployments in one session. **Need: comprehensive test suite that runs ALL procedures on Friendster before any deployment.**

2. **Similarity procedure timeout**: `xray.similarity()` on vertex 13594 (137K neighbors) takes >10 minutes on Friendster. Needs optimization or a progress limit.

3. **SF1 in-memory re-benchmark**: The Cypher store was wiped during mmap testing. Need to reload SF1 and re-run BFS 1-10 + LDBC queries with the final binary to get clean numbers. The IS3/IC2 regression was fixed (194ms → 0.1ms) but needs verification with data loaded.

4. **GPU not verified on latest binary**: The .preinit_array CUDA fix was removed due to linker corruption. GPU acceleration status is unknown on the current binary.

5. **mmap mode not fully tested**: mmap SIGSEGV was fixed for loading (65.6M nodes loaded successfully), but mmap BFS benchmarks were only done with one binary version. Need re-run with final binary.

### Important (For Credibility)

6. **Edge count in Cypher store**: `MATCH (n) RETURN count(n)` returns 0 when only CSR data is loaded. The CSR fallthrough for count queries may not be in the latest binary.

7. **Transaction timeout**: Must run with `--query-execution-timeout-sec=0` for Friendster analytics. Default timeout kills PageRank 20-iter and Betweenness.

8. **Competitor re-run**: Memgraph only had 4M of 13M edges. Should reload with full edge set for fair comparison. Also need to note which protocol each benchmark used.

## Files and Locations

### On Server 216.152.147.171
```
/opt/xraybench-results/                          — All benchmark result JSONs
/opt/xraybench-results/FULL_REGRESSION.log        — Latest regression test log
/opt/xraybench-results/FULL_MATRIX_FINAL.json     — Matrix benchmark results
/opt/xraybench-results/friendster_full_results.json — Friendster analytics
/opt/xraybench-results/competitors/               — All competitor results
/opt/xraybench-results/tpch/                      — TPC-H results
/neo4j/csr_default/                               — Friendster CSR (15GB)
/var/lib/xraygraphdb-sf10/                        — LDBC SF10 mmap data
/opt/xraygraph-bench/scripts/                     — All benchmark scripts
```

### In Repo
```
results/COMPETITOR-RESULTS-20260418.md             — Comprehensive competitor comparison
docs/PLANNER-FIX-VARIABLE-LENGTH-BFS.md           — BFS planner optimization details
docs/MMAP-CRASH-REPORT-V2-20260419.md             — mmap crash investigation
docs/FRIENDSTER-EDGE-DEGRADATION-20260420.md       — Edge insertion degradation analysis
docs/HANDOFF-TO-DB-TEAM-20260420.md                — Full handoff document
docs/AUDIT-SUMMARY-20260421.md                     — This document
```

## Timeline of Fixes During Session

| Fix | Before | After | Binary Deploys |
|-----|--------|-------|----------------|
| BFS DFS→BFS planner | 390,000ms | 51ms | 1 |
| VectorizedBFS routing | 51ms | 36ms | 1 |
| GID fix in VectorizedBFS | 2 vertices | 9,162 vertices | 1 |
| Upper bound extraction | bounds=[1,100] | bounds=[1,5] | 1 |
| Hop threshold (upper≤2) | 36ms for 2-hop | 1.2ms for 2-hop | 1 |
| IS3/IC2 regression | 194ms | 0.1ms | 1 |
| Edge count O(1) | 7s | <1ms | 1 |
| mmap SIGSEGV | crash at 2.6M | 65.6M loaded | 3 |
| Community Detection crash | SIGSEGV | 4ms | 4 |
| HITS crash | SIGSEGV | 133s | 3 |
| PersonalizedPR crash | SIGSEGV | 1,100s | 2 |
| Link Prediction crash | SIGSEGV | 23.7s | 2 |
| CSR auto-detect | "empty graph" | 65.6M vertices found | 3 |
| Betweenness optimization | timeout (>10min) | 174s | 1 |
| **Total binary deploys** | | | **~20+** |

The frequency of crash-fix-crash-fix cycles is the core issue. Each procedure works in isolation but the combination at Friendster scale (65.6M vertices, 1.8B edges) exposes edge cases that unit tests don't cover. The DB team needs integration tests at scale before deploying.
