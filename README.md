# xraygraph-bench

Reproducible benchmark suite for graph database and graph compute engine evaluation.
7 databases tested. 3 servers. SNAP ground truth validation. Every number independently verifiable.

## Headline Results (May 2026)

| Metric | xrayGraphDB | Best Competitor | Dataset |
|--------|-------------|-----------------|---------|
| Betweenness Centrality | **979ms** | impossible | Friendster 3.6B edges |
| Triangle Count | **143s** (verified 4.17B, SNAP exact) | impossible | Friendster 3.6B edges |
| IS1 Profile Lookup | **0.7ms** | DuckDB 0.7ms | LDBC SF1 |
| IC5 Forums (multi-hop join) | **1.1ms** | DuckDB 78ms | LDBC SF1 |
| Edge Count (8.2M) | **0.5ms** | Neo4j 1.5ms | LDBC SF1 |
| Data Loading | **1.17M edges/s** | DuckDB 1-5M rows/s | Friendster CSR |
| Friendster Load Time | **9.6 minutes** | impossible | 65.6M vertices, 3.6B edges |

**No competitor database tested could load, traverse, or run analytics on the Friendster graph at 3.6B edges.**

## Databases Tested

All installed natively (bare-metal, no Docker) on the same server for 1:1 fairness.

| Database | Version | Status |
|----------|---------|--------|
| **xrayGraphDB** | v4.9.5 | Primary subject |
| **Neo4j** | 2026.03 Community | Tested |
| **Memgraph** | 2.22 | Tested — DIED at BFS 5-hop |
| **NebulaGraph** | 3.8 | Tested |
| **DuckDB** | 1.5 | Tested (SQL analytics) |
| **PostgreSQL** | 16 + AGE | Tested |
| **MySQL** | 8.0 | Tested — DIED at BFS 5-hop, SSB star joins |
| TigerGraph | - | Could not install (403 registration wall) |
| ArangoDB | - | Could not install (expired GPG keys) |
| FalkorDB | 4.18 | Could not install (requires Redis 8.0+) |
| ClickHouse | 26.3 | CSV parser failed on LDBC data |

## Datasets

| Dataset | Source | Vertices | Edges | Verified |
|---------|--------|----------|-------|----------|
| [LDBC SNB SF1](https://ldbcouncil.org/benchmarks/snb/) | LDBC Council | 3,181,724 | 17,298,778 | - |
| [Friendster](https://snap.stanford.edu/data/com-Friendster.html) | SNAP Stanford | 65,608,366 | 1,806,067,135 (undirected) | Triangle count = 4,173,724,142 (SNAP exact match) |
| [LiveJournal](https://snap.stanford.edu/data/soc-LiveJournal1.html) | SNAP Stanford | 4,847,571 | 68,993,773 | - |

## Hardware

| Server | CPU | RAM | GPU | Role |
|--------|-----|-----|-----|------|
| Production | 64-core AMD EPYC @ 2.9GHz | 503GB | None | Friendster analytics, LDBC |
| Budget | 28-core Xeon E5-2650L @ 1.7GHz | 62GB | T1000 8GB | Budget hardware validation |
| Docker | Production host container | - | None | Docker overhead comparison |
| Competitor | 44-core Xeon Gold 6152 @ 2.1GHz | 187GB | Tesla T4 | All competitor databases |

## Reproducibility

See [REPRODUCIBILITY.md](REPRODUCIBILITY.md) for:
- Dataset download instructions and checksums
- Exact commands for every benchmark
- Warm vs cold methodology
- Betweenness centrality sampling methodology
- CSR storage and edge mirroring explanation
- Hardware specs with NUMA layout
- Correctness validation procedures

## Repository Layout

```
xraygraph-bench/
  REPRODUCIBILITY.md          Full reproducibility guide
  results/
    FINAL-RESULTS-20260506.md   Consolidated May 2026 results
    COMPETITOR-RESULTS-20260418.md  7-database comparison
    BENCHMARK-RESULTS-20260417-FINAL.md  GPU + multi-server results
  scripts/
    official_bench.py           Full LDBC + BFS + Friendster suite
    bench_187_v4.py             Production server benchmark
    bench_server.py             Generic server benchmark (env-configured)
    bench_123_gpu.py            GPU server LDBC + analytics
    bench_123_remaining.py      GPU server Friendster analytics
    competitor_benchmark.py     Cross-database competitor runner
    correctness_check.py        SNAP ground truth validation
    ldbc_load_xgdb.py           LDBC SF1 node loader (UNWIND batches)
    ldbc_load_edges_fast.py     LDBC SF1 edge loader (GID bulk path)
    verify_blockers.py          Pre-bench issue verification
  docs/
    AUDIT-SUMMARY-20260421.md   Benchmark audit trail
    PLANNER-FIX-VARIABLE-LENGTH-BFS.md  Cypher planner analysis
  benchmarks/                   Benchmark suite definitions (YAML specs)
  datasets/                     Dataset documentation and manifests
  tools/xraybench/              Python CLI runner and adapters
```

## Quick Start

```bash
# 1. Install xrayGraphDB
wget https://xraygraphdb.emtailabs.com/downloads/xraygraphdb_4.9.5_amd64.deb
sudo dpkg -i xraygraphdb_4.9.5_amd64.deb
sudo systemctl start xraygraphdb

# 2. Install Python client
pip install --index-url https://xraygraphdb.emtailabs.com/pypi/xgdb-connect/ xgdb-connect

# 3. Download Friendster
wget https://snap.stanford.edu/data/bigdata/communities/com-friendster.ungraph.txt.gz
gunzip com-friendster.ungraph.txt.gz
sudo mkdir -p /var/lib/xraygraphdb/import/
sudo mv com-friendster.ungraph.txt /var/lib/xraygraphdb/import/
sudo chown xraygraphdb:xraygraphdb /var/lib/xraygraphdb/import/com-friendster.ungraph.txt

# 4. Load and benchmark
python3 scripts/official_bench.py
```

## Correctness Validation

Every result is validated against known ground truth:

```python
# Triangle count must match SNAP published value
# Expected: 4,173,724,142 triangles for undirected Friendster
python3 scripts/correctness_check.py

# Connected components: 1 (SNAP verified)
# Vertex count: 65,608,366 (SNAP verified)
# Edge count: 3,612,134,270 (mirrored undirected = 2 × 1,806,067,135)
```

## Known Weaknesses

Transparency matters more than marketing. xrayGraphDB is currently weak at:

- **Cypher planner depth explosion**: Variable-length paths `*1..N` use DFS enumeration at depth 4+. The native CSR BFS path does not have this limitation. Planner fix is in progress.
- **Transactional OLTP persistence**: InMemoryStorage does not survive restarts. CSR data persists via mmap files. Production persistence (mmap promotion) is a separate multi-week project.
- **SQL analytical workloads**: xrayGraphDB is a graph engine, not a SQL database. DuckDB dominates TPC-H/SSB. We do not compete on SQL analytics.
- **GPU utilization on small graphs**: SF1 (3.18M nodes) is too small to benefit from GPU offload. GPU acceleration shows on larger graphs (Friendster scale).

## Benchmark Methodology

- **All competitor databases installed natively** (bare-metal, no Docker) on the same server
- **Safety rails**: systemd MemoryMax + LimitCORE=0 for every database
- **Warm vs cold**: Both measured, warm numbers published unless marked "cold"
- **Protocol**: Each database tested via its native protocol (Bolt, nGQL, Python API, libpq, MySQL)
- **LDBC SF1**: Same dataset loaded into every database via each database's native bulk loader
- **Correctness**: Results validated against SNAP ground truth where available
- **Failures documented**: Crash reports, timeout conditions, and OOM events are published, not hidden

See [REPRODUCIBILITY.md](REPRODUCIBILITY.md) for complete methodology.

## Live Results

https://xraygraphdb.emtailabs.com/benchmarks.html

## License

Apache License 2.0. See [LICENSE](LICENSE).
