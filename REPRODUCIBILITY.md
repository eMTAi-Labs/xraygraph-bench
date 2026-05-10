# Benchmark Reproducibility Guide

## Datasets

All datasets are publicly available:

| Dataset | Source | Vertices | Edges | Checksum |
|---------|--------|----------|-------|----------|
| LDBC SNB SF1 | [ldbcouncil.org](https://ldbcouncil.org/benchmarks/snb/) | 3,181,724 | 17,298,778 | CsvCompositeMergeForeign format |
| Friendster | [SNAP Stanford](https://snap.stanford.edu/data/com-Friendster.html) | 65,608,366 | 1,806,067,135 (undirected) | `com-friendster.ungraph.txt.gz` |
| LiveJournal | [SNAP Stanford](https://snap.stanford.edu/data/soc-LiveJournal1.html) | 4,847,571 | 68,993,773 | `soc-LiveJournal1.txt.gz` |

### Friendster Edge Count Clarification

The SNAP Friendster file contains 1,806,067,135 undirected edges. xrayGraphDB's CSR builder mirrors each edge for bidirectional traversal, producing 3,612,134,270 directed entries in the CSR. Health reports show the mirrored count (3.6B). All analytics operate on the full mirrored CSR.

## Hardware

### Server A — Production (.187)
- CPU: AMD EPYC 7542 (64 cores / 128 threads @ 2.9GHz)
- RAM: 503GB DDR4
- Storage: NVMe SSD (3.4TB RAID)
- GPU: None
- OS: Ubuntu 24.04 LTS
- xrayGraphDB: v4.9.5 (custom binary with CSR build optimizations)

### Server B — Docker (.29)
- Host: Production application server
- Container: `xraygraphdb.emtailabs.com/xraygraphdb:v4.9.5`
- GPU: None
- xrayProtocol port: 17689

### Server C — Budget GPU (.68)
- CPU: Intel Xeon E5-2650L v4 (28 cores / 56 threads @ 1.70GHz)
- RAM: 62GB DDR4
- Storage: 2x 960GB Samsung SM863
- GPU: NVIDIA T1000 8GB (compute capability 7.5, Turing)
- NVIDIA Driver: 470.256.02, CUDA 11.4
- OS: Ubuntu 24.04 LTS

### Competitor Server (Apr 2026)
- CPU: Intel Xeon Gold 6152 (44 cores / 88 threads @ 2.1GHz)
- RAM: 187GB DDR4
- Storage: NVMe SSD (3.5TB)
- GPU: Tesla T4 16GB
- OS: Ubuntu 24.04 LTS
- All databases installed natively (bare-metal, no Docker) for 1:1 fairness

## Methodology

### Warm vs Cold
- **Cold**: First execution after server restart or first use of a procedure
- **Warm**: Average of N subsequent executions (typically 5-10 runs)
- Published numbers are warm unless marked "cold"
- BC pair-sampled "COLD" includes one-time scratch pool allocation (~16s); subsequent calls reuse the pool

### Protocol
- All xrayGraphDB measurements via xrayProtocol (port 7689) unless noted as Bolt
- Competitor measurements via each database's native protocol (Bolt for Neo4j/Memgraph, nGQL for NebulaGraph, Python API for DuckDB, libpq for PostgreSQL, MySQL protocol for MySQL)
- LDBC queries use Cypher on graph databases, SQL equivalents on relational databases

### Betweenness Centrality Sampling
- Procedure: `xray.betweenness_pair_sampled(epsilon, delta, label, target_buckets, max_k_multiplier)`
- Method: Uniform-random pair sampling with deterministic seed
- Statistical guarantee: (epsilon, delta) bound — at epsilon=0.05, delta=0.05, confidence >= 95%
- k_pairs computed as: ceil(log(N) / (2 * epsilon^2))
- Friendster (N=65.6M): k_pairs ~1,540 at epsilon=0.05
- `target_buckets=1` for headline sub-second numbers (reduces output resolution, not accuracy)
- Reproducible: same graph + same epsilon + same delta = same scores (deterministic seed)

### CSR Storage
- Compressed Sparse Row format with mmap backing
- Files: offsets.bin, targets.bin, id_map.bin, manifest.bin (XRAYCSR3 format)
- Undirected graphs: each edge stored in both directions (A->B and B->A)
- Build via: `bulk_import_file('/path/to/edge-list.txt')` — auto-detects `.ungraph.` filename for undirected mirroring
- Build time: 9.6 min on 64-core EPYC, ~3 hours on 28-core 1.7GHz Xeon

### Triangle Count
- Algorithm: Degree-ordered merge intersection over sorted adjacency lists
- Undirected CSR: each triangle counted 3x (once per directed edge in mirrored CSR)
- YIELD column order issue (as of May 6): `triangles` field returns edge count, `time_ms` field returns actual triangle count. Being fixed.
- SNAP ground truth for Friendster: 4,173,724,142 triangles (verified exact match)

### Competitor Testing
- Each database installed natively on the same server (no Docker)
- systemd MemoryMax + LimitCORE=0 safety rails for each database
- LDBC SF1 loaded via each database's native bulk loader
- Same dataset, same queries (Cypher or SQL equivalent), same hardware
- Databases tested: Neo4j 2026.03, Memgraph 2.22, NebulaGraph 3.8, DuckDB 1.5, PostgreSQL 16 + AGE, MySQL 8.0
- Databases that could not be installed: TigerGraph (403 registration wall), ArangoDB (expired GPG keys), FalkorDB (requires Redis 8.0+), ClickHouse (CSV parser failure)

## Reproducing the Benchmarks

### 1. Install xrayGraphDB
```bash
# Native install
wget https://xraygraphdb.emtailabs.com/downloads/xraygraphdb_4.9.5_amd64.deb
sudo dpkg -i xraygraphdb_4.9.5_amd64.deb
sudo systemctl start xraygraphdb

# Or Docker
docker run -d -p 7687:7687 -p 7689:7689 xraygraphdb.emtailabs.com/xraygraphdb:v4.9.5
```

### 2. Install Python client
```bash
pip install --index-url https://xraygraphdb.emtailabs.com/pypi/xgdb-connect/ xgdb-connect
```

### 3. Download datasets
```bash
# Friendster
wget https://snap.stanford.edu/data/bigdata/communities/com-friendster.ungraph.txt.gz
gunzip com-friendster.ungraph.txt.gz
mkdir -p /var/lib/xraygraphdb/import/
mv com-friendster.ungraph.txt /var/lib/xraygraphdb/import/

# LDBC SF1
# Download from https://ldbcouncil.org/benchmarks/snb/
```

### 4. Load Friendster CSR
```python
from xgdb_connect.protocol import XrayProtocolClient
c = XrayProtocolClient(host="127.0.0.1", port=7689,
    auth_token="admin:<password>", database="<db>", read_timeout=7200)
c.bulk_import_file("/var/lib/xraygraphdb/import/com-friendster.ungraph.txt")
```

### 5. Run benchmarks
```bash
python3 scripts/official_bench.py
python3 scripts/bench_187_v4.py
python3 scripts/correctness_check.py
```

## Verification

### Triangle Count (SNAP ground truth)
```cypher
CALL xray.triangle_count("") YIELD triangles, edges_checked, time_ms, vertices RETURN *
-- Expected: time_ms field = 4,173,724,142 (SNAP verified)
```

### Connected Components
```cypher
CALL xray.connected_components("") YIELD component_size, num_components, time_ms
RETURN component_size, num_components ORDER BY component_size DESC LIMIT 1
-- Expected: num_components=1, component_size=65,608,366
```

### Betweenness Reproducibility
```cypher
-- Run twice, same connection — scores must be identical (deterministic seed)
CALL xray.betweenness_pair_sampled(0.05, 0.05, "", 1, 1)
YIELD node_id, centrality RETURN node_id, centrality ORDER BY centrality DESC LIMIT 5
```
