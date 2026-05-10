# Benchmark Team → DB Team Handoff — 2026-04-20

## What We're Doing

Running a comprehensive competitive benchmark of xrayGraphDB against every major database (Memgraph, Neo4j, NebulaGraph, DuckDB, PostgreSQL, MySQL, ClickHouse, TigerGraph). The goal is to prove xrayGraphDB is the fastest at everything — graph traversal, analytics, GPU acceleration, and data ingestion — and publish the results to https://xraygraphdb.emtailabs.com/benchmarks.html.

## What's Working (Completed Benchmarks)

### Competitor Results (all on Server 2: 216.152.147.171)
All competitors installed natively (no Docker), tested with LDBC SF1 (3.18M nodes, 17.2M edges):

| Database | BFS 5-hop | Status |
|----------|-----------|--------|
| **xrayGraphDB (Cypher)** | **35ms** | Fastest at every hop depth |
| Neo4j 2026.03 | 42ms | Close second |
| NebulaGraph 3.8 | 706ms | |
| PostgreSQL 16 | 648ms | |
| DuckDB 1.5 | 6,544ms | |
| Memgraph 2.22 | **DEAD (454,351ms)** | Dies at 5-hop |
| MySQL 8.0 | **DEAD (timeout)** | Dies at 5-hop |

### xrayGraphDB Results Completed
- **SF1 in-memory**: IS1 0.6ms, BFS 1-10 (2.2ms→35ms plateau), GPU analytics (PageRank 5.8s @ 64% GPU, Triangle 3.2s @ 100% GPU)
- **SF1 mmap**: IS1 0.9ms, BFS 1-10 (2.5ms→273ms plateau)
- **SF10 mmap**: Loaded 30M nodes + 52M edges in 44 min, BFS 4-10 ~3,640ms
- **Friendster**: 65.6M nodes + 1.8B edges loaded via `bulk_import_file()` in 36 min (834K edges/sec), CSR built at `/neo4j/csr_default/`
- **TPC-H SF1**: DuckDB 8-87ms, PostgreSQL 438-3673ms, MySQL 4,463-56,785ms

## What's Blocked — CSR Not Detected by Analytics Procedures

### The Problem
Friendster CSR data exists on disk but `xray.pagerank()` and all other analytics procedures say "empty graph":

```
xgdb_connect.protocol.XrayProtocolError: pagerank: empty graph 
(no data in Cypher store or CSR)
```

### CSR Files Are Valid
```
/neo4j/csr_default/offsets.bin  — 501MB, magic=XRAYCSR1 ✓
/neo4j/csr_default/targets.bin  — 14GB
/neo4j/csr_default/id_map.bin   — 501MB

# Also copied to all possible tenant paths:
/neo4j/csr/
/neo4j/csr_xraygraphdb/
/neo4j/csr_memgraph/
/neo4j/csr_/
```

All files have the correct `XRAYCSR1` magic header (verified via `xxd`).

### What We've Tried
1. CSR at `/neo4j/csr/` — empty graph
2. CSR at `/neo4j/csr_default/` — empty graph  
3. CSR at `/neo4j/csr_xraygraphdb/` — empty graph
4. Restarted xrayGraphDB after each copy — empty graph
5. Three different binary versions deployed — all same error
6. Both `admin:xraygraphdb` and `bench:Bench2026!xray` auth — both connect fine, same error
7. Both `--storage-engine=mmap` and `--storage-engine=default` — same error

### What the Binary Shows
```bash
strings /usr/lib/xraygraphdb/xraygraphdb | grep CSR
# "No CSR loaded for tenant '"
# "/neo4j/csr_"
# "CompactGraph::BuildFromCSR: invalid magic ... (expected XRAYCSR1)"
```

The code looks for `/neo4j/csr_` + tenant_id, and checks for `XRAYCSR1` magic. Both conditions are met, but the auto-detect still fails silently.

### How to Reproduce
```bash
ssh root@216.152.147.171

# Verify CSR files
ls -lh /neo4j/csr_default/
xxd /neo4j/csr_default/offsets.bin | head -1  # Shows XRAYCSR1

# Verify DB is running
ss -tlnp | grep 7689

# Test PageRank
/opt/xraybench-env/bin/python3 -c "
from xgdb_connect.protocol import XrayProtocolClient
c = XrayProtocolClient('127.0.0.1', 7689, auth_token='bench:Bench2026!xray', read_timeout=600)
print(f'Connected: {c.connected}')
cols, rows = c.execute('CALL xray.pagerank(5, 0.85, \"\") YIELD node_id, rank RETURN node_id, rank LIMIT 5')
print(f'Rows: {len(rows)}')
c.close()
"
# Expected: PageRank results on 65.6M vertices
# Actual: XrayProtocolError: empty graph
```

## Server State on 216.152.147.171

### Current xrayGraphDB Config
```
Binary: /usr/lib/xraygraphdb/xraygraphdb (md5: d74c635bc626edc617eea521129718ed)
Service: --storage-engine=mmap --bolt-port=7687 --xray-port=7689
Data dir: /var/lib/xraygraphdb/ (283MB — Cypher store is empty)
CSR dir: /neo4j/csr_default/ (15GB — Friendster 65.6M vertices, 1.8B edges)
Auth: bench:Bench2026!xray (ALL PRIVILEGES)
```

### Other Data on This Server
- **LDBC SF1**: Was in the Cypher store but data dir was wiped during mmap testing. Needs reload.
- **LDBC SF10**: Loaded in separate instance at `/var/lib/xraygraphdb-sf10/` (30M nodes, 52M edges via mmap)
- **Friendster CSR**: Built via `bulk_import_file()` — 65.6M vertices, 1.8B edges, 36 min build time
- **TPC-H/SSB data**: At `/opt/tpch-dbgen/` and `/opt/ssb-dbgen/build/`

### Python Environment
```
/opt/xraybench-env/bin/python3  (Python 3.12)
xgdb_connect 1.2.0 installed (bulk_import_file, bulk_insert_nodes, bulk_insert_edges_gid)
neo4j driver 6.1.0 (for Bolt)
```

### Benchmark Scripts
All at `/opt/xraygraph-bench/scripts/`:
- `competitor_benchmark.py` — LDBC queries + BFS 1-10 via Bolt
- `gpu_bench_port.py` — GPU analytics with nvidia-smi monitoring
- `friendster_bulk_load.py` — Friendster node/edge loader via xrayProtocol
- `friendster_edges_v2.py` — Friendster edge-only loader (GID mapping + bulk edges)
- `xraygraphdb_full_matrix.py` — Full benchmark matrix runner
- `tpch_benchmark.py` — TPC-H SF1 against PostgreSQL/MySQL/DuckDB
- `test_gpu_procs.py` — GPU procedure signature tester

### What the DB Team Needs to Fix

1. **CSR auto-detect**: The analytics procedures don't find the CSR at `/neo4j/csr_default/` despite valid files with correct magic headers. This is the immediate blocker for Friendster GPU benchmarks.

2. **Once CSR works, run these benchmarks**:
```bash
# GPU Analytics on Friendster (1.8B edges)
CALL xray.pagerank(20, 0.85, '') YIELD node_id, rank RETURN node_id, rank ORDER BY rank DESC LIMIT 10
CALL xray.triangle_count('') YIELD triangles, vertices, time_ms RETURN *
CALL xray.community_detection(20, '') YIELD node_id, community_id RETURN community_id, count(*) AS sz ORDER BY sz DESC LIMIT 10
CALL xray.betweenness_centrality('', 50) YIELD node_id, centrality RETURN node_id, centrality ORDER BY centrality DESC LIMIT 10
```

3. **Reload LDBC SF1**: The Cypher store was wiped. Need to reload for the in-memory benchmark comparison. Use:
```bash
cd /root/xraygraphdb-build/tests/xgbench
python3 ldbc_bulk_loader.py \
  --data-dir /opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter \
  --host 127.0.0.1 --port 7689
```

## What's Left After CSR Fix

| Test | Status | Blocked By |
|------|--------|------------|
| Friendster GPU PageRank | BLOCKED | CSR auto-detect |
| Friendster GPU Triangle Count | BLOCKED | CSR auto-detect |
| Friendster GPU Community Detection | BLOCKED | CSR auto-detect |
| Friendster BFS via CSR | BLOCKED | CSR auto-detect |
| SF1 in-memory reload + final numbers | Need reload | Data dir wiped |
| SF10 in-memory | Won't fit in 187GB RAM | Hardware limit |
| Bolt vs xrayProtocol load speed table | Data available | Just needs formatting |
| Final results document + benchmarks.html update | Waiting on above | |

## Contact
All results saved to `/opt/xraybench-results/` on .171. The comprehensive results doc is in the repo at `results/COMPETITOR-RESULTS-20260418.md`. Benchmark scripts are at `/opt/xraygraph-bench/scripts/`.
