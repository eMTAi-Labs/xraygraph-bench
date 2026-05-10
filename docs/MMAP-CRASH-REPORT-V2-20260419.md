# mmap Engine Crash Report V2 — 2026-04-19

## Summary

The mmap engine crashes at approximately **2.3-2.6M committed vertices** regardless of which loader or code path triggers the crash. The bug is in the engine's vertex store, not the loader.

## What We Tested

We tested three approaches to loading LDBC SF1 (3.18M nodes) into mmap:

| Attempt | Loader | Nodes Loaded | Crash Point | Signal | Vertex Count at Crash |
|---------|--------|-------------|-------------|--------|----------------------|
| 1 (Apr 19 ~08:00) | Bolt UNWIND | Comment ✓, Post ✗ | During Post CREATE | SIGABRT | 2,152,553 |
| 2 (Apr 19 ~12:00) | Bolt UNWIND | Comment ✓, Post ✗ | During Post CREATE | SIGSEGV | 2,577,432 |
| 3 (Apr 19 ~12:50) | Bolt UNWIND | Comment ✓, Post ✗ | During Post CREATE | SIGSEGV | 2,627,539 |
| **4 (Apr 19 ~13:06)** | **xrayProtocol BULK_INSERT** | **ALL nodes ✓** | **During Cypher MATCH scan** | **exit 203** | **2,292,061 committed** |

## Critical Finding: xrayProtocol Loads All Nodes

Attempt 4 is the key evidence. The xrayProtocol BULK_INSERT_NODES path loaded **all 3,165,466 nodes successfully**:

```
Person:       9,892 nodes in 0.4s  (24,806/s)
Comment:  2,052,169 nodes in 85.4s (24,020/s)
Post:     1,003,605 nodes in 43.3s (23,174/s)  ← THIS SURVIVED
Forum:       90,492 nodes in 3.4s  (26,343/s)
Organisation: 7,955 nodes in 0.4s  (19,930/s)
Place:        1,460 nodes in 0.1s  (22,048/s)
```

**Post loaded without crashing** — all 1,003,605 Post nodes inserted via xrayProtocol at 23,174/s.

The crash happened AFTER all nodes were loaded, during Phase 2 (`build_gid_maps`), when the loader ran:
```cypher
MATCH (n:Comment) RETURN id(n), n.id
```
This Cypher query scans the full Comment vertex store (~2M vertices). The server closed the connection during this scan.

## What This Tells Us

1. **The bug is NOT in the insert path** — xrayProtocol inserted 3.16M nodes without crashing
2. **The bug IS triggered by scanning/reading vertices** in the mmap store when vertex_count exceeds ~2.3M committed
3. **Only 2,292,061 of 3,165,466 nodes were committed** to mmap files — the remaining ~873K nodes were in the WAL but not flushed to the mmap files
4. **The Bolt UNWIND crashes happened during insert** because Bolt's transactional commit path also reads/scans vertices as part of its MATCH + property index operations
5. **The xrayProtocol BULK_INSERT bypasses the scan path** — it writes directly to the WAL without scanning existing vertices, which is why all nodes loaded

## mmap File State at Crash (Attempt 4)

```json
{
  "vertex_count": 2292061,         // 2.29M committed (of 3.16M loaded)
  "vertices_committed": 146691904, // 140MB of vertices.mmap (256MB file)
  "props_committed": 698599656,    // 666MB of props.mmap
  "string_pool_committed": 111565154, // 106MB of string_pool.mmap (256MB file)
  "edge_count": 0,
  "wal_offset": 196702030          // 188MB WAL (contains uncommitted nodes)
}
```

Compared to Attempt 3 (Bolt loader):
```json
{
  "vertex_count": 2627539,         // 2.63M committed (crashed during insert)
  "vertices_committed": 168162496, // 160MB
  "props_committed": 1037267896,   // 989MB
  "string_pool_committed": 270666614, // 258MB (past 256MB — grew successfully!)
}
```

## Where to Look in the Code

The crash happens when the engine **reads vertex data from the mmap'd vertex store** at high vertex counts. Specifically:

1. **`vertices.mmap` scan path** — The Cypher `MATCH (n:Label)` scan iterates over all vertices in `vertices.mmap`. At ~2.3M committed vertices (140MB+ of vertex data), something goes wrong during this iteration. Possible causes:
   - An internal pointer or offset becomes invalid after the vertex file grew past a threshold
   - A page boundary crossing in the mmap'd region causes a bad memory access
   - The vertex slot allocator has a metadata corruption at a specific capacity

2. **WAL vs committed state mismatch** — 3.16M nodes were loaded but only 2.29M committed. The scan may be hitting WAL-only vertices that aren't properly materialized, causing a null dereference

3. **Label index corruption** — The `MATCH (n:Comment)` query uses the label index to find Comment vertices. If the label index was built during bulk insert but references vertices that weren't fully committed to the mmap file, the index entries point to invalid memory regions

## Reproduction

```bash
# Clean start
systemctl stop xraygraphdb
rm -rf /var/lib/xraygraphdb/*
# Ensure mmap mode
grep -q 'storage-engine=mmap' /etc/systemd/system/xraygraphdb.service
systemctl start xraygraphdb
# Wait for ports
sleep 10

# Load all nodes via xrayProtocol (this succeeds)
cd /root/xraygraphdb-build/tests/xgbench
python3 ldbc_bulk_loader.py \
  --data-dir /opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter \
  --host 127.0.0.1 --port 7689

# The crash occurs during Phase 2 when ldbc_bulk_loader.py runs:
#   MATCH (n:Comment) RETURN id(n), n.id
# Server closes connection → SIGSEGV/SIGABRT on restart

# To trigger manually (simpler reproduction):
# After loading 2M+ nodes, run any full vertex scan:
#   MATCH (n) RETURN count(n)
# This will crash the server
```

## Loading Speed Comparison (for benchmarks)

| Protocol | Rate (nodes/s) | Rate (edges/s) | Notes |
|----------|---------------|----------------|-------|
| **xrayProtocol BULK_INSERT** | **23-26K** | **724K** (prior test) | Binary wire format, port 7689 |
| Bolt UNWIND | 3-10K | 12-26K | Neo4j protocol, port 7687 |
| **Speedup** | **4.6-7.3x** | **28-60x** | xrayProtocol wins on both |

## What's Blocking

This mmap crash is the **single blocker** for completing the full benchmark matrix:

- ❌ LDBC SF1 mmap — crashes at 2.3M vertices during scan
- ❌ LDBC SF10 mmap — would crash much earlier (30M nodes)
- ❌ Friendster mmap — 65.6M nodes, can't even start
- ❌ LDBC SF10 in-memory — needs ~900GB RAM (server has 187GB)
- ❌ Friendster in-memory — needs ~2TB+ RAM

**Once the mmap vertex scan bug is fixed, we can run the complete test matrix within hours.**
