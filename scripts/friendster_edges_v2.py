#!/usr/bin/env python3
"""Friendster edge loader v2 — use Bolt for GID mapping (handles streaming),
xrayProtocol for edge insertion (GID fast path)."""
import sys
import time

sys.path.insert(0, "/root/xraygraphdb-build/tests/xgbench")
from ldbc_bulk_loader import xray_connect, bulk_insert_edges_gid

HOST = "127.0.0.1"
XRAY_PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 7709
BOLT_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 7707
EDGE_FILE = sys.argv[3] if len(sys.argv) > 3 else "/opt/datasets/friendster/com-friendster.ungraph.txt"
EDGE_BATCH = 50000

print(f"=== Friendster Edge Loader v2 ===", flush=True)
print(f"xrayProtocol: {HOST}:{XRAY_PORT} (edges)", flush=True)
print(f"Bolt: {HOST}:{BOLT_PORT} (GID mapping)", flush=True)

# Phase 3: GID mapping via Bolt (handles 65.6M row streaming)
print(f"\nPhase 3: Building GID mapping via Bolt...", flush=True)
start = time.perf_counter()

from neo4j import GraphDatabase
driver = GraphDatabase.driver(f"bolt://{HOST}:{BOLT_PORT}")
gid_map = {}

# Chunk by ID range — each chunk must finish under the 600s server timeout
# Friendster node IDs range from ~100 to ~124,836,180
# 10M IDs per chunk at 33K rows/sec = ~5 min per chunk (under 600s limit)
CHUNK_SIZE = 10000000
max_id = 125000000  # safe upper bound for Friendster IDs

for lo in range(0, max_id, CHUNK_SIZE):
    hi = lo + CHUNK_SIZE
    query = f"MATCH (n:Node) WHERE n.id >= {lo} AND n.id < {hi} RETURN id(n) AS gid, n.id AS nid"
    chunk_start = time.perf_counter()
    try:
        with driver.session() as session:
            result = session.run(query)
            chunk_count = 0
            for record in result:
                gid_map[record["nid"]] = record["gid"]
                chunk_count += 1
        chunk_time = time.perf_counter() - chunk_start
        if chunk_count > 0:
            elapsed = time.perf_counter() - start
            print(f"  {len(gid_map):,} mappings (chunk {lo/1e6:.0f}-{hi/1e6:.0f}M: {chunk_count:,} in {chunk_time:.0f}s, total {elapsed:.0f}s)", flush=True)
    except Exception as e:
        print(f"  Chunk {lo}-{hi} error: {str(e)[:100]}", flush=True)

driver.close()
map_time = time.perf_counter() - start
print(f"  TOTAL: {len(gid_map):,} GID mappings in {map_time:.1f}s ({len(gid_map)/map_time:,.0f}/s)", flush=True)

# Phase 4: Load edges via xrayProtocol GID fast path
print(f"\nPhase 4: Loading 1.8B edges via GID fast path...", flush=True)
start = time.perf_counter()

sock = xray_connect(HOST, XRAY_PORT)
loaded = 0
skipped = 0
src_batch = []
dst_batch = []
total_edges = 1806067135

with open(EDGE_FILE) as f:
    for line in f:
        if line[0] == '#':
            continue
        tab = line.find('\t')
        if tab > 0:
            src = int(line[:tab])
            dst = int(line[tab+1:])
            src_gid = gid_map.get(src)
            dst_gid = gid_map.get(dst)
            if src_gid is not None and dst_gid is not None:
                src_batch.append(src_gid)
                dst_batch.append(dst_gid)
                if len(src_batch) >= EDGE_BATCH:
                    try:
                        bulk_insert_edges_gid(sock, "FRIEND", src_batch, dst_batch)
                        loaded += len(src_batch)
                    except Exception as e:
                        print(f"  Error at {loaded:,}: {e}", flush=True)
                        try:
                            sock.close()
                        except:
                            pass
                        sock = xray_connect(HOST, XRAY_PORT)
                        print(f"  Reconnected", flush=True)
                    src_batch = []
                    dst_batch = []
                    if loaded % 10000000 == 0:
                        elapsed = time.perf_counter() - start
                        rate = loaded / elapsed
                        eta = (total_edges - loaded) / rate / 60 if rate > 0 else 0
                        print(f"  {loaded:,} / {total_edges:,} edges ({rate:,.0f}/s, ETA {eta:.0f}min)", flush=True)
            else:
                skipped += 1

if src_batch:
    try:
        bulk_insert_edges_gid(sock, "FRIEND", src_batch, dst_batch)
        loaded += len(src_batch)
    except:
        pass

edge_time = time.perf_counter() - start

print(f"\n{'='*60}", flush=True)
print(f"  COMPLETE in {(map_time + edge_time)/60:.1f} min", flush=True)
print(f"  GID mappings: {len(gid_map):,}", flush=True)
print(f"  Edges loaded: {loaded:,}", flush=True)
print(f"  Edges skipped: {skipped:,}", flush=True)
print(f"  Edge rate: {loaded/edge_time:,.0f}/s", flush=True)
print(f"{'='*60}", flush=True)

sock.close()
