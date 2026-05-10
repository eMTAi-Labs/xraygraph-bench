#!/usr/bin/env python3
"""Friendster Phase 3+4 only — fix GID mapping and load remaining edges.
Nodes already loaded (65.6M). Just need correct GID map + edge insertion."""
import sys
import time

sys.path.insert(0, "/root/xraygraphdb-build/tests/xgbench")
from ldbc_bulk_loader import xray_connect, cypher_execute, bulk_insert_edges_gid

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 7709
EDGE_FILE = sys.argv[3] if len(sys.argv) > 3 else "/opt/datasets/friendster/com-friendster.ungraph.txt"
EDGE_BATCH = 50000

print(f"=== Friendster Edge Loader (Phase 3+4) ===", flush=True)
print(f"Target: {HOST}:{PORT}", flush=True)

# Phase 3: Build complete GID mapping by scanning ALL nodes
print(f"\nPhase 3: Building GID mapping for all 65.6M nodes...", flush=True)
start = time.perf_counter()

sock = xray_connect(HOST, PORT)

# Query ALL nodes — no chunking, let the wire protocol handle pagination
query = "MATCH (n:Node) RETURN id(n) AS gid, n.id AS nid"
print(f"  Running: {query}", flush=True)
cols, rows = cypher_execute(sock, query)

gid_map = {}
for row in rows:
    gid_map[row[1]] = row[0]  # nid -> internal gid

map_time = time.perf_counter() - start
print(f"  {len(gid_map):,} GID mappings in {map_time:.1f}s ({len(gid_map)/map_time:,.0f}/s)", flush=True)

if len(gid_map) < 60000000:
    print(f"  WARNING: Only {len(gid_map):,} mappings — expected ~65.6M", flush=True)
    print(f"  Trying alternative query...", flush=True)
    # Try without label filter
    cols2, rows2 = cypher_execute(sock, "MATCH (n) RETURN id(n) AS gid, n.id AS nid")
    for row in rows2:
        if row[1] not in gid_map:
            gid_map[row[1]] = row[0]
    print(f"  After retry: {len(gid_map):,} GID mappings", flush=True)

sock.close()

# Phase 4: Load ALL edges
print(f"\nPhase 4: Loading 1.8B edges via GID fast path...", flush=True)
start = time.perf_counter()

sock = xray_connect(HOST, PORT)
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
                        sock = xray_connect(HOST, PORT)
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
print(f"  Edges skipped (missing GID): {skipped:,}", flush=True)
print(f"  Edge rate: {loaded/edge_time:,.0f}/s", flush=True)
print(f"{'='*60}", flush=True)

sock.close()
