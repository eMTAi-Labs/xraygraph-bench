#!/usr/bin/env python3
"""Load Friendster via xrayProtocol BULK_INSERT_NODES + BULK_INSERT_EDGES.
Uses the same wire format as ldbc_bulk_loader.py."""
import socket
import struct
import time
import sys
import os

# Import the proven wire format code from ldbc_bulk_loader
sys.path.insert(0, "/root/xraygraphdb-build/tests/xgbench")
from ldbc_bulk_loader import (
    xray_connect, recv_frame, bulk_insert_edges_gid,
    _bulk_insert_node_batch, cypher_execute
)

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 7709
EDGE_FILE = sys.argv[3] if len(sys.argv) > 3 else "/opt/datasets/friendster/com-friendster.ungraph.txt"
NODE_BATCH = 5000
EDGE_BATCH = 50000

print(f"=== Friendster Bulk Loader ===")
print(f"Target: {HOST}:{PORT}")
print(f"File: {EDGE_FILE}")

sock = xray_connect(HOST, PORT)
print(f"Connected")

# Phase 1: Scan edge file, collect unique node IDs
print(f"\nPhase 1: Scanning edge file...")
start = time.perf_counter()
node_ids = set()
edge_count = 0
with open(EDGE_FILE) as f:
    for line in f:
        if line[0] == '#':
            continue
        tab = line.find('\t')
        if tab > 0:
            node_ids.add(int(line[:tab]))
            node_ids.add(int(line[tab+1:]))
            edge_count += 1
            if edge_count % 100000000 == 0:
                print(f"  {edge_count/1e6:.0f}M edges scanned, {len(node_ids)/1e6:.1f}M unique nodes...")
scan_time = time.perf_counter() - start
print(f"  {len(node_ids):,} unique nodes, {edge_count:,} edges in {scan_time:.1f}s")

# Phase 2: Create nodes via BULK_INSERT_NODES
print(f"\nPhase 2: Creating {len(node_ids):,} nodes via BULK_INSERT_NODES...")
start = time.perf_counter()
node_list = sorted(node_ids)
created = 0
prop_names = ["id"]

for i in range(0, len(node_list), NODE_BATCH):
    batch_ids = node_list[i:i+NODE_BATCH]
    # Format: each row is [str(id)] — the bulk loader expects list of lists
    batch_rows = [[str(nid)] for nid in batch_ids]
    try:
        n = _bulk_insert_node_batch(sock, "Node", prop_names, batch_rows)
        created += n
        if created % 1000000 == 0:
            elapsed = time.perf_counter() - start
            rate = created / elapsed
            eta = (len(node_list) - created) / rate if rate > 0 else 0
            print(f"  {created:,} / {len(node_list):,} nodes ({rate:,.0f}/s, ETA {eta/60:.0f}min)")
    except Exception as e:
        print(f"  Error at {created}: {e}")
        # Reconnect
        try:
            sock.close()
        except:
            pass
        sock = xray_connect(HOST, PORT)
        print(f"  Reconnected")

node_time = time.perf_counter() - start
print(f"  {created:,} nodes in {node_time:.1f}s ({created/node_time:,.0f}/s)")

# Phase 3: Build GID mapping via Cypher query
print(f"\nPhase 3: Building GID mapping ({created:,} nodes)...")
start = time.perf_counter()
sock2 = xray_connect(HOST, PORT)

gid_map = {}
# Query in chunks by ID range to avoid 65M row result set
chunk_size = 5000000
for chunk_start in range(0, len(node_list), chunk_size):
    chunk_end_idx = min(chunk_start + chunk_size, len(node_list))
    lo = node_list[chunk_start]
    hi = node_list[chunk_end_idx - 1]
    query = f"MATCH (n:Node) WHERE n.id >= {lo} AND n.id <= {hi} RETURN id(n) AS gid, n.id AS nid"
    try:
        cols, rows = cypher_execute(sock2, query)
        for row in rows:
            gid_map[row[1]] = row[0]  # nid -> gid
        print(f"  Mapped {len(gid_map):,} / {created:,} nodes...")
    except Exception as e:
        print(f"  GID mapping error: {e}")
        break

map_time = time.perf_counter() - start
print(f"  {len(gid_map):,} GID mappings in {map_time:.1f}s")

# Phase 4: Load edges via GID fast path
print(f"\nPhase 4: Loading {edge_count:,} edges...")
start = time.perf_counter()
loaded = 0
src_batch = []
dst_batch = []

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
                        if loaded % 10000000 == 0:
                            elapsed = time.perf_counter() - start
                            rate = loaded / elapsed
                            eta = (edge_count - loaded) / rate if rate > 0 else 0
                            print(f"  {loaded:,} / {edge_count:,} edges ({rate:,.0f}/s, ETA {eta/60:.0f}min)")
                    except Exception as e:
                        print(f"  Error at {loaded}: {e}")
                        try: sock.close()
                        except: pass
                        sock = xray_connect(HOST, PORT)
                    src_batch = []
                    dst_batch = []

if src_batch:
    try:
        bulk_insert_edges_gid(sock, "FRIEND", src_batch, dst_batch)
        loaded += len(src_batch)
    except:
        pass

edge_time = time.perf_counter() - start
total_time = scan_time + node_time + map_time + edge_time

print(f"\n{'='*60}")
print(f"  COMPLETE in {total_time:.1f}s ({total_time/60:.1f} min)")
print(f"  Nodes: {created:,}")
print(f"  Edges: {loaded:,}")
print(f"  Node rate: {created/node_time:,.0f}/s")
print(f"  Edge rate: {loaded/edge_time:,.0f}/s")
print(f"{'='*60}")

sock.close()
sock2.close()
