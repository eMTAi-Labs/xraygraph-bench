#!/usr/bin/env python3
"""Load Friendster via xrayProtocol — create nodes + edges from SNAP edge list."""
import socket
import struct
import time
import sys

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 7709
EDGE_FILE = sys.argv[3] if len(sys.argv) > 3 else "/opt/datasets/friendster/com-friendster.ungraph.txt"

def recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Connection closed by server")
        buf.extend(chunk)
    return bytes(buf)

def recv_frame(sock):
    hdr = recv_exact(sock, 8)
    plen, mtype, flags, qid = struct.unpack("<IBBH", hdr)
    resp = recv_exact(sock, plen) if plen > 0 else b""
    return mtype, resp

def xray_connect(host, port):
    sock = socket.create_connection((host, port), timeout=60)
    sock.settimeout(3600)
    token = b"admin:admin"
    payload = struct.pack("<HHI", 1, 0, len(token)) + token
    header = struct.pack("<IBBH", len(payload), 0x01, 0, 0)
    sock.sendall(header + payload)
    resp_hdr = recv_exact(sock, 8)
    plen, mtype = struct.unpack_from("<IB", resp_hdr, 0)
    if plen > 0:
        recv_exact(sock, plen)
    if mtype != 0x02:
        raise RuntimeError(f"HELLO failed: 0x{mtype:02x}")
    return sock

def cypher_execute(sock, query, qid=1):
    qb = query.encode("utf-8")
    payload = struct.pack("<B", 0) + struct.pack("<I", len(qb)) + qb
    payload += struct.pack("<I", 0) + struct.pack("<I", 0) + struct.pack("<H", 0)
    header = struct.pack("<IBBH", len(payload), 0x03, 0, qid)
    sock.sendall(header + payload)
    while True:
        mtype, resp = recv_frame(sock)
        if mtype == 0x06:  # COMPLETE
            return
        if mtype == 0x07:  # ERROR
            raise RuntimeError(f"Cypher error: {resp.decode('utf-8', errors='replace')[:200]}")

def bulk_insert_edges_gid(sock, edge_type, src_gids, dst_gids):
    """BULK_INSERT_EDGES GID fast path (0x22)."""
    count = len(src_gids)
    # Header: count, prop_count=0, GID_MARKER=0xFFFFFFFF
    payload = struct.pack("<III", count, 0, 0xFFFFFFFF)
    # Edge type string
    etb = edge_type.encode("utf-8")
    payload += struct.pack("<I", len(etb)) + etb
    # GID pairs
    for i in range(count):
        payload += struct.pack("<QQ", src_gids[i], dst_gids[i])
    header = struct.pack("<IBBH", len(payload), 0x22, 0, 0)
    sock.sendall(header + payload)
    mtype, resp = recv_frame(sock)
    if mtype == 0x25 and len(resp) >= 4:
        return struct.unpack("<I", resp[:4])[0]
    elif mtype == 0x26:
        raise RuntimeError(f"BULK_INSERT error: {resp.decode('utf-8', errors='replace')[:100]}")
    return 0

print(f"=== Friendster Loader via xrayProtocol ===")
print(f"Target: {HOST}:{PORT}")
print(f"File: {EDGE_FILE}")

sock = xray_connect(HOST, PORT)
print(f"Connected")

# Phase 1: Read edge file, collect unique node IDs, create nodes via Cypher
print(f"\nPhase 1: Scanning edge file for unique node IDs...")
start = time.perf_counter()
node_ids = set()
edge_count = 0
with open(EDGE_FILE) as f:
    for line in f:
        if line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) >= 2:
            node_ids.add(int(parts[0]))
            node_ids.add(int(parts[1]))
            edge_count += 1
scan_time = time.perf_counter() - start
print(f"  {len(node_ids):,} unique nodes, {edge_count:,} edges scanned in {scan_time:.1f}s")

# Phase 2: Create nodes in batches via Cypher UNWIND
print(f"\nPhase 2: Creating {len(node_ids):,} nodes...")
start = time.perf_counter()
node_list = sorted(node_ids)
batch_size = 10000
created = 0
for i in range(0, len(node_list), batch_size):
    batch = node_list[i:i+batch_size]
    ids_str = ",".join(str(n) for n in batch)
    query = f"UNWIND [{ids_str}] AS nid CREATE (:Node {{id: nid}})"
    try:
        cypher_execute(sock, query)
        created += len(batch)
        if created % 1000000 == 0:
            elapsed = time.perf_counter() - start
            rate = created / elapsed
            print(f"  {created:,} / {len(node_list):,} nodes ({rate:,.0f}/s)")
    except Exception as e:
        print(f"  Error at {created}: {e}")
        break
node_time = time.perf_counter() - start
print(f"  {created:,} nodes created in {node_time:.1f}s ({created/node_time:,.0f}/s)")

# Phase 3: Build GID mapping
print(f"\nPhase 3: Building GID mapping...")
start = time.perf_counter()
# For GID fast path, we need internal GID for each node
# Query all nodes to build id -> gid mapping
# This is expensive for 65M nodes — do in chunks by label
sock2 = xray_connect(HOST, PORT)  # separate connection for queries

gid_map = {}
qb = "MATCH (n:Node) RETURN id(n) AS gid, n.id AS nid".encode("utf-8")
payload = struct.pack("<B", 0) + struct.pack("<I", len(qb)) + qb
payload += struct.pack("<I", 0) + struct.pack("<I", 0) + struct.pack("<H", 0)
header = struct.pack("<IBBH", len(payload), 0x03, 0, 1)
sock2.sendall(header + payload)

while True:
    mtype, resp = recv_frame(sock2)
    if mtype == 0x04:  # SCHEMA
        continue
    elif mtype == 0x05:  # BATCH
        # Parse batch rows
        offset = 0
        row_count = struct.unpack_from("<I", resp, offset)[0]
        offset += 4
        for _ in range(row_count):
            # Read gid (int64) and nid (int64)
            if offset + 16 <= len(resp):
                gid = struct.unpack_from("<q", resp, offset)[0]
                offset += 8
                nid = struct.unpack_from("<q", resp, offset)[0]
                offset += 8
                gid_map[nid] = gid
    elif mtype == 0x06:  # COMPLETE
        break
    elif mtype == 0x07:  # ERROR
        print(f"  GID mapping error: {resp.decode('utf-8', errors='replace')[:200]}")
        break

map_time = time.perf_counter() - start
print(f"  {len(gid_map):,} GID mappings in {map_time:.1f}s")

# Phase 4: Load edges via GID fast path
print(f"\nPhase 4: Loading {edge_count:,} edges via GID fast path...")
start = time.perf_counter()
loaded = 0
batch_src = []
batch_dst = []
EDGE_BATCH = 50000

with open(EDGE_FILE) as f:
    for line in f:
        if line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) >= 2:
            src_id = int(parts[0])
            dst_id = int(parts[1])
            src_gid = gid_map.get(src_id)
            dst_gid = gid_map.get(dst_id)
            if src_gid is not None and dst_gid is not None:
                batch_src.append(src_gid)
                batch_dst.append(dst_gid)

                if len(batch_src) >= EDGE_BATCH:
                    try:
                        bulk_insert_edges_gid(sock, "FRIEND", batch_src, batch_dst)
                        loaded += len(batch_src)
                        if loaded % 10000000 == 0:
                            elapsed = time.perf_counter() - start
                            rate = loaded / elapsed
                            print(f"  {loaded:,} / {edge_count:,} edges ({rate:,.0f}/s)")
                    except Exception as e:
                        print(f"  Error at {loaded}: {e}")
                        break
                    batch_src = []
                    batch_dst = []

if batch_src:
    try:
        bulk_insert_edges_gid(sock, "FRIEND", batch_src, batch_dst)
        loaded += len(batch_src)
    except Exception as e:
        print(f"  Final batch error: {e}")

edge_time = time.perf_counter() - start
total_time = scan_time + node_time + map_time + edge_time

print(f"\n{'='*60}")
print(f"  COMPLETE in {total_time:.1f}s")
print(f"  Nodes: {created:,}")
print(f"  Edges: {loaded:,}")
print(f"  Edge rate: {loaded/edge_time:,.0f} edges/sec")
print(f"{'='*60}")

sock.close()
sock2.close()
