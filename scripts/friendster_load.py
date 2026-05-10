#!/usr/bin/env python3
"""Load Friendster edge list via xrayProtocol BULK_IMPORT_FILE (0x2B).
The server reads the edge file directly from disk and builds CSR."""
import socket
import struct
import time
import sys

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 7709
EDGE_FILE = sys.argv[3] if len(sys.argv) > 3 else "/opt/datasets/friendster/com-friendster.ungraph.txt"

def send_frame(sock, mtype, payload):
    header = struct.pack("<II", mtype, len(payload))
    sock.sendall(header + payload)

def recv_frame(sock):
    hdr = b""
    while len(hdr) < 8:
        chunk = sock.recv(8 - len(hdr))
        if not chunk:
            raise ConnectionError("Connection closed")
        hdr += chunk
    mtype, length = struct.unpack("<II", hdr)
    payload = b""
    while len(payload) < length:
        chunk = sock.recv(min(length - len(payload), 65536))
        if not chunk:
            raise ConnectionError("Connection closed during payload")
        payload += chunk
    return mtype, payload

# Connect
print(f"Connecting to {HOST}:{PORT}...")
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((HOST, PORT))
sock.settimeout(3600)  # 1 hour timeout for large loads

# HELLO handshake
hello_payload = struct.pack("<I", 0)  # version 0, no caps
send_frame(sock, 0x01, hello_payload)
mtype, resp = recv_frame(sock)
print(f"HELLO response: type=0x{mtype:02x}, {len(resp)} bytes")

# BULK_IMPORT_FILE (0x2B)
# Wire format: u32 path_len, path_bytes
path_bytes = EDGE_FILE.encode("utf-8")
payload = struct.pack("<I", len(path_bytes)) + path_bytes

print(f"Sending BULK_IMPORT_FILE: {EDGE_FILE}")
print(f"  File: 65.6M nodes, 1.8B edges (31GB)")
print(f"  This will take several minutes...")

start = time.perf_counter()
send_frame(sock, 0x2B, payload)

# Wait for response — this takes a long time for 1.8B edges
mtype, resp = recv_frame(sock)
elapsed = time.perf_counter() - start

if mtype == 0x25:  # ACK
    if len(resp) >= 8:
        edges = struct.unpack("<Q", resp[:8])[0]
        print(f"SUCCESS: {edges:,} edges loaded in {elapsed:.1f}s ({edges/elapsed:,.0f} edges/sec)")
    else:
        print(f"SUCCESS: loaded in {elapsed:.1f}s")
elif mtype == 0x26:  # ERROR
    print(f"ERROR: {resp.decode('utf-8', errors='replace')}")
else:
    print(f"Unknown response: type=0x{mtype:02x}, {len(resp)} bytes")
    print(f"  {resp[:200]}")

sock.close()
