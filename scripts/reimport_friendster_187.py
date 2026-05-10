#!/usr/bin/env python3
"""Re-import Friendster as undirected CSR on .187 (bench tenant).

The new binary (commit 590f32803) auto-detects undirected from .ungraph.* filenames
and mirrors edges. Expected: 65,608,366 vertices, 3,612,134,270 edges.
"""
import time, sys

sys.stdout = open("/tmp/reimport_friendster.log", "w", buffering=1)

from xgdb_connect.protocol import XrayProtocolClient

print("=" * 70, flush=True)
print("FRIENDSTER RE-IMPORT (undirected) — 216.106.185.187", flush=True)
print(f"Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}", flush=True)
print("=" * 70, flush=True)

c = XrayProtocolClient(
    host="127.0.0.1", port=7689,
    auth_token="bench:Bench2026!xray",
    database="bench",
    read_timeout=7200  # 2 hours for large import
)
print(f"Connected: {c.connected}", flush=True)

# Check current state
print("\n--- Pre-import health check ---", flush=True)
try:
    cols, rows = c.execute('CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *')
    for row in rows:
        print(f"  {row}", flush=True)
except Exception as e:
    print(f"  Health check: {e}", flush=True)

# Import Friendster
filepath = "/tmp/xraygraphdb-import/com-friendster.ungraph.txt"
print(f"\n--- Starting bulk_import_file: {filepath} ---", flush=True)
print(f"Start time: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}", flush=True)

start = time.perf_counter()
try:
    result = c.bulk_import_file(filepath)
    elapsed = time.perf_counter() - start
    print(f"\nImport completed in {elapsed:.1f}s ({elapsed/60:.1f} min)", flush=True)
    print(f"Result: {result}", flush=True)
except Exception as e:
    elapsed = time.perf_counter() - start
    print(f"\nImport failed after {elapsed:.1f}s: {e}", flush=True)

# Post-import verification
print(f"\n--- Post-import health check ---", flush=True)
try:
    cols, rows = c.execute('CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *')
    for row in rows:
        print(f"  {row}", flush=True)
except Exception as e:
    print(f"  Health check: {e}", flush=True)

# Verify edge count
print(f"\n--- Verification ---", flush=True)
try:
    cols, rows = c.execute('CALL xray.graph_stats("") YIELD metric, value RETURN *')
    for row in rows:
        print(f"  {row}", flush=True)
except Exception as e:
    print(f"  Graph stats: {e}", flush=True)

# Quick frontier check on vertex 13594
print(f"\n--- Frontier check (vertex 13594) ---", flush=True)
try:
    cols, rows = c.execute('CALL xray.frontier_profile(13594, 3, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *')
    for row in rows:
        print(f"  {row}", flush=True)
except Exception as e:
    print(f"  Frontier check: {e}", flush=True)

c.close()
print(f"\nDone.", flush=True)
