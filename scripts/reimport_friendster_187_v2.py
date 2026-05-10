#!/usr/bin/env python3
"""Fresh Friendster import on .187 after CSR corruption cleanup.

Server restarted with no CSR loaded. This import should:
1. Build undirected CSR (auto-detected from .ungraph. filename)
2. Write to /neo4j/csr_default/ (bench user tenant_id=default)
3. Produce 65,608,366 vertices, 3,612,134,270 edges
4. manifest.bin with XRAYCSR2 magic when complete
"""
import time, sys

sys.stdout = open("/tmp/reimport_friendster_v2.log", "w", buffering=1)

from xgdb_connect.protocol import XrayProtocolClient

print("=" * 70, flush=True)
print("FRESH FRIENDSTER IMPORT — 216.106.185.187", flush=True)
print(f"Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}", flush=True)
print("=" * 70, flush=True)

c = XrayProtocolClient(
    host="127.0.0.1", port=7689,
    auth_token="bench:Bench2026!xray",
    database="bench",
    read_timeout=7200  # 2 hours
)
print(f"Connected: {c.connected}", flush=True)

# Pre-import health
print("\n--- Pre-import ---", flush=True)
try:
    cols, rows = c.execute('CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *')
    for r in rows: print(f"  {r}", flush=True)
except Exception as e:
    print(f"  Health: {e}", flush=True)

# Import
filepath = "/tmp/xraygraphdb-import/com-friendster.ungraph.txt"
print(f"\n--- bulk_import_file: {filepath} ---", flush=True)
print(f"Start: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}", flush=True)

start = time.perf_counter()
try:
    result = c.bulk_import_file(filepath)
    elapsed = time.perf_counter() - start
    print(f"\nImport completed in {elapsed:.1f}s ({elapsed/60:.1f} min)", flush=True)
    print(f"Result: {result}", flush=True)
except Exception as e:
    elapsed = time.perf_counter() - start
    print(f"\nImport exception after {elapsed:.1f}s ({elapsed/60:.1f} min): {e}", flush=True)

print(f"End: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}", flush=True)

# Post-import verification
print(f"\n--- Post-import verification ---", flush=True)
try:
    c2 = XrayProtocolClient(
        host="127.0.0.1", port=7689,
        auth_token="bench:Bench2026!xray",
        database="bench",
        read_timeout=600
    )
    cols, rows = c2.execute('CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *')
    for r in rows: print(f"  {r}", flush=True)

    print(flush=True)
    # Frontier check
    for v in [13594, 100000, 1000000]:
        try:
            cols, rows = c2.execute(f'CALL xray.frontier_profile({v}, 1, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *')
            for r in rows:
                if r[0] == '1':
                    print(f"  Frontier v={v}: frontier_size={r[1]}", flush=True)
        except Exception as e:
            print(f"  Frontier v={v}: {str(e)[:100]}", flush=True)

    c2.close()
except Exception as e:
    print(f"  Verification error: {e}", flush=True)

try:
    c.close()
except:
    pass

print(f"\nDone.", flush=True)
