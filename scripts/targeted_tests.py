#!/usr/bin/env python3
"""Targeted tests requested by DB team:
1. Common Neighbors / Jaccard / Link Prediction perf on hub pairs
2. HITS / PersonalizedPR / Clustering Coefficient standalone
3. Community Detection with verbatim error capture
"""
import time, sys, traceback
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

def fresh():
    return XrayProtocolClient(host="127.0.0.1", port=7689, auth_token="bench:Bench2026!xray", database="bench", read_timeout=3600)

# Get hub vertex pair
c = fresh()
cols, rows = c.execute('CALL xray.pagerank(1, 0.85, "") YIELD node_id, rank RETURN node_id, rank ORDER BY rank DESC LIMIT 5')
hubs = [int(r[0]) for r in rows]
print(f"Hub vertices: {hubs[:3]}")
c.close()

v1, v2 = hubs[0], hubs[1]

# ═══════════════════════════════════════════════════════════
# TEST 1: Common Neighbors / Jaccard / Link Prediction perf
# ═══════════════════════════════════════════════════════════
print(f"\n=== TEST 1: Pair queries on v={v1}, v={v2} ===")

for name, q in [
    ("Common Neighbors", f'CALL xray.common_neighbors({v1}, {v2}) YIELD neighbor_id, count RETURN *'),
    ("Jaccard Similarity", f'CALL xray.jaccard_similarity({v1}, {v2}) YIELD jaccard, overlap, common, total_union, degree_a, degree_b RETURN *'),
    ("Link Prediction", f'CALL xray.link_prediction({v1}, 10, "adamic_adar") YIELD node_id, score, method, rank RETURN *'),
]:
    c = fresh()
    s = time.perf_counter()
    try:
        cols, rows = c.execute(q)
        ms = (time.perf_counter() - s) * 1000
        print(f"  {name:<25} {ms:.1f}ms, {len(rows)} rows")
        if rows: print(f"    {rows[0]}")
    except Exception as e:
        ms = (time.perf_counter() - s) * 1000
        print(f"  {name:<25} ERROR ({ms:.0f}ms): {str(e)[:150]}")
    c.close()

# ═══════════════════════════════════════════════════════════
# TEST 2: HITS / PersonalizedPR / Clustering standalone
# ═══════════════════════════════════════════════════════════
print(f"\n=== TEST 2: Standalone procedure tests ===")

for name, q in [
    ("HITS 3iter", 'CALL xray.hits(3, "") YIELD node_id, hub, authority, time_ms RETURN node_id, hub, authority, time_ms ORDER BY authority DESC LIMIT 5'),
    ("PersonalizedPR 5iter", f'CALL xray.personalized_pagerank({v1}, 0.85, 5, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5'),
    ("Clustering Coefficient", 'CALL xray.clustering_coefficient("") YIELD node_id, coefficient, triangles RETURN node_id, coefficient, triangles ORDER BY coefficient DESC LIMIT 5'),
]:
    print(f"\n  {name}...")
    c = fresh()
    s = time.perf_counter()
    try:
        cols, rows = c.execute(q)
        ms = (time.perf_counter() - s) * 1000
        print(f"  {name:<25} {ms:.0f}ms ({ms/1000:.1f}s), {len(rows)} rows")
        if rows: print(f"    {rows[0]}")
    except Exception as e:
        ms = (time.perf_counter() - s) * 1000
        print(f"  {name:<25} ERROR ({ms:.0f}ms)")
        print(f"  VERBATIM EXCEPTION: {repr(e)}")
        print(f"  EXCEPTION TYPE: {type(e).__name__}")
        print(f"  FULL MESSAGE: {str(e)}")
    c.close()

# ═══════════════════════════════════════════════════════════
# TEST 3: Community Detection with full error capture
# ═══════════════════════════════════════════════════════════
print(f"\n=== TEST 3: Community Detection (verbatim error capture) ===")
c = fresh()
s = time.perf_counter()
try:
    cols, rows = c.execute('CALL xray.community_detection(3, "") YIELD community_size, num_communities RETURN community_size, num_communities ORDER BY community_size DESC LIMIT 5')
    ms = (time.perf_counter() - s) * 1000
    print(f"  Community 3iter: {ms:.0f}ms ({ms/1000:.1f}s), {len(rows)} rows")
    if rows: print(f"    {rows[0]}")
except Exception as e:
    ms = (time.perf_counter() - s) * 1000
    print(f"  Community 3iter: ERROR after {ms:.0f}ms ({ms/1000:.1f}s)")
    print(f"  VERBATIM EXCEPTION: {repr(e)}")
    print(f"  EXCEPTION TYPE: {type(e).__name__}")
    print(f"  FULL MESSAGE: {str(e)}")
    print(f"  TRACEBACK:")
    traceback.print_exc()
c.close()

# Check for slow proc warnings
print(f"\n=== Daemon slow-proc log ===")
import subprocess
result = subprocess.run(["journalctl", "-u", "xraygraphdb", "--since", "5 min ago", "--no-pager", "-q"],
                       capture_output=True, text=True, timeout=10)
for line in result.stdout.split("\n"):
    if "xray_proc_slow" in line or "community" in line.lower():
        print(f"  {line}")
if not any("xray_proc_slow" in l or "community" in l.lower() for l in result.stdout.split("\n")):
    print("  (no xray_proc_slow or community entries found)")

print("\nDone")
