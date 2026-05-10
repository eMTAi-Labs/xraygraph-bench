#!/usr/bin/env python3
"""Retest all three regressions on new binary (PID 3308204)."""
import time, sys
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

def fresh():
    return XrayProtocolClient(host="127.0.0.1", port=7689, auth_token="bench:Bench2026!xray", database="bench", read_timeout=7200)

# 1. Graph Stats
print("1. Graph Stats...")
c = fresh()
s = time.perf_counter()
cols, rows = c.execute('CALL xray.graph_stats("") YIELD metric, value RETURN *')
ms = (time.perf_counter() - s) * 1000
print(f"   {ms:.0f}ms ({ms/1000:.1f}s), {len(rows)} rows")
if rows: print(f"   {rows[0]}")
c.close()

# 2. Find Path Budgeted
print("\n2. Find Path Budgeted (81306110 -> 20676652)...")
c = fresh()
s = time.perf_counter()
cols, rows = c.execute('CALL xray.find_path_budgeted(81306110, 20676652, 10.0, "") YIELD path_nodes, total_cost, hops, explored_nodes RETURN *')
cold = (time.perf_counter() - s) * 1000
print(f"   Cold: {cold:.0f}ms, {len(rows)} rows")
if rows: print(f"   {rows[0]}")
times = []
for _ in range(3):
    s = time.perf_counter()
    c.execute('CALL xray.find_path_budgeted(81306110, 20676652, 10.0, "") YIELD path_nodes, total_cost, hops, explored_nodes RETURN *')
    times.append((time.perf_counter() - s) * 1000)
warm = sum(times)/len(times)
print(f"   Warm avg: {warm:.0f}ms")
c.close()

# 3. Betweenness k=1000 (new random sampling)
print("\n3. Betweenness k=1000 (random sampling)...")
c = fresh()
s = time.perf_counter()
cols, rows = c.execute('CALL xray.betweenness_centrality("", 1000) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 5')
ms = (time.perf_counter() - s) * 1000
print(f"   {ms:.0f}ms ({ms/1000:.1f}s), {len(rows)} rows")
for r in rows[:3]: print(f"   {r}")
c.close()

# 4. Betweenness k=100 (fresh connection to avoid cascade)
print("\n4. Betweenness k=100 (random sampling)...")
c = fresh()
s = time.perf_counter()
cols, rows = c.execute('CALL xray.betweenness_centrality("", 100) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 5')
ms = (time.perf_counter() - s) * 1000
print(f"   {ms:.0f}ms ({ms/1000:.1f}s), {len(rows)} rows")
for r in rows[:3]: print(f"   {r}")
c.close()

# 5. PageRank(1) crash repro (fresh connection)
print("\n5. PageRank(1 iter) crash repro...")
c = fresh()
s = time.perf_counter()
try:
    cols, rows = c.execute('CALL xray.pagerank(1, 0.85, "") YIELD node_id, rank RETURN node_id, rank ORDER BY rank DESC LIMIT 5')
    ms = (time.perf_counter() - s) * 1000
    print(f"   {ms:.0f}ms ({ms/1000:.1f}s), {len(rows)} rows")
    for r in rows[:3]: print(f"   {r}")
except Exception as e:
    ms = (time.perf_counter() - s) * 1000
    print(f"   ERROR after {ms:.0f}ms: {str(e)[:150]}")
c.close()

print("\nDone")
