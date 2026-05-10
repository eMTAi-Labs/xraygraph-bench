#!/usr/bin/env python3
"""Test new random-sampling betweenness at k=1000 and k=100."""
import time, sys
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

c = XrayProtocolClient(host="127.0.0.1", port=7689, auth_token="bench:Bench2026!xray", database="bench", read_timeout=7200)
print("Connected:", c.connected)

print("\nBetweenness k=1000 (random sampling, epsilon~0.07)...")
s = time.perf_counter()
cols, rows = c.execute('CALL xray.betweenness_centrality("", 1000) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10')
ms = (time.perf_counter() - s) * 1000
print(f"  {ms:.0f}ms ({ms/1000:.1f}s), {len(rows)} rows")
for r in rows[:5]: print(f"  {r}")

print("\nBetweenness k=100 (random sampling, epsilon~0.21)...")
s = time.perf_counter()
cols, rows = c.execute('CALL xray.betweenness_centrality("", 100) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10')
ms = (time.perf_counter() - s) * 1000
print(f"  {ms:.0f}ms ({ms/1000:.1f}s), {len(rows)} rows")
for r in rows[:5]: print(f"  {r}")

c.close()
print("\nDone")
