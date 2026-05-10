#!/usr/bin/env python3
"""Try the new betweenness_centrality_sampled procedure."""
import time, sys
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

c = XrayProtocolClient(host="127.0.0.1", port=7689, auth_token="bench:Bench2026!xray", database="bench", read_timeout=600)
print("Connected:", c.connected)

# Try different call signatures for the sampled procedure
attempts = [
    ('xray.betweenness_centrality_sampled(1000, "")', 'CALL xray.betweenness_centrality_sampled(1000, "") YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 5'),
    ('xray.betweenness_centrality_sampled("", 1000)', 'CALL xray.betweenness_centrality_sampled("", 1000) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 5'),
]

for name, q in attempts:
    print(f"\nTrying {name}...")
    s = time.perf_counter()
    try:
        cols, rows = c.execute(q)
        ms = (time.perf_counter() - s) * 1000
        print(f"  {ms:.0f}ms ({ms/1000:.1f}s), {len(rows)} rows")
        for r in rows[:3]: print(f"  {r}")
        break
    except Exception as e:
        ms = (time.perf_counter() - s) * 1000
        print(f"  ERROR ({ms:.0f}ms): {str(e)[:150]}")

c.close()
print("\nDone")
