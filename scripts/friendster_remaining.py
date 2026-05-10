#!/usr/bin/env python3
"""Run remaining Friendster analytics that timed out."""
import time, json, sys
sys.stdout = open("/opt/xraybench-results/friendster_remaining.log", "w", buffering=1)

from xgdb_connect.protocol import XrayProtocolClient
c = XrayProtocolClient("127.0.0.1", 7689, auth_token="bench:Bench2026!xray", read_timeout=1800)
print(f"Connected: {c.connected}", flush=True)

results = []
for name, q in [
    ("Betweenness 100", 'CALL xray.betweenness_centrality("", 100) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 5'),
    ("K-Core", 'CALL xray.kcore("") YIELD core_number, time_ms RETURN core_number, count(*) AS cnt, time_ms ORDER BY core_number DESC LIMIT 5'),
    ("Community 20iter", 'CALL xray.community_detection(20, "") YIELD community_size, num_communities RETURN * ORDER BY community_size DESC LIMIT 5'),
    ("HITS 20iter", 'CALL xray.hits(20, "") YIELD node_id, hub, authority, time_ms RETURN node_id, hub, authority, time_ms ORDER BY authority DESC LIMIT 5'),
    ("PersonalizedPR", 'CALL xray.personalized_pagerank(13594, 0.85, 20, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5'),
]:
    print(f"{name}...", end=" ", flush=True)
    try:
        s = time.perf_counter()
        cols, rows = c.execute(q)
        ms = (time.perf_counter() - s) * 1000
        print(f"{ms:.0f}ms, {len(rows)} rows", flush=True)
        if rows: print(f"  {rows[0]}", flush=True)
        results.append({"name": name, "ms": round(ms, 1), "rows": len(rows)})
    except Exception as e:
        print(f"ERROR: {str(e)[:120]}", flush=True)
        results.append({"name": name, "error": str(e)[:300]})

c.close()
json.dump(results, open("/opt/xraybench-results/friendster_remaining.json", "w"), indent=2, default=str)
print("\nDone.", flush=True)
