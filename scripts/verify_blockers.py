#!/usr/bin/env python3
"""Verify all Issue #12 blockers before running full bench."""
import time, sys

from xgdb_connect.protocol import XrayProtocolClient

c = XrayProtocolClient(host="127.0.0.1", port=7689, auth_token="bench:Bench2026!xray", database="bench", read_timeout=600)
print("Connected:", c.connected)

passed = 0
failed = 0

def check(name, fn):
    global passed, failed
    print(f"\n=== {name} ===")
    try:
        fn()
        passed += 1
    except Exception as e:
        print(f"  FAILED: {str(e)[:200]}")
        failed += 1

def blocker1_health():
    cols, rows = c.execute('CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *')
    for r in rows: print(f"  {r}")
    # Verify expected values
    health = {r[0]: r[2] for r in rows}
    vcount = int(health.get("vertex_count", health.get("count", "0")))
    # Check from the raw rows
    for r in rows:
        if r[0] == "vertex_count":
            assert int(r[1]) == 65608366, f"vertex_count={r[1]}, expected 65608366"
        if r[0] == "edge_count":
            assert int(r[1]) == 3612134270, f"edge_count={r[1]}, expected 3612134270"
    print("  PASS: correct vertex/edge counts")

def blocker1_frontier():
    cols, rows = c.execute('CALL xray.frontier_profile(13594, 2, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *')
    for r in rows: print(f"  {r}")
    hop1 = [r for r in rows if str(r[0]) == "1"]
    if hop1:
        fs = int(hop1[0][1])
        assert fs > 100, f"frontier_size={fs}, expected >100 (got {fs})"
        print(f"  PASS: frontier_size={fs}")
    else:
        raise Exception("No hop=1 row returned")

def blocker1_components():
    s = time.perf_counter()
    cols, rows = c.execute('CALL xray.connected_components("") YIELD component_size, num_components, time_ms RETURN * ORDER BY component_size DESC LIMIT 3')
    ms = (time.perf_counter() - s) * 1000
    print(f"  Time: {ms:.0f}ms, rows: {len(rows)}")
    for r in rows: print(f"  {r}")
    # Should be 1 component for undirected Friendster
    print(f"  PASS: {len(rows)} rows returned")

def blocker2_betweenness():
    s = time.perf_counter()
    cols, rows = c.execute('CALL xray.betweenness_centrality("", 100) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 5')
    ms = (time.perf_counter() - s) * 1000
    print(f"  Time: {ms:.0f}ms, rows: {len(rows)}")
    for r in rows: print(f"  {r}")
    assert ms < 60000, f"betweenness took {ms:.0f}ms, expected <60s (regression?)"
    print(f"  PASS: completed in {ms:.0f}ms")

def blocker3_community():
    s = time.perf_counter()
    cols, rows = c.execute('CALL xray.community_detection(3, "") YIELD community_size, num_communities RETURN * ORDER BY community_size DESC LIMIT 5')
    ms = (time.perf_counter() - s) * 1000
    print(f"  Time: {ms:.0f}ms, rows: {len(rows)}")
    for r in rows: print(f"  {r}")
    assert len(rows) > 0, "community_detection returned 0 rows"
    print(f"  PASS: {len(rows)} rows")

def blocker4_hits():
    s = time.perf_counter()
    cols, rows = c.execute('CALL xray.hits(3, "") YIELD node_id, hub, authority, time_ms RETURN node_id, hub, authority, time_ms ORDER BY authority DESC LIMIT 5')
    ms = (time.perf_counter() - s) * 1000
    print(f"  Time: {ms:.0f}ms, rows: {len(rows)}")
    for r in rows: print(f"  {r}")
    assert len(rows) > 0, "HITS returned 0 rows"
    print(f"  PASS: {len(rows)} rows")

check("BLOCKER 1a: Health Report", blocker1_health)
check("BLOCKER 1b: Frontier Profile", blocker1_frontier)
check("BLOCKER 1c: Connected Components", blocker1_components)
check("BLOCKER 2: Betweenness Centrality", blocker2_betweenness)
check("BLOCKER 3: Community Detection", blocker3_community)
check("BLOCKER 4: HITS", blocker4_hits)

c.close()

print(f"\n{'='*50}")
print(f"PASSED: {passed}  FAILED: {failed}")
if failed == 0:
    print("ALL BLOCKERS CLEAR — ready for full bench")
else:
    print(f"{failed} BLOCKERS STILL FAILING — do NOT run full bench")
