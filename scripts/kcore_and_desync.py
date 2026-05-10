#!/usr/bin/env python3
"""Two tests:
1. K-Core with k=5 — time it
2. Wire desync repro — trigger a failed query then capture what the next query gets
"""
import time, sys
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

def fresh():
    return XrayProtocolClient(host="127.0.0.1", port=7689, auth_token="bench:Bench2026!xray", database="bench", read_timeout=3600)

# ═══════════════════════════════════════════════════════════
# TEST 1: K-Core k=5
# ═══════════════════════════════════════════════════════════
print("=== TEST 1: K-Core k=5 ===")
c = fresh()
s = time.perf_counter()
try:
    cols, rows = c.execute('CALL xray.kcore("") YIELD core_number, time_ms RETURN core_number, time_ms ORDER BY core_number DESC LIMIT 5')
    ms = (time.perf_counter() - s) * 1000
    print(f"  K-Core: {ms:.0f}ms ({ms/1000:.1f}s), {len(rows)} rows")
    for r in rows: print(f"    {r}")
except Exception as e:
    ms = (time.perf_counter() - s) * 1000
    print(f"  K-Core ERROR ({ms:.0f}ms): {str(e)[:200]}")
c.close()

# ═══════════════════════════════════════════════════════════
# TEST 2: Wire desync repro
# ═══════════════════════════════════════════════════════════
print("\n=== TEST 2: Wire desync repro ===")
print("Using SINGLE connection. Sending a query that will fail,")
print("then sending a valid query to see if the response is desynced.")

c = fresh()
print(f"  Connected: {c.connected}")

# Query A: deliberately fail — vertex 999999999 won't exist
print("\n  Step 1: Send FAILING query (frontier_profile with invalid vertex)")
try:
    cols, rows = c.execute('CALL xray.frontier_profile(999999999, 1, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *')
    print(f"  Query A result: {len(rows)} rows (unexpected success)")
    for r in rows: print(f"    {r}")
except Exception as e:
    print(f"  Query A error (EXPECTED): {str(e)[:150]}")

# Query B: valid query — should return health report
print("\n  Step 2: Send VALID query (health report) on SAME connection")
try:
    cols, rows = c.execute('CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *')
    print(f"  Query B result: {len(rows)} rows")
    for r in rows: print(f"    {r}")
    # Check if this is actually the health report or a leaked error
    if rows and rows[0][0] == 'vertex_count':
        print("  VERDICT: Response matches query B (health report) — NO DESYNC")
    else:
        print(f"  VERDICT: Response does NOT look like health report — DESYNC DETECTED")
        print(f"  First row: {rows[0] if rows else 'empty'}")
except Exception as e:
    err = str(e)
    if "frontier_profile" in err or "start_id" in err:
        print(f"  Query B got Query A's error: {err[:150]}")
        print("  VERDICT: DESYNC CONFIRMED — Query B received Query A's error response")
    elif "health" in err.lower():
        print(f"  Query B error (own error): {err[:150]}")
        print("  VERDICT: No desync, but health report itself failed")
    else:
        print(f"  Query B error: {err[:150]}")
        print("  VERDICT: Need to check if this error belongs to Query A or B")

# Query C: another valid query to see continued desync
print("\n  Step 3: Send another VALID query (degree_distribution) on SAME connection")
try:
    cols, rows = c.execute('CALL xray.degree_distribution("") YIELD degree, count, cumulative_pct RETURN * LIMIT 5')
    print(f"  Query C result: {len(rows)} rows")
    for r in rows: print(f"    {r}")
    if rows and len(rows) > 0:
        # degree_distribution returns numeric degree values
        try:
            int(rows[0][0])
            print("  VERDICT: Response looks like degree_distribution — connection recovered")
        except:
            print(f"  VERDICT: Response doesn't look like degree_distribution — still desynced")
    else:
        print("  VERDICT: 0 rows — possible desync or empty result")
except Exception as e:
    print(f"  Query C error: {str(e)[:150]}")
    print("  VERDICT: Connection still broken after desync")

c.close()
print("\nDone")
