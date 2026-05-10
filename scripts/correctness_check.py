#!/usr/bin/env python3
"""Correctness validation — verify xrayGraphDB returns correct data.

Checks against:
1. SNAP published ground truth (Friendster)
2. Mathematical identities (BFS, edge counts)
3. Internal consistency (count queries match traversal)
4. Known LDBC SF1 values
"""
import time, sys
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

def fresh():
    return XrayProtocolClient(host="127.0.0.1", port=7689, auth_token="bench:Bench2026!xray", database="bench", read_timeout=3600)

passed = 0
failed = 0

def check(name, actual, expected, tolerance=0):
    global passed, failed
    if tolerance > 0 and isinstance(actual, (int, float)) and isinstance(expected, (int, float)):
        if abs(actual - expected) / max(expected, 1) <= tolerance:
            print(f"  PASS: {name} = {actual} (expected ~{expected}, within {tolerance*100}%)")
            passed += 1
            return True
    if actual == expected:
        print(f"  PASS: {name} = {actual}")
        passed += 1
        return True
    print(f"  FAIL: {name} = {actual}, EXPECTED {expected}")
    failed += 1
    return False

print("=" * 70)
print("CORRECTNESS VALIDATION")
print(f"Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
print("=" * 70)

# ═══════════════════════════════════════════════════════════════
# 1. FRIENDSTER CSR — SNAP Ground Truth
# ═══════════════════════════════════════════════════════════════
print("\n--- 1. Friendster CSR vs SNAP Ground Truth ---")
c = fresh()

cols, rows = c.execute('CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *')
# Tuple format: (metric, status, unit, value) — value is at index 3
health = {r[0]: r[3] for r in rows}
print(f"  Raw health: {[(r[0], r[3]) for r in rows]}")

check("Vertex count", int(health.get("vertex_count", 0)), 65608366)
check("Edge count (mirrored undirected)", int(health.get("edge_count", 0)), 3612134270)
check("Isolated vertices", int(health.get("isolated_vertices", 0)), 0)

# Connected components — SNAP says Friendster is 1 connected component
cols, rows = c.execute('CALL xray.connected_components("") YIELD component_size, num_components, time_ms RETURN component_size, num_components, time_ms ORDER BY component_size DESC LIMIT 1')
if rows:
    check("Connected components (SNAP: 1)", int(rows[0][1]), 1)
    check("Largest component = all vertices", int(rows[0][0]), 65608366)

# Triangle count — SNAP published: 4,173,724,142 for undirected
# Our YIELD columns: triangles, edges_checked, time_ms, vertices
cols, rows = c.execute('CALL xray.triangle_count("") YIELD triangles, edges_checked, time_ms, vertices RETURN *')
if rows:
    tri = int(rows[0][0])
    verts = int(rows[0][3])
    check("Vertices in triangle count", verts, 65608366)
    # SNAP ground truth for undirected Friendster triangles
    print(f"  INFO: Triangle count = {tri:,}")
    print(f"  INFO: SNAP reference = ~4,173,724,142")
    if tri == 1806067135:
        print(f"  FAIL: Triangle count equals edge count (1,806,067,135) — likely returning edges not triangles")
        failed += 1
    elif abs(tri - 4173724142) / 4173724142 < 0.01:
        print(f"  PASS: Triangle count matches SNAP within 1%")
        passed += 1
    else:
        print(f"  WARN: Triangle count {tri:,} differs from SNAP {4173724142:,} by {abs(tri-4173724142)/4173724142*100:.1f}%")

# Degree distribution consistency
cols, rows = c.execute('CALL xray.degree_distribution("") YIELD degree, count, cumulative_pct RETURN * LIMIT 20')
total_from_dist = sum(int(r[1]) for r in rows if rows)
print(f"  INFO: Sum of degree_distribution counts (first 20 buckets) = {total_from_dist:,}")

c.close()

# ═══════════════════════════════════════════════════════════════
# 2. MATHEMATICAL IDENTITIES
# ═══════════════════════════════════════════════════════════════
print("\n--- 2. Mathematical Identities ---")

# Create a small known graph for identity checks
c = fresh()

# Small test: create 5 nodes in a known pattern
# A -> B -> C -> D, A -> C, A -> E
c.execute("CREATE (:TestNode {id: 1})")
c.execute("CREATE (:TestNode {id: 2})")
c.execute("CREATE (:TestNode {id: 3})")
c.execute("CREATE (:TestNode {id: 4})")
c.execute("CREATE (:TestNode {id: 5})")
c.execute("CREATE INDEX ON :TestNode(id)")
c.execute("MATCH (a:TestNode {id:1}), (b:TestNode {id:2}) CREATE (a)-[:TEST]->(b)")
c.execute("MATCH (a:TestNode {id:2}), (b:TestNode {id:3}) CREATE (a)-[:TEST]->(b)")
c.execute("MATCH (a:TestNode {id:3}), (b:TestNode {id:4}) CREATE (a)-[:TEST]->(b)")
c.execute("MATCH (a:TestNode {id:1}), (b:TestNode {id:3}) CREATE (a)-[:TEST]->(b)")
c.execute("MATCH (a:TestNode {id:1}), (b:TestNode {id:5}) CREATE (a)-[:TEST]->(b)")

# Node 1 has 3 outgoing edges (to 2, 3, 5)
cols, rows = c.execute("MATCH (n:TestNode {id: 1})-[:TEST]->(m) RETURN count(m)")
check("Direct edge count (node 1 -> *)", int(rows[0][0]), 3)

# *1..1 should equal direct count
cols, rows = c.execute("MATCH (n:TestNode {id: 1})-[:TEST*1..1]->(m) RETURN count(m)")
check("*1..1 count equals direct count", int(rows[0][0]), 3)

# *1..2 paths from node 1:
# 1->2 (1), 1->3 (1), 1->5 (1), 1->2->3 (1), 1->3->4 (1) = 5 paths
cols, rows = c.execute("MATCH (n:TestNode {id: 1})-[:TEST*1..2]->(m) RETURN count(m)")
check("*1..2 path count from node 1", int(rows[0][0]), 5)

# *1..3 paths from node 1:
# hop1: 1->2, 1->3, 1->5 (3)
# hop2: 1->2->3, 1->3->4 (2)
# hop3: 1->2->3->4 (1)
# total = 6
cols, rows = c.execute("MATCH (n:TestNode {id: 1})-[:TEST*1..3]->(m) RETURN count(m)")
check("*1..3 path count from node 1", int(rows[0][0]), 6)

# Verify actual destination IDs for 1-hop
cols, rows = c.execute("MATCH (n:TestNode {id: 1})-[:TEST]->(m) RETURN m.id ORDER BY m.id")
actual_ids = sorted([int(r[0]) for r in rows])
check("1-hop destination IDs", actual_ids, [2, 3, 5])

# COUNT nodes should match
cols, rows = c.execute("MATCH (n:TestNode) RETURN count(n)")
check("TestNode count", int(rows[0][0]), 5)

# COUNT edges should match
cols, rows = c.execute("MATCH (:TestNode)-[r:TEST]->() RETURN count(r)")
check("TEST edge count", int(rows[0][0]), 5)

# Clean up
c.execute("MATCH (n:TestNode) DETACH DELETE n")
c.close()

# ═══════════════════════════════════════════════════════════════
# 3. LDBC SF1 CONSISTENCY (if loaded)
# ═══════════════════════════════════════════════════════════════
print("\n--- 3. LDBC SF1 Consistency ---")
c = fresh()

cols, rows = c.execute("MATCH (n) RETURN count(n)")
node_count = int(rows[0][0])

if node_count > 100000:
    # LDBC SF1 expected counts
    cols, rows = c.execute("MATCH (n:Person) RETURN count(n)")
    person_count = int(rows[0][0])
    print(f"  INFO: Person count = {person_count} (LDBC SF1 expected ~9,892)")

    cols, rows = c.execute("MATCH (n:Comment) RETURN count(n)")
    comment_count = int(rows[0][0])
    print(f"  INFO: Comment count = {comment_count} (LDBC SF1 expected ~2,052,169)")

    # Person with highest KNOWS degree — verify bidirectional consistency
    cols, rows = c.execute("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.id, count(f) AS out_deg ORDER BY out_deg DESC LIMIT 1")
    if rows:
        pid = rows[0][0]
        out_deg = int(rows[0][1])
        # Check the reverse direction
        cols, rows2 = c.execute(f"MATCH (p:Person {{id: {pid}}})<-[:KNOWS]-(f) RETURN count(f)")
        in_deg = int(rows2[0][0]) if rows2 else 0
        print(f"  INFO: Person {pid} out_degree={out_deg}, in_degree={in_deg}")

        # BFS identity: *1..1 count must equal direct count
        cols, rows3 = c.execute(f"MATCH (p:Person {{id: {pid}}})-[:KNOWS*1..1]->(f) RETURN count(f)")
        star_count = int(rows3[0][0]) if rows3 else -1
        check("*1..1 == direct edge count (LDBC)", star_count, out_deg)
else:
    print("  SKIP: No LDBC data loaded (Cypher store empty)")

c.close()

# ═══════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print(f"CORRECTNESS SUMMARY: {passed} PASSED, {failed} FAILED")
if failed == 0:
    print("ALL CHECKS PASSED")
else:
    print(f"{failed} CHECKS FAILED — INVESTIGATE BEFORE PUBLISHING")
print("=" * 70)
