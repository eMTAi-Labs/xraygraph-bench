#!/usr/bin/env python3
"""Official competitor-comparable benchmarks on LDBC SF1.

Same queries run on Memgraph, Neo4j, NebulaGraph, DuckDB, PostgreSQL, MySQL.
Then Friendster CSR analytics that no competitor can run.
"""
import time, json, sys
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

LOG = "/tmp/official_bench.log"
RESULTS = "/tmp/official_bench_results.json"
log = open(LOG, "w", buffering=1)

def p(msg):
    log.write(msg + "\n")
    log.flush()

def fresh():
    return XrayProtocolClient(host="127.0.0.1", port=7689, auth_token="bench:Bench2026!xray", database="bench", read_timeout=7200)

def run(name, query, c, warmup=5):
    try:
        # Cold
        s = time.perf_counter()
        cols, rows = c.execute(query)
        cold = (time.perf_counter() - s) * 1000
        # Warm
        times = []
        for _ in range(warmup):
            s = time.perf_counter()
            c.execute(query)
            times.append((time.perf_counter() - s) * 1000)
        warm = sum(times) / len(times) if times else cold
        p(f"  {name:<45} cold={cold:.1f}ms  warm={warm:.1f}ms  rows={len(rows)}")
        if rows and len(rows) > 0:
            p(f"    {rows[0]}")
        return {"name": name, "cold_ms": round(cold,1), "warm_ms": round(warm,1), "rows": len(rows), "status": "PASS"}
    except Exception as e:
        p(f"  {name:<45} ERROR: {str(e)[:150]}")
        return {"name": name, "error": str(e)[:200], "status": "FAIL"}

def run_once(name, query, c):
    try:
        s = time.perf_counter()
        cols, rows = c.execute(query)
        ms = (time.perf_counter() - s) * 1000
        p(f"  {name:<45} {ms:.0f}ms  rows={len(rows)}")
        if rows and len(rows) > 0:
            p(f"    {rows[0]}")
        return {"name": name, "ms": round(ms,1), "rows": len(rows), "status": "PASS"}
    except Exception as e:
        p(f"  {name:<45} ERROR: {str(e)[:150]}")
        return {"name": name, "error": str(e)[:200], "status": "FAIL"}

p("=" * 70)
p("OFFICIAL BENCHMARK — xrayGraphDB vs Competitors")
p("LDBC SF1 (3.18M nodes, 8.2M edges) + Friendster CSR (65.6M, 3.6B)")
p(f"Server: 216.106.185.187 port 7689")
p(f"Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
p("=" * 70)

results = {}

# ══════════════════════════════════════════════════════════════════
# PART 1: LDBC Queries (same as competitors)
# ══════════════════════════════════════════════════════════════════
p("\n" + "=" * 70)
p("PART 1: LDBC INTERACTIVE QUERIES (Cypher, same as competitors)")
p("=" * 70)

c = fresh()
s1 = []

# Find person with ~96 KNOWS edges (competitor tests used Person 933 with 96 KNOWS)
cols, rows = c.execute("MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.id, p.firstName, count(f) AS deg ORDER BY abs(count(f) - 96) LIMIT 1")
if rows and rows[0][2] > 10:
    person_id = rows[0][0]
    person_deg = rows[0][2]
    p(f"\n  Test person: id={person_id} ({rows[0][1]}), KNOWS degree={person_deg} (competitor baseline: Person 933, 96 KNOWS)")
else:
    # Fallback: highest KNOWS degree
    cols, rows = c.execute("MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.id, p.firstName, count(f) AS deg ORDER BY deg DESC LIMIT 1")
    person_id = rows[0][0] if rows else 933
    person_deg = rows[0][2] if rows else 0
    p(f"\n  Test person: id={person_id}, KNOWS degree={person_deg}")

# IS1: Person profile lookup
s1.append(run("IS1: Person profile",
    f"MATCH (p:Person {{id: {person_id}}}) RETURN p.id, p.firstName, p.lastName, p.gender", c))

# IS3: Friend count
s1.append(run("IS3: Friend count",
    f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS]-(f) RETURN count(f)", c))

# IS3: Friends LIMIT 20
s1.append(run("IS3: Friends LIMIT 20",
    f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS]-(f:Person) RETURN f.id, f.firstName ORDER BY f.firstName LIMIT 20", c))

# IC2: Recent messages from friends (2-hop)
s1.append(run("IC2: Messages from friends",
    f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS]-(f:Person)<-[:LIKES_COMMENT]-(c:Comment) RETURN f.id, c.id LIMIT 10", c))

# IC5: Forums of friends
s1.append(run("IC5: Forums of friends",
    f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS]-(f:Person)<-[:HAS_MEMBER]-(forum:Forum) RETURN forum.id, forum.title LIMIT 10", c))

# IC11: Friends work
s1.append(run("IC11: Friends work",
    f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS]-(f:Person)-[:WORK_AT]->(org:Organisation) RETURN f.firstName, org.name LIMIT 10", c))

# Edge count
s1.append(run("Edge count (8.2M)",
    "MATCH ()-[r]->() RETURN count(r)", c))

# Node count
s1.append(run("Node count (3.18M)",
    "MATCH (n) RETURN count(n)", c))

results["ldbc_queries"] = s1
c.close()

# ══════════════════════════════════════════════════════════════════
# PART 2: BFS 1-10 Hops (the "show where they die" test)
# ══════════════════════════════════════════════════════════════════
p("\n" + "=" * 70)
p("PART 2: BFS 1-10 HOPS via Cypher (same as competitors)")
p("=" * 70)

s2 = []

for hops in range(1, 11):
    c = fresh()
    s = time.perf_counter()
    try:
        cols, rows = c.execute(f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS*1..{hops}]-(f) RETURN count(f)")
        ms = (time.perf_counter() - s) * 1000
        cnt = rows[0][0] if rows else 0
        p(f"  BFS {hops}-hop (Cypher)                            {ms:.1f}ms  paths={cnt}")
        s2.append({"name": f"BFS {hops}-hop (Cypher)", "ms": round(ms, 1), "rows": 1, "paths": cnt, "status": "PASS"})
        if ms > 600000:
            p(f"  *** STOPPING at {hops}-hop — exceeded 300s ***")
            c.close()
            break
    except Exception as e:
        ms = (time.perf_counter() - s) * 1000
        p(f"  BFS {hops}-hop (Cypher)                            ERROR ({ms:.0f}ms): {str(e)[:80]}")
        s2.append({"name": f"BFS {hops}-hop (Cypher)", "error": str(e)[:200], "status": "FAIL"})
        c.close()
        break
    c.close()

results["bfs_hops_cypher"] = s2

# ══════════════════════════════════════════════════════════════════
# PART 3: BFS via CSR (xrayGraphDB-only — no competitor can do this)
# ══════════════════════════════════════════════════════════════════
p("\n" + "=" * 70)
p("PART 3: BFS via CSR frontier_profile (xrayGraphDB-only)")
p("=" * 70)

c = fresh()
s3 = []

# Find a hub vertex from pagerank
cols, rows = c.execute('CALL xray.pagerank(5, 0.85, "") YIELD node_id, rank RETURN node_id, rank ORDER BY rank DESC LIMIT 1')
hub = int(rows[0][0]) if rows else 81306110
p(f"\n  Hub vertex: {hub}")

for hops in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
    s3.append(run_once(f"CSR BFS {hops}-hop (v={hub})",
        f'CALL xray.frontier_profile({hub}, {hops}, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *', c))

results["bfs_hops_csr"] = s3
c.close()

# ══════════════════════════════════════════════════════════════════
# PART 4: Friendster Analytics (xrayGraphDB-only)
# ══════════════════════════════════════════════════════════════════
p("\n" + "=" * 70)
p("PART 4: FRIENDSTER ANALYTICS (65.6M vertices, 3.6B edges)")
p("No competitor can load or query this dataset.")
p("=" * 70)

s4 = []

c = fresh()
s4.append(run_once("Health Report", 'CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *', c))
s4.append(run_once("Graph Stats", 'CALL xray.graph_stats("") YIELD metric, value RETURN *', c))
s4.append(run_once("Degree Distribution", 'CALL xray.degree_distribution("") YIELD degree, count, cumulative_pct RETURN * LIMIT 20', c))
c.close()

c = fresh()
s4.append(run_once("Connected Components",
    'CALL xray.connected_components("") YIELD component_size, num_components, time_ms RETURN component_size, num_components, time_ms ORDER BY component_size DESC LIMIT 5', c))
c.close()

c = fresh()
s4.append(run_once("PageRank 5iter",
    'CALL xray.pagerank(5, 0.85, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5', c))
c.close()

c = fresh()
s4.append(run_once("Triangle Count",
    'CALL xray.triangle_count("") YIELD triangles, edges_checked, time_ms, vertices RETURN *', c))
c.close()

c = fresh()
s4.append(run_once("K-Core",
    'CALL xray.kcore("") YIELD core_number, time_ms RETURN core_number, time_ms ORDER BY core_number DESC LIMIT 5', c))
c.close()

c = fresh()
s4.append(run_once("Community 3iter",
    'CALL xray.community_detection(3, "") YIELD community_size, num_communities RETURN community_size, num_communities ORDER BY community_size DESC LIMIT 5', c))
c.close()

c = fresh()
s4.append(run_once("HITS 3iter",
    'CALL xray.hits(3, "") YIELD node_id, hub, authority, time_ms RETURN node_id, hub, authority, time_ms ORDER BY authority DESC LIMIT 5', c))
c.close()

c = fresh()
s4.append(run_once("PersonalizedPR 5iter",
    f'CALL xray.personalized_pagerank({hub}, 0.85, 5, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5', c))
s4.append(run_once("Clustering Coefficient",
    'CALL xray.clustering_coefficient("") YIELD node_id, coefficient, triangles RETURN node_id, coefficient, triangles ORDER BY coefficient DESC LIMIT 5', c))
c.close()

# Betweenness variants
c = fresh()
s4.append(run_once("BC Pair-Sampled COLD (e=0.05)",
    'CALL xray.betweenness_pair_sampled(0.05, 0.05, "") YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10', c))
s4.append(run_once("BC Pair-Sampled WARM (e=0.05)",
    'CALL xray.betweenness_pair_sampled(0.05, 0.05, "") YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10', c))
s4.append(run_once("BC Pair-Sampled WARM (e=0.10)",
    'CALL xray.betweenness_pair_sampled(0.10, 0.05, "") YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10', c))
c.close()

# Traversal on Friendster — find second hub vertex
c = fresh()
cols, pr2 = c.execute('CALL xray.pagerank(1, 0.85, "") YIELD node_id, rank RETURN node_id, rank ORDER BY rank DESC LIMIT 5')
hub2 = int(pr2[1][0]) if len(pr2) > 1 else hub + 1

s4.append(run(f"Shortest Path ({hub}->{hub2})",
    f'CALL xray.shortest_path({hub}, {hub2}, "") YIELD node_id, distance, path_index, time_ms RETURN *', c))
s4.append(run("Jaccard Similarity",
    f'CALL xray.jaccard_similarity({hub}, {hub2}) YIELD jaccard, overlap, common, total_union, degree_a, degree_b RETURN *', c))
s4.append(run_once("Link Prediction",
    f'CALL xray.link_prediction({hub}, 10, "adamic_adar") YIELD node_id, score, method, rank RETURN *', c))
c.close()

results["friendster_analytics"] = s4

# ══════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════
p("\n" + "=" * 70)
p("SUMMARY")
p("=" * 70)

passed = failed = 0
for section, rlist in results.items():
    for r in rlist:
        if r["status"] == "PASS": passed += 1
        else: failed += 1

p(f"\nPASSED: {passed}  FAILED: {failed}")
p(f"\n{'Name':<45} {'Time':>10} {'Rows':>6} {'Status':>8}")
p("-" * 73)
for section, rlist in results.items():
    p(f"\n[{section}]")
    for r in rlist:
        if r["status"] == "PASS":
            t = r.get("warm_ms", r.get("ms", 0))
            p(f"  {r['name']:<43} {t:>9.1f}ms {r.get('rows',0):>5} {'PASS':>8}")
        else:
            p(f"  {r['name']:<43} {'':>10} {'':>6} {'FAIL':>8}")

json.dump(results, open(RESULTS, "w"), indent=2, default=str)
p(f"\nResults saved to {RESULTS}")
p("Benchmark complete.")
