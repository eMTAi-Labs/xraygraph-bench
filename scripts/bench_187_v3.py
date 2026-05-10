#!/usr/bin/env python3
"""Full benchmark suite v3 for 216.106.185.187 — undirected Friendster CSR.

Post-rebuild expectations (commit 590f32803, undirected mirroring):
  - 65,608,366 vertices, 3,612,134,270 edges (doubled)
  - connected_components = 1 (single WCC)
  - avg_degree ~= 55.1
  - triangle_count ~= 4,173,724,142

Note: bench user tenant_id="default" -> CSR at /neo4j/csr_default/
"""
import time, json, sys

sys.stdout = open("/tmp/bench_187_v3.log", "w", buffering=1)

from xgdb_connect.protocol import XrayProtocolClient

ISSUE_FILE = "/tmp/bench-issues.txt"
issue_count = 9  # Continue from existing issues

def log_issue(title, error, details=""):
    global issue_count
    issue_count += 1
    with open(ISSUE_FILE, "a") as f:
        f.write(f"\n### Start Issue #{issue_count} ###\n")
        f.write(f"TITLE: {title}\n")
        f.write(f"ERROR: {error}\n")
        if details: f.write(f"DETAILS: {details}\n")
        f.write(f"TIMESTAMP: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n")
        f.write(f"### End Issue #{issue_count} ###\n")
    print(f"  *** ISSUE #{issue_count} LOGGED: {title} ***", flush=True)

def run_bench(name, query, c, warmup=3):
    try:
        s = time.perf_counter()
        cols, rows = c.execute(query)
        cold = (time.perf_counter() - s) * 1000
        times = []
        for _ in range(warmup):
            s = time.perf_counter()
            c.execute(query)
            times.append((time.perf_counter() - s) * 1000)
        warm = sum(times) / len(times) if times else cold
        print(f"  {name:<35} cold={cold:.1f}ms warm={warm:.1f}ms rows={len(rows)}", flush=True)
        if rows: print(f"    {rows[0]}", flush=True)
        return {"name": name, "cold_ms": round(cold,1), "warm_ms": round(warm,1), "rows": len(rows), "status": "PASS"}
    except Exception as e:
        err = str(e)[:200]
        print(f"  {name:<35} ERROR: {err}", flush=True)
        return {"name": name, "error": err, "status": "FAIL"}

def run_analytics(name, query, c):
    try:
        s = time.perf_counter()
        cols, rows = c.execute(query)
        ms = (time.perf_counter() - s) * 1000
        print(f"  {name:<35} {ms:.0f}ms, {len(rows)} rows", flush=True)
        if rows: print(f"    {rows[0]}", flush=True)
        return {"name": name, "ms": round(ms,1), "rows": len(rows), "status": "PASS"}
    except Exception as e:
        err = str(e)[:200]
        print(f"  {name:<35} ERROR: {err}", flush=True)
        return {"name": name, "error": err, "status": "FAIL"}

def verify(name, actual, expected, tolerance=0.1):
    """Verify a value against expected with tolerance. Log issue if wrong."""
    if expected is None:
        return True
    if isinstance(expected, (int, float)):
        if abs(actual - expected) / max(expected, 1) > tolerance:
            msg = f"{name}: got {actual}, expected {expected} (>{tolerance*100}% off)"
            print(f"  *** VERIFY FAIL: {msg} ***", flush=True)
            log_issue(f"Verification failed: {name}", msg)
            return False
    print(f"  VERIFY OK: {name} = {actual} (expected ~{expected})", flush=True)
    return True

print("=" * 70, flush=True)
print("FULL BENCHMARK v3 — 216.106.185.187 (503GB EPYC, no GPU)", flush=True)
print("UNDIRECTED FRIENDSTER CSR (3.6B edges, commit 590f32803)", flush=True)
print(f"Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}", flush=True)
print("xgdb_connect 1.2.0 (native database= parameter)", flush=True)
print("=" * 70, flush=True)

c = XrayProtocolClient(host="127.0.0.1", port=7689, auth_token="bench:Bench2026!xray", database="bench", read_timeout=3600)
print(f"\nConnected: {c.connected}", flush=True)

results = {}

# ── SECTION 1: Health + Verification ────────────────────────────────
print("\n--- SECTION 1: Health + Verification ---", flush=True)
s1 = []
r = run_analytics("Health Report", 'CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *', c)
s1.append(r)

r = run_analytics("Graph Stats", 'CALL xray.graph_stats("") YIELD metric, value RETURN *', c)
s1.append(r)

r = run_analytics("Degree Distribution", 'CALL xray.degree_distribution("") YIELD degree, count, cumulative_pct RETURN * LIMIT 20', c)
s1.append(r)

results["health"] = s1

# ── SECTION 2: Traversal ────────────────────────────────────────────
# Use vertex 100000 (confirmed to have neighbors on .187's id_map)
# Also test 13594 to see if undirected rebuild fixed it
print("\n--- SECTION 2: Traversal ---", flush=True)
s2 = []

# Test both vertices to verify undirected mirroring
s2.append(run_analytics("Frontier 5-hop v=13594 OUT",
    'CALL xray.frontier_profile(13594, 5, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *', c))
s2.append(run_analytics("Frontier 5-hop v=100000 OUT",
    'CALL xray.frontier_profile(100000, 5, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *', c))
s2.append(run_analytics("Frontier 10-hop v=100000 OUT",
    'CALL xray.frontier_profile(100000, 10, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *', c))

s2.append(run_bench("Shortest Path 13594->13600",
    'CALL xray.shortest_path(13594, 13600, "") YIELD node_id, distance, path_index, time_ms RETURN *', c))
s2.append(run_bench("Shortest Path 100000->100001",
    'CALL xray.shortest_path(100000, 100001, "") YIELD node_id, distance, path_index, time_ms RETURN *', c))
s2.append(run_bench("Find Path Budgeted 13594->13600",
    'CALL xray.find_path_budgeted(13594, 13600, 10.0, "") YIELD path_nodes, total_cost, hops, explored_nodes RETURN *', c))

s2.append(run_bench("Common Neighbors 13594,13596",
    'CALL xray.common_neighbors(13594, 13596) YIELD neighbor_id, count RETURN *', c))
s2.append(run_bench("Jaccard Similarity 13594,13596",
    'CALL xray.jaccard_similarity(13594, 13596) YIELD jaccard, overlap, common, total_union, degree_a, degree_b RETURN *', c))
s2.append(run_analytics("Link Prediction v=13594",
    'CALL xray.link_prediction(13594, 10, "adamic_adar") YIELD node_id, score, method, rank RETURN *', c))

results["traversal"] = s2

# ── SECTION 3: Global Analytics ─────────────────────────────────────
print("\n--- SECTION 3: Global Analytics ---", flush=True)
s3 = []
s3.append(run_analytics("Connected Components",
    'CALL xray.connected_components("") YIELD component_size, num_components, time_ms RETURN * ORDER BY component_size DESC LIMIT 5', c))
s3.append(run_analytics("PageRank 5iter",
    'CALL xray.pagerank(5, 0.85, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5', c))
s3.append(run_analytics("Triangle Count",
    'CALL xray.triangle_count("") YIELD triangles, edges_checked, time_ms, vertices RETURN *', c))
s3.append(run_analytics("Betweenness 100",
    'CALL xray.betweenness_centrality("", 100) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 5', c))
s3.append(run_analytics("K-Core",
    'CALL xray.kcore("") YIELD core_number, time_ms RETURN core_number, count(*) AS cnt, time_ms ORDER BY core_number DESC LIMIT 5', c))

results["global_analytics"] = s3

# ── SECTION 4: Community ────────────────────────────────────────────
print("\n--- SECTION 4: Community ---", flush=True)
s4 = []
s4.append(run_analytics("Community 3iter",
    'CALL xray.community_detection(3, "") YIELD community_size, num_communities RETURN * ORDER BY community_size DESC LIMIT 5', c))
s4.append(run_analytics("HITS 3iter",
    'CALL xray.hits(3, "") YIELD node_id, hub, authority, time_ms RETURN node_id, hub, authority, time_ms ORDER BY authority DESC LIMIT 5', c))

results["community"] = s4

# ── SECTION 5: Vertex-Level ─────────────────────────────────────────
print("\n--- SECTION 5: Vertex-Level ---", flush=True)
s5 = []
s5.append(run_analytics("PersonalizedPR 5iter v=13594",
    'CALL xray.personalized_pagerank(13594, 0.85, 5, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5', c))
s5.append(run_analytics("PersonalizedPR 5iter v=100000",
    'CALL xray.personalized_pagerank(100000, 0.85, 5, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5', c))
s5.append(run_analytics("Clustering Coefficient",
    'CALL xray.clustering_coefficient("") YIELD node_id, coefficient, triangles RETURN * ORDER BY coefficient DESC LIMIT 5', c))

results["vertex_level"] = s5

c.close()

# ── SUMMARY ─────────────────────────────────────────────────────────
print("\n" + "=" * 70, flush=True)
print("SUMMARY", flush=True)
print("=" * 70, flush=True)

passed = failed = 0
for section, rlist in results.items():
    for r in rlist:
        if r["status"] == "PASS":
            passed += 1
        else:
            failed += 1
            log_issue(f"{r['name']} failed", r.get("error","unknown"), f"Section: {section}")

print(f"\nPASSED: {passed}  FAILED: {failed}", flush=True)
print(f"\n{'Name':<35} {'Time':>10} {'Rows':>6} {'Status':>8}", flush=True)
print("-" * 63, flush=True)
for section, rlist in results.items():
    print(f"\n[{section}]", flush=True)
    for r in rlist:
        if r["status"] == "PASS":
            t = r.get("warm_ms", r.get("ms", 0))
            print(f"  {r['name']:<33} {t:>9.1f}ms {r.get('rows',0):>5} {'PASS':>8}", flush=True)
        else:
            print(f"  {r['name']:<33} {'':>10} {'':>6} {'FAIL':>8}", flush=True)

json.dump(results, open("/tmp/bench_187_v3_results.json", "w"), indent=2, default=str)
print(f"\nResults saved to /tmp/bench_187_v3_results.json", flush=True)
print("Benchmark complete.", flush=True)
