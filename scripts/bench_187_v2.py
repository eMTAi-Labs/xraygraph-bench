#!/usr/bin/env python3
"""Full benchmark suite v2 for 216.106.185.187 — native xgdb_connect 1.2.0."""
import time, json, sys

sys.stdout = open("/tmp/bench_187_v2.log", "w", buffering=1)

from xgdb_connect.protocol import XrayProtocolClient

ISSUE_FILE = "/tmp/bench-issues.txt"
issue_count = 6  # Continue from existing issues

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
        print(f"  {name:<30} cold={cold:.1f}ms warm={warm:.1f}ms rows={len(rows)}", flush=True)
        if rows: print(f"    {rows[0]}", flush=True)
        return {"name": name, "cold_ms": round(cold,1), "warm_ms": round(warm,1), "rows": len(rows), "status": "PASS"}
    except Exception as e:
        err = str(e)[:200]
        print(f"  {name:<30} ERROR: {err}", flush=True)
        return {"name": name, "error": err, "status": "FAIL"}

def run_analytics(name, query, c):
    try:
        s = time.perf_counter()
        cols, rows = c.execute(query)
        ms = (time.perf_counter() - s) * 1000
        print(f"  {name:<30} {ms:.0f}ms, {len(rows)} rows", flush=True)
        if rows: print(f"    {rows[0]}", flush=True)
        return {"name": name, "ms": round(ms,1), "rows": len(rows), "status": "PASS"}
    except Exception as e:
        err = str(e)[:200]
        print(f"  {name:<30} ERROR: {err}", flush=True)
        return {"name": name, "error": err, "status": "FAIL"}

print("=" * 70, flush=True)
print("FULL BENCHMARK v2 — 216.106.185.187 (503GB EPYC, no GPU)", flush=True)
print(f"Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}", flush=True)
print("xgdb_connect 1.2.0 (native database= parameter)", flush=True)
print("=" * 70, flush=True)

c = XrayProtocolClient(host="127.0.0.1", port=7689, auth_token="bench:Bench2026!xray", database="bench", read_timeout=1800)
print(f"\nConnected: {c.connected}", flush=True)

results = {}

# Section 1: Health
print("\n--- SECTION 1: Health ---", flush=True)
s1 = []
s1.append(run_analytics("Health Report", 'CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *', c))
s1.append(run_analytics("Graph Stats", 'CALL xray.graph_stats("") YIELD metric, value RETURN *', c))
results["health"] = s1

# Section 2: Traversal
print("\n--- SECTION 2: Traversal ---", flush=True)
s2 = []
s2.append(run_analytics("Frontier 5-hop (13594)", 'CALL xray.frontier_profile(13594, 5, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *', c))
s2.append(run_analytics("Frontier 10-hop (13594)", 'CALL xray.frontier_profile(13594, 10, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *', c))
s2.append(run_bench("Shortest Path 13594->13600", 'CALL xray.shortest_path(13594, 13600, "") YIELD node_id, distance, path_index, time_ms RETURN *', c))
s2.append(run_bench("Find Path Budgeted", 'CALL xray.find_path_budgeted(13594, 13600, 10.0, "") YIELD path_nodes, total_cost, hops, explored_nodes RETURN *', c))
s2.append(run_bench("Common Neighbors", 'CALL xray.common_neighbors(13594, 13596) YIELD neighbor_id, count RETURN *', c))
s2.append(run_bench("Jaccard Similarity", 'CALL xray.jaccard_similarity(13594, 13596) YIELD jaccard, overlap, common, total_union, degree_a, degree_b RETURN *', c))
s2.append(run_analytics("Link Prediction", 'CALL xray.link_prediction(13594, 10, "adamic_adar") YIELD node_id, score, method, rank RETURN *', c))
results["traversal"] = s2

# Section 3: Global Analytics
print("\n--- SECTION 3: Global Analytics ---", flush=True)
s3 = []
s3.append(run_analytics("Connected Components", 'CALL xray.connected_components("") YIELD component_size, num_components, time_ms RETURN * ORDER BY component_size DESC LIMIT 5', c))
s3.append(run_analytics("PageRank 5iter", 'CALL xray.pagerank(5, 0.85, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5', c))
s3.append(run_analytics("Triangle Count", 'CALL xray.triangle_count("") YIELD triangles, edges_checked, time_ms, vertices RETURN *', c))
s3.append(run_analytics("Betweenness 100", 'CALL xray.betweenness_centrality("", 100) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 5', c))
s3.append(run_analytics("K-Core", 'CALL xray.kcore("") YIELD core_number, time_ms RETURN core_number, count(*) AS cnt, time_ms ORDER BY core_number DESC LIMIT 5', c))
results["global_analytics"] = s3

# Section 4: Community
print("\n--- SECTION 4: Community ---", flush=True)
s4 = []
s4.append(run_analytics("Community 3iter", 'CALL xray.community_detection(3, "") YIELD community_size, num_communities RETURN * ORDER BY community_size DESC LIMIT 5', c))
s4.append(run_analytics("HITS 3iter", 'CALL xray.hits(3, "") YIELD node_id, hub, authority, time_ms RETURN node_id, hub, authority, time_ms ORDER BY authority DESC LIMIT 5', c))
results["community"] = s4

# Section 5: Vertex-Level
print("\n--- SECTION 5: Vertex-Level ---", flush=True)
s5 = []
s5.append(run_analytics("PersonalizedPR 5iter", 'CALL xray.personalized_pagerank(13594, 0.85, 5, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5', c))
results["vertex_level"] = s5

c.close()

# Summary
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
print(f"\n{'Name':<30} {'Time':>10} {'Rows':>6} {'Status':>8}", flush=True)
print("-" * 58, flush=True)
for section, rlist in results.items():
    print(f"\n[{section}]", flush=True)
    for r in rlist:
        if r["status"] == "PASS":
            t = r.get("warm_ms", r.get("ms", 0))
            print(f"  {r['name']:<28} {t:>9.1f}ms {r.get('rows',0):>5} {'PASS':>8}", flush=True)
        else:
            print(f"  {r['name']:<28} {'':>10} {'':>6} {'FAIL':>8}", flush=True)

json.dump(results, open("/tmp/bench_187_v2_results.json", "w"), indent=2, default=str)
print(f"\nResults saved to /tmp/bench_187_v2_results.json", flush=True)
print("Benchmark complete.", flush=True)
