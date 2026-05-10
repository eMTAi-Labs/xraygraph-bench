#!/usr/bin/env python3
"""Full benchmark suite v4 for 216.106.185.187 — undirected Friendster 3.6B edges.

Compares against baselines and flags regressions.
Binary: check md5 at runtime
"""
import time, json, sys

sys.stdout = open("/tmp/bench_187_v4.log", "w", buffering=1)

from xgdb_connect.protocol import XrayProtocolClient

ISSUE_FILE = "/tmp/bench-issues.txt"
issue_count = 16

# Baselines: name -> (expected_ms, source, tolerance_multiplier)
# tolerance_multiplier: flag if actual > expected * multiplier
BASELINES = {
    "Health Report":        (200,      ".171 Apr21", 5),
    "Graph Stats":          (55000,    ".171 Apr21", 3),
    "Connected Components": (40000,    ".171 Apr21 x2", 4),
    "PageRank 5iter":       (350000,   ".171 Apr21 x2", 3),
    "Triangle Count":       (250000,   ".171 Apr21 x8", 3),
    "Betweenness 100":      (40000,    ".187 Apr24 x2", 5),
    "K-Core":               (215000,   ".171 Apr21 x2", 3),
    "Community 3iter":      (10000,    ".171 Apr21", 10),
    "HITS 3iter":           (270000,   ".171 Apr21 x2", 3),
    "Shortest Path":        (500,      ".171 Apr21", 5),
    "Find Path Budgeted":   (100,      ".171 Apr21", 5),
    "Common Neighbors":     (10,       ".171 Apr21", 10),
    "Jaccard Similarity":   (10,       ".171 Apr21", 10),
    "Link Prediction":      (48000,    ".171 Apr21 x2", 3),
    "PersonalizedPR 5iter": (500000,   ".171 Apr21", 3),
}

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

def check_regression(name, actual_ms):
    if name in BASELINES:
        expected, source, mult = BASELINES[name]
        if actual_ms > expected * mult:
            ratio = actual_ms / expected
            msg = f"REGRESSION: {name} took {actual_ms:.0f}ms, expected ~{expected}ms (from {source}), {ratio:.1f}x slower"
            print(f"  *** {msg} ***", flush=True)
            log_issue(f"Regression: {name}", msg, f"actual={actual_ms:.0f}ms expected={expected}ms ratio={ratio:.1f}x")
            return False
        elif actual_ms > expected * 2:
            print(f"  WARNING: {name} {actual_ms:.0f}ms vs expected ~{expected}ms ({actual_ms/expected:.1f}x)", flush=True)
    return True

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
        check_regression(name, warm)
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
        check_regression(name, ms)
        return {"name": name, "ms": round(ms,1), "rows": len(rows), "status": "PASS"}
    except Exception as e:
        err = str(e)[:200]
        print(f"  {name:<35} ERROR: {err}", flush=True)
        return {"name": name, "error": err, "status": "FAIL"}

# Connect
print("=" * 70, flush=True)
print("FULL BENCHMARK v4 — 216.106.185.187", flush=True)
print("UNDIRECTED FRIENDSTER CSR (3.6B edges)", flush=True)
print(f"Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}", flush=True)
print("=" * 70, flush=True)

c = XrayProtocolClient(host="127.0.0.1", port=7689, auth_token="bench:Bench2026!xray", database="bench", read_timeout=3600)
print(f"\nConnected: {c.connected}", flush=True)

results = {}

# ── SECTION 1: Health ───────────────────────────────────────────────
print("\n--- SECTION 1: Health ---", flush=True)
s1 = []
s1.append(run_analytics("Health Report", 'CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *', c))
s1.append(run_analytics("Graph Stats", 'CALL xray.graph_stats("") YIELD metric, value RETURN *', c))
s1.append(run_analytics("Degree Distribution", 'CALL xray.degree_distribution("") YIELD degree, count, cumulative_pct RETURN * LIMIT 20', c))
results["health"] = s1

# Find hub vertex via quick PageRank check
print("\n--- Finding hub vertex ---", flush=True)
try:
    cols, rows = c.execute('CALL xray.pagerank(1, 0.85, "") YIELD node_id, rank, time_ms RETURN node_id, rank ORDER BY rank DESC LIMIT 1')
    hub_vertex = int(rows[0][0]) if rows else 13594
    print(f"  Hub vertex: {hub_vertex}", flush=True)
except:
    hub_vertex = 13594
    print(f"  Using default vertex: {hub_vertex}", flush=True)

# ── SECTION 2: Traversal ───────────────────────────────────────────
print("\n--- SECTION 2: Traversal ---", flush=True)
s2 = []
s2.append(run_analytics(f"Frontier 5-hop v={hub_vertex}",
    f'CALL xray.frontier_profile({hub_vertex}, 5, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *', c))
s2.append(run_analytics(f"Frontier 10-hop v={hub_vertex}",
    f'CALL xray.frontier_profile({hub_vertex}, 10, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *', c))
# Find a second valid vertex from pagerank for pair queries
hub2 = hub_vertex  # fallback
try:
    cols, pr_rows = c.execute('CALL xray.pagerank(1, 0.85, "") YIELD node_id, rank RETURN node_id, rank ORDER BY rank DESC LIMIT 5')
    pr_ids = [int(r[0]) for r in pr_rows]
    if len(pr_ids) >= 2:
        hub2 = pr_ids[1]
    print(f"  Test vertex pair: {hub_vertex}, {hub2}", flush=True)
except:
    print(f"  Using single hub vertex: {hub_vertex}", flush=True)

s2.append(run_bench("Shortest Path",
    f'CALL xray.shortest_path({hub_vertex}, {hub2}, "") YIELD node_id, distance, path_index, time_ms RETURN *', c))
s2.append(run_bench("Find Path Budgeted",
    f'CALL xray.find_path_budgeted({hub_vertex}, {hub2}, 10.0, "") YIELD path_nodes, total_cost, hops, explored_nodes RETURN *', c))
s2.append(run_bench("Common Neighbors",
    f'CALL xray.common_neighbors({hub_vertex}, {hub2}) YIELD neighbor_id, count RETURN *', c))
s2.append(run_bench("Jaccard Similarity",
    f'CALL xray.jaccard_similarity({hub_vertex}, {hub2}) YIELD jaccard, overlap, common, total_union, degree_a, degree_b RETURN *', c))
s2.append(run_analytics("Link Prediction",
    f'CALL xray.link_prediction({hub_vertex}, 10, "adamic_adar") YIELD node_id, score, method, rank RETURN *', c))
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
    'CALL xray.kcore("") YIELD core_number, time_ms RETURN core_number, time_ms ORDER BY core_number DESC LIMIT 5', c))
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
s5.append(run_analytics("PersonalizedPR 5iter",
    f'CALL xray.personalized_pagerank({hub_vertex}, 0.85, 5, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5', c))
s5.append(run_analytics("Clustering Coefficient",
    'CALL xray.clustering_coefficient("") YIELD node_id, coefficient, triangles RETURN * ORDER BY coefficient DESC LIMIT 5', c))
results["vertex_level"] = s5

c.close()

# ── SUMMARY ─────────────────────────────────────────────────────────
print("\n" + "=" * 70, flush=True)
print("SUMMARY", flush=True)
print("=" * 70, flush=True)

passed = failed = regressions = 0
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

json.dump(results, open("/tmp/bench_187_v4_results.json", "w"), indent=2, default=str)
print(f"\nResults saved to /tmp/bench_187_v4_results.json", flush=True)
print("Benchmark complete.", flush=True)
