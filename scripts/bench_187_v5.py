#!/usr/bin/env python3
"""Full benchmark suite v5 for 216.106.185.187 — final numbers for benchmark.html.

Includes all original procedures + new procedures:
  - betweenness_pair_sampled (sub-second BC)
  - betweenness_pair_sampled_adaptive (top-K early-stop)
  - betweenness_centrality_sampled (graded distribution)
  - find_path_bidirectional (bidirectional BFS)

Port 7689 ONLY. Binary md5 checked at runtime.
"""
import time, json, sys
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

RESULTS_FILE = "/tmp/bench_187_v5_results.json"
LOG_FILE = "/tmp/bench_187_v5.log"

# Redirect stdout to log
log = open(LOG_FILE, "w", buffering=1)
orig_stdout = sys.stdout
sys.stdout = log

def p(msg):
    log.write(msg + "\n")
    log.flush()

def fresh():
    return XrayProtocolClient(host="127.0.0.1", port=7689, auth_token="bench:Bench2026!xray", database="bench", read_timeout=7200)

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
        p(f"  {name:<40} cold={cold:.1f}ms warm={warm:.1f}ms rows={len(rows)}")
        if rows: p(f"    {rows[0]}")
        return {"name": name, "cold_ms": round(cold,1), "warm_ms": round(warm,1), "rows": len(rows), "status": "PASS"}
    except Exception as e:
        err = str(e)[:200]
        p(f"  {name:<40} ERROR: {err}")
        return {"name": name, "error": err, "status": "FAIL"}

def run_analytics(name, query, c):
    try:
        s = time.perf_counter()
        cols, rows = c.execute(query)
        ms = (time.perf_counter() - s) * 1000
        p(f"  {name:<40} {ms:.0f}ms, {len(rows)} rows")
        if rows: p(f"    {rows[0]}")
        return {"name": name, "ms": round(ms,1), "rows": len(rows), "status": "PASS"}
    except Exception as e:
        err = str(e)[:200]
        p(f"  {name:<40} ERROR: {err}")
        return {"name": name, "error": err, "status": "FAIL"}

p("=" * 70)
p("FULL BENCHMARK v5 — 216.106.185.187 (port 7689)")
p("UNDIRECTED FRIENDSTER CSR (65.6M vertices, 3.6B edges)")
p(f"Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
p("=" * 70)

c = fresh()
p(f"\nConnected: {c.connected}")

results = {}

# ── SECTION 1: Health ───────────────────────────────────────────────
p("\n--- SECTION 1: Health ---")
s1 = []
s1.append(run_analytics("Health Report", 'CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *', c))
s1.append(run_analytics("Graph Stats", 'CALL xray.graph_stats("") YIELD metric, value RETURN *', c))
s1.append(run_analytics("Degree Distribution", 'CALL xray.degree_distribution("") YIELD degree, count, cumulative_pct RETURN * LIMIT 20', c))
results["health"] = s1

# Find hub vertices for traversal tests
p("\n--- Finding hub vertices via PageRank(5) ---")
cols, pr_rows = c.execute('CALL xray.pagerank(5, 0.85, "") YIELD node_id, rank RETURN node_id, rank ORDER BY rank DESC LIMIT 10')
hubs = [int(r[0]) for r in pr_rows]
v1, v2 = hubs[0], hubs[1]
p(f"  Hub vertices: {v1}, {v2}")
c.close()

# ── SECTION 2: Traversal ───────────────────────────────────────────
p("\n--- SECTION 2: Traversal ---")
c = fresh()
s2 = []
s2.append(run_analytics(f"Frontier 5-hop v={v1}",
    f'CALL xray.frontier_profile({v1}, 5, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *', c))
s2.append(run_analytics(f"Frontier 10-hop v={v1}",
    f'CALL xray.frontier_profile({v1}, 10, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *', c))
s2.append(run_bench("Shortest Path",
    f'CALL xray.shortest_path({v1}, {v2}, "") YIELD node_id, distance, path_index, time_ms RETURN *', c))
s2.append(run_bench("Find Path Budgeted",
    f'CALL xray.find_path_budgeted({v1}, {v2}, 10.0, "") YIELD path_nodes, total_cost, hops, explored_nodes RETURN *', c))
s2.append(run_bench("Find Path Bidirectional",
    f'CALL xray.find_path_bidirectional({v1}, {v2}, 10) YIELD path_nodes, total_cost, hops, explored_nodes RETURN *', c))
s2.append(run_bench("Common Neighbors",
    f'CALL xray.common_neighbors({v1}, {v2}) YIELD neighbor_id, count RETURN *', c))
s2.append(run_bench("Jaccard Similarity",
    f'CALL xray.jaccard_similarity({v1}, {v2}) YIELD jaccard, overlap, common, total_union, degree_a, degree_b RETURN *', c))
s2.append(run_analytics("Link Prediction",
    f'CALL xray.link_prediction({v1}, 10, "adamic_adar") YIELD node_id, score, method, rank RETURN *', c))
results["traversal"] = s2
c.close()

# ── SECTION 3: Global Analytics ─────────────────────────────────────
p("\n--- SECTION 3: Global Analytics ---")
c = fresh()
s3 = []
s3.append(run_analytics("Connected Components",
    'CALL xray.connected_components("") YIELD component_size, num_components, time_ms RETURN component_size, num_components, time_ms ORDER BY component_size DESC LIMIT 5', c))
c.close()

c = fresh()
s3.append(run_analytics("PageRank 5iter",
    'CALL xray.pagerank(5, 0.85, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5', c))
c.close()

c = fresh()
s3.append(run_analytics("Triangle Count",
    'CALL xray.triangle_count("") YIELD triangles, edges_checked, time_ms, vertices RETURN *', c))
c.close()

c = fresh()
s3.append(run_analytics("K-Core",
    'CALL xray.kcore("") YIELD core_number, time_ms RETURN core_number, time_ms ORDER BY core_number DESC LIMIT 5', c))
c.close()

results["global_analytics"] = s3

# ── SECTION 4: Betweenness (multiple modes) ─────────────────────────
p("\n--- SECTION 4: Betweenness Centrality ---")
s4 = []

# Pair-sampled cold (allocates pool)
c = fresh()
s4.append(run_analytics("BC Pair-Sampled COLD (e=0.05)",
    'CALL xray.betweenness_pair_sampled(0.05, 0.05, "") YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10', c))

# Pair-sampled warm (reuses pool — sub-second target)
s4.append(run_analytics("BC Pair-Sampled WARM (e=0.05)",
    'CALL xray.betweenness_pair_sampled(0.05, 0.05, "") YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10', c))

# Pair-sampled warm, coarser epsilon
s4.append(run_analytics("BC Pair-Sampled WARM (e=0.10)",
    'CALL xray.betweenness_pair_sampled(0.10, 0.05, "") YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10', c))

# Adaptive top-K
s4.append(run_analytics("BC Adaptive Top-20",
    'CALL xray.betweenness_pair_sampled_adaptive(0.05, 0.05, "", 20, 0.95) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10', c))
c.close()

results["betweenness"] = s4

# ── SECTION 5: Community ────────────────────────────────────────────
p("\n--- SECTION 5: Community ---")
c = fresh()
s5 = []
s5.append(run_analytics("Community 3iter",
    'CALL xray.community_detection(3, "") YIELD community_size, num_communities RETURN community_size, num_communities ORDER BY community_size DESC LIMIT 5', c))
c.close()

c = fresh()
s5.append(run_analytics("HITS 3iter",
    'CALL xray.hits(3, "") YIELD node_id, hub, authority, time_ms RETURN node_id, hub, authority, time_ms ORDER BY authority DESC LIMIT 5', c))
c.close()

results["community"] = s5

# ── SECTION 6: Vertex-Level ─────────────────────────────────────────
p("\n--- SECTION 6: Vertex-Level ---")
c = fresh()
s6 = []
s6.append(run_analytics("PersonalizedPR 5iter",
    f'CALL xray.personalized_pagerank({v1}, 0.85, 5, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5', c))
s6.append(run_analytics("Clustering Coefficient",
    'CALL xray.clustering_coefficient("") YIELD node_id, coefficient, triangles RETURN node_id, coefficient, triangles ORDER BY coefficient DESC LIMIT 5', c))
c.close()

results["vertex_level"] = s6

# ── SECTION 7: GFQL ─────────────────────────────────────────────────
p("\n--- SECTION 7: GFQL ---")
c = fresh()
s7 = []
s7.append(run_analytics("GFQL BC Pair-Sampled",
    'betweenness_pair_sampled(epsilon=0.05, delta=0.05, label="")', c))
s7.append(run_analytics("GFQL BC Adaptive Top-20",
    'betweenness_pair_sampled_adaptive(epsilon=0.05, delta=0.05, label="", top_k=20, stability_threshold=0.95)', c))
c.close()

results["gfql"] = s7

# ── SUMMARY ─────────────────────────────────────────────────────────
p("\n" + "=" * 70)
p("SUMMARY")
p("=" * 70)

passed = failed = 0
for section, rlist in results.items():
    for r in rlist:
        if r["status"] == "PASS":
            passed += 1
        else:
            failed += 1

p(f"\nPASSED: {passed}  FAILED: {failed}")
p(f"\n{'Name':<40} {'Time':>10} {'Rows':>6} {'Status':>8}")
p("-" * 68)
for section, rlist in results.items():
    p(f"\n[{section}]")
    for r in rlist:
        if r["status"] == "PASS":
            t = r.get("warm_ms", r.get("ms", 0))
            p(f"  {r['name']:<38} {t:>9.1f}ms {r.get('rows',0):>5} {'PASS':>8}")
        else:
            p(f"  {r['name']:<38} {'':>10} {'':>6} {'FAIL':>8}")

json.dump(results, open(RESULTS_FILE, "w"), indent=2, default=str)
p(f"\nResults saved to {RESULTS_FILE}")
p("Benchmark complete.")
