#!/usr/bin/env python3
"""Parts 3-4: CSR BFS + Friendster analytics."""
import time, json, sys
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

LOG = "/tmp/remaining_bench.log"
log = open(LOG, "w", buffering=1)

def p(msg):
    log.write(msg + "\n")
    log.flush()

def fresh():
    return XrayProtocolClient(host="127.0.0.1", port=7689, auth_token="bench:Bench2026!xray", database="bench", read_timeout=7200)

def run_once(name, query, c):
    try:
        s = time.perf_counter()
        cols, rows = c.execute(query)
        ms = (time.perf_counter() - s) * 1000
        p(f"  {name:<45} {ms:.0f}ms  rows={len(rows)}")
        if rows: p(f"    {rows[0]}")
        return {"name": name, "ms": round(ms,1), "rows": len(rows), "status": "PASS"}
    except Exception as e:
        ms = (time.perf_counter() - s) * 1000
        p(f"  {name:<45} ERROR ({ms:.0f}ms): {str(e)[:150]}")
        return {"name": name, "error": str(e)[:200], "status": "FAIL"}

def run_warm(name, query, c, warmup=3):
    try:
        s = time.perf_counter()
        cols, rows = c.execute(query)
        cold = (time.perf_counter() - s) * 1000
        times = []
        for _ in range(warmup):
            s = time.perf_counter()
            c.execute(query)
            times.append((time.perf_counter() - s) * 1000)
        warm = sum(times)/len(times)
        p(f"  {name:<45} cold={cold:.1f}ms warm={warm:.1f}ms rows={len(rows)}")
        if rows: p(f"    {rows[0]}")
        return {"name": name, "cold_ms": round(cold,1), "warm_ms": round(warm,1), "rows": len(rows), "status": "PASS"}
    except Exception as e:
        p(f"  {name:<45} ERROR: {str(e)[:150]}")
        return {"name": name, "error": str(e)[:200], "status": "FAIL"}

results = {}

# PART 3: CSR BFS
p("=" * 70)
p("PART 3: CSR BFS on Friendster (65.6M vertices, 3.6B edges)")
p("=" * 70)

c = fresh()
cols, rows = c.execute('CALL xray.pagerank(5, 0.85, "") YIELD node_id, rank RETURN node_id, rank ORDER BY rank DESC LIMIT 5')
hubs = [int(r[0]) for r in rows]
hub = hubs[0]
hub2 = hubs[1] if len(hubs) > 1 else hub + 1
p(f"\n  Hub vertex: {hub}")
c.close()

s3 = []
for hops in range(1, 11):
    c = fresh()
    s3.append(run_once(f"CSR BFS {hops}-hop (v={hub})",
        f'CALL xray.frontier_profile({hub}, {hops}, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *', c))
    c.close()
results["bfs_hops_csr"] = s3

# PART 4: Friendster Analytics
p("\n" + "=" * 70)
p("PART 4: FRIENDSTER ANALYTICS (65.6M vertices, 3.6B edges)")
p("=" * 70)

s4 = []
c = fresh()
s4.append(run_once("Health Report", 'CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *', c))
s4.append(run_once("Graph Stats", 'CALL xray.graph_stats("") YIELD metric, value RETURN *', c))
s4.append(run_once("Degree Distribution", 'CALL xray.degree_distribution("") YIELD degree, count, cumulative_pct RETURN * LIMIT 20', c))
c.close()

for name, q in [
    ("Connected Components", 'CALL xray.connected_components("") YIELD component_size, num_components, time_ms RETURN component_size, num_components, time_ms ORDER BY component_size DESC LIMIT 5'),
    ("PageRank 5iter", 'CALL xray.pagerank(5, 0.85, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5'),
    ("Triangle Count", 'CALL xray.triangle_count("") YIELD triangles, edges_checked, time_ms, vertices RETURN *'),
    ("K-Core", 'CALL xray.kcore("") YIELD core_number, time_ms RETURN core_number, time_ms ORDER BY core_number DESC LIMIT 5'),
    ("Community 3iter", 'CALL xray.community_detection(3, "") YIELD community_size, num_communities RETURN community_size, num_communities ORDER BY community_size DESC LIMIT 5'),
    ("HITS 3iter", 'CALL xray.hits(3, "") YIELD node_id, hub, authority, time_ms RETURN node_id, hub, authority, time_ms ORDER BY authority DESC LIMIT 5'),
]:
    c = fresh()
    s4.append(run_once(name, q, c))
    c.close()

c = fresh()
s4.append(run_once("PersonalizedPR 5iter",
    f'CALL xray.personalized_pagerank({hub}, 0.85, 5, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5', c))
s4.append(run_once("Clustering Coefficient",
    'CALL xray.clustering_coefficient("") YIELD node_id, coefficient, triangles RETURN node_id, coefficient, triangles ORDER BY coefficient DESC LIMIT 5', c))
c.close()

# BC variants
c = fresh()
s4.append(run_once("BC Pair-Sampled COLD (e=0.05, b=1)",
    'CALL xray.betweenness_pair_sampled(0.05, 0.05, "", 1, 1) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10', c))
s4.append(run_once("BC Pair-Sampled WARM (e=0.05, b=1)",
    'CALL xray.betweenness_pair_sampled(0.05, 0.05, "", 1, 1) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10', c))
s4.append(run_once("BC Pair-Sampled WARM (e=0.10, b=1)",
    'CALL xray.betweenness_pair_sampled(0.10, 0.05, "", 1, 1) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10', c))
c.close()

# Traversal
c = fresh()
s4.append(run_warm("Shortest Path",
    f'CALL xray.shortest_path({hub}, {hub2}, "") YIELD node_id, distance, path_index, time_ms RETURN *', c))
s4.append(run_warm("Jaccard Similarity",
    f'CALL xray.jaccard_similarity({hub}, {hub2}) YIELD jaccard, overlap, common, total_union, degree_a, degree_b RETURN *', c))
s4.append(run_once("Link Prediction",
    f'CALL xray.link_prediction({hub}, 10, "adamic_adar") YIELD node_id, score, method, rank RETURN *', c))
c.close()

results["friendster_analytics"] = s4

# Summary
p("\n" + "=" * 70)
p("SUMMARY")
p("=" * 70)
passed = failed = 0
for sec, rlist in results.items():
    for r in rlist:
        if r["status"] == "PASS": passed += 1
        else: failed += 1
p(f"\nPASSED: {passed}  FAILED: {failed}")

json.dump(results, open("/tmp/remaining_bench_results.json", "w"), indent=2, default=str)
p("Benchmark complete.")
