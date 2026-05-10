#!/usr/bin/env python3
"""Full regression test — ALL procedures on Friendster CSR. No skips."""
import time, json, sys
sys.stdout = open("/opt/xraybench-results/FULL_REGRESSION.log", "w", buffering=1)

from xgdb_connect.protocol import XrayProtocolClient
c = XrayProtocolClient("127.0.0.1", 7689, auth_token="bench:Bench2026!xray", read_timeout=1800)
print(f"Connected: {c.connected}", flush=True)
print(f"Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}", flush=True)

results = []

ALL = [
    ("Health Report", 'CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *'),
    ("Graph Stats", 'CALL xray.graph_stats("") YIELD metric, value RETURN *'),
    ("Degree Distribution", 'CALL xray.degree_distribution("") YIELD degree, count, cumulative_pct RETURN * LIMIT 10'),
    ("Frontier 5-hop", 'CALL xray.frontier_profile(13594, 5, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *'),
    ("Frontier 10-hop", 'CALL xray.frontier_profile(13594, 10, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *'),
    ("Shortest Path", 'CALL xray.shortest_path(13594, 13600, "") YIELD node_id, distance, path_index, time_ms RETURN *'),
    ("Find Path Budgeted", 'CALL xray.find_path_budgeted(13594, 13600, 10.0, "") YIELD path_nodes, total_cost, hops, explored_nodes RETURN *'),
    ("TopK Reachable", 'CALL xray.topk_reachable(13594, 10, 3, "degree", "OUTGOING") YIELD node_id, name, score, distance RETURN *'),
    ("Common Neighbors", 'CALL xray.common_neighbors(13594, 13596) YIELD neighbor_id, count RETURN *'),
    ("Jaccard Similarity", 'CALL xray.jaccard_similarity(13594, 13596) YIELD jaccard, overlap, common, total_union, degree_a, degree_b RETURN *'),
    ("Link Prediction", 'CALL xray.link_prediction(13594, 10, "adamic_adar") YIELD node_id, score, method, rank RETURN *'),
    ("Similarity", 'CALL xray.similarity(13594, "jaccard", 10, "") YIELD node_id, name, score, common_count, method RETURN *'),
    ("Connected Components", 'CALL xray.connected_components("") YIELD component_size, num_components, time_ms RETURN * ORDER BY component_size DESC LIMIT 5'),
    ("PageRank 5iter", 'CALL xray.pagerank(5, 0.85, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5'),
    ("Triangle Count", 'CALL xray.triangle_count("") YIELD triangles, edges_checked, time_ms, vertices RETURN *'),
    ("Betweenness 100", 'CALL xray.betweenness_centrality("", 100) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 5'),
    ("K-Core", 'CALL xray.kcore("") YIELD core_number, time_ms RETURN core_number, count(*) AS cnt, time_ms ORDER BY core_number DESC LIMIT 5'),
    ("Community 3iter", 'CALL xray.community_detection(3, "") YIELD community_size, num_communities RETURN * ORDER BY community_size DESC LIMIT 5'),
    ("HITS 3iter", 'CALL xray.hits(3, "") YIELD node_id, hub, authority, time_ms RETURN node_id, hub, authority, time_ms ORDER BY authority DESC LIMIT 5'),
    ("PersonalizedPR", 'CALL xray.personalized_pagerank(13594, 0.85, 5, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5'),
    ("Clustering Coefficient", 'CALL xray.clustering_coefficient("") YIELD node_id, coefficient, triangles RETURN * ORDER BY coefficient DESC LIMIT 5'),
]

print(f"\n{'='*70}", flush=True)
print(f"FULL REGRESSION — Friendster (65.6M vertices, 1.8B edges)", flush=True)
print(f"{'='*70}\n", flush=True)

for name, query in ALL:
    print(f"{name}...", end=" ", flush=True)
    try:
        s = time.perf_counter()
        cols, rows = c.execute(query)
        ms = (time.perf_counter() - s) * 1000
        print(f"{ms:.0f}ms, {len(rows)} rows", flush=True)
        if rows:
            print(f"  {rows[0]}", flush=True)
        results.append({"name": name, "ms": round(ms, 1), "rows": len(rows)})
    except Exception as e:
        print(f"ERROR: {str(e)[:120]}", flush=True)
        results.append({"name": name, "error": str(e)[:300]})

c.close()

# Summary
print(f"\n{'='*70}", flush=True)
print("REGRESSION SUMMARY", flush=True)
print(f"{'='*70}", flush=True)
passed = sum(1 for r in results if "error" not in r and r.get("rows", 0) > 0)
failed = sum(1 for r in results if "error" in r)
empty = sum(1 for r in results if "error" not in r and r.get("rows", 0) == 0)
print(f"PASSED: {passed}/{len(results)}", flush=True)
print(f"FAILED: {failed}/{len(results)}", flush=True)
print(f"EMPTY:  {empty}/{len(results)}", flush=True)
print(f"\n{'Name':<25} {'Time':>10} {'Rows':>6}", flush=True)
print("-" * 45, flush=True)
for r in results:
    if "error" in r:
        print(f"{r['name']:<25} {'ERROR':>10} {'':>6}", flush=True)
    else:
        print(f"{r['name']:<25} {r['ms']:>9.0f}ms {r['rows']:>5}", flush=True)

json.dump(results, open("/opt/xraybench-results/FULL_REGRESSION.json", "w"), indent=2, default=str)
print(f"\nResults saved to FULL_REGRESSION.json", flush=True)
