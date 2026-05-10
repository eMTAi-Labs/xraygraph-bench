#!/usr/bin/env python3
"""Test with fresh connection per query to avoid protocol desync."""
import time, sys
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

def fresh():
    return XrayProtocolClient(host="127.0.0.1", port=7689, auth_token="bench:Bench2026!xray", database="bench", read_timeout=3600)

# Find hub vertices
c = fresh()
cols, rows = c.execute('CALL xray.pagerank(1, 0.85, "") YIELD node_id, rank RETURN node_id, rank ORDER BY rank DESC LIMIT 10')
hubs = [int(r[0]) for r in rows]
print("Top PageRank vertices:", hubs[:5])
c.close()

# Check frontier for each hub
for hub in hubs[:5]:
    c = fresh()
    try:
        cols, rows = c.execute(f'CALL xray.frontier_profile({hub}, 1, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *')
        for r in rows:
            if str(r[0]) == "1":
                print(f"  v={hub}: frontier_size={r[1]}")
    except Exception as e:
        print(f"  v={hub}: {str(e)[:80]}")
    c.close()

# Fresh connection for each global analytics
tests = [
    ("Connected Components", 'CALL xray.connected_components("") YIELD component_size, num_components, time_ms RETURN component_size, num_components, time_ms ORDER BY component_size DESC LIMIT 3'),
    ("PageRank 5iter", 'CALL xray.pagerank(5, 0.85, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5'),
    ("Triangle Count", 'CALL xray.triangle_count("") YIELD triangles, edges_checked, time_ms, vertices RETURN *'),
    ("Betweenness 100", 'CALL xray.betweenness_centrality("", 100) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 5'),
    ("K-Core", 'CALL xray.kcore("") YIELD core_number, time_ms RETURN core_number, time_ms ORDER BY core_number DESC LIMIT 5'),
    ("Community 3iter", 'CALL xray.community_detection(3, "") YIELD community_size, num_communities RETURN community_size, num_communities ORDER BY community_size DESC LIMIT 5'),
    ("HITS 3iter", 'CALL xray.hits(3, "") YIELD node_id, hub, authority, time_ms RETURN node_id, hub, authority, time_ms ORDER BY authority DESC LIMIT 5'),
    ("PersonalizedPR 5iter", f'CALL xray.personalized_pagerank({hubs[0]}, 0.85, 5, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5'),
    ("Clustering Coefficient", 'CALL xray.clustering_coefficient("") YIELD node_id, coefficient, triangles RETURN node_id, coefficient, triangles ORDER BY coefficient DESC LIMIT 5'),
]

print("\n--- Global Analytics (fresh conn each) ---")
for name, q in tests:
    c = fresh()
    s = time.perf_counter()
    try:
        cols, rows = c.execute(q)
        ms = (time.perf_counter() - s) * 1000
        print(f"  {name:<30} {ms:.0f}ms, {len(rows)} rows")
        if rows: print(f"    {rows[0]}")
    except Exception as e:
        ms = (time.perf_counter() - s) * 1000
        print(f"  {name:<30} ERROR ({ms:.0f}ms): {str(e)[:120]}")
    c.close()

# Traversal with valid vertex pairs
print("\n--- Traversal (hub vertex pairs) ---")
if len(hubs) >= 2:
    v1, v2 = hubs[0], hubs[1]
    trav_tests = [
        ("Shortest Path", f'CALL xray.shortest_path({v1}, {v2}, "") YIELD node_id, distance, path_index, time_ms RETURN *'),
        ("Find Path Budgeted", f'CALL xray.find_path_budgeted({v1}, {v2}, 10.0, "") YIELD path_nodes, total_cost, hops, explored_nodes RETURN *'),
        ("Common Neighbors", f'CALL xray.common_neighbors({v1}, {v2}) YIELD neighbor_id, count RETURN *'),
        ("Jaccard Similarity", f'CALL xray.jaccard_similarity({v1}, {v2}) YIELD jaccard, overlap, common, total_union, degree_a, degree_b RETURN *'),
        ("Link Prediction", f'CALL xray.link_prediction({v1}, 10, "adamic_adar") YIELD node_id, score, method, rank RETURN *'),
    ]
    for name, q in trav_tests:
        c = fresh()
        s = time.perf_counter()
        try:
            cols, rows = c.execute(q)
            ms = (time.perf_counter() - s) * 1000
            print(f"  {name:<30} {ms:.0f}ms, {len(rows)} rows")
            if rows: print(f"    {rows[0]}")
        except Exception as e:
            ms = (time.perf_counter() - s) * 1000
            print(f"  {name:<30} ERROR ({ms:.0f}ms): {str(e)[:120]}")
        c.close()

print("\nDone")
