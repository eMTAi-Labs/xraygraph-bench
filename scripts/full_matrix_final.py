#!/usr/bin/env python3
"""Full benchmark matrix — final re-run with all fixes."""
import time, json, sys, subprocess
sys.stdout = open("/opt/xraybench-results/FULL_MATRIX_FINAL.log", "w", buffering=1)

print("=" * 70, flush=True)
print("FULL BENCHMARK MATRIX — ALL CONFIGS", flush=True)
print("=" * 70, flush=True)

# Phase 1: SF1 already loaded (skip)
print("\nPHASE 1: SF1 already loaded (3.18M nodes, 4.5M edges)", flush=True)

# Phase 2: In-Memory SF1 Benchmarks
print("\n" + "=" * 70, flush=True)
print("PHASE 2: IN-MEMORY SF1 BENCHMARKS", flush=True)
print("=" * 70, flush=True)

from xgdb_connect.protocol import XrayProtocolClient
c = XrayProtocolClient("127.0.0.1", 7689, auth_token="bench:Bench2026!xray", read_timeout=600)

cols, rows = c.execute("MATCH (n) RETURN count(n) AS cnt")
node_count = rows[0][0] if rows else 0
cols, rows = c.execute("MATCH ()-[r]->() RETURN count(r) AS cnt")
edge_count = rows[0][0] if rows else 0
print(f"Data: {node_count} nodes, {edge_count} edges", flush=True)

cols, rows = c.execute("MATCH (p:Person) RETURN p.id AS id LIMIT 1")
pid = rows[0][0] if rows else 933
print(f"Person ID: {pid}", flush=True)

def bench(name, query, warmup=3):
    try:
        s = time.perf_counter()
        cols, rows = c.execute(query)
        cold = (time.perf_counter() - s) * 1000
        times = []
        for _ in range(warmup):
            s = time.perf_counter()
            c.execute(query)
            times.append((time.perf_counter() - s) * 1000)
        warm = sum(times) / len(times)
        print(f"  {name:<25} cold={cold:.1f}ms warm={warm:.1f}ms rows={len(rows)}", flush=True)
        return {"name": name, "cold_ms": round(cold, 1), "warm_ms": round(warm, 1), "rows": len(rows)}
    except Exception as e:
        print(f"  {name:<25} ERROR: {str(e)[:80]}", flush=True)
        return {"name": name, "error": str(e)[:200]}

print("\n--- LDBC Queries ---", flush=True)
inmem = []
inmem.append(bench("IS1: Profile", f'MATCH (p:Person {{id: {pid}}}) RETURN p.firstName, p.lastName'))
inmem.append(bench("IS3: Friends", f'MATCH (p:Person {{id: {pid}}})-[:KNOWS]-(f) RETURN count(f) AS cnt'))
inmem.append(bench("IC2: Messages", f'MATCH (p:Person {{id: {pid}}})-[:KNOWS]-(f:Person)<-[:HAS_CREATOR]-(m) RETURN f.firstName, m.id ORDER BY m.creationDate DESC LIMIT 10'))
inmem.append(bench("IC5: Forums", f'MATCH (p:Person {{id: {pid}}})-[:KNOWS*1..2]-(f:Person)<-[:HAS_MEMBER]-(forum:Forum) RETURN forum.title, count(DISTINCT f) AS members ORDER BY members DESC LIMIT 10'))
inmem.append(bench("IC11: Work", f'MATCH (p:Person {{id: {pid}}})-[:KNOWS*1..2]-(f:Person)-[:WORK_AT]->(org:Organisation) WHERE f.id <> {pid} RETURN f.firstName, org.name LIMIT 10'))
inmem.append(bench("Edge count", "MATCH ()-[r]->() RETURN count(r) AS cnt"))
inmem.append(bench("Node count", "MATCH (n) RETURN count(n) AS cnt"))

print("\n--- BFS 1-10 ---", flush=True)
for hop in range(1, 11):
    if hop == 1:
        q = f'MATCH (p:Person {{id: {pid}}})-[:KNOWS]-(f) RETURN count(f) AS cnt'
    else:
        q = f'MATCH (p:Person {{id: {pid}}})-[:KNOWS*1..{hop}]-(f) RETURN count(DISTINCT f) AS cnt'
    inmem.append(bench(f"BFS {hop}-hop", q))

print("\n--- GPU Analytics (SF1) ---", flush=True)
gpu_sf1 = []
for name, q in [
    ("PageRank 20iter", 'CALL xray.pagerank(20, 0.85, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5'),
    ("Triangle Count", 'CALL xray.triangle_count("") YIELD triangles, time_ms, vertices RETURN *'),
    ("Connected Components", 'CALL xray.connected_components("") YIELD component_size, num_components, time_ms RETURN * ORDER BY component_size DESC LIMIT 5'),
    ("Betweenness 50", 'CALL xray.betweenness_centrality("", 50) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 5'),
    ("Community 20iter", 'CALL xray.community_detection(20, "") YIELD community_size, num_communities RETURN * ORDER BY community_size DESC LIMIT 5'),
]:
    print(f"  {name}...", end=" ", flush=True)
    try:
        s = time.perf_counter()
        cols, rows = c.execute(q)
        ms = (time.perf_counter() - s) * 1000
        print(f"{ms:.0f}ms, {len(rows)} rows", flush=True)
        if rows: print(f"    {rows[0]}", flush=True)
        gpu_sf1.append({"name": name, "ms": round(ms, 1), "rows": len(rows)})
    except Exception as e:
        print(f"ERROR: {str(e)[:100]}", flush=True)
        gpu_sf1.append({"name": name, "error": str(e)[:200]})

c.close()

# Phase 3: Friendster CSR Analytics
print("\n" + "=" * 70, flush=True)
print("PHASE 3: FRIENDSTER CSR (65.6M vertices, 1.8B edges)", flush=True)
print("=" * 70, flush=True)

c = XrayProtocolClient("127.0.0.1", 7689, auth_token="bench:Bench2026!xray", read_timeout=1800)

friendster = []
for name, q in [
    ("Health Report", 'CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *'),
    ("Graph Stats", 'CALL xray.graph_stats("") YIELD metric, value RETURN *'),
    ("Frontier 5-hop", 'CALL xray.frontier_profile(13594, 5, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes RETURN *'),
    ("Frontier 10-hop", 'CALL xray.frontier_profile(13594, 10, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes RETURN *'),
    ("Shortest Path", 'CALL xray.shortest_path(13594, 13600, "") YIELD node_id, distance, time_ms RETURN *'),
    ("Common Neighbors", 'CALL xray.common_neighbors(13594, 13596) YIELD neighbor_id, count RETURN *'),
    ("Jaccard", 'CALL xray.jaccard_similarity(13594, 13596) YIELD jaccard, common, degree_a, degree_b RETURN *'),
    ("Connected Components", 'CALL xray.connected_components("") YIELD component_size, num_components, time_ms RETURN * ORDER BY component_size DESC LIMIT 5'),
    ("PageRank 5iter", 'CALL xray.pagerank(5, 0.85, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5'),
    ("Triangle Count", 'CALL xray.triangle_count("") YIELD triangles, time_ms, vertices RETURN *'),
    ("Betweenness 100", 'CALL xray.betweenness_centrality("", 100) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 5'),
    ("K-Core", 'CALL xray.kcore("") YIELD core_number, time_ms RETURN core_number, count(*) AS cnt, time_ms ORDER BY core_number DESC LIMIT 5'),
    ("Community 20iter", 'CALL xray.community_detection(20, "") YIELD community_size, num_communities RETURN * ORDER BY community_size DESC LIMIT 5'),
    ("HITS 20iter", 'CALL xray.hits(20, "") YIELD node_id, hub, authority, time_ms RETURN node_id, hub, authority, time_ms ORDER BY authority DESC LIMIT 5'),
    ("PersonalizedPR", 'CALL xray.personalized_pagerank(13594, 0.85, 20, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5'),
]:
    print(f"  {name}...", end=" ", flush=True)
    try:
        s = time.perf_counter()
        cols, rows = c.execute(q)
        ms = (time.perf_counter() - s) * 1000
        print(f"{ms:.0f}ms, {len(rows)} rows", flush=True)
        if rows: print(f"    {rows[0]}", flush=True)
        friendster.append({"name": name, "ms": round(ms, 1), "rows": len(rows)})
    except Exception as e:
        print(f"ERROR: {str(e)[:100]}", flush=True)
        friendster.append({"name": name, "error": str(e)[:200]})

c.close()

# Save
all_results = {
    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "sf1_inmem": {"ldbc": inmem, "gpu": gpu_sf1},
    "friendster_csr": friendster,
}
json.dump(all_results, open("/opt/xraybench-results/FULL_MATRIX_FINAL.json", "w"), indent=2, default=str)

print("\n" + "=" * 70, flush=True)
print("ALL DONE.", flush=True)
print("=" * 70, flush=True)
