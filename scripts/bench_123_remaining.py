#!/usr/bin/env python3
"""Remaining benchmarks on GPU server .123 — Friendster CSR load + analytics."""
import time, sys
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

AUTH = "admin:5Aajiie2vMkoMPBeRVN0wA2Y"
DB = "xraygraphdb"
LOG = "/tmp/bench_123_remaining.log"
log = open(LOG, "w", buffering=1)

def p(msg):
    log.write(msg + "\n")
    log.flush()

def fresh():
    return XrayProtocolClient(host="127.0.0.1", port=7689, auth_token=AUTH, database=DB, read_timeout=7200)

def run_once(name, query, c):
    s = time.perf_counter()
    try:
        cols, rows = c.execute(query)
        ms = (time.perf_counter() - s) * 1000
        p(f"  {name:<45} {ms:.0f}ms  rows={len(rows)}")
        if rows: p(f"    {rows[0]}")
        return {"name": name, "ms": round(ms,1), "rows": len(rows), "status": "PASS"}
    except Exception as e:
        ms = (time.perf_counter() - s) * 1000
        p(f"  {name:<45} ERROR ({ms:.0f}ms): {str(e)[:120]}")
        return {"name": name, "error": str(e)[:200], "status": "FAIL"}

def bench(name, query, c, warmup=5):
    s = time.perf_counter()
    cols, rows = c.execute(query)
    cold = (time.perf_counter() - s) * 1000
    times = []
    for _ in range(warmup):
        s = time.perf_counter()
        c.execute(query)
        times.append((time.perf_counter() - s) * 1000)
    warm = sum(times)/len(times) if times else cold
    p(f"  {name:<45} cold={cold:.1f}  warm={warm:.1f}ms  rows={len(rows)}")
    if rows: p(f"    {rows[0]}")
    return {"name": name, "cold_ms": round(cold,1), "warm_ms": round(warm,1), "rows": len(rows), "status": "PASS"}

p("=" * 70)
p("REMAINING BENCHMARKS — 216.152.151.123 (Tesla P4)")
p(f"Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
p("=" * 70)

# ═══════════════════════════════════════════════════════════════
# PART 1: Load Friendster CSR
# ═══════════════════════════════════════════════════════════════
p("\n--- Friendster CSR Load ---")
c = fresh()

# Check if Friendster file exists
import os
friendster_path = "/tmp/xraygraphdb-import/com-friendster.ungraph.txt"
if not os.path.exists(friendster_path):
    # Try to find it
    for path in ["/neo4j/datasets_friendster/com-friendster.ungraph.txt",
                 "/opt/datasets/friendster/com-friendster.ungraph.txt"]:
        if os.path.exists(path):
            os.makedirs("/tmp/xraygraphdb-import", exist_ok=True)
            os.symlink(path, friendster_path)
            p(f"  Symlinked {path} -> {friendster_path}")
            break

if os.path.exists(friendster_path):
    p(f"  Importing {friendster_path}...")
    s = time.perf_counter()
    try:
        result = c.bulk_import_file(friendster_path)
        elapsed = time.perf_counter() - s
        p(f"  Import: {elapsed:.1f}s ({elapsed/60:.1f} min)")
        p(f"  Result: {result}")
    except Exception as e:
        elapsed = time.perf_counter() - s
        p(f"  Import error after {elapsed:.1f}s: {str(e)[:150]}")
else:
    p("  Friendster file not found — need to copy from .187")
    p("  Checking CSR health anyway...")

c.close()

# Verify CSR
c = fresh()
r = run_once("Health Report", 'CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *', c)
c.close()

# ═══════════════════════════════════════════════════════════════
# PART 2: Friendster Analytics (GPU + CPU)
# ═══════════════════════════════════════════════════════════════
p("\n--- Friendster Analytics ---")

# Find hub vertex
c = fresh()
try:
    cols, rows = c.execute('CALL xray.pagerank(5, 0.85, "") YIELD node_id, rank RETURN node_id, rank ORDER BY rank DESC LIMIT 5')
    hubs = [int(r[0]) for r in rows]
    hub = hubs[0]
    hub2 = hubs[1] if len(hubs) > 1 else hub + 1
    p(f"  Hub vertices: {hub}, {hub2}")
except Exception as e:
    hub, hub2 = 81306110, 20676652
    p(f"  Using default hubs: {hub}, {hub2} ({str(e)[:60]})")
c.close()

# Global analytics — fresh connection each
for name, q in [
    ("Connected Components", 'CALL xray.connected_components("") YIELD component_size, num_components, time_ms RETURN component_size, num_components, time_ms ORDER BY component_size DESC LIMIT 5'),
    ("PageRank 5iter", 'CALL xray.pagerank(5, 0.85, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5'),
    ("Triangle Count", 'CALL xray.triangle_count("") YIELD triangles, edges_checked, time_ms, vertices RETURN *'),
    ("K-Core", 'CALL xray.kcore("") YIELD core_number, time_ms RETURN core_number, time_ms ORDER BY core_number DESC LIMIT 5'),
    ("Community 3iter", 'CALL xray.community_detection(3, "") YIELD community_size, num_communities RETURN community_size, num_communities ORDER BY community_size DESC LIMIT 5'),
    ("HITS 3iter", 'CALL xray.hits(3, "") YIELD node_id, hub, authority, time_ms RETURN node_id, hub, authority, time_ms ORDER BY authority DESC LIMIT 5'),
]:
    c = fresh()
    run_once(name, q, c)
    c.close()

# Vertex-level
c = fresh()
run_once("PersonalizedPR 5iter", f'CALL xray.personalized_pagerank({hub}, 0.85, 5, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5', c)
run_once("Clustering Coefficient", 'CALL xray.clustering_coefficient("") YIELD node_id, coefficient, triangles RETURN node_id, coefficient, triangles ORDER BY coefficient DESC LIMIT 5', c)
c.close()

# BC variants
c = fresh()
run_once("BC Pair-Sampled COLD (e=0.05, b=1)", 'CALL xray.betweenness_pair_sampled(0.05, 0.05, "", 1, 1) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10', c)
run_once("BC Pair-Sampled WARM (e=0.05, b=1)", 'CALL xray.betweenness_pair_sampled(0.05, 0.05, "", 1, 1) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10', c)
run_once("BC Pair-Sampled WARM (e=0.10, b=1)", 'CALL xray.betweenness_pair_sampled(0.10, 0.05, "", 1, 1) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10', c)
c.close()

# Traversal
c = fresh()
bench("Shortest Path", f'CALL xray.shortest_path({hub}, {hub2}, "") YIELD node_id, distance, path_index, time_ms RETURN *', c)
bench("Jaccard Similarity", f'CALL xray.jaccard_similarity({hub}, {hub2}) YIELD jaccard, overlap, common, total_union, degree_a, degree_b RETURN *', c)
run_once("Link Prediction", f'CALL xray.link_prediction({hub}, 10, "adamic_adar") YIELD node_id, score, method, rank RETURN *', c)
c.close()

# Frontier BFS
p("\n--- CSR BFS on Friendster ---")
for hops in range(1, 11):
    c = fresh()
    run_once(f"CSR BFS {hops}-hop (v={hub})", f'CALL xray.frontier_profile({hub}, {hops}, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *', c)
    c.close()

# GPU monitoring during a procedure
p("\n--- GPU Utilization Check ---")
import subprocess
try:
    result = subprocess.run(["nvidia-smi", "--query-gpu=utilization.gpu,memory.used,temperature.gpu", "--format=csv,noheader"], capture_output=True, text=True, timeout=5)
    p(f"  GPU after analytics: {result.stdout.strip()}")
except:
    p("  nvidia-smi not available")

p("\n" + "=" * 70)
p("Benchmark complete.")
