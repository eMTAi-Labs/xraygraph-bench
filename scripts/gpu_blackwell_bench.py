#!/usr/bin/env python3
"""Full benchmark on RTX PRO 6000 Blackwell (96GB VRAM, 16 vCPU, 144GB RAM).
$1.74/hr — run fast."""
import time, sys, os, csv, subprocess, threading
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

AUTH = "admin:xraygraphdb"
DB = "__system__"
LOG = "/tmp/gpu_blackwell_bench.log"
log = open(LOG, "w", buffering=1)

def p(msg):
    log.write(msg + "\n")
    log.flush()

def fresh():
    return XrayProtocolClient(host="127.0.0.1", port=7689, auth_token=AUTH, database=DB, read_timeout=7200)

def gpu():
    try:
        r = subprocess.run(["nvidia-smi", "--query-gpu=utilization.gpu,memory.used", "--format=csv,noheader"], capture_output=True, text=True, timeout=5)
        return r.stdout.strip()
    except: return "n/a"

# GPU monitor thread — captures peak utilization during a procedure
gpu_peak = [0]
gpu_monitoring = [False]
def gpu_monitor_thread():
    while gpu_monitoring[0]:
        try:
            r = subprocess.run(["nvidia-smi", "--query-gpu=utilization.gpu", "--format=csv,noheader,nounits"], capture_output=True, text=True, timeout=2)
            val = int(r.stdout.strip())
            if val > gpu_peak[0]: gpu_peak[0] = val
        except: pass
        time.sleep(0.1)

def run_with_gpu(name, query):
    c = fresh()
    gpu_peak[0] = 0
    gpu_monitoring[0] = True
    t = threading.Thread(target=gpu_monitor_thread, daemon=True)
    t.start()
    s = time.perf_counter()
    try:
        cols, rows = c.execute(query)
        ms = (time.perf_counter() - s) * 1000
        gpu_monitoring[0] = False
        time.sleep(0.2)
        g = gpu()
        p(f"  {name:<40} {ms:.0f}ms  rows={len(rows)}  GPU_peak={gpu_peak[0]}%  GPU_now={g}")
        if rows: p(f"    {rows[0]}")
    except Exception as e:
        ms = (time.perf_counter() - s) * 1000
        gpu_monitoring[0] = False
        p(f"  {name:<40} ERROR ({ms:.0f}ms): {str(e)[:100]}")
    c.close()

def bench(name, query, c, warmup=10):
    s = time.perf_counter()
    cols, rows = c.execute(query)
    cold = (time.perf_counter() - s) * 1000
    times = []
    for _ in range(warmup):
        s = time.perf_counter()
        c.execute(query)
        times.append((time.perf_counter() - s) * 1000)
    warm = sum(times)/len(times) if times else cold
    p50 = sorted(times)[len(times)//2] if times else cold
    p(f"  {name:<40} cold={cold:.2f}  warm={warm:.2f}  p50={p50:.2f}ms  rows={len(rows)}")
    if rows: p(f"    {rows[0]}")

p("=" * 70)
p("GPU BENCHMARK — RTX PRO 6000 Blackwell (96GB VRAM)")
p(f"16 vCPU, 144GB RAM, Docker container")
p(f"Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
p(f"GPU: {gpu()}")
p("=" * 70)

# Protocol latency
p("\n--- Protocol Latency ---")
c = fresh()
bench("RETURN 1", "RETURN 1", c)
bench("RETURN 1+1", "RETURN 1+1", c)
c.close()

# Friendster CSR import
p("\n--- Friendster CSR Import ---")
c = fresh()
s = time.perf_counter()
try:
    result = c.bulk_import_file("/var/lib/xraygraphdb/import/com-friendster.ungraph.txt")
    elapsed = time.perf_counter() - s
    p(f"  Import: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    p(f"  Result: {result}")
except Exception as e:
    elapsed = time.perf_counter() - s
    p(f"  Import error after {elapsed:.1f}s: {str(e)[:150]}")
c.close()

# Verify
c = fresh()
cols, rows = c.execute('CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *')
p("\n--- Health ---")
for r in rows: p(f"  {r[0]}={r[3]}")
c.close()

# GPU Analytics on Friendster
p("\n--- Friendster GPU Analytics ---")
gpu_tests = [
    ("Triangle Count", 'CALL xray.triangle_count("") YIELD triangles, edges_checked, time_ms, vertices RETURN *'),
    ("PageRank 5iter", 'CALL xray.pagerank(5, 0.85, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5'),
    ("K-Core", 'CALL xray.kcore("") YIELD core_number, time_ms RETURN core_number, time_ms ORDER BY core_number DESC LIMIT 5'),
    ("Connected Components", 'CALL xray.connected_components("") YIELD component_size, num_components, time_ms RETURN component_size, num_components, time_ms ORDER BY component_size DESC LIMIT 5'),
    ("Community 3iter", 'CALL xray.community_detection(3, "") YIELD community_size, num_communities RETURN community_size, num_communities ORDER BY community_size DESC LIMIT 5'),
    ("HITS 3iter", 'CALL xray.hits(3, "") YIELD node_id, hub, authority, time_ms RETURN node_id, hub, authority, time_ms ORDER BY authority DESC LIMIT 5'),
    ("PersonalizedPR 5iter", 'CALL xray.personalized_pagerank(81306110, 0.85, 5, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5'),
    ("Clustering Coefficient", 'CALL xray.clustering_coefficient("") YIELD node_id, coefficient, triangles RETURN node_id, coefficient, triangles ORDER BY coefficient DESC LIMIT 5'),
]
for name, q in gpu_tests:
    run_with_gpu(name, q)

# BC variants
p("\n--- Betweenness Centrality ---")
c = fresh()
gpu_peak[0] = 0; gpu_monitoring[0] = True
t = threading.Thread(target=gpu_monitor_thread, daemon=True); t.start()
s = time.perf_counter()
cols, rows = c.execute('CALL xray.betweenness_pair_sampled(0.05, 0.05, "", 1, 1) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10')
ms = (time.perf_counter() - s) * 1000
gpu_monitoring[0] = False
p(f"  BC COLD e=0.05 b=1                         {ms:.0f}ms  rows={len(rows)}  GPU_peak={gpu_peak[0]}%")
if rows: p(f"    {rows[0]}")

s = time.perf_counter()
cols, rows = c.execute('CALL xray.betweenness_pair_sampled(0.05, 0.05, "", 1, 1) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10')
ms = (time.perf_counter() - s) * 1000
p(f"  BC WARM e=0.05 b=1                         {ms:.0f}ms  rows={len(rows)}")
if rows: p(f"    {rows[0]}")

s = time.perf_counter()
cols, rows = c.execute('CALL xray.betweenness_pair_sampled(0.10, 0.05, "", 1, 1) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10')
ms = (time.perf_counter() - s) * 1000
p(f"  BC WARM e=0.10 b=1                         {ms:.0f}ms  rows={len(rows)}")
if rows: p(f"    {rows[0]}")
c.close()

# Traversal
p("\n--- Friendster Traversal ---")
c = fresh()
bench("Shortest Path", 'CALL xray.shortest_path(81306110, 20676652, "") YIELD node_id, distance, path_index, time_ms RETURN *', c, warmup=5)
bench("Jaccard Similarity", 'CALL xray.jaccard_similarity(81306110, 20676652) YIELD jaccard, overlap, common, total_union, degree_a, degree_b RETURN *', c, warmup=5)
s = time.perf_counter()
cols, rows = c.execute('CALL xray.link_prediction(81306110, 10, "adamic_adar") YIELD node_id, score, method, rank RETURN *')
p(f"  Link Prediction                            {(time.perf_counter()-s)*1000:.0f}ms  rows={len(rows)}")
c.close()

# CSR BFS
p("\n--- CSR BFS on Friendster ---")
for hops in range(1, 11):
    c = fresh()
    s = time.perf_counter()
    cols, rows = c.execute(f'CALL xray.frontier_profile(81306110, {hops}, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *')
    ms = (time.perf_counter() - s) * 1000
    p(f"  CSR BFS {hops}-hop: {ms:.0f}ms  rows={len(rows)}")
    c.close()

p(f"\nGPU final: {gpu()}")
p("\n" + "=" * 70)
p("Benchmark complete.")
