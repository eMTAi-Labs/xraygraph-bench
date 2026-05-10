#!/usr/bin/env python3
"""Full GPU benchmark rerun on Blackwell — DB team GPU code improvements.

Tests ALL GPU-accelerated analytics on Friendster (65.6M vertices, 3.6B edges).
Measures cold, warm1, warm2 for each algorithm.
Records GPU utilization via nvidia-smi sampling.
Uses standard source vertex 71768986 (degree 5214) for BFS.
"""
import time
import sys
import json
import threading
import subprocess

sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

AUTH = "admin:xraygraphdb"
DB = "xraygraphdb"
SOURCE = 71768986  # standard source vertex

gpu_samples = []
gpu_active = False

def gpu_monitor():
    global gpu_samples, gpu_active
    while gpu_active:
        try:
            out = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used",
                 "--format=csv,noheader,nounits"], timeout=2
            ).decode().strip()
            parts = out.split(",")
            gpu_samples.append({"util": int(parts[0].strip()), "mem": int(parts[1].strip())})
        except Exception:
            pass
        time.sleep(0.1)

def start_gpu():
    global gpu_samples, gpu_active
    gpu_samples = []
    gpu_active = True
    threading.Thread(target=gpu_monitor, daemon=True).start()

def stop_gpu():
    global gpu_active
    gpu_active = False
    time.sleep(0.2)
    if gpu_samples:
        u = [s["util"] for s in gpu_samples]
        m = [s["mem"] for s in gpu_samples]
        return {"avg_util": round(sum(u)/len(u), 1), "max_util": max(u),
                "peak_mem_mb": max(m), "samples": len(u)}
    return {}

def fresh():
    return XrayProtocolClient(host="127.0.0.1", port=7689,
                              auth_token=AUTH, database=DB, read_timeout=7200)

def fmt(t):
    if t >= 1e9: return "{:.2f} GTEPS".format(t/1e9)
    elif t >= 1e6: return "{:.2f} MTEPS".format(t/1e6)
    else: return "{:.0f} TEPS".format(t)

print("=" * 72)
print("  xrayGraphDB GPU Benchmark — Blackwell Rerun")
print("  DB team GPU code improvements, May 2026")
print("=" * 72)
print()

# Health check
c = fresh()
cols, rows = c.execute("CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *")
health = {r[0]: r[3] for r in rows}
vcount = int(health.get("vertex_count", 0))
ecount = int(health.get("edge_count", 0))
storage = health.get("storage_engine", "?")
print("Vertices:  {:,}".format(vcount))
print("Edges:     {:,}".format(ecount))
print("Storage:   " + storage)
c.close()

out = subprocess.check_output(["nvidia-smi", "--query-gpu=name,memory.total,driver_version",
                                "--format=csv,noheader"], timeout=5).decode().strip()
print("GPU:       " + out)
print("Source:    vertex {} (degree 5214)".format(SOURCE))
print()

results = {}

# ─── Protocol Latency ───
print("=" * 72)
print("  Protocol Latency")
print("=" * 72)
c = fresh()
times = []
for _ in range(100):
    t0 = time.perf_counter()
    c.execute("RETURN 1")
    times.append((time.perf_counter() - t0) * 1000)
p50 = sorted(times)[50]
print("  RETURN 1 p50: {:.2f}ms".format(p50))
results["return1_p50_ms"] = round(p50, 3)
c.close()
print()

# ─── BFS TEPS (frontier_profile) ───
print("=" * 72)
print("  BFS TEPS — 16 sources")
print("=" * 72)
c = fresh()
cols, rows = c.execute("CALL xray.pagerank(1, 0.85, '') YIELD node_id, rank "
                        "RETURN node_id ORDER BY rank DESC LIMIT 16")
sources = [int(r[0]) for r in rows]
c.close()
print("  Sources: " + str(sources[:5]) + "...")

bfs_results = []
for i, src in enumerate(sources):
    c = fresh()
    start_gpu()
    t0 = time.perf_counter()
    cols, rows = c.execute(
        "CALL xray.frontier_profile({}, 20, 'OUTGOING') YIELD hop, frontier_size, "
        "cumulative_nodes, new_edges, avg_degree, max_degree RETURN *".format(src))
    elapsed = time.perf_counter() - t0
    gpu = stop_gpu()
    cn = {col[0]: i for i, col in enumerate(cols)}
    total_v = 0
    max_hop = 0
    for r in rows:
        h = int(float(r[cn["hop"]]))
        cum = int(float(r[cn["cumulative_nodes"]]))
        total_v = max(total_v, cum)
        max_hop = max(max_hop, h)
    coverage = total_v / vcount if vcount > 0 else 0
    edges_trav = int(ecount * coverage)
    teps = edges_trav / elapsed if elapsed > 0 else 0
    print("  {:2d}/16 v={}: {:.1f}s {:,} vertices ({:.1f}%) {} hops {} GPU={}%".format(
        i+1, src, elapsed, total_v, coverage*100, max_hop, fmt(teps),
        gpu.get("avg_util", "?")))
    bfs_results.append({"source": src, "elapsed_s": round(elapsed, 3),
                        "vertices": total_v, "teps": teps, "gpu": gpu})
    c.close()

teps_list = sorted([r["teps"] for r in bfs_results])
n = len(teps_list)
median_teps = teps_list[n // 2]
hmean = n / sum(1/t for t in teps_list if t > 0) if all(t > 0 for t in teps_list) else 0
print("  Median: {}  Harmonic mean: {}".format(fmt(median_teps), fmt(hmean)))
results["bfs_teps"] = {"median": median_teps, "hmean": hmean, "runs": bfs_results}
print()

# ─── Hop-by-hop from standard source ───
print("=" * 72)
print("  Hop-by-hop from vertex {} (standard source)".format(SOURCE))
print("=" * 72)
c = fresh()
start_gpu()
t0 = time.perf_counter()
cols, rows = c.execute(
    "CALL xray.frontier_profile({}, 15, 'OUTGOING') YIELD hop, frontier_size, "
    "cumulative_nodes, new_edges, avg_degree, max_degree RETURN *".format(SOURCE))
elapsed = time.perf_counter() - t0
gpu = stop_gpu()
cn = {col[0]: i for i, col in enumerate(cols)}
hops = []
for r in rows:
    h = int(float(r[cn["hop"]]))
    f = int(float(r[cn["frontier_size"]]))
    cum = int(float(r[cn["cumulative_nodes"]]))
    hops.append({"hop": h, "frontier": f, "cumulative": cum})
    print("  hop {:2d}: {:>14,} cumulative  (frontier {:>12,})".format(h, cum, f))
print("  Total: {:.3f}s  GPU avg={}%".format(elapsed, gpu.get("avg_util", "?")))
results["hop_by_hop"] = {"elapsed_s": round(elapsed, 3), "hops": hops, "gpu": gpu}
c.close()
print()

# ─── GPU Analytics Suite ───
algos = [
    ("PageRank 20 iter", "CALL xray.pagerank(20, 0.85, '') YIELD node_id, rank RETURN count(node_id)"),
    ("Triangle Count", "CALL xray.triangle_count('') YIELD node_id, triangles RETURN sum(triangles)"),
    ("Community Detection 20 iter", "CALL xray.community_detection(20, '') YIELD node_id, community RETURN count(node_id)"),
    ("Betweenness eps=0.10", "CALL xray.betweenness_centrality('', 50) YIELD node_id, centrality RETURN count(node_id)"),
    ("Connected Components", "CALL xray.connected_components('') YIELD node_id, component RETURN count(DISTINCT component)"),
    ("K-Core", "CALL xray.kcore('') YIELD node_id, core_number RETURN max(core_number)"),
]

for algo_name, query in algos:
    print("=" * 72)
    print("  " + algo_name)
    print("=" * 72)
    for label in ["cold", "warm1", "warm2"]:
        c = fresh()
        start_gpu()
        t0 = time.perf_counter()
        try:
            cols, rows = c.execute(query)
            elapsed = time.perf_counter() - t0
            gpu = stop_gpu()
            result = rows[0][0] if rows else "?"
            print("  {:6s}: {:.2f}s  result={}  GPU avg={}% peak={}%  mem={:,}MB".format(
                label, elapsed, result, gpu.get("avg_util", "?"),
                gpu.get("max_util", "?"), gpu.get("peak_mem_mb", 0)))
            results[algo_name.lower().replace(" ", "_") + "_" + label] = {
                "elapsed_s": round(elapsed, 3), "result": str(result), "gpu": gpu}
        except Exception as e:
            elapsed = time.perf_counter() - t0
            gpu = stop_gpu()
            print("  {:6s}: FAILED ({:.1f}s): {}".format(label, elapsed, str(e)[:80]))
            results[algo_name.lower().replace(" ", "_") + "_" + label] = {
                "error": str(e)[:200], "elapsed_s": round(elapsed, 3)}
        c.close()
    print()

# ─── Summary ───
print("=" * 72)
print("  BENCHMARK COMPLETE")
print("=" * 72)
print()

json_path = "/tmp/blackwell_gpu_rerun_results.json"
output = {
    "benchmark": "blackwell_gpu_rerun",
    "date": "2026-05-10",
    "vertices": vcount, "edges": ecount,
    "hardware": "RTX PRO 6000 Blackwell 96GB, 16 vCPU, 144GB RAM",
    "storage": storage,
    "results": results,
}
with open(json_path, "w") as f:
    json.dump(output, f, indent=2, default=str)
print("Results saved to " + json_path)
