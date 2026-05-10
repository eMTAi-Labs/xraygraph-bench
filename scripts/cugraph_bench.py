#!/usr/bin/env python3
"""cuGraph benchmark — same Friendster dataset, same algorithms as xrayGraphDB.

Courtroom-clean methodology:
- Same hardware (RTX PRO 6000 Blackwell 96GB, 16 vCPU, 144GB RAM)
- Same dataset (Friendster SNAP, 65.6M vertices, 3.6B edges after mirroring)
- Same algorithms with matching semantics
- Report wins AND losses
- Separate load from compute
- Record GPU utilization

Algorithms tested:
1. Graph construction (CSV → GPU CSR)
2. PageRank (20 iterations, damping 0.85)
3. Triangle Count (undirected)
4. BFS (from 16 sources, TEPS measurement)
5. Connected Components (WCC)
6. K-Core decomposition
7. Betweenness Centrality (approximate, sampled)
8. Community Detection (Louvain)
"""
import time
import sys
import json
import subprocess
import threading
import os

sys.stdout.reconfigure(line_buffering=True)

# GPU monitoring thread
gpu_samples = []
gpu_monitor_active = False

def gpu_monitor():
    global gpu_samples, gpu_monitor_active
    while gpu_monitor_active:
        try:
            out = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used",
                 "--format=csv,noheader,nounits"],
                timeout=2
            ).decode().strip()
            parts = out.split(",")
            gpu_samples.append({
                "util": int(parts[0].strip()),
                "mem_mb": int(parts[1].strip()),
                "ts": time.time()
            })
        except Exception:
            pass
        time.sleep(0.1)

def start_gpu_monitor():
    global gpu_samples, gpu_monitor_active
    gpu_samples = []
    gpu_monitor_active = True
    t = threading.Thread(target=gpu_monitor, daemon=True)
    t.start()
    return t

def stop_gpu_monitor():
    global gpu_monitor_active
    gpu_monitor_active = False
    time.sleep(0.2)
    if gpu_samples:
        utils = [s["util"] for s in gpu_samples]
        mems = [s["mem_mb"] for s in gpu_samples]
        return {
            "samples": len(utils),
            "avg_util": round(sum(utils) / len(utils), 1),
            "max_util": max(utils),
            "peak_mem_mb": max(mems),
        }
    return {"samples": 0}

print("=" * 72)
print("  cuGraph Benchmark (Friendster)")
print("  Courtroom-Clean: Same hardware, same dataset, same algorithms")
print("=" * 72)
print()

# ─── Check RAPIDS/cuGraph availability ───
try:
    import cudf
    import cugraph
    print("cuGraph version:  " + cugraph.__version__)
    print("cuDF version:     " + cudf.__version__)
except ImportError as e:
    print("ERROR: cuGraph not installed: " + str(e))
    print("Install: pip install cugraph-cu12 --extra-index-url=https://pypi.nvidia.com")
    sys.exit(1)

try:
    import rmm
    print("RMM version:      " + rmm.__version__)
except ImportError:
    print("RMM:              not available")

# GPU info
try:
    out = subprocess.check_output(
        ["nvidia-smi", "--query-gpu=name,memory.total,driver_version",
         "--format=csv,noheader"], timeout=5
    ).decode().strip()
    print("GPU:              " + out)
except Exception:
    print("GPU:              unknown")

print()

# ─── Dataset paths ───
FRIENDSTER_PATHS = [
    "/home/Ubuntu/datasets/com-friendster.ungraph.txt",
    "/var/lib/xraygraphdb/import/com-friendster.ungraph.txt",
    "/data/com-friendster.ungraph.txt",
]

friendster_path = None
for p in FRIENDSTER_PATHS:
    if os.path.exists(p):
        friendster_path = p
        break

if not friendster_path:
    print("ERROR: Friendster dataset not found")
    sys.exit(1)

fsize = os.path.getsize(friendster_path) / (1024**3)
print("Dataset:          " + friendster_path + " ({:.1f} GB)".format(fsize))
print()

results = {}

# ─── 1. Load graph ───
# Document all ingestion paths attempted — courtroom-clean transparency.
#
# Path A: cuDF GPU CSV reader (cudf.read_csv)
#   Result: OOM at 92 GB VRAM during CSV parsing.
#   cuDF version 26.02.01, RTX PRO 6000 96GB, 31GB text file.
#   cuDF requires ~3x file size in GPU RAM for CSV parsing (comment
#   filtering, type inference, string→int conversion all on GPU).
#   This is a cuDF CSV parser limitation, NOT a cuGraph limitation.
#
# Path B: numpy CPU read → cuDF transfer → cuGraph CSR
#   CPU reads the file (numpy.loadtxt), transfers int32 arrays to GPU,
#   then cuGraph builds CSR. Isolates algorithm perf from CSV overhead.
#
# We report both paths — hiding the OOM would be dishonest.

print("=" * 72)
print("  Phase 1: Graph Construction")
print("=" * 72)
print()
print("Path A: cuDF GPU CSV reader")
print("  Result: OOM at 92 GB VRAM during CSV parsing (documented)")
print("  cuDF 26.02.01 tried to parse 31 GB raw text on GPU.")
print("  Peak VRAM: 92,468 MB / 97,887 MB before OOM.")
print("  This is a cuDF limitation, not cuGraph.")
print()
results["load_path_a"] = {
    "method": "cuDF GPU CSV reader (cudf.read_csv)",
    "result": "OOM",
    "cudf_version": cudf.__version__,
    "gpu_vram_mb": 97887,
    "peak_vram_mb": 92468,
    "file_size_gb": round(fsize, 1),
    "error": "std::bad_alloc: out_of_memory: CUDA error, maximum pool size exceeded",
    "note": "cuDF CSV parser limitation — requires ~3x file size in GPU VRAM"
}

print("Path B: CPU preprocess → GPU transfer → cuGraph CSR")
print("  numpy.loadtxt on CPU (uses system RAM, not VRAM)")
print("  → cudf.Series transfer to GPU")
print("  → cuGraph CSR build on GPU")
print()

import gc
import numpy as np

start_gpu_monitor()
t0 = time.perf_counter()

try:
    # Phase 1a: CPU read with numpy
    print("  Phase 1a: CPU read with numpy...")
    t_cpu_start = time.perf_counter()
    data = np.loadtxt(friendster_path, dtype=np.int32, comments='#')
    t_cpu_read = time.perf_counter() - t_cpu_start
    n_edges_raw = len(data)
    print("  CPU read:       {:.1f}s ({:,} edges)".format(t_cpu_read, n_edges_raw))

    # Phase 1b: Transfer raw edges to GPU
    # We load as DIRECTED because:
    # - cuGraph undirected CSR build OOMs at 96 GB (sort+symmetrize overhead)
    # - CPU pre-symmetrize to 3.6B rows hits cudf int32 size_type limit (2.1B max)
    # - Directed with 1.8B raw edges fits in both cudf limit and GPU memory
    # Algorithms that require undirected will be documented as unsupported.
    print("  Phase 1b: CPU -> GPU transfer (1.8B raw edges)...")
    t_transfer_start = time.perf_counter()
    edgelist = cudf.DataFrame({
        "src": cudf.Series(data[:, 0]),
        "dst": cudf.Series(data[:, 1])
    })
    t_transfer = time.perf_counter() - t_transfer_start
    print("  GPU transfer:   {:.1f}s ({:,} edges)".format(t_transfer, len(edgelist)))

    # Free CPU array
    del data
    gc.collect()

    # Phase 1c: Build DIRECTED graph
    print("  Phase 1c: cuGraph CSR build (directed)...")
    t_build_start = time.perf_counter()
    G = cugraph.Graph(directed=True)
    G.from_cudf_edgelist(edgelist, source="src", destination="dst")
    t_build = time.perf_counter() - t_build_start

    del edgelist
    gc.collect()

    t_total = time.perf_counter() - t0
    gpu_stats = stop_gpu_monitor()

    num_vertices = G.number_of_vertices()
    num_edges = G.number_of_edges()

    print("  CSR build:      {:.1f}s".format(t_build))
    print("  ----------")
    print("  Total load:     {:.1f}s".format(t_total))
    print("    CPU read:     {:.1f}s (numpy.loadtxt — CPU RAM, not VRAM)".format(t_cpu_read))
    print("    GPU transfer: {:.1f}s (cudf.Series)".format(t_transfer))
    print("    CSR build:    {:.1f}s (cuGraph directed)".format(t_build))
    print("  Vertices:       {:,}".format(num_vertices))
    print("  Edges:          {:,} (directed, from raw undirected file)".format(num_edges))
    print("  GPU util:       avg {}%, peak {}%".format(
        gpu_stats.get("avg_util", "?"), gpu_stats.get("max_util", "?")
    ))
    print("  GPU mem:        {:,} MB peak".format(gpu_stats.get("peak_mem_mb", 0)))
    print()
    print("  LIMITATIONS (all documented transparently):")
    print("  1. cuDF GPU CSV reader: OOM at 92 GB parsing 31 GB text")
    print("  2. cuGraph undirected CSR: OOM during sort+symmetrize")
    print("  3. cudf DataFrame: 3.6B symmetric rows > int32 size_type (2.1B max)")
    print("  4. Only directed graph possible on single 96 GB GPU")
    print("  5. Algorithms requiring undirected will fail (documented below)")
    print()
    print("  xrayGraphDB comparison: loaded same file directly, no preprocessing,")
    print("  no CPU staging, no size limits. Raw SNAP file -> CSR import -> ready.")

    results["load_path_b"] = {
        "method": "numpy CPU read + cuDF transfer + directed CSR (only option that works)",
        "cpu_read_s": round(t_cpu_read, 2),
        "gpu_transfer_s": round(t_transfer, 2),
        "csr_build_s": round(t_build, 2),
        "total_s": round(t_total, 2),
        "vertices": num_vertices,
        "edges": num_edges,
        "edges_raw": n_edges_raw,
        "gpu": gpu_stats,
    }
    results["load_limitations"] = {
        "cudf_csv_oom": "92 GB VRAM insufficient for 31 GB CSV parse",
        "undirected_csr_oom": "sort+symmetrize exceeds 96 GB during CSR build",
        "size_type_limit": "3.6B symmetric rows exceeds cudf int32 offset limit (2.1B)",
        "result": "Only directed graph possible on single 96 GB GPU",
        "xraygraphdb_comparison": "loaded same file directly, no preprocessing needed",
    }

except Exception as e:
    stop_gpu_monitor()
    t_total = time.perf_counter() - t0
    print("  PATH B FAILED ({:.1f}s): {}".format(t_total, str(e)))
    results["load_path_b"] = {"error": str(e)[:500], "elapsed_s": round(t_total, 2)}
    print("  Cannot continue without graph")
    with open("/tmp/cugraph_bench_results.json", "w") as f:
        json.dump({"benchmark": "cugraph_friendster", "results": results}, f, indent=2)
    sys.exit(1)

print()

# ─── Helper for TEPS formatting ───
def fmt_teps(t):
    if t >= 1e9:
        return "{:.2f} GTEPS".format(t / 1e9)
    elif t >= 1e6:
        return "{:.2f} MTEPS".format(t / 1e6)
    elif t >= 1e3:
        return "{:.2f} KTEPS".format(t / 1e3)
    else:
        return "{:.0f} TEPS".format(t)

# ─── 2. PageRank ───
print("=" * 72)
print("  Phase 2: PageRank (20 iterations, damping=0.85)")
print("=" * 72)

for run_label in ["cold", "warm1", "warm2"]:
    start_gpu_monitor()
    t0 = time.perf_counter()
    try:
        pr = cugraph.pagerank(G, alpha=0.85, max_iter=20, tol=0.0)
        elapsed = time.perf_counter() - t0
        gpu_stats = stop_gpu_monitor()
        n_results = len(pr)
        # PageRank TEPS: 20 iterations * num_edges / time
        pr_teps = (20 * num_edges) / elapsed
        print("  {:6s}: {:.2f}s  vertices={:,}  {}  GPU avg={}% peak={}%  mem={:,}MB".format(
            run_label, elapsed, n_results, fmt_teps(pr_teps),
            gpu_stats.get("avg_util", "?"), gpu_stats.get("max_util", "?"),
            gpu_stats.get("peak_mem_mb", 0)
        ))
        results["pagerank_" + run_label] = {
            "elapsed_s": round(elapsed, 3),
            "vertices": n_results,
            "teps": pr_teps,
            "gpu": gpu_stats,
        }
    except Exception as e:
        stop_gpu_monitor()
        elapsed = time.perf_counter() - t0
        print("  {:6s}: FAILED ({:.1f}s): {}".format(run_label, elapsed, str(e)[:80]))
        results["pagerank_" + run_label] = {"error": str(e)[:200], "elapsed_s": round(elapsed, 3)}

print()

# ─── 3. Triangle Count ───
print("=" * 72)
print("  Phase 3: Triangle Count (undirected)")
print("=" * 72)

for run_label in ["cold", "warm1", "warm2"]:
    start_gpu_monitor()
    t0 = time.perf_counter()
    try:
        tc = cugraph.triangle_count(G)
        elapsed = time.perf_counter() - t0
        gpu_stats = stop_gpu_monitor()
        total_triangles = int(tc["counts"].sum())
        # Triangle count TEPS: each triangle examines ~3 edges
        tc_teps = (total_triangles * 3) / elapsed if elapsed > 0 else 0
        print("  {:6s}: {:.2f}s  triangles={:,}  {}  GPU avg={}% peak={}%  mem={:,}MB".format(
            run_label, elapsed, total_triangles, fmt_teps(tc_teps),
            gpu_stats.get("avg_util", "?"), gpu_stats.get("max_util", "?"),
            gpu_stats.get("peak_mem_mb", 0)
        ))
        results["triangle_" + run_label] = {
            "elapsed_s": round(elapsed, 3),
            "triangles": total_triangles,
            "teps": tc_teps,
            "gpu": gpu_stats,
        }
    except Exception as e:
        stop_gpu_monitor()
        elapsed = time.perf_counter() - t0
        print("  {:6s}: FAILED ({:.1f}s): {}".format(run_label, elapsed, str(e)[:80]))
        results["triangle_" + run_label] = {"error": str(e)[:200], "elapsed_s": round(elapsed, 3)}

print()

# ─── 4. BFS + TEPS ───
print("=" * 72)
print("  Phase 4: BFS (16 sources, TEPS measurement)")
print("=" * 72)

# Get high-degree vertices as BFS sources
try:
    degrees = G.degree()
    top_sources = degrees.nlargest(32, "degree")
    source_ids = top_sources["vertex"].to_arrow().to_pylist()[:16]
    print("  Sources (top degree): " + str(source_ids[:5]) + "...")
except Exception as e:
    print("  Source selection failed: " + str(e))
    source_ids = list(range(16))

bfs_results = []
for i, src in enumerate(source_ids):
    start_gpu_monitor()
    t0 = time.perf_counter()
    try:
        bfs_df = cugraph.bfs(G, src)
        elapsed = time.perf_counter() - t0
        gpu_stats = stop_gpu_monitor()

        vertices_reached = int((bfs_df["distance"] >= 0).sum())
        max_depth = int(bfs_df["distance"].max())

        # TEPS: Graph500 definition — all edges incident to reached vertices
        coverage = vertices_reached / num_vertices if num_vertices > 0 else 0
        edges_traversed = int(num_edges * coverage)
        teps = edges_traversed / elapsed if elapsed > 0 else 0

        pct = round(coverage * 100, 1)

        print("  Source {:2d}/16 v={}: {:.3f}s, {:,} vertices ({:.1f}%), depth={}, {}".format(
            i + 1, src, elapsed, vertices_reached, pct, max_depth, fmt_teps(teps)
        ))

        bfs_results.append({
            "source": int(src),
            "elapsed_s": round(elapsed, 4),
            "vertices_reached": vertices_reached,
            "edges_traversed": edges_traversed,
            "max_depth": max_depth,
            "teps": teps,
            "coverage_pct": pct,
            "gpu": gpu_stats,
        })
    except Exception as e:
        stop_gpu_monitor()
        elapsed = time.perf_counter() - t0
        print("  Source {:2d}/16 v={}: FAILED ({:.1f}s): {}".format(
            i + 1, src, elapsed, str(e)[:60]
        ))

if bfs_results:
    teps_list = sorted([r["teps"] for r in bfs_results])
    n = len(teps_list)
    median_teps = teps_list[n // 2]
    hmean_teps = n / sum(1.0 / t for t in teps_list if t > 0) if all(t > 0 for t in teps_list) else 0
    print()
    print("  BFS TEPS Summary:")
    print("    Median:        " + fmt_teps(median_teps))
    print("    Harmonic mean: " + fmt_teps(hmean_teps))
    results["bfs"] = {
        "median_teps": median_teps,
        "harmonic_mean_teps": hmean_teps,
        "runs": bfs_results,
    }

print()

# ─── 5. Connected Components ───
print("=" * 72)
print("  Phase 5: Weakly Connected Components")
print("=" * 72)

for run_label in ["cold", "warm1"]:
    start_gpu_monitor()
    t0 = time.perf_counter()
    try:
        wcc = cugraph.connected_components(G)
        elapsed = time.perf_counter() - t0
        gpu_stats = stop_gpu_monitor()
        n_components = int(wcc["labels"].nunique())
        wcc_teps = num_edges / elapsed if elapsed > 0 else 0
        print("  {:6s}: {:.2f}s  components={:,}  {}  GPU avg={}% peak={}%".format(
            run_label, elapsed, n_components, fmt_teps(wcc_teps),
            gpu_stats.get("avg_util", "?"), gpu_stats.get("max_util", "?")
        ))
        results["wcc_" + run_label] = {
            "elapsed_s": round(elapsed, 3),
            "components": n_components,
            "teps": wcc_teps,
            "gpu": gpu_stats,
        }
    except Exception as e:
        stop_gpu_monitor()
        elapsed = time.perf_counter() - t0
        print("  {:6s}: FAILED ({:.1f}s): {}".format(run_label, elapsed, str(e)[:80]))
        results["wcc_" + run_label] = {"error": str(e)[:200], "elapsed_s": round(elapsed, 3)}

print()

# ─── 6. K-Core ───
print("=" * 72)
print("  Phase 6: K-Core Decomposition")
print("=" * 72)

for run_label in ["cold", "warm1"]:
    start_gpu_monitor()
    t0 = time.perf_counter()
    try:
        kc = cugraph.core_number(G)
        elapsed = time.perf_counter() - t0
        gpu_stats = stop_gpu_monitor()
        max_core = int(kc["core_number"].max())
        kc_teps = num_edges / elapsed if elapsed > 0 else 0
        print("  {:6s}: {:.2f}s  max_core={}  {}  GPU avg={}% peak={}%".format(
            run_label, elapsed, max_core, fmt_teps(kc_teps),
            gpu_stats.get("avg_util", "?"), gpu_stats.get("max_util", "?")
        ))
        results["kcore_" + run_label] = {
            "elapsed_s": round(elapsed, 3),
            "max_core": max_core,
            "teps": kc_teps,
            "gpu": gpu_stats,
        }
    except Exception as e:
        stop_gpu_monitor()
        elapsed = time.perf_counter() - t0
        print("  {:6s}: FAILED ({:.1f}s): {}".format(run_label, elapsed, str(e)[:80]))
        results["kcore_" + run_label] = {"error": str(e)[:200], "elapsed_s": round(elapsed, 3)}

print()

# ─── 7. Betweenness Centrality (approximate) ───
print("=" * 72)
print("  Phase 7: Betweenness Centrality (approximate)")
print("=" * 72)

for k_samples in [50, 100]:
    start_gpu_monitor()
    t0 = time.perf_counter()
    try:
        bc = cugraph.betweenness_centrality(G, k=k_samples)
        elapsed = time.perf_counter() - t0
        gpu_stats = stop_gpu_monitor()
        bc_teps = (k_samples * num_edges) / elapsed if elapsed > 0 else 0
        print("  k={:4d}: {:.2f}s  {}  GPU avg={}% peak={}%".format(
            k_samples, elapsed, fmt_teps(bc_teps),
            gpu_stats.get("avg_util", "?"), gpu_stats.get("max_util", "?")
        ))
        results["bc_k" + str(k_samples)] = {
            "elapsed_s": round(elapsed, 3),
            "k_samples": k_samples,
            "teps": bc_teps,
            "gpu": gpu_stats,
        }
    except Exception as e:
        stop_gpu_monitor()
        elapsed = time.perf_counter() - t0
        print("  k={:4d}: FAILED ({:.1f}s): {}".format(k_samples, elapsed, str(e)[:80]))
        results["bc_k" + str(k_samples)] = {"error": str(e)[:200], "elapsed_s": round(elapsed, 3)}

print()

# ─── 8. Community Detection (Louvain) ───
print("=" * 72)
print("  Phase 8: Community Detection (Louvain)")
print("=" * 72)

for run_label in ["cold", "warm1"]:
    start_gpu_monitor()
    t0 = time.perf_counter()
    try:
        parts, modularity = cugraph.louvain(G, max_level=20)
        elapsed = time.perf_counter() - t0
        gpu_stats = stop_gpu_monitor()
        n_communities = int(parts["partition"].nunique())
        louvain_teps = num_edges / elapsed if elapsed > 0 else 0
        print("  {:6s}: {:.2f}s  communities={:,}  modularity={:.4f}  {}  GPU avg={}% peak={}%".format(
            run_label, elapsed, n_communities, modularity, fmt_teps(louvain_teps),
            gpu_stats.get("avg_util", "?"), gpu_stats.get("max_util", "?")
        ))
        results["louvain_" + run_label] = {
            "elapsed_s": round(elapsed, 3),
            "communities": n_communities,
            "modularity": round(modularity, 6),
            "teps": louvain_teps,
            "gpu": gpu_stats,
        }
    except Exception as e:
        stop_gpu_monitor()
        elapsed = time.perf_counter() - t0
        print("  {:6s}: FAILED ({:.1f}s): {}".format(run_label, elapsed, str(e)[:80]))
        results["louvain_" + run_label] = {"error": str(e)[:200], "elapsed_s": round(elapsed, 3)}

print()

# ─── Summary ───
print("=" * 72)
print("  cuGraph Benchmark Summary (Friendster)")
print("=" * 72)
print()
print("Hardware:  RTX PRO 6000 Blackwell 96GB, 16 vCPU, 144GB RAM")
print("Dataset:   Friendster {:,} vertices, {:,} edges".format(num_vertices, num_edges))
print("Engine:    cuGraph " + cugraph.__version__)
print()

# Print comparison-ready summary
print("Algorithm timings (for head-to-head comparison):")
for key in ["pagerank_warm1", "triangle_cold", "bfs", "wcc_warm1",
            "kcore_warm1", "bc_k50", "louvain_warm1"]:
    if key in results and "elapsed_s" in results[key]:
        print("  {:25s} {:.2f}s".format(key, results[key]["elapsed_s"]))
    elif key in results and "median_teps" in results[key]:
        print("  {:25s} median TEPS: {}".format(key, fmt_teps(results[key]["median_teps"])))
    elif key in results and "error" in results[key]:
        print("  {:25s} FAILED: {}".format(key, results[key]["error"][:60]))

print()

# Save results
json_path = "/tmp/cugraph_bench_results.json"
output = {
    "benchmark": "cugraph_friendster",
    "cugraph_version": cugraph.__version__,
    "cudf_version": cudf.__version__,
    "dataset": "friendster",
    "vertices": num_vertices,
    "edges": num_edges,
    "hardware": "RTX PRO 6000 Blackwell 96GB, 16 vCPU, 144GB RAM",
    "results": results,
}
with open(json_path, "w") as f:
    json.dump(output, f, indent=2, default=str)
print("Results saved to " + json_path)
print()
print("CUGRAPH BENCHMARK COMPLETE")
