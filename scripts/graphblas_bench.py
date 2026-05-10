#!/usr/bin/env python3
"""GraphBLAS/LAGraph benchmark — CPU graph analytics baseline.

Same Friendster dataset, same algorithms as xrayGraphDB and cuGraph.
GraphBLAS is pure CPU (OpenMP-parallel, no GPU).
This is the "best possible CPU" baseline.

SuiteSparse:GraphBLAS 9.4.5, python-graphblas 2025.2.0
graphblas-algorithms 2023.10.0
"""
import time
import sys
import json
import os
import gc

sys.stdout.reconfigure(line_buffering=True)

print("=" * 72)
print("  GraphBLAS/LAGraph Benchmark (Friendster)")
print("  CPU-only baseline — same hardware, same dataset")
print("=" * 72)
print()

import graphblas as gb

# Must init before using any gb objects
ncpu = os.cpu_count()
# Auto-initialized on import; set nthreads
print("GraphBLAS:      " + str(gb.__version__))

import graphblas_algorithms as gba
import numpy as np

print("Algorithms:     " + str(gba.__version__))

# System info
import platform
print("CPU:            " + platform.processor() + " (" + str(ncpu) + " vCPU)")
ram_gb = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / (1024**3)
print("RAM:            " + str(round(ram_gb)) + " GB")
print("GPU:            NOT USED (CPU-only library)")

nthreads = ncpu
print("Threads:        " + str(nthreads))
print()

# Dataset
FRIENDSTER = "/home/Ubuntu/datasets/com-friendster.ungraph.txt"
if not os.path.exists(FRIENDSTER):
    print("ERROR: Friendster not found at " + FRIENDSTER)
    sys.exit(1)

fsize = os.path.getsize(FRIENDSTER) / (1024**3)
print("Dataset:        " + FRIENDSTER + " ({:.1f} GB)".format(fsize))
print()

results = {}

# ─── 1. Load graph ───
print("=" * 72)
print("  Phase 1: Graph Construction")
print("=" * 72)
print("  Loading SNAP edge list → scipy sparse → GraphBLAS Matrix")
print()

t0 = time.perf_counter()

# Step 1: numpy read (same as cuGraph benchmark for fair comparison)
print("  Step 1: numpy CPU read...")
t_read_start = time.perf_counter()
data = np.loadtxt(FRIENDSTER, dtype=np.int64, comments='#')
t_read = time.perf_counter() - t_read_start
n_edges = len(data)
print("  Read: {:.1f}s ({:,} edges)".format(t_read, n_edges))

# Step 2: Build scipy sparse → GraphBLAS
# Use int32 indices and build directed first to save memory,
# then let GraphBLAS handle symmetry.
print("  Step 2: scipy CSR → GraphBLAS Matrix...")
t_build_start = time.perf_counter()

import scipy.sparse as sp

max_id = max(int(data[:, 0].max()), int(data[:, 1].max())) + 1
print("  Max vertex ID: {:,} → matrix {:,}x{:,}".format(max_id - 1, max_id, max_id))

# Build directed CSR first (1.8B entries, not 3.6B) to save memory
# int32 indices save 50% vs int64
src = data[:, 0].astype(np.int32)
dst = data[:, 1].astype(np.int32)
vals = np.ones(len(src), dtype=np.float64)
del data
gc.collect()

A_scipy = sp.csr_matrix((vals, (src, dst)), shape=(max_id, max_id))
del src, dst, vals
gc.collect()
t_scipy = time.perf_counter() - t_build_start
print("  scipy CSR (directed): {:.1f}s, nnz={:,}".format(t_scipy, A_scipy.nnz))

# Convert to GraphBLAS
t_gb_start = time.perf_counter()
A = gb.io.from_scipy_sparse(A_scipy)
del A_scipy
gc.collect()

# Make symmetric: A = A + A^T (undirected)
print("  Symmetrizing in GraphBLAS (A + A^T)...")
AT = A.T.new()
A_sym = A.ewise_add(AT, gb.binary.plus).new()
del A, AT
gc.collect()
A = A_sym
t_gb = time.perf_counter() - t_gb_start

t_total = time.perf_counter() - t0

print("  GraphBLAS Matrix: {:.1f}s".format(t_gb))
print("  ----------")
print("  Total load:    {:.1f}s".format(t_total))
print("  Dimensions:    {:,} x {:,}".format(A.nrows, A.ncols))
print("  Entries:       {:,} (symmetric)".format(A.nvals))
print()

# Build graph object for algorithms
G = gba.Graph(A)

results["load"] = {
    "method": "numpy → scipy COO → CSR → GraphBLAS Matrix",
    "cpu_read_s": round(t_read, 2),
    "scipy_build_s": round(t_scipy, 2),
    "graphblas_convert_s": round(t_gb, 2),
    "total_s": round(t_total, 2),
    "matrix_size": max_id,
    "entries": A.nvals,
    "edges_raw": n_edges,
}

def fmt_teps(t):
    if t >= 1e9:
        return "{:.2f} GTEPS".format(t / 1e9)
    elif t >= 1e6:
        return "{:.2f} MTEPS".format(t / 1e6)
    else:
        return "{:.0f} TEPS".format(t)

num_edges = A.nvals

# ─── 2. BFS ───
print("=" * 72)
print("  Phase 2: BFS (3 sources, hop-by-hop)")
print("=" * 72)

# Pick source vertices — use highest-degree vertices
print("  Finding high-degree sources...")
t0 = time.perf_counter()
degrees = A.reduce_rowwise(gb.monoid.plus).new()
deg_vals = degrees.to_dense(fill_value=0)
top_indices = np.argsort(deg_vals)[-5:][::-1]
sources = [int(idx) for idx in top_indices if deg_vals[idx] > 0][:3]
print("  Sources: " + str(sources) + " (degree: " + str([int(deg_vals[s]) for s in sources]) + ")")
print("  Source selection: {:.1f}s".format(time.perf_counter() - t0))

bfs_results = []
for i, src in enumerate(sources):
    t0 = time.perf_counter()
    try:
        layers = list(gba.bfs_layers(G, src))
        elapsed = time.perf_counter() - t0

        # Count vertices reached from layers
        reached = sum(len(layer) if hasattr(layer, '__len__') else layer.nvals for layer in layers)
        coverage = reached / max_id
        edges_traversed = int(num_edges * coverage)
        teps = edges_traversed / elapsed if elapsed > 0 else 0

        print("  Source {:d}/{:d} v={}: {:.2f}s, {:,} vertices ({:.1f}%), {} depth={}".format(
            i + 1, len(sources), src, elapsed, reached,
            coverage * 100, fmt_teps(teps), len(layers)
        ))

        bfs_results.append({
            "source": src,
            "elapsed_s": round(elapsed, 3),
            "vertices_reached": reached,
            "teps": teps,
            "coverage_pct": round(coverage * 100, 1),
        })
    except Exception as e:
        elapsed = time.perf_counter() - t0
        print("  Source {:d}/{:d} v={}: FAILED ({:.1f}s): {}".format(
            i + 1, len(sources), src, elapsed, str(e)[:80]
        ))

if bfs_results:
    teps_list = [r["teps"] for r in bfs_results]
    median = sorted(teps_list)[len(teps_list) // 2]
    print("  Median TEPS: " + fmt_teps(median))
    results["bfs"] = {"runs": bfs_results, "median_teps": median}

print()

# ─── 3. PageRank ───
print("=" * 72)
print("  Phase 3: PageRank (20 iterations, damping=0.85)")
print("=" * 72)

for label in ["cold", "warm1", "warm2"]:
    t0 = time.perf_counter()
    try:
        pr = gba.pagerank(G, alpha=0.85, max_iter=20, tol=0.0)
        elapsed = time.perf_counter() - t0
        pr_teps = (20 * num_edges) / elapsed
        print("  {:6s}: {:.2f}s  {}".format(label, elapsed, fmt_teps(pr_teps)))
        results["pagerank_" + label] = {"elapsed_s": round(elapsed, 3), "teps": pr_teps}
    except Exception as e:
        elapsed = time.perf_counter() - t0
        print("  {:6s}: FAILED ({:.1f}s): {}".format(label, elapsed, str(e)[:80]))
        results["pagerank_" + label] = {"error": str(e)[:200], "elapsed_s": round(elapsed, 3)}

print()

# ─── 4. Triangle Count ───
print("=" * 72)
print("  Phase 4: Triangle Count")
print("=" * 72)

for label in ["cold", "warm1"]:
    t0 = time.perf_counter()
    try:
        tc = gba.total_triangles(G)
        elapsed = time.perf_counter() - t0
        print("  {:6s}: {:.2f}s  triangles={:,}".format(label, elapsed, tc))
        results["triangle_" + label] = {"elapsed_s": round(elapsed, 3), "triangles": tc}
    except Exception as e:
        elapsed = time.perf_counter() - t0
        print("  {:6s}: FAILED ({:.1f}s): {}".format(label, elapsed, str(e)[:80]))
        results["triangle_" + label] = {"error": str(e)[:200], "elapsed_s": round(elapsed, 3)}

print()

# ─── 5. Connected Components (WCC) ───
print("=" * 72)
print("  Phase 5: Connected Components")
print("=" * 72)

for label in ["cold", "warm1"]:
    t0 = time.perf_counter()
    try:
        cc_result = gba.is_connected(G)
        elapsed = time.perf_counter() - t0
        print("  {:6s}: {:.2f}s  connected={}".format(label, elapsed, cc_result))
        results["wcc_" + label] = {"elapsed_s": round(elapsed, 3), "connected": bool(cc_result)}
    except Exception as e:
        elapsed = time.perf_counter() - t0
        print("  {:6s}: FAILED ({:.1f}s): {}".format(label, elapsed, str(e)[:80]))
        results["wcc_" + label] = {"error": str(e)[:200], "elapsed_s": round(elapsed, 3)}

print()

# ─── 6. Betweenness Centrality ───
print("=" * 72)
print("  Phase 6: Katz Centrality")
print("=" * 72)

t0 = time.perf_counter()
try:
    kc = gba.katz_centrality(G)
    elapsed = time.perf_counter() - t0
    print("  katz: {:.2f}s".format(elapsed))
    results["katz"] = {"elapsed_s": round(elapsed, 3)}
except Exception as e:
    elapsed = time.perf_counter() - t0
    print("  katz: FAILED ({:.1f}s): {}".format(elapsed, str(e)[:80]))
    results["katz"] = {"error": str(e)[:200], "elapsed_s": round(elapsed, 3)}

print()

# ─── Summary ───
print("=" * 72)
print("  GraphBLAS Benchmark Summary (Friendster)")
print("=" * 72)
print()
print("Engine:    SuiteSparse:GraphBLAS " + str(gb.__version__) + " (CPU-only, " + str(ncpu) + " threads)")
print("Dataset:   Friendster {:,} edges (symmetric)".format(num_edges))
print("Hardware:  16 vCPU, 144 GB RAM (GPU NOT USED)")
print()

json_path = "/tmp/graphblas_bench_results.json"
output = {
    "benchmark": "graphblas_friendster",
    "graphblas_version": gb.__version__,
    "algorithms_version": gba.__version__,
    "dataset": "friendster",
    "entries": num_edges,
    "hardware": "16 vCPU, 144 GB RAM, CPU-only (no GPU)",
    "results": results,
}
with open(json_path, "w") as f:
    json.dump(output, f, indent=2, default=str)
print("Results saved to " + json_path)
print()
print("GRAPHBLAS BENCHMARK COMPLETE")
