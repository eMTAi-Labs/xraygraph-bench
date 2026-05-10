#!/usr/bin/env python3
"""Kuzu benchmark — embedded graph DB on Friendster.

Kuzu 0.11.3, embedded graph database optimized for analytics.
Same hardware, same dataset, same algorithms.
"""
import time
import sys
import json
import os

sys.stdout.reconfigure(line_buffering=True)

import kuzu

print("=" * 72)
print("  Kuzu Benchmark (Friendster)")
print("  Embedded graph DB — same hardware, same dataset")
print("=" * 72)
print()
print("Kuzu:    " + kuzu.__version__)
print()

results = {}
FRIENDSTER = "/home/Ubuntu/datasets/com-friendster.ungraph.txt"
DB_PATH = "/tmp/kuzu_friendster"

# Clean previous
if os.path.exists(DB_PATH):
    import shutil
    shutil.rmtree(DB_PATH)

# ─── 1. Load ───
print("=" * 72)
print("  Phase 1: Load Friendster")
print("=" * 72)

db = kuzu.Database(DB_PATH)
conn = kuzu.Connection(db)

# Create schema
conn.execute("CREATE NODE TABLE Node(id INT64, PRIMARY KEY(id))")
conn.execute("CREATE REL TABLE KNOWS(FROM Node TO Node)")

# Kuzu can COPY FROM CSV — need to prepare files
# Nodes: one column of unique IDs
# Edges: two columns (src, dst)

# First create nodes CSV from SNAP file
print("  Creating node/edge CSVs for Kuzu...")
t0 = time.perf_counter()

# Kuzu needs separate node and edge files
# Edge file: just the tab-separated edges without comments
EDGE_FILE = "/tmp/kuzu_edges.csv"
NODE_FILE = "/tmp/kuzu_nodes.csv"

import numpy as np
print("  Reading SNAP file...")
data = np.loadtxt(FRIENDSTER, dtype=np.int64, comments="#")
t_read = time.perf_counter() - t0
print("  Read: {:.1f}s ({:,} edges)".format(t_read, len(data)))

# Write node file
print("  Writing node CSV...")
t0 = time.perf_counter()
unique_ids = np.unique(np.concatenate([data[:, 0], data[:, 1]]))
np.savetxt(NODE_FILE, unique_ids, fmt="%d")
t_nodes_csv = time.perf_counter() - t0
print("  Nodes: {:,} in {:.1f}s".format(len(unique_ids), t_nodes_csv))
del unique_ids

# Write edge file
print("  Writing edge CSV...")
t0 = time.perf_counter()
np.savetxt(EDGE_FILE, data, fmt="%d", delimiter=",")
t_edges_csv = time.perf_counter() - t0
print("  Edges: {:,} in {:.1f}s".format(len(data), t_edges_csv))
del data

# COPY into Kuzu
print("  COPY nodes into Kuzu...")
t0 = time.perf_counter()
conn.execute("COPY Node FROM '{}' (HEADER=false)".format(NODE_FILE))
t_copy_nodes = time.perf_counter() - t0
print("  Nodes COPY: {:.1f}s".format(t_copy_nodes))

print("  COPY edges into Kuzu...")
t0 = time.perf_counter()
conn.execute("COPY KNOWS FROM '{}' (HEADER=false)".format(EDGE_FILE))
t_copy_edges = time.perf_counter() - t0
print("  Edges COPY: {:.1f}s".format(t_copy_edges))

# Verify
result = conn.execute("MATCH (n) RETURN count(n)").get_next()
n_nodes = result[0]
result = conn.execute("MATCH ()-[r]->() RETURN count(r)").get_next()
n_edges = result[0]
print("  Verified: {:,} nodes, {:,} edges".format(n_nodes, n_edges))

t_total = t_read + t_nodes_csv + t_edges_csv + t_copy_nodes + t_copy_edges
print("  Total load: {:.1f}s".format(t_total))

results["load"] = {
    "read_s": round(t_read, 2),
    "nodes_csv_s": round(t_nodes_csv, 2),
    "edges_csv_s": round(t_edges_csv, 2),
    "copy_nodes_s": round(t_copy_nodes, 2),
    "copy_edges_s": round(t_copy_edges, 2),
    "total_s": round(t_total, 2),
    "nodes": n_nodes, "edges": n_edges,
}
print()

# ─── 2. Basic queries ───
print("=" * 72)
print("  Phase 2: Basic Queries")
print("=" * 72)

for name, q in [
    ("Node count", "MATCH (n) RETURN count(n)"),
    ("Edge count", "MATCH ()-[r]->() RETURN count(r)"),
]:
    t0 = time.perf_counter()
    result = conn.execute(q).get_next()
    elapsed = time.perf_counter() - t0
    print("  {:15s}: {:.3f}s  result={}".format(name, elapsed, result[0]))
    results[name.lower().replace(" ", "_")] = {"elapsed_s": round(elapsed, 3)}

print()

# ─── 3. BFS Hops ───
print("=" * 72)
print("  Phase 3: BFS Hops (Cypher variable-length paths)")
print("=" * 72)

# Find high-degree node
result = conn.execute(
    "MATCH (n)-[r]-() RETURN n.id, count(r) AS deg ORDER BY deg DESC LIMIT 1"
).get_next()
src_id = result[0]
src_deg = result[1]
print("  Source: id={}, degree={}".format(src_id, src_deg))

for hops in range(1, 8):
    t0 = time.perf_counter()
    try:
        result = conn.execute(
            "MATCH (a:Node {{id: {}}})-[:KNOWS*1..{}]-(b) RETURN count(b)".format(src_id, hops)
        ).get_next()
        elapsed = time.perf_counter() - t0
        paths = result[0]
        print("  {}-hop: {:.1f}s  paths={:,}".format(hops, elapsed, paths))
        results["bfs_" + str(hops)] = {"elapsed_s": round(elapsed, 3), "paths": paths}
    except Exception as e:
        elapsed = time.perf_counter() - t0
        print("  {}-hop: FAILED ({:.1f}s): {}".format(hops, elapsed, str(e)[:80]))
        results["bfs_" + str(hops)] = {"error": str(e)[:200], "elapsed_s": round(elapsed, 3)}
    if elapsed > 600:
        print("  STOPPING (>600s)")
        break

print()

# ─── 4. Shortest path ───
print("=" * 72)
print("  Phase 4: Shortest Path")
print("=" * 72)

t0 = time.perf_counter()
try:
    result = conn.execute(
        "MATCH p = shortestPath((a:Node {{id: {}}})-[:KNOWS*]-(b:Node {{id: 0}})) RETURN length(p)".format(src_id)
    ).get_next()
    elapsed = time.perf_counter() - t0
    print("  Shortest path: {:.1f}s  length={}".format(elapsed, result[0]))
    results["shortest_path"] = {"elapsed_s": round(elapsed, 3), "length": result[0]}
except Exception as e:
    elapsed = time.perf_counter() - t0
    print("  Shortest path: FAILED ({:.1f}s): {}".format(elapsed, str(e)[:80]))
    results["shortest_path"] = {"error": str(e)[:200], "elapsed_s": round(elapsed, 3)}

print()
print("=" * 72)
print("  Kuzu Benchmark Summary")
print("=" * 72)
print()
print("Engine:  Kuzu " + kuzu.__version__)
print("Nodes:   {:,}".format(n_nodes))
print("Edges:   {:,}".format(n_edges))
print()

json_path = "/tmp/kuzu_bench_results.json"
with open(json_path, "w") as f:
    json.dump({"benchmark": "kuzu_friendster", "version": kuzu.__version__, "results": results}, f, indent=2, default=str)
print("Results saved to " + json_path)
print()
print("KUZU BENCHMARK COMPLETE")
