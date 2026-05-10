#!/usr/bin/env python3
"""Memgraph benchmark — fork parent comparison on Friendster.

Memgraph 2.22.0, in-memory graph database.
Same hardware, same dataset, same algorithms as xrayGraphDB.
"""
import time
import sys
import json

sys.stdout.reconfigure(line_buffering=True)

print("=" * 72)
print("  Memgraph Benchmark (Friendster)")
print("  Fork parent — same hardware, same dataset")
print("=" * 72)
print()

import mgclient

conn = mgclient.connect(host="127.0.0.1", port=7687)
conn.autocommit = True
cur = conn.cursor()

def run(q, timeout=7200):
    t0 = time.perf_counter()
    try:
        cur.execute(q)
        rows = cur.fetchall()
        elapsed = time.perf_counter() - t0
        return rows, elapsed, None
    except Exception as e:
        elapsed = time.perf_counter() - t0
        return [], elapsed, str(e)

results = {}

# ─── 1. Load Friendster ───
print("=" * 72)
print("  Phase 1: Load Friendster (SNAP edge list)")
print("=" * 72)
print()

# Check if data already loaded
rows, elapsed, err = run("MATCH (n) RETURN count(n)")
existing = rows[0][0] if rows else 0
print("  Existing nodes: {:,}".format(existing))

if existing < 1000:
    # Load via LOAD CSV or bulk — Memgraph uses LOAD CSV
    # But Friendster is tab-separated with comments — need preprocessing
    # Use Python bulk load via Bolt/mgclient
    import numpy as np

    print("  Loading via Python bulk insert...")
    t0 = time.perf_counter()

    print("  Reading SNAP file...")
    t_read = time.perf_counter()
    data = np.loadtxt("/home/Ubuntu/datasets/com-friendster.ungraph.txt",
                      dtype=np.int64, comments="#")
    t_read = time.perf_counter() - t_read
    n_edges = len(data)
    print("  Read: {:.1f}s ({:,} edges)".format(t_read, n_edges))

    # Create nodes first (unique vertex IDs)
    print("  Creating nodes...")
    t_nodes = time.perf_counter()
    unique_ids = set(data[:, 0].tolist() + data[:, 1].tolist())
    n_nodes = len(unique_ids)
    print("  Unique nodes: {:,}".format(n_nodes))

    # Batch create nodes
    BATCH = 50000
    id_list = sorted(unique_ids)
    del unique_ids
    created = 0
    for i in range(0, len(id_list), BATCH):
        batch = id_list[i:i+BATCH]
        params = [{"id": int(v)} for v in batch]
        try:
            cur.execute("UNWIND $batch AS p CREATE (:Node {id: p.id})",
                       {"batch": params})
            created += len(batch)
        except Exception as e:
            # Try one by one on failure
            for v in batch:
                try:
                    cur.execute("CREATE (:Node {id: $id})", {"id": int(v)})
                    created += 1
                except:
                    pass
        if created % 1000000 == 0 and created > 0:
            print("    nodes: {:,}...".format(created))
    t_nodes = time.perf_counter() - t_nodes
    print("  Nodes: {:,} in {:.1f}s".format(created, t_nodes))

    # Create index on id
    print("  Creating index...")
    run("CREATE INDEX ON :Node(id)")
    time.sleep(5)  # Wait for index to build

    # Create edges
    print("  Creating edges...")
    t_edges = time.perf_counter()
    edge_count = 0
    EDGE_BATCH = 10000
    for i in range(0, n_edges, EDGE_BATCH):
        batch_end = min(i + EDGE_BATCH, n_edges)
        batch_data = data[i:batch_end]
        params = [{"s": int(r[0]), "d": int(r[1])} for r in batch_data]
        try:
            cur.execute(
                "UNWIND $batch AS e "
                "MATCH (a:Node {id: e.s}), (b:Node {id: e.d}) "
                "CREATE (a)-[:KNOWS]->(b)",
                {"batch": params}
            )
            edge_count += len(params)
        except Exception as e:
            if edge_count == 0:
                print("  Edge batch error: " + str(e)[:100])
                break
        if edge_count % 1000000 == 0 and edge_count > 0:
            elapsed_so_far = time.perf_counter() - t_edges
            rate = int(edge_count / max(elapsed_so_far, 0.001))
            print("    edges: {:,}... ({:,}/s)".format(edge_count, rate))
        if edge_count > 0 and (time.perf_counter() - t_edges) > 7200:
            print("  STOPPING after 2 hours")
            break

    t_edges = time.perf_counter() - t_edges
    t_total = time.perf_counter() - t0
    del data

    rate = int(edge_count / max(t_edges, 0.001))
    print("  Edges: {:,} in {:.1f}s ({:,}/s)".format(edge_count, t_edges, rate))
    print("  Total load: {:.1f}s".format(t_total))

    results["load"] = {
        "nodes": created,
        "edges": edge_count,
        "read_s": round(t_read, 2),
        "nodes_s": round(t_nodes, 2),
        "edges_s": round(t_edges, 2),
        "total_s": round(t_total, 2),
        "rate_edges_per_s": rate,
    }
else:
    print("  Data already loaded: {:,} nodes".format(existing))
    rows, _, _ = run("MATCH ()-[r]->() RETURN count(r)")
    edge_count = rows[0][0] if rows else 0
    print("  Edges: {:,}".format(edge_count))
    results["load"] = {"nodes": existing, "edges": edge_count, "pre_loaded": True}

print()

# Verify counts
rows, elapsed, _ = run("MATCH (n) RETURN count(n)")
n_nodes = rows[0][0] if rows else 0
rows, elapsed, _ = run("MATCH ()-[r]->() RETURN count(r)")
n_edges = rows[0][0] if rows else 0
print("  Verified: {:,} nodes, {:,} edges".format(n_nodes, n_edges))
print()

if n_nodes < 1000:
    print("  NOT ENOUGH DATA — skipping algorithms")
    with open("/tmp/memgraph_bench_results.json", "w") as f:
        json.dump({"benchmark": "memgraph_friendster", "results": results}, f, indent=2)
    sys.exit(0)

# ─── 2. BFS Hops ───
print("=" * 72)
print("  Phase 2: BFS Hops (Cypher variable-length paths)")
print("=" * 72)

# Find a high-degree node
rows, _, _ = run("MATCH (n:Node)-[r]-() RETURN n.id, count(r) AS deg ORDER BY deg DESC LIMIT 1")
if rows:
    src_id = rows[0][0]
    src_deg = rows[0][1]
    print("  Source: id={}, degree={}".format(src_id, src_deg))
else:
    src_id = 0
    print("  Using source id=0")

for hops in range(1, 10):
    t0 = time.perf_counter()
    rows, elapsed, err = run(
        "MATCH (n:Node {{id: {}}})-[:KNOWS*1..{}]-(m) RETURN count(m)".format(src_id, hops),
    )
    if err:
        print("  {}-hop: FAILED ({:.1f}s): {}".format(hops, elapsed, err[:80]))
        results["bfs_" + str(hops)] = {"error": err[:200], "elapsed_s": round(elapsed, 3)}
        break
    paths = rows[0][0] if rows else 0
    print("  {}-hop: {:.1f}s  paths={:,}".format(hops, elapsed, paths))
    results["bfs_" + str(hops)] = {"elapsed_s": round(elapsed, 3), "paths": paths}
    if elapsed > 600:
        print("  STOPPING (>600s)")
        break

print()

# ─── 3. Node/Edge count timing ───
print("=" * 72)
print("  Phase 3: Basic queries")
print("=" * 72)

for label, q in [
    ("Node count", "MATCH (n) RETURN count(n)"),
    ("Edge count", "MATCH ()-[r]->() RETURN count(r)"),
]:
    rows, elapsed, err = run(q)
    result = rows[0][0] if rows else (err or "?")
    print("  {:15s}: {:.3f}s  result={}".format(label, elapsed, result))
    results[label.lower().replace(" ", "_")] = {"elapsed_s": round(elapsed, 3)}

print()

# ─── 4. MAGE algorithms (if available) ───
print("=" * 72)
print("  Phase 4: MAGE Analytics (if available)")
print("=" * 72)

for algo_name, query in [
    ("PageRank", "CALL pagerank.get() YIELD node, rank RETURN count(node)"),
    ("Community", "CALL community_detection.get() YIELD node, community_id RETURN count(node)"),
    ("Betweenness", "CALL betweenness_centrality.get() YIELD node, betweenness_centrality RETURN count(node)"),
]:
    t0 = time.perf_counter()
    rows, elapsed, err = run(query)
    if err:
        print("  {:15s}: NOT AVAILABLE ({:.1f}s): {}".format(algo_name, elapsed, err[:60]))
        results[algo_name.lower()] = {"error": err[:200], "elapsed_s": round(elapsed, 3)}
    else:
        result = rows[0][0] if rows else "?"
        print("  {:15s}: {:.2f}s  result={}".format(algo_name, elapsed, result))
        results[algo_name.lower()] = {"elapsed_s": round(elapsed, 3), "result": str(result)}

print()
print("=" * 72)
print("  Memgraph Benchmark Summary")
print("=" * 72)
print()
print("Engine:  Memgraph 2.22.0 (in-memory, CPU)")
print("Nodes:   {:,}".format(n_nodes))
print("Edges:   {:,}".format(n_edges))
print()

json_path = "/tmp/memgraph_bench_results.json"
with open(json_path, "w") as f:
    json.dump({"benchmark": "memgraph_friendster", "results": results}, f, indent=2, default=str)
print("Results saved to " + json_path)
print()
print("MEMGRAPH BENCHMARK COMPLETE")
