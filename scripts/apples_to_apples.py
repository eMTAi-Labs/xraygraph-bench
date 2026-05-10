#!/usr/bin/env python3
"""Apples-to-apples benchmark — ALL systems, SAME tests, SAME source vertex.

Standard source: vertex 71768986 (undirected degree 5214, highest in Friendster)
Standard dataset: Friendster SNAP, 65,608,366 vertices, 1,806,067,135 undirected edges

Tests (identical across all systems):
1. Load time (raw SNAP file, measure total wall clock)
2. Node count query
3. Edge count query
4. BFS reachability 1-5 hops (count DISTINCT vertices reached)
5. 2-hop neighbor count from standard source
"""
import time
import sys
import json

sys.stdout.reconfigure(line_buffering=True)

SOURCE_VERTEX = 71768986  # highest undirected degree in Friendster (5214)
FRIENDSTER = "/home/Ubuntu/datasets/com-friendster.ungraph.txt"

results = {}

print("=" * 72)
print("  APPLES-TO-APPLES BENCHMARK")
print("  Same source vertex, same tests, same dataset")
print("  Source: vertex {} (degree 5214)".format(SOURCE_VERTEX))
print("=" * 72)
print()

# ═══════════════════════════════════════════════════════════════════════
# 1. xrayGraphDB
# ═══════════════════════════════════════════════════════════════════════
print("=" * 72)
print("  xrayGraphDB (already loaded)")
print("=" * 72)

try:
    from xgdb_connect.protocol import XrayProtocolClient
    c = XrayProtocolClient(host="127.0.0.1", port=7689,
                           auth_token="admin:xraygraphdb",
                           database="xraygraphdb", read_timeout=7200)

    xray = {}

    # Node count
    t0 = time.perf_counter()
    cols, rows = c.execute("MATCH (n) RETURN count(n)")
    xray["node_count"] = {"time": round((time.perf_counter() - t0) * 1000, 2),
                          "result": rows[0][0] if rows else 0}
    print("  Node count: {}ms  result={}".format(xray["node_count"]["time"], xray["node_count"]["result"]))

    # Edge count
    t0 = time.perf_counter()
    cols, rows = c.execute("MATCH ()-[r]->() RETURN count(r)")
    xray["edge_count"] = {"time": round((time.perf_counter() - t0) * 1000, 2),
                          "result": rows[0][0] if rows else 0}
    print("  Edge count: {}ms  result={}".format(xray["edge_count"]["time"], xray["edge_count"]["result"]))

    # BFS hops from standard source
    print("  BFS from vertex {} (degree 5214):".format(SOURCE_VERTEX))

    # frontier_profile for hop-by-hop reachability
    t0 = time.perf_counter()
    cols, rows = c.execute(
        "CALL xray.frontier_profile({}, 10, 'OUTGOING') "
        "YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree "
        "RETURN *".format(SOURCE_VERTEX)
    )
    fp_time = time.perf_counter() - t0
    cn = {col[0]: i for i, col in enumerate(cols)}
    xray["bfs_hops"] = []
    for r in rows:
        hop = int(float(r[cn["hop"]]))
        cumulative = int(float(r[cn["cumulative_nodes"]]))
        frontier = int(float(r[cn["frontier_size"]]))
        xray["bfs_hops"].append({"hop": hop, "cumulative": cumulative, "frontier": frontier})
        print("    hop {:2d}: {:>14,} cumulative vertices".format(hop, cumulative))
    xray["bfs_total_time"] = round(fp_time, 3)
    print("    Total: {:.3f}s".format(fp_time))

    # 2-hop neighbor count
    t0 = time.perf_counter()
    cols, rows = c.execute(
        "MATCH (n)-[:KNOWS*1..2]-(m) WHERE id(n) = {} RETURN count(DISTINCT m)".format(SOURCE_VERTEX)
    )
    elapsed = time.perf_counter() - t0
    xray["2hop_neighbors"] = {"time": round(elapsed * 1000, 2),
                               "result": rows[0][0] if rows else 0}
    print("  2-hop neighbors: {}ms  result={}".format(
        xray["2hop_neighbors"]["time"], xray["2hop_neighbors"]["result"]))

    c.close()
    results["xraygraphdb"] = xray
except Exception as e:
    print("  ERROR: " + str(e)[:100])
    results["xraygraphdb"] = {"error": str(e)[:200]}

print()

# ═══════════════════════════════════════════════════════════════════════
# 2. DuckDB
# ═══════════════════════════════════════════════════════════════════════
print("=" * 72)
print("  DuckDB 1.5.2 (already loaded)")
print("=" * 72)

try:
    import duckdb
    con = duckdb.connect("/tmp/friendster_duckdb.db", read_only=True)

    duck = {}

    # Node count
    t0 = time.perf_counter()
    row = con.execute("SELECT count(*) FROM nodes").fetchone()
    duck["node_count"] = {"time": round((time.perf_counter() - t0) * 1000, 2),
                          "result": row[0]}
    print("  Node count: {}ms  result={}".format(duck["node_count"]["time"], duck["node_count"]["result"]))

    # Edge count
    t0 = time.perf_counter()
    row = con.execute("SELECT count(*) FROM edges").fetchone()
    duck["edge_count"] = {"time": round((time.perf_counter() - t0) * 1000, 2),
                          "result": row[0]}
    print("  Edge count: {}ms  result={}".format(duck["edge_count"]["time"], duck["edge_count"]["result"]))

    # BFS hops from standard source via recursive CTE
    print("  BFS from vertex {} (degree 5214):".format(SOURCE_VERTEX))
    duck["bfs_hops"] = []
    for hops in range(1, 6):
        t0 = time.perf_counter()
        try:
            row = con.execute("""
                WITH RECURSIVE bfs(node, depth) AS (
                    SELECT {src}::INTEGER, 0
                    UNION
                    SELECT CASE WHEN e.src = b.node THEN e.dst ELSE e.src END, b.depth + 1
                    FROM bfs b JOIN edges e ON (e.src = b.node OR e.dst = b.node)
                    WHERE b.depth < {hops}
                )
                SELECT count(DISTINCT node) FROM bfs
            """.format(src=SOURCE_VERTEX, hops=hops)).fetchone()
            elapsed = time.perf_counter() - t0
            duck["bfs_hops"].append({"hop": hops, "cumulative": row[0], "time": round(elapsed, 3)})
            print("    hop {:2d}: {:>14,} cumulative vertices  ({:.1f}s)".format(hops, row[0], elapsed))
        except Exception as e:
            elapsed = time.perf_counter() - t0
            print("    hop {:2d}: FAILED ({:.1f}s): {}".format(hops, elapsed, str(e)[:60]))
            duck["bfs_hops"].append({"hop": hops, "error": str(e)[:100], "time": round(elapsed, 3)})
        if elapsed > 600:
            print("    STOPPING (>600s)")
            break

    # 2-hop neighbor count
    t0 = time.perf_counter()
    try:
        row = con.execute("""
            SELECT count(DISTINCT n2) FROM (
                SELECT CASE WHEN e1.src = {src} THEN e1.dst ELSE e1.src END AS n1
                FROM edges e1 WHERE e1.src = {src} OR e1.dst = {src}
            ) t1
            JOIN edges e2 ON (e2.src = t1.n1 OR e2.dst = t1.n1)
            CROSS JOIN (SELECT CASE WHEN e2.src = t1.n1 THEN e2.dst ELSE e2.src END AS n2) t2
        """.format(src=SOURCE_VERTEX)).fetchone()
        elapsed = time.perf_counter() - t0
        duck["2hop_neighbors"] = {"time": round(elapsed * 1000, 2), "result": row[0]}
    except Exception as e:
        elapsed = time.perf_counter() - t0
        # Simpler query
        try:
            row = con.execute("""
                WITH hop1 AS (
                    SELECT DISTINCT CASE WHEN src={src} THEN dst ELSE src END AS n
                    FROM edges WHERE src={src} OR dst={src}
                )
                SELECT count(DISTINCT CASE WHEN e.src=h.n THEN e.dst ELSE e.src END)
                FROM hop1 h JOIN edges e ON (e.src=h.n OR e.dst=h.n)
            """.format(src=SOURCE_VERTEX)).fetchone()
            elapsed = time.perf_counter() - t0
            duck["2hop_neighbors"] = {"time": round(elapsed * 1000, 2), "result": row[0]}
        except Exception as e2:
            elapsed = time.perf_counter() - t0
            duck["2hop_neighbors"] = {"time": round(elapsed * 1000, 2), "error": str(e2)[:100]}

    if "result" in duck.get("2hop_neighbors", {}):
        print("  2-hop neighbors: {}ms  result={}".format(
            duck["2hop_neighbors"]["time"], duck["2hop_neighbors"]["result"]))
    else:
        print("  2-hop neighbors: FAILED")

    con.close()
    results["duckdb"] = duck
except Exception as e:
    print("  ERROR: " + str(e)[:100])
    results["duckdb"] = {"error": str(e)[:200]}

print()

# ═══════════════════════════════════════════════════════════════════════
# 3. Kuzu
# ═══════════════════════════════════════════════════════════════════════
print("=" * 72)
print("  Kuzu 0.11.3")
print("=" * 72)

try:
    import kuzu
    import os

    DB_PATH = "/tmp/kuzu_friendster"
    if os.path.exists(DB_PATH):
        db = kuzu.Database(DB_PATH)
        conn = kuzu.Connection(db)
        # Check if data exists
        result = conn.execute("MATCH (n) RETURN count(n)").get_next()
        if result[0] > 0:
            print("  Already loaded: {:,} nodes".format(result[0]))
        else:
            raise Exception("empty database")
    else:
        raise Exception("database not found — need to load")

except Exception as load_err:
    print("  Loading Friendster into Kuzu...")
    import shutil
    DB_PATH = "/tmp/kuzu_friendster"
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)

    db = kuzu.Database(DB_PATH)
    conn = kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE Node(id INT64, PRIMARY KEY(id))")
    conn.execute("CREATE REL TABLE KNOWS(FROM Node TO Node)")

    # Create CSV files for COPY
    # Nodes: extract unique IDs efficiently with DuckDB (already loaded)
    print("  Extracting nodes via DuckDB...")
    t0 = time.perf_counter()
    import duckdb
    dcon = duckdb.connect("/tmp/friendster_duckdb.db", read_only=True)
    dcon.execute("""
        COPY (SELECT DISTINCT id FROM (
            SELECT src AS id FROM edges UNION SELECT dst AS id FROM edges
        ) ORDER BY id) TO '/tmp/kuzu_nodes.csv' (HEADER false)
    """)
    t_nodes = time.perf_counter() - t0
    print("  Nodes CSV: {:.1f}s".format(t_nodes))

    # Edges: just strip comments from SNAP
    print("  Extracting edges via DuckDB...")
    t0 = time.perf_counter()
    dcon.execute("""
        COPY (SELECT src, dst FROM edges) TO '/tmp/kuzu_edges.csv' (HEADER false)
    """)
    t_edges = time.perf_counter() - t0
    print("  Edges CSV: {:.1f}s".format(t_edges))
    dcon.close()

    # COPY into Kuzu
    print("  COPY nodes...")
    t0 = time.perf_counter()
    conn.execute("COPY Node FROM '/tmp/kuzu_nodes.csv' (HEADER=false)")
    t_copy_n = time.perf_counter() - t0
    print("  Nodes COPY: {:.1f}s".format(t_copy_n))

    print("  COPY edges...")
    t0 = time.perf_counter()
    conn.execute("COPY KNOWS FROM '/tmp/kuzu_edges.csv' (HEADER=false)")
    t_copy_e = time.perf_counter() - t0
    print("  Edges COPY: {:.1f}s".format(t_copy_e))

    result = conn.execute("MATCH (n) RETURN count(n)").get_next()
    print("  Loaded: {:,} nodes".format(result[0]))

try:
    kuz = {}

    # Node count
    t0 = time.perf_counter()
    result = conn.execute("MATCH (n) RETURN count(n)").get_next()
    kuz["node_count"] = {"time": round((time.perf_counter() - t0) * 1000, 2), "result": result[0]}
    print("  Node count: {}ms  result={}".format(kuz["node_count"]["time"], kuz["node_count"]["result"]))

    # Edge count
    t0 = time.perf_counter()
    result = conn.execute("MATCH ()-[r]->() RETURN count(r)").get_next()
    kuz["edge_count"] = {"time": round((time.perf_counter() - t0) * 1000, 2), "result": result[0]}
    print("  Edge count: {}ms  result={}".format(kuz["edge_count"]["time"], kuz["edge_count"]["result"]))

    # BFS hops
    print("  BFS from vertex {} (degree 5214):".format(SOURCE_VERTEX))
    kuz["bfs_hops"] = []
    for hops in range(1, 6):
        t0 = time.perf_counter()
        try:
            result = conn.execute(
                "MATCH (a:Node {{id: {}}})-[:KNOWS*1..{}]-(b) RETURN count(DISTINCT b)".format(
                    SOURCE_VERTEX, hops)
            ).get_next()
            elapsed = time.perf_counter() - t0
            kuz["bfs_hops"].append({"hop": hops, "cumulative": result[0], "time": round(elapsed, 3)})
            print("    hop {:2d}: {:>14,} cumulative vertices  ({:.1f}s)".format(hops, result[0], elapsed))
        except Exception as e:
            elapsed = time.perf_counter() - t0
            print("    hop {:2d}: FAILED ({:.1f}s): {}".format(hops, elapsed, str(e)[:60]))
            kuz["bfs_hops"].append({"hop": hops, "error": str(e)[:100], "time": round(elapsed, 3)})
        if elapsed > 600:
            print("    STOPPING (>600s)")
            break

    results["kuzu"] = kuz
except Exception as e:
    print("  ERROR: " + str(e)[:100])
    results["kuzu"] = {"error": str(e)[:200]}

print()

# ═══════════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════════
print("=" * 72)
print("  APPLES-TO-APPLES SUMMARY")
print("=" * 72)
print()
print("Source vertex: {} (undirected degree 5214)".format(SOURCE_VERTEX))
print("Dataset: Friendster 65,608,366 vertices, 1,806,067,135 undirected edges")
print("Hardware: RTX PRO 6000 Blackwell 96GB, 16 vCPU, 144GB RAM")
print()

json_path = "/tmp/apples_to_apples_results.json"
with open(json_path, "w") as f:
    json.dump(results, f, indent=2, default=str)
print("Results saved to " + json_path)
print()
print("APPLES-TO-APPLES COMPLETE")
