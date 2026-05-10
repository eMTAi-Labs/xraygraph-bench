#!/usr/bin/env python3
"""DuckDB benchmark — embedded analytical engine on Friendster.

DuckDB 1.5.2 with SQL graph queries.
Same hardware, same dataset, same workload class where applicable.
"""
import time
import sys
import json

sys.stdout.reconfigure(line_buffering=True)

import duckdb

print("=" * 72)
print("  DuckDB Benchmark (Friendster)")
print("  Embedded analytical engine — same hardware, same dataset")
print("=" * 72)
print()
print("DuckDB:  " + duckdb.__version__)
print()

results = {}
FRIENDSTER = "/home/Ubuntu/datasets/com-friendster.ungraph.txt"

# ─── 1. Load ───
print("=" * 72)
print("  Phase 1: Load Friendster")
print("=" * 72)

con = duckdb.connect("/tmp/friendster_duckdb.db")

# DuckDB can read CSV directly — extremely fast
print("  Loading SNAP edge list via read_csv...")
t0 = time.perf_counter()
con.execute("""
    CREATE OR REPLACE TABLE edges AS
    SELECT column0 AS src, column1 AS dst
    FROM read_csv('{}',
        delim='\t', header=false, comment='#',
        columns={{'column0': 'INTEGER', 'column1': 'INTEGER'}},
        auto_detect=false, quote='', escape='')
""".format(FRIENDSTER))
t_load = time.perf_counter() - t0

row = con.execute("SELECT count(*) FROM edges").fetchone()
n_edges = row[0]
print("  Load:    {:.1f}s ({:,} edges)".format(t_load, n_edges))

# Create node table from unique IDs
print("  Building node table...")
t0 = time.perf_counter()
con.execute("""
    CREATE OR REPLACE TABLE nodes AS
    SELECT DISTINCT id FROM (
        SELECT src AS id FROM edges
        UNION
        SELECT dst AS id FROM edges
    )
""")
t_nodes = time.perf_counter() - t0
row = con.execute("SELECT count(*) FROM nodes").fetchone()
n_nodes = row[0]
print("  Nodes:   {:.1f}s ({:,} unique)".format(t_nodes, n_nodes))

# Create indexes
print("  Creating indexes...")
t0 = time.perf_counter()
con.execute("CREATE INDEX IF NOT EXISTS idx_src ON edges(src)")
con.execute("CREATE INDEX IF NOT EXISTS idx_dst ON edges(dst)")
t_idx = time.perf_counter() - t0
print("  Indexes: {:.1f}s".format(t_idx))

t_total = t_load + t_nodes + t_idx
print("  Total:   {:.1f}s".format(t_total))

results["load"] = {
    "edges": n_edges, "nodes": n_nodes,
    "load_s": round(t_load, 2), "nodes_s": round(t_nodes, 2),
    "index_s": round(t_idx, 2), "total_s": round(t_total, 2),
}
print()

# ─── 2. Basic queries ───
print("=" * 72)
print("  Phase 2: Basic SQL Queries")
print("=" * 72)

queries = [
    ("Edge count", "SELECT count(*) FROM edges"),
    ("Node count", "SELECT count(*) FROM nodes"),
    ("Degree dist (top 5)", "SELECT src, count(*) AS deg FROM edges GROUP BY src ORDER BY deg DESC LIMIT 5"),
    ("Avg degree", "SELECT avg(deg) FROM (SELECT src, count(*) AS deg FROM edges GROUP BY src)"),
]

for name, q in queries:
    times = []
    result = None
    for i in range(3):
        t0 = time.perf_counter()
        try:
            rows = con.execute(q).fetchall()
            elapsed = time.perf_counter() - t0
            times.append(elapsed)
            if result is None:
                result = str(rows[0]) if rows else "?"
        except Exception as e:
            elapsed = time.perf_counter() - t0
            times.append(elapsed)
            result = "ERROR: " + str(e)[:60]
    median = sorted(times)[1]
    print("  {:25s}: {:.3f}s  result={}".format(name, median, result))
    results[name.lower().replace(" ", "_")] = {"median_s": round(median, 3), "result": result}

print()

# ─── 3. BFS via recursive CTE ───
print("=" * 72)
print("  Phase 3: BFS via Recursive CTE")
print("=" * 72)

# Find high-degree node
row = con.execute("SELECT src, count(*) AS deg FROM edges GROUP BY src ORDER BY deg DESC LIMIT 1").fetchone()
src_id = row[0]
src_deg = row[1]
print("  Source: id={}, degree={}".format(src_id, src_deg))

for hops in range(1, 6):
    t0 = time.perf_counter()
    try:
        rows = con.execute("""
            WITH RECURSIVE bfs(node, depth) AS (
                SELECT {src}::INTEGER, 0
                UNION
                SELECT e.dst, b.depth + 1
                FROM bfs b JOIN edges e ON e.src = b.node
                WHERE b.depth < {hops}
            )
            SELECT count(DISTINCT node) FROM bfs
        """.format(src=src_id, hops=hops)).fetchall()
        elapsed = time.perf_counter() - t0
        result = rows[0][0] if rows else 0
        print("  {}-hop: {:.1f}s  vertices={:,}".format(hops, elapsed, result))
        results["bfs_" + str(hops)] = {"elapsed_s": round(elapsed, 3), "vertices": result}
    except Exception as e:
        elapsed = time.perf_counter() - t0
        print("  {}-hop: FAILED ({:.1f}s): {}".format(hops, elapsed, str(e)[:80]))
        results["bfs_" + str(hops)] = {"error": str(e)[:200], "elapsed_s": round(elapsed, 3)}
    if elapsed > 600:
        print("  STOPPING (>600s)")
        break

print()

# ─── 4. Analytical queries ───
print("=" * 72)
print("  Phase 4: Analytical Queries")
print("=" * 72)

analytics = [
    ("Triangle count (SQL)", """
        SELECT count(*) FROM edges e1
        JOIN edges e2 ON e1.dst = e2.src
        JOIN edges e3 ON e2.dst = e3.src AND e3.dst = e1.src
    """),
    ("2-hop neighbor count", """
        SELECT count(DISTINCT e2.dst)
        FROM edges e1 JOIN edges e2 ON e1.dst = e2.src
        WHERE e1.src = {}
    """.format(src_id)),
]

for name, q in analytics:
    t0 = time.perf_counter()
    try:
        rows = con.execute(q).fetchall()
        elapsed = time.perf_counter() - t0
        result = rows[0][0] if rows else "?"
        print("  {:30s}: {:.1f}s  result={}".format(name, elapsed, result))
        results[name.lower().replace(" ", "_")] = {"elapsed_s": round(elapsed, 3), "result": str(result)}
    except Exception as e:
        elapsed = time.perf_counter() - t0
        print("  {:30s}: FAILED ({:.1f}s): {}".format(name, elapsed, str(e)[:80]))
        results[name.lower().replace(" ", "_")] = {"error": str(e)[:200], "elapsed_s": round(elapsed, 3)}
    if elapsed > 3600:
        print("  STOPPING")
        break

print()

con.close()

print("=" * 72)
print("  DuckDB Benchmark Summary")
print("=" * 72)
print()

json_path = "/tmp/duckdb_bench_results.json"
with open(json_path, "w") as f:
    json.dump({"benchmark": "duckdb_friendster", "version": duckdb.__version__, "results": results}, f, indent=2, default=str)
print("Results saved to " + json_path)
print()
print("DUCKDB BENCHMARK COMPLETE")
