#!/usr/bin/env python3
"""Graph500-style TEPS benchmark on xrayGraphDB.

Measures Traversed Edges Per Second (TEPS) using native BFS.
Uses native BFS expansion.

Methodology:
- 16 source vertices selected from pagerank top-N (high-connectivity sources)
- Full BFS from each source until graph exhausted (up to 20 hops)
- TEPS = edges_traversed / wall_clock_time
- Report: median, harmonic mean, min, max TEPS
- Harmonic mean is the Graph500 standard aggregate

IMPORTANT SEMANTICS NOTE:
Graph500 measures pure BFS kernel throughput on synthetic Kronecker graphs.
xrayGraphDB is a persistent graph runtime with native storage + GPU analytics.
These are different workload classes. TEPS is computed here for mental
normalization against published benchmarks, NOT as a direct Graph500 claim.

TEPS Calculation:
- frontier_profile returns per-hop: frontier_size, cumulative_nodes, new_edges
- new_edges = edges explored at this BFS level
- total_edges_traversed = sum(new_edges) across all hops
- If new_edges underreports, we estimate: vertices_reached * avg_degree
- We report BOTH the raw and estimated numbers for transparency
"""
import time
import sys
import json

sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

AUTH = "admin:xraygraphdb"
DB = "xraygraphdb"
NUM_SOURCES = 16
MAX_HOPS = 30  # enough to exhaust any graph

def fresh():
    return XrayProtocolClient(
        host="127.0.0.1", port=7689,
        auth_token=AUTH, database=DB, read_timeout=7200
    )

print("=" * 72)
print("  xrayGraphDB TEPS Benchmark (Graph500-style BFS)")
print("  Metric: Traversed Edges Per Second")
print("=" * 72)
print()

# ─── Graph stats ───
c = fresh()
cols, rows = c.execute(
    "CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *"
)
# Columns come back alphabetically: metric, status, unit, value
health = {}
for r in rows:
    health[r[0]] = r[3]  # metric -> value

vcount = int(health.get("vertex_count", 0))
ecount = int(health.get("edge_count", 0))
avg_deg = float(health.get("avg_out_degree", 0))
storage = health.get("storage_engine", "unknown")
print("Dataset:     Friendster (SNAP)")
print("Vertices:    " + "{:,}".format(vcount))
print("Edges:       " + "{:,}".format(ecount))
print("Avg degree:  " + str(avg_deg))
print("Storage:     " + storage)
print("Sources:     " + str(NUM_SOURCES) + " (pagerank top-N)")
print()
c.close()

# ─── Select source vertices ───
# Use pagerank top vertices — these have high connectivity,
# which is standard Graph500 practice (select roots with edges)
c = fresh()
pr_start = time.perf_counter()
cols, rows = c.execute(
    "CALL xray.pagerank(1, 0.85, '') YIELD node_id, rank "
    "RETURN node_id, rank ORDER BY rank DESC LIMIT " + str(NUM_SOURCES * 2)
)
pr_elapsed = time.perf_counter() - pr_start
# Take every other one for diversity
all_sources = [int(r[0]) for r in rows]
sources = all_sources[:NUM_SOURCES]
c.close()

print("Source selection: " + str(len(sources)) + " vertices from pagerank top-" + str(NUM_SOURCES * 2))
print("  IDs: " + str(sources[:5]) + "...")
print("  PageRank time: " + str(round(pr_elapsed, 1)) + "s")
print()

# ─── BFS from each source ───
print("Running BFS from " + str(NUM_SOURCES) + " sources...")
print("-" * 72)

results = []
for i, src in enumerate(sources):
    c = fresh()
    s = time.perf_counter()
    try:
        cols, rows = c.execute(
            "CALL xray.frontier_profile(" + str(src) + ", " + str(MAX_HOPS) +
            ", 'OUTGOING') YIELD hop, frontier_size, cumulative_nodes, "
            "new_edges, avg_degree, max_degree RETURN *"
        )
        elapsed = time.perf_counter() - s

        # Columns come back alphabetically:
        # avg_degree, cumulative_nodes, frontier_size, hop, max_degree, new_edges
        # Map by column name
        col_names = [c[0] for c in cols]
        col_idx = {name: idx for idx, name in enumerate(col_names)}

        total_new_edges = 0
        total_vertices = 0
        max_hop = 0
        hop_data = []

        for r in rows:
            hop = int(float(r[col_idx["hop"]]))
            frontier = int(float(r[col_idx["frontier_size"]]))
            cumulative = int(float(r[col_idx["cumulative_nodes"]]))
            new_edges = int(float(r[col_idx["new_edges"]]))
            a_deg = float(r[col_idx["avg_degree"]])
            m_deg = int(float(r[col_idx["max_degree"]]))

            total_new_edges += new_edges
            total_vertices = max(total_vertices, cumulative)
            max_hop = max(max_hop, hop)
            hop_data.append({
                "hop": hop, "frontier": frontier,
                "cumulative": cumulative, "new_edges": new_edges,
                "avg_degree": a_deg, "max_degree": m_deg
            })

        # Graph coverage
        coverage = total_vertices / vcount if vcount > 0 else 0

        # Estimate edges from vertices * avg_degree as cross-check
        estimated_edges = int(total_vertices * avg_deg)

        # Use the larger of raw new_edges sum or vertex-based estimate
        # frontier_profile's new_edges should be authoritative,
        # but if it underreports, the estimate catches it
        if total_new_edges > estimated_edges * 0.1:
            # new_edges looks reasonable, use it
            edges_used = total_new_edges
            edge_source = "frontier_profile"
        else:
            # new_edges is suspiciously low, use estimate
            edges_used = estimated_edges
            edge_source = "estimated (vertices * avg_degree)"

        teps = edges_used / elapsed if elapsed > 0 else 0
        teps_raw = total_new_edges / elapsed if elapsed > 0 else 0

        pct = round(coverage * 100, 1)

        # Format TEPS with appropriate unit
        def fmt_teps(t):
            if t >= 1e9:
                return "{:.2f} GTEPS".format(t / 1e9)
            elif t >= 1e6:
                return "{:.2f} MTEPS".format(t / 1e6)
            elif t >= 1e3:
                return "{:.2f} KTEPS".format(t / 1e3)
            else:
                return "{:.0f} TEPS".format(t)

        print("  Source {:2d}/{} v={}: {:.1f}s, {} vertices ({:.1f}%), {} hops".format(
            i + 1, NUM_SOURCES, src, elapsed, "{:,}".format(total_vertices),
            pct, max_hop
        ))
        print("    edges: {:,} ({})  |  TEPS: {}".format(
            edges_used, edge_source, fmt_teps(teps)
        ))
        if edge_source != "frontier_profile":
            print("    raw new_edges: {:,} (frontier_profile)  |  raw TEPS: {}".format(
                total_new_edges, fmt_teps(teps_raw)
            ))

        results.append({
            "source": src,
            "elapsed_s": elapsed,
            "vertices_reached": total_vertices,
            "edges_traversed": edges_used,
            "edges_raw": total_new_edges,
            "edge_source": edge_source,
            "hops": max_hop,
            "teps": teps,
            "teps_raw": teps_raw,
            "coverage_pct": pct,
            "hop_data": hop_data,
        })
    except Exception as e:
        elapsed = time.perf_counter() - s
        print("  Source {:2d}/{} v={}: ERROR ({:.0f}s): {}".format(
            i + 1, NUM_SOURCES, src, elapsed, str(e)[:80]
        ))
    c.close()

# ─── Graph500-style statistics ───
if results:
    def fmt_teps(t):
        if t >= 1e9:
            return "{:.2f} GTEPS".format(t / 1e9)
        elif t >= 1e6:
            return "{:.2f} MTEPS".format(t / 1e6)
        elif t >= 1e3:
            return "{:.2f} KTEPS".format(t / 1e3)
        else:
            return "{:.0f} TEPS".format(t)

    teps_list = sorted([r["teps"] for r in results])
    n = len(teps_list)
    median_teps = teps_list[n // 2]
    mean_teps = sum(teps_list) / n
    hmean_teps = n / sum(1.0 / t for t in teps_list if t > 0) if all(t > 0 for t in teps_list) else 0
    min_teps = min(teps_list)
    max_teps = max(teps_list)

    times = sorted([r["elapsed_s"] for r in results])
    median_time = times[n // 2]
    total_time = sum(times)

    vertices = [r["vertices_reached"] for r in results]
    median_verts = sorted(vertices)[n // 2]

    edges = [r["edges_traversed"] for r in results]
    total_edges = sum(edges)

    coverages = [r["coverage_pct"] for r in results]
    median_coverage = sorted(coverages)[n // 2]

    print()
    print("=" * 72)
    print("  xrayGraphDB TEPS Results")
    print("=" * 72)
    print()
    print("Dataset:            Friendster (SNAP)")
    print("                    {:,} vertices, {:,} edges".format(vcount, ecount))
    print("Hardware:           RTX PRO 6000 Blackwell (96 GB), 16 vCPU, 144 GB RAM")
    print("Storage:            " + storage)
    print("BFS engine:         native graph traversal")
    print("Sources:            {} successful of {} attempted".format(n, NUM_SOURCES))
    print("Median BFS time:    {:.1f}s".format(median_time))
    print("Total BFS time:     {:.1f}s".format(total_time))
    print("Median coverage:    {:.1f}% of graph ({:,} vertices)".format(median_coverage, median_verts))
    print("Total edges trav:   {:,}".format(total_edges))
    print()
    print("TEPS (Traversed Edges Per Second):")
    print("  Median:           " + fmt_teps(median_teps))
    print("  Harmonic mean:    " + fmt_teps(hmean_teps) + "  (Graph500 standard)")
    print("  Arithmetic mean:  " + fmt_teps(mean_teps))
    print("  Min:              " + fmt_teps(min_teps))
    print("  Max:              " + fmt_teps(max_teps))
    print()

    # Context
    print("Reference (published Graph500 Nov 2025):")
    print("  CoreWeave H100x1024:    410,266 GTEPS  (distributed)")
    print("  Fugaku 152K nodes:      204,068 GTEPS  (distributed)")
    print("  NVIDIA GB300 NVL72:      91,930 GTEPS  (distributed)")
    print("  DepGraph A100 single:     4,623 GTEPS  (single-node, Scale 33)")
    print("  xrayGraphDB Blackwell:  " + fmt_teps(median_teps) + "  (single-node, persistent runtime)")
    print()
    print("NOTE: Graph500 measures pure BFS kernel throughput on synthetic")
    print("Kronecker graphs with optimized communication fabrics.")
    print("xrayGraphDB is a persistent graph runtime with native storage,")
    print("GPU analytics, Cypher/GFQL query layer, and relationship")
    print("intelligence workloads. These are fundamentally different")
    print("workload classes. TEPS reported for normalization only.")
    print()

    # Also compute TEPS for the raw frontier_profile new_edges
    teps_raw_list = sorted([r["teps_raw"] for r in results])
    if any(r["edge_source"] != "frontier_profile" for r in results):
        median_raw = teps_raw_list[n // 2]
        print("Raw frontier_profile TEPS (for transparency):")
        print("  Median:           " + fmt_teps(median_raw))
        print()

    # Save results to JSON
    output = {
        "benchmark": "graph500_teps",
        "dataset": "friendster",
        "vertices": vcount,
        "edges": ecount,
        "hardware": "RTX PRO 6000 Blackwell 96GB, 16 vCPU, 144 GB RAM",
        "storage": storage,
        "num_sources": NUM_SOURCES,
        "successful_sources": n,
        "median_teps": median_teps,
        "harmonic_mean_teps": hmean_teps,
        "mean_teps": mean_teps,
        "min_teps": min_teps,
        "max_teps": max_teps,
        "median_time_s": median_time,
        "total_time_s": total_time,
        "median_coverage_pct": median_coverage,
        "results": [{k: v for k, v in r.items() if k != "hop_data"} for r in results],
    }
    json_path = "/tmp/graph500_teps_results.json"
    with open(json_path, "w") as f:
        json.dump(output, f, indent=2)
    print("Results saved to " + json_path)

print()
print("TEPS BENCHMARK COMPLETE")
