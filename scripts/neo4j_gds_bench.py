#!/usr/bin/env python3
"""Neo4j GDS benchmark — same Friendster dataset, same algorithms.

Neo4j Community 2025.04.0 with Graph Data Science library.
CPU-only (no GPU acceleration in Community edition).

Algorithms tested via GDS:
1. Graph projection (native projection into GDS in-memory graph)
2. PageRank
3. Triangle Count
4. BFS
5. WCC (Weakly Connected Components)
6. K-Core (K-1 Coloring or Core decomposition)
7. Louvain Community Detection
8. Betweenness Centrality (sampled)
"""
import time
import sys
import json
import subprocess

sys.stdout.reconfigure(line_buffering=True)

BOLT_URI = "bolt://localhost:7690"

def cypher(query, timeout=7200):
    """Run a Cypher query via cypher-shell and return output."""
    cmd = [
        "/home/Ubuntu/neo4j-community-2025.04.0/bin/cypher-shell",
        "-a", BOLT_URI,
        "--format", "plain",
        query
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return result.stdout.strip(), result.stderr.strip()
    except subprocess.TimeoutExpired:
        return "", "TIMEOUT after " + str(timeout) + "s"

print("=" * 72)
print("  Neo4j GDS Benchmark (Friendster)")
print("  CPU-only — same hardware, same dataset")
print("=" * 72)
print()

# Check Neo4j version and data
out, err = cypher("CALL dbms.components() YIELD name, versions RETURN name, versions")
print("Neo4j:          " + out.split("\n")[-1] if out else "ERROR: " + err[:80])

out, err = cypher("MATCH (n) RETURN count(n) AS nodes")
nodes_line = out.strip().split("\n")[-1] if out else "0"
print("Nodes:          " + nodes_line)

out, err = cypher("MATCH ()-[r]->() RETURN count(r) AS rels")
rels_line = out.strip().split("\n")[-1] if out else "0"
print("Relationships:  " + rels_line)
print()

results = {}

# Check if GDS is available
out, err = cypher("RETURN gds.version() AS version")
if "gds" in out.lower() or not err:
    print("GDS version:    " + out.strip().split("\n")[-1] if out else "unknown")
    has_gds = True
else:
    print("GDS:            NOT AVAILABLE (" + err[:80] + ")")
    has_gds = False
    # Try installing GDS plugin
    print("  Note: Neo4j Community 2025.04 may not include GDS by default")
    print("  GDS requires separate download for Community edition")
    results["gds_available"] = False

print()

if not has_gds:
    # Run basic Cypher benchmarks without GDS
    print("Running basic Cypher benchmarks (no GDS)...")
    print()

    # Node count
    t0 = time.perf_counter()
    out, err = cypher("MATCH (n) RETURN count(n)")
    elapsed = time.perf_counter() - t0
    print("  Node count: {:.2f}s  result={}".format(elapsed, out.strip().split("\n")[-1] if out else err[:40]))
    results["node_count"] = {"elapsed_s": round(elapsed, 3)}

    # Rel count
    t0 = time.perf_counter()
    out, err = cypher("MATCH ()-[r]->() RETURN count(r)")
    elapsed = time.perf_counter() - t0
    print("  Rel count:  {:.2f}s  result={}".format(elapsed, out.strip().split("\n")[-1] if out else err[:40]))
    results["rel_count"] = {"elapsed_s": round(elapsed, 3)}

    # BFS 1-hop
    print()
    print("  BFS hops (Cypher variable-length paths):")
    for hops in range(1, 6):
        t0 = time.perf_counter()
        out, err = cypher(
            "MATCH (n) WITH n LIMIT 1 "
            "MATCH (n)-[*1.." + str(hops) + "]-(m) RETURN count(m)",
            timeout=600
        )
        elapsed = time.perf_counter() - t0
        result = out.strip().split("\n")[-1] if out else err[:60]
        print("    {}-hop: {:.1f}s  paths={}".format(hops, elapsed, result))
        results["bfs_" + str(hops) + "hop"] = {"elapsed_s": round(elapsed, 3), "result": result}
        if elapsed > 300:
            print("    STOPPING (>300s)")
            break

    print()
    print("  Note: Without GDS, only basic Cypher queries are possible.")
    print("  GDS algorithms (PageRank, TriangleCount, Louvain, etc.) require")
    print("  the GDS plugin which is not bundled with Neo4j Community 2025.04.")

else:
    # ─── GDS Graph Projection ───
    print("=" * 72)
    print("  Phase 1: GDS Graph Projection")
    print("=" * 72)

    # First estimate memory
    out, err = cypher(
        "CALL gds.graph.project.estimate('*', '*') "
        "YIELD requiredMemory, nodeCount, relationshipCount "
        "RETURN requiredMemory, nodeCount, relationshipCount"
    )
    print("  Memory estimate: " + (out.strip().split("\n")[-1] if out else err[:80]))

    # Project the graph
    t0 = time.perf_counter()
    out, err = cypher(
        "CALL gds.graph.project('friendster', '*', '*') "
        "YIELD graphName, nodeCount, relationshipCount, projectMillis "
        "RETURN graphName, nodeCount, relationshipCount, projectMillis",
        timeout=3600
    )
    elapsed = time.perf_counter() - t0
    print("  Projection: {:.1f}s".format(elapsed))
    print("  Result: " + (out.strip().split("\n")[-1] if out else "FAILED: " + err[:100]))
    results["projection"] = {"elapsed_s": round(elapsed, 3), "output": out[:200] if out else err[:200]}

    if err and "MemoryEstimationNotSufficientException" in err:
        print("  GDS OOM during projection — heap too small for Friendster")
        results["projection"]["error"] = "OOM"
    elif not err or "friendster" in out.lower():
        # ─── PageRank ───
        print()
        print("=" * 72)
        print("  Phase 2: PageRank (20 iterations)")
        print("=" * 72)

        for label in ["cold", "warm1"]:
            t0 = time.perf_counter()
            out, err = cypher(
                "CALL gds.pageRank.stats('friendster', {maxIterations: 20, dampingFactor: 0.85}) "
                "YIELD ranIterations, didConverge, computeMillis "
                "RETURN ranIterations, didConverge, computeMillis",
                timeout=3600
            )
            elapsed = time.perf_counter() - t0
            result = out.strip().split("\n")[-1] if out else err[:80]
            print("  {:6s}: {:.2f}s  {}".format(label, elapsed, result))
            results["pagerank_" + label] = {"elapsed_s": round(elapsed, 3), "output": result}

        # ─── Triangle Count ───
        print()
        print("=" * 72)
        print("  Phase 3: Triangle Count")
        print("=" * 72)

        t0 = time.perf_counter()
        out, err = cypher(
            "CALL gds.triangleCount.stats('friendster') "
            "YIELD globalTriangleCount, nodeCount, computeMillis "
            "RETURN globalTriangleCount, nodeCount, computeMillis",
            timeout=7200
        )
        elapsed = time.perf_counter() - t0
        result = out.strip().split("\n")[-1] if out else err[:80]
        print("  {:.2f}s  {}".format(elapsed, result))
        results["triangle_count"] = {"elapsed_s": round(elapsed, 3), "output": result}

        # ─── WCC ───
        print()
        print("=" * 72)
        print("  Phase 4: Weakly Connected Components")
        print("=" * 72)

        t0 = time.perf_counter()
        out, err = cypher(
            "CALL gds.wcc.stats('friendster') "
            "YIELD componentCount, computeMillis "
            "RETURN componentCount, computeMillis",
            timeout=3600
        )
        elapsed = time.perf_counter() - t0
        result = out.strip().split("\n")[-1] if out else err[:80]
        print("  {:.2f}s  {}".format(elapsed, result))
        results["wcc"] = {"elapsed_s": round(elapsed, 3), "output": result}

        # ─── Louvain ───
        print()
        print("=" * 72)
        print("  Phase 5: Louvain Community Detection")
        print("=" * 72)

        t0 = time.perf_counter()
        out, err = cypher(
            "CALL gds.louvain.stats('friendster') "
            "YIELD communityCount, modularity, computeMillis "
            "RETURN communityCount, modularity, computeMillis",
            timeout=3600
        )
        elapsed = time.perf_counter() - t0
        result = out.strip().split("\n")[-1] if out else err[:80]
        print("  {:.2f}s  {}".format(elapsed, result))
        results["louvain"] = {"elapsed_s": round(elapsed, 3), "output": result}

        # ─── Betweenness Centrality ───
        print()
        print("=" * 72)
        print("  Phase 6: Betweenness Centrality (sampled)")
        print("=" * 72)

        t0 = time.perf_counter()
        out, err = cypher(
            "CALL gds.betweenness.stats('friendster', {samplingSize: 50}) "
            "YIELD centralityDistribution, computeMillis "
            "RETURN computeMillis",
            timeout=3600
        )
        elapsed = time.perf_counter() - t0
        result = out.strip().split("\n")[-1] if out else err[:80]
        print("  {:.2f}s  {}".format(elapsed, result))
        results["bc_k50"] = {"elapsed_s": round(elapsed, 3), "output": result}

        # Cleanup
        cypher("CALL gds.graph.drop('friendster')")

print()
print("=" * 72)
print("  Neo4j Benchmark Summary")
print("=" * 72)
print()

json_path = "/tmp/neo4j_gds_results.json"
with open(json_path, "w") as f:
    json.dump({"benchmark": "neo4j_friendster", "results": results}, f, indent=2)
print("Results saved to " + json_path)
print()
print("NEO4J BENCHMARK COMPLETE")
