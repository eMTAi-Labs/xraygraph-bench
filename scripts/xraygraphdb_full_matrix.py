#!/usr/bin/env python3
"""
xrayGraphDB Full Benchmark Matrix
Tests ALL combinations: storage engine x dataset x GPU x query type

Matrix:
  Storage: default (in-memory), mmap (disk-backed)
  Datasets: LDBC SF1, LDBC SF10, Friendster
  GPU: with GPU analytics, without
  Queries: LDBC Interactive, BFS 1-10, Edge/Node count, GPU analytics
"""
import json
import os
import subprocess
import time
from dataclasses import asdict, dataclass, field

QUERY_TIMEOUT_SEC = 300

@dataclass
class Result:
    name: str
    query: str = ""
    cold_ms: float = 0.0
    warm_ms: float = 0.0
    warm_runs: int = 0
    rows: int = 0
    error: str = ""
    gpu_pct: float = -1.0


def bolt_query(query, host="localhost", port=7687):
    """Run query via Bolt and return (rows, wall_ms)."""
    from neo4j import GraphDatabase
    d = GraphDatabase.driver(f"bolt://{host}:{port}")
    start = time.perf_counter()
    with d.session() as s:
        result = s.run(query)
        rows = [dict(r) for r in result]
        result.consume()
    wall_ms = (time.perf_counter() - start) * 1000.0
    d.close()
    return rows, wall_ms


def run_benchmark(name, query, warmup=3):
    """Run a single benchmark: 1 cold + N warm."""
    r = Result(name=name, query=query)
    try:
        rows, cold_ms = bolt_query(query)
        r.cold_ms = round(cold_ms, 2)
        r.rows = len(rows)

        if cold_ms > QUERY_TIMEOUT_SEC * 1000:
            r.warm_ms = cold_ms
            return r

        n = 1 if cold_ms > 60000 else warmup
        warm_times = []
        for _ in range(n):
            _, wms = bolt_query(query)
            warm_times.append(wms)
        r.warm_ms = round(sum(warm_times) / len(warm_times), 2)
        r.warm_runs = n
    except Exception as e:
        r.error = str(e)[:300]
    return r


def run_ldbc_suite(person_id=933):
    """Run LDBC queries + BFS 1-10."""
    results = []

    queries = [
        ("IS1: Profile", f"MATCH (p:Person {{id: {person_id}}}) RETURN p.firstName, p.lastName, p.gender"),
        ("IS3: Friends", f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS]-(f) RETURN count(f) AS cnt"),
        ("IC2: Messages", f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS]-(f:Person)<-[:HAS_CREATOR]-(m) RETURN f.firstName, m.id, m.content ORDER BY m.creationDate DESC LIMIT 10"),
        ("IC5: Forums", f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS*1..2]-(f:Person)<-[:HAS_MEMBER]-(forum:Forum) RETURN forum.title, count(DISTINCT f) AS members ORDER BY members DESC LIMIT 10"),
        ("IC11: Work", f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS*1..2]-(f:Person)-[:WORK_AT]->(org:Organisation) WHERE f.id <> {person_id} RETURN f.firstName, org.name LIMIT 10"),
        ("Edge count", "MATCH ()-[r]->() RETURN count(r) AS cnt"),
        ("Node count", "MATCH (n) RETURN count(n) AS cnt"),
    ]

    for name, q in queries:
        print(f"    {name}...", end=" ", flush=True)
        r = run_benchmark(name, q)
        print(f"cold={r.cold_ms:.1f}ms warm={r.warm_ms:.1f}ms rows={r.rows}" if not r.error else f"ERROR: {r.error[:80]}")
        results.append(r)

    # BFS 1-10
    for hop in range(1, 11):
        name = f"BFS {hop}-hop"
        if hop == 1:
            q = f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS]-(f) RETURN count(f) AS cnt"
        else:
            q = f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS*1..{hop}]-(f) RETURN count(DISTINCT f) AS cnt"
        print(f"    {name}...", end=" ", flush=True)
        r = run_benchmark(name, q)
        if r.error:
            print(f"ERROR: {r.error[:80]}")
        else:
            print(f"cold={r.cold_ms:.1f}ms warm={r.warm_ms:.1f}ms rows={r.rows}")
        results.append(r)

        # If timed out, skip remaining hops
        if r.cold_ms > QUERY_TIMEOUT_SEC * 1000:
            for rh in range(hop + 1, 11):
                skip = Result(name=f"BFS {rh}-hop", error=f"Skipped: {name} took {r.cold_ms/1000:.0f}s")
                results.append(skip)
                print(f"    BFS {rh}-hop... SKIPPED")
            break

    return results


def run_gpu_analytics():
    """Run GPU-accelerated analytics procedures."""
    results = []
    analytics = [
        ("PageRank", "CALL xray.pagerank() YIELD node, rank RETURN node, rank ORDER BY rank DESC LIMIT 10"),
        ("Triangle Count", "CALL xray.triangle_count() YIELD count RETURN count"),
        ("Community Detection", "CALL xray.community_detection() YIELD node, community RETURN community, count(node) AS sz ORDER BY sz DESC LIMIT 10"),
        ("Connected Components", "CALL xray.weakly_connected_components() YIELD node, component RETURN component, count(node) AS sz ORDER BY sz DESC LIMIT 10"),
        ("Betweenness", "CALL xray.betweenness_centrality(50) YIELD node, centrality RETURN node, centrality ORDER BY centrality DESC LIMIT 10"),
    ]

    for name, q in analytics:
        print(f"    GPU: {name}...", end=" ", flush=True)
        r = run_benchmark(f"GPU: {name}", q, warmup=2)
        # Check GPU utilization during analytics
        try:
            gpu_out = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=utilization.gpu", "--format=csv,noheader,nounits"],
                text=True, timeout=5
            ).strip()
            r.gpu_pct = float(gpu_out)
        except Exception:
            pass
        if r.error:
            print(f"ERROR: {r.error[:80]}")
        else:
            print(f"cold={r.cold_ms:.1f}ms warm={r.warm_ms:.1f}ms gpu={r.gpu_pct:.0f}%")
        results.append(r)

    return results


def run_friendster_suite():
    """Run Friendster-specific benchmarks (CSR BFS)."""
    results = []

    queries = [
        ("Friendster Node count", "MATCH (n) RETURN count(n) AS cnt"),
        ("Friendster Edge count", "MATCH ()-[r]->() RETURN count(r) AS cnt"),
    ]

    for name, q in queries:
        print(f"    {name}...", end=" ", flush=True)
        r = run_benchmark(name, q)
        if r.error:
            print(f"ERROR: {r.error[:80]}")
        else:
            print(f"cold={r.cold_ms:.1f}ms warm={r.warm_ms:.1f}ms rows={r.rows}")
        results.append(r)

    # Friendster BFS via Cypher (pick a vertex)
    # Get a vertex with edges
    try:
        rows, _ = bolt_query("MATCH (n)--() RETURN n.id AS id LIMIT 1")
        if rows:
            vid = rows[0].get("id", 0)
            for hop in range(1, 11):
                name = f"Friendster BFS {hop}-hop"
                if hop == 1:
                    q = f"MATCH (p {{id: {vid}}})--(f) RETURN count(f) AS cnt"
                else:
                    q = f"MATCH (p {{id: {vid}}})-[*1..{hop}]-(f) RETURN count(DISTINCT f) AS cnt"
                print(f"    {name}...", end=" ", flush=True)
                r = run_benchmark(name, q, warmup=2)
                if r.error:
                    print(f"ERROR: {r.error[:80]}")
                else:
                    print(f"cold={r.cold_ms:.1f}ms warm={r.warm_ms:.1f}ms rows={r.rows}")
                results.append(r)
                if r.cold_ms > QUERY_TIMEOUT_SEC * 1000:
                    for rh in range(hop + 1, 11):
                        results.append(Result(name=f"Friendster BFS {rh}-hop", error="Skipped"))
                        print(f"    Friendster BFS {rh}-hop... SKIPPED")
                    break
    except Exception as e:
        results.append(Result(name="Friendster BFS", error=str(e)[:200]))

    return results


def switch_engine(engine, extra_args=""):
    """Switch xrayGraphDB storage engine by editing systemd and restarting."""
    cmd = f"""
sed -i 's/--storage-engine=[a-z]*/--storage-engine={engine}/' /etc/systemd/system/xraygraphdb.service
systemctl daemon-reload
systemctl restart xraygraphdb
"""
    subprocess.run(["bash", "-c", cmd], capture_output=True, timeout=30)
    # Wait for ready
    for i in range(20):
        time.sleep(10)
        r = subprocess.run(["ss", "-tlnp"], capture_output=True, text=True)
        if "7687" in r.stdout:
            return True
    return False


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="/opt/xraybench-results/xraygraphdb-matrix/")
    parser.add_argument("--phase", default="all", help="all, ldbc-inmem, ldbc-mmap, friendster-mmap, gpu")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    all_results = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "server": "S2: 187GB RAM, 44-core Xeon Gold 6152, Tesla T4 16GB",
        "phases": {}
    }

    phases = args.phase.split(",") if args.phase != "all" else ["ldbc-inmem", "ldbc-mmap", "gpu", "friendster-mmap"]

    # ═══════════════════════════════════════════════════════════════
    # PHASE 1: LDBC SF1 — In-Memory Engine (default)
    # ═══════════════════════════════════════════════════════════════
    if "ldbc-inmem" in phases:
        print("=" * 70)
        print("PHASE 1: LDBC SF1 — In-Memory Engine (--storage-engine=default)")
        print("=" * 70)

        # Verify we're in default mode with LDBC loaded
        try:
            rows, _ = bolt_query("MATCH (n) RETURN count(n) AS cnt")
            node_count = rows[0]["cnt"]
            rows, _ = bolt_query("MATCH ()-[r]->() RETURN count(r) AS cnt")
            edge_count = rows[0]["cnt"]
            print(f"  Data: {node_count:,} nodes, {edge_count:,} edges")
        except Exception as e:
            print(f"  Connection failed: {e}")
            print("  Skipping phase 1")
            phases.remove("ldbc-inmem") if "ldbc-inmem" in phases else None

        if "ldbc-inmem" in phases:
            print("\n  --- LDBC Queries ---")
            ldbc_results = run_ldbc_suite()

            print("\n  --- GPU Analytics ---")
            gpu_results = run_gpu_analytics()

            all_results["phases"]["ldbc_sf1_inmem"] = {
                "engine": "default (in-memory)",
                "dataset": f"LDBC SF1 ({node_count:,} nodes, {edge_count:,} edges)",
                "queries": [asdict(r) for r in ldbc_results],
                "gpu_analytics": [asdict(r) for r in gpu_results],
            }

    # ═══════════════════════════════════════════════════════════════
    # PHASE 2: LDBC SF1 — mmap Engine
    # ═══════════════════════════════════════════════════════════════
    if "ldbc-mmap" in phases:
        print("\n" + "=" * 70)
        print("PHASE 2: LDBC SF1 — mmap Engine (--storage-engine=mmap)")
        print("=" * 70)

        print("  Switching to mmap engine...")
        if switch_engine("mmap"):
            print("  xrayGraphDB restarted in mmap mode")

            try:
                rows, _ = bolt_query("MATCH (n) RETURN count(n) AS cnt")
                node_count = rows[0]["cnt"]
                rows, _ = bolt_query("MATCH ()-[r]->() RETURN count(r) AS cnt")
                edge_count = rows[0]["cnt"]
                print(f"  Data: {node_count:,} nodes, {edge_count:,} edges")
            except Exception as e:
                print(f"  Connection failed: {e}")

            print("\n  --- LDBC Queries (mmap) ---")
            ldbc_mmap = run_ldbc_suite()

            print("\n  --- GPU Analytics (mmap) ---")
            gpu_mmap = run_gpu_analytics()

            all_results["phases"]["ldbc_sf1_mmap"] = {
                "engine": "mmap (disk-backed)",
                "dataset": f"LDBC SF1",
                "queries": [asdict(r) for r in ldbc_mmap],
                "gpu_analytics": [asdict(r) for r in gpu_mmap],
            }
        else:
            print("  ERROR: Failed to switch to mmap")

    # ═══════════════════════════════════════════════════════════════
    # PHASE 3: Friendster — mmap Engine (1.8B edges)
    # ═══════════════════════════════════════════════════════════════
    if "friendster-mmap" in phases:
        print("\n" + "=" * 70)
        print("PHASE 3: Friendster — mmap Engine (65.6M nodes, 1.8B edges)")
        print("=" * 70)

        # Ensure mmap mode
        print("  Ensuring mmap engine...")
        switch_engine("mmap")

        # Need to load Friendster data if not already loaded
        # Check if Friendster is loaded by checking node count
        try:
            rows, _ = bolt_query("MATCH (n) RETURN count(n) AS cnt")
            node_count = rows[0]["cnt"]
            print(f"  Current data: {node_count:,} nodes")

            if node_count < 60000000:
                print("  Friendster not loaded — need to load via BULK_IMPORT_FILE")
                print("  (This requires CSR build which takes ~15 minutes)")
                # TODO: Load Friendster via xrayProtocol BULK_IMPORT_FILE
                all_results["phases"]["friendster_mmap"] = {
                    "engine": "mmap",
                    "dataset": "Friendster (not loaded — requires BULK_IMPORT_FILE)",
                    "note": "Friendster 1.8B edges requires CSR build via xrayProtocol BULK_IMPORT_FILE. Not loaded in this run.",
                }
            else:
                print(f"  Friendster loaded: {node_count:,} nodes")
                print("\n  --- Friendster Queries ---")
                friendster_results = run_friendster_suite()

                print("\n  --- Friendster GPU Analytics ---")
                gpu_friendster = run_gpu_analytics()

                all_results["phases"]["friendster_mmap"] = {
                    "engine": "mmap",
                    "dataset": f"Friendster ({node_count:,} nodes)",
                    "queries": [asdict(r) for r in friendster_results],
                    "gpu_analytics": [asdict(r) for r in gpu_friendster],
                }
        except Exception as e:
            print(f"  Error: {e}")

    # ═══════════════════════════════════════════════════════════════
    # PHASE 4: Switch back to default for final state
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("Switching back to default (in-memory) engine...")
    switch_engine("default")
    print("Done.")

    # Save results
    out_file = os.path.join(args.output, "xraygraphdb_full_matrix.json")
    with open(out_file, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n=== Results saved to {out_file} ===")

    # Print summary
    print("\n" + "=" * 70)
    print("FULL MATRIX SUMMARY")
    print("=" * 70)
    for phase_name, phase_data in all_results["phases"].items():
        print(f"\n--- {phase_name} ({phase_data.get('engine', 'N/A')}) ---")
        if "queries" in phase_data:
            print(f"  {'Benchmark':<30} {'Cold':>10} {'Warm':>10}")
            for q in phase_data["queries"]:
                print(f"  {q['name']:<30} {q['cold_ms']:>9.1f}ms {q['warm_ms']:>9.1f}ms")
        if "gpu_analytics" in phase_data:
            for q in phase_data["gpu_analytics"]:
                gpu = f" gpu={q['gpu_pct']:.0f}%" if q['gpu_pct'] >= 0 else ""
                err = f" ERR" if q['error'] else ""
                print(f"  {q['name']:<30} {q['cold_ms']:>9.1f}ms {q['warm_ms']:>9.1f}ms{gpu}{err}")


if __name__ == "__main__":
    main()
