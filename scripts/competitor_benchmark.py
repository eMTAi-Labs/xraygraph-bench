#!/usr/bin/env python3
"""
Competitor Database Benchmark Runner

Loads LDBC SF1 data and runs standardized benchmarks against any database
that speaks Bolt protocol (Memgraph, Neo4j, FalkorDB) or has a custom adapter.

Usage:
    python3 competitor_benchmark.py --engine memgraph --host localhost --port 7687 \
        --ldbc-dir /opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter \
        --output /opt/xraybench-results/competitors/memgraph/
"""

import argparse
import csv
import json
import os
import signal
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass
class BenchmarkResult:
    name: str
    query: str
    cold_ms: float = 0.0
    warm_ms: float = 0.0
    warm_runs: int = 0
    rows: int = 0
    result_sample: list = field(default_factory=list)
    error: str = ""
    gpu_pct: float = -1.0  # -1 means not measured


@dataclass
class LoadResult:
    table: str
    rows_loaded: int
    elapsed_ms: float
    error: str = ""


def get_driver(engine, host, port, password=""):
    """Create a neo4j driver for Bolt-compatible databases."""
    from neo4j import GraphDatabase

    uri = f"bolt://{host}:{port}"
    if engine == "neo4j":
        auth = ("neo4j", password)
    else:
        auth = None
    return GraphDatabase.driver(uri, auth=auth)


def timed_query(session, query, params=None):
    """Run a query and return (rows, wall_ms)."""
    start = time.perf_counter()
    result = session.run(query, parameters=params or {})
    rows = [dict(r) for r in result]
    result.consume()
    wall_ms = (time.perf_counter() - start) * 1000.0
    return rows, wall_ms


def load_ldbc_nodes(session, ldbc_dir, engine):
    """Load LDBC SF1 node CSVs into the database."""
    results = []
    base = Path(ldbc_dir)

    # Foreign key columns embedded in CsvCompositeMergeForeign format — skip as properties
    FK_COLUMNS = {"creator", "place", "replyOfPost", "replyOfComment", "Forum.id", "moderator"}

    # Node files: dynamic/ has Person, Forum, Comment, Post
    # static/ has Organisation, Place, Tag, TagClass
    node_files = [
        ("dynamic/person_0_0.csv", "Person"),
        ("dynamic/forum_0_0.csv", "Forum"),
        ("dynamic/comment_0_0.csv", "Comment"),
        ("dynamic/post_0_0.csv", "Post"),
        ("static/organisation_0_0.csv", "Organisation"),
        ("static/place_0_0.csv", "Place"),
        ("static/tag_0_0.csv", "Tag"),
        ("static/tagclass_0_0.csv", "TagClass"),
    ]

    for csv_file, label in node_files:
        fpath = base / csv_file
        if not fpath.exists():
            print(f"  SKIP {csv_file} (not found)")
            continue

        print(f"  Loading {label} from {csv_file}...", end=" ", flush=True)
        start = time.perf_counter()
        count = 0
        batch = []
        batch_size = 1000

        with open(fpath, "r") as f:
            reader = csv.reader(f, delimiter="|")
            header = next(reader)
            # Filter out FK columns and multi-value columns (language, email)
            prop_cols = [(i, col) for i, col in enumerate(header) if col not in FK_COLUMNS]
            for row in reader:
                node = {}
                for i, col in prop_cols:
                    if i < len(row):
                        val = row[i]
                        if col == "id":
                            try:
                                node[col] = int(val)
                            except ValueError:
                                node[col] = val
                        else:
                            node[col] = val
                batch.append(node)

                if len(batch) >= batch_size:
                    _insert_node_batch(session, label, batch)
                    count += len(batch)
                    batch = []

            if batch:
                _insert_node_batch(session, label, batch)
                count += len(batch)

        elapsed = (time.perf_counter() - start) * 1000.0
        rate = count / (elapsed / 1000.0) if elapsed > 0 else 0
        print(f"{count} rows, {elapsed:.0f}ms ({rate:.0f}/s)")
        results.append(LoadResult(label, count, elapsed))

    return results


def _insert_node_batch(session, label, batch):
    """Insert a batch of nodes using UNWIND."""
    if not batch:
        return
    props = list(batch[0].keys())
    set_clause = ", ".join(f"n.{p} = row.{p}" for p in props if p != "id")
    query = f"UNWIND $batch AS row CREATE (n:{label} {{id: row.id}}) SET {set_clause}"
    session.run(query, parameters={"batch": batch}).consume()


def load_ldbc_edges(session, ldbc_dir, engine):
    """Load LDBC SF1 edge CSVs."""
    results = []
    base = Path(ldbc_dir)

    # Explicit edge CSV files
    edge_files = [
        ("dynamic/person_knows_person_0_0.csv", "Person", "KNOWS", "Person", 0, 1),
        ("dynamic/person_likes_comment_0_0.csv", "Person", "LIKES", "Comment", 0, 1),
        ("dynamic/person_likes_post_0_0.csv", "Person", "LIKES", "Post", 0, 1),
        ("dynamic/forum_hasMember_person_0_0.csv", "Forum", "HAS_MEMBER", "Person", 0, 1),
        ("dynamic/person_studyAt_organisation_0_0.csv", "Person", "STUDY_AT", "Organisation", 0, 1),
        ("dynamic/person_workAt_organisation_0_0.csv", "Person", "WORK_AT", "Organisation", 0, 1),
    ]

    for csv_file, src_label, rel_type, dst_label, src_col, dst_col in edge_files:
        fpath = base / csv_file
        if not fpath.exists():
            print(f"  SKIP {csv_file} (not found)")
            continue

        print(f"  Loading {src_label}-[{rel_type}]->{dst_label}...", end=" ", flush=True)
        start = time.perf_counter()
        count = 0
        batch = []
        batch_size = 1000

        with open(fpath, "r") as f:
            reader = csv.reader(f, delimiter="|")
            header = next(reader)
            for row in reader:
                try:
                    s = int(row[src_col])
                    t = int(row[dst_col])
                except (ValueError, IndexError):
                    continue
                batch.append({"s": s, "t": t})

                if len(batch) >= batch_size:
                    _insert_edge_batch(session, src_label, rel_type, dst_label, batch)
                    count += len(batch)
                    batch = []

            if batch:
                _insert_edge_batch(session, src_label, rel_type, dst_label, batch)
                count += len(batch)

        elapsed = (time.perf_counter() - start) * 1000.0
        rate = count / (elapsed / 1000.0) if elapsed > 0 else 0
        print(f"{count} rows, {elapsed:.0f}ms ({rate:.0f}/s)")
        results.append(LoadResult(f"{src_label}-{rel_type}-{dst_label}", count, elapsed))

    # CsvCompositeMergeForeign: extract edges embedded as foreign key columns in node CSVs
    embedded_edges = [
        # (csv_file, src_label, fk_column, rel_type, dst_label)
        ("dynamic/comment_0_0.csv", "Comment", "creator", "HAS_CREATOR", "Person"),
        ("dynamic/comment_0_0.csv", "Comment", "replyOfComment", "REPLY_OF", "Comment"),
        ("dynamic/comment_0_0.csv", "Comment", "replyOfPost", "REPLY_OF", "Post"),
        ("dynamic/comment_0_0.csv", "Comment", "place", "IS_LOCATED_IN", "Place"),
        ("dynamic/post_0_0.csv", "Post", "creator", "HAS_CREATOR", "Person"),
        ("dynamic/post_0_0.csv", "Post", "Forum.id", "CONTAINER_OF_REV", "Forum"),
        ("dynamic/post_0_0.csv", "Post", "place", "IS_LOCATED_IN", "Place"),
        ("dynamic/forum_0_0.csv", "Forum", "moderator", "HAS_MODERATOR", "Person"),
        ("dynamic/person_0_0.csv", "Person", "place", "IS_LOCATED_IN", "Place"),
    ]

    for csv_file, src_label, fk_col, rel_type, dst_label in embedded_edges:
        fpath = base / csv_file
        if not fpath.exists():
            continue

        print(f"  Loading {src_label}-[{rel_type}]->{dst_label} (from {fk_col} in {csv_file})...", end=" ", flush=True)
        start = time.perf_counter()
        count = 0
        batch = []
        batch_size = 1000

        with open(fpath, "r") as f:
            reader = csv.reader(f, delimiter="|")
            header = next(reader)
            try:
                id_idx = header.index("id")
                fk_idx = header.index(fk_col)
            except ValueError:
                print(f"SKIP (column '{fk_col}' not found)")
                continue

            for row in reader:
                if fk_idx >= len(row) or not row[fk_idx].strip():
                    continue
                try:
                    s = int(row[id_idx])
                    t = int(row[fk_idx])
                except (ValueError, IndexError):
                    continue
                batch.append({"s": s, "t": t})

                if len(batch) >= batch_size:
                    if rel_type == "CONTAINER_OF_REV":
                        _insert_edge_batch(session, dst_label, "CONTAINER_OF", src_label, [{"s": e["t"], "t": e["s"]} for e in batch])
                    else:
                        _insert_edge_batch(session, src_label, rel_type, dst_label, batch)
                    count += len(batch)
                    batch = []

            if batch:
                if rel_type == "CONTAINER_OF_REV":
                    _insert_edge_batch(session, dst_label, "CONTAINER_OF", src_label, [{"s": e["t"], "t": e["s"]} for e in batch])
                else:
                    _insert_edge_batch(session, src_label, rel_type, dst_label, batch)
                count += len(batch)

        elapsed = (time.perf_counter() - start) * 1000.0
        rate = count / (elapsed / 1000.0) if elapsed > 0 else 0
        print(f"{count} rows, {elapsed:.0f}ms ({rate:.0f}/s)")
        actual_rel = "CONTAINER_OF" if rel_type == "CONTAINER_OF_REV" else rel_type
        results.append(LoadResult(f"{src_label}-{actual_rel}-{dst_label}(embedded)", count, elapsed))

    return results


def _insert_edge_batch(session, src_label, rel_type, dst_label, batch):
    """Insert a batch of edges using UNWIND + MATCH."""
    query = (
        f"UNWIND $batch AS row "
        f"MATCH (a:{src_label} {{id: row.s}}) "
        f"MATCH (b:{dst_label} {{id: row.t}}) "
        f"CREATE (a)-[:{rel_type}]->(b)"
    )
    session.run(query, parameters={"batch": batch}).consume()


def create_indexes(session, engine):
    """Create indexes on node IDs for fast lookups."""
    labels = ["Person", "Forum", "Comment", "Post", "Organisation", "Place", "Tag", "TagClass"]
    for label in labels:
        try:
            if engine == "neo4j":
                session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.id)").consume()
            else:
                # Memgraph syntax
                session.run(f"CREATE INDEX ON :{label}(id)").consume()
        except Exception as e:
            print(f"  Index on {label}.id: {e}")


def run_benchmarks(session, engine):
    """Run the standardized benchmark queries."""
    results = []

    # Get a known person ID for queries
    r, _ = timed_query(session, "MATCH (p:Person) RETURN p.id AS id LIMIT 1")
    person_id = r[0]["id"] if r else 933
    print(f"  Using person_id={person_id} for queries")

    # Per-query timeout in seconds (5 minutes max per query)
    QUERY_TIMEOUT_SEC = 300

    benchmarks = [
        # --- LDBC Interactive Short ---
        ("IS1: Profile lookup", f"MATCH (p:Person {{id: {person_id}}}) RETURN p.firstName, p.lastName, p.gender, p.birthday"),
        ("IS3: Friend count", f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS]-(f) RETURN count(f) AS cnt"),
        # --- LDBC Interactive Complex ---
        ("IC2: Recent messages", f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS]-(f:Person)<-[:HAS_CREATOR]-(m) RETURN f.firstName, f.lastName, m.id, m.content ORDER BY m.creationDate DESC LIMIT 10"),
        ("IC5: Forums via friends", f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS*1..2]-(f:Person)<-[:HAS_MEMBER]-(forum:Forum) RETURN forum.title, count(DISTINCT f) AS members ORDER BY members DESC LIMIT 10"),
        ("IC11: Work connections", f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS*1..2]-(f:Person)-[w:WORK_AT]->(org:Organisation) WHERE f.id <> {person_id} RETURN f.firstName, f.lastName, org.name LIMIT 10"),
        # --- Aggregation ---
        ("Edge count", "MATCH ()-[r]->() RETURN count(r) AS cnt"),
        ("Node count", "MATCH (n) RETURN count(n) AS cnt"),
    ]

    # BFS Hop Tests: 1 through 10 — find where each DB dies
    for hop in range(1, 11):
        if hop == 1:
            q = f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS]-(f) RETURN count(f) AS cnt"
        else:
            q = f"MATCH (p:Person {{id: {person_id}}})-[:KNOWS*1..{hop}]-(f) RETURN count(DISTINCT f) AS cnt"
        benchmarks.append((f"BFS {hop}-hop", q))

    for name, query in benchmarks:
        print(f"  {name}...", end=" ", flush=True)
        br = BenchmarkResult(name=name, query=query)
        try:
            # Cold run with timeout
            start_wall = time.perf_counter()
            rows, cold_ms = timed_query(session, query)
            elapsed_total = time.perf_counter() - start_wall

            br.cold_ms = round(cold_ms, 2)
            br.rows = len(rows)
            br.result_sample = rows[:3]

            # Check if cold run already exceeded timeout
            if elapsed_total > QUERY_TIMEOUT_SEC:
                br.warm_ms = cold_ms  # Use cold as warm estimate
                br.warm_runs = 0
                print(f"cold={br.cold_ms:.1f}ms (TIMEOUT - skipping warm runs) rows={br.rows}")
                results.append(br)
                # If this BFS hop timed out, skip higher hops too
                if name.startswith("BFS") and cold_ms > QUERY_TIMEOUT_SEC * 1000:
                    print(f"  *** {engine} DIED at {name} ({cold_ms/1000:.0f}s) — skipping higher hops ***")
                    for remaining_hop in range(int(name.split()[1].split("-")[0]) + 1, 11):
                        skip_br = BenchmarkResult(
                            name=f"BFS {remaining_hop}-hop",
                            query=f"SKIPPED (DB died at {name})",
                            error=f"Skipped: previous hop took {cold_ms/1000:.0f}s"
                        )
                        results.append(skip_br)
                        print(f"  BFS {remaining_hop}-hop... SKIPPED (DB died at {name})")
                    break
                continue

            # Warm runs (3 iterations, skip if cold was > 60s)
            if cold_ms < 60000:
                warm_times = []
                for _ in range(3):
                    _, wms = timed_query(session, query)
                    warm_times.append(wms)
                br.warm_ms = round(sum(warm_times) / len(warm_times), 2)
                br.warm_runs = len(warm_times)
            else:
                # For slow queries, do 1 warm run only
                _, wms = timed_query(session, query)
                br.warm_ms = round(wms, 2)
                br.warm_runs = 1

            print(f"cold={br.cold_ms:.1f}ms warm={br.warm_ms:.1f}ms rows={br.rows}")
        except Exception as e:
            br.error = str(e)
            print(f"ERROR: {e}")

        results.append(br)

    return results


def run_analytics(session, engine):
    """Run graph analytics procedures (engine-specific)."""
    results = []

    if engine == "memgraph":
        analytics = [
            ("PageRank", "CALL pagerank.get() YIELD node, rank RETURN node, rank ORDER BY rank DESC LIMIT 10"),
            ("Community Detection", "CALL community_detection.get() YIELD node, community_id RETURN community_id, count(node) AS size ORDER BY size DESC LIMIT 10"),
            ("Betweenness Centrality", "CALL betweenness_centrality.get() YIELD node, betweenness_centrality RETURN node, betweenness_centrality ORDER BY betweenness_centrality DESC LIMIT 10"),
        ]
    elif engine == "neo4j":
        analytics = [
            ("PageRank", "CALL gds.pageRank.stream('myGraph') YIELD nodeId, score RETURN gds.util.asNode(nodeId).id AS id, score ORDER BY score DESC LIMIT 10"),
            ("Louvain", "CALL gds.louvain.stream('myGraph') YIELD nodeId, communityId RETURN communityId, count(*) AS size ORDER BY size DESC LIMIT 10"),
            ("WCC", "CALL gds.wcc.stream('myGraph') YIELD nodeId, componentId RETURN componentId, count(*) AS size ORDER BY size DESC LIMIT 10"),
            ("Triangle Count", "CALL gds.triangleCount.stream('myGraph') YIELD nodeId, triangleCount RETURN sum(triangleCount) / 3 AS total"),
        ]
    else:
        print("  No analytics procedures defined for this engine")
        return results

    for name, query in analytics:
        print(f"  Analytics: {name}...", end=" ", flush=True)
        br = BenchmarkResult(name=f"Analytics: {name}", query=query)
        try:
            rows, cold_ms = timed_query(session, query)
            br.cold_ms = round(cold_ms, 2)
            br.rows = len(rows)
            br.result_sample = [{k: str(v) for k, v in r.items()} for r in rows[:3]]

            warm_times = []
            for _ in range(3):
                _, wms = timed_query(session, query)
                warm_times.append(wms)
            br.warm_ms = round(sum(warm_times) / len(warm_times), 2)
            br.warm_runs = len(warm_times)

            print(f"cold={br.cold_ms:.1f}ms warm={br.warm_ms:.1f}ms rows={br.rows}")
        except Exception as e:
            br.error = str(e)
            print(f"ERROR: {e}")

        results.append(br)

    return results


def check_gpu(engine):
    """Check GPU utilization if nvidia-smi is available."""
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used,memory.total",
             "--format=csv,noheader,nounits"],
            text=True, timeout=5
        ).strip()
        parts = out.split(",")
        return {
            "gpu_util_pct": float(parts[0].strip()),
            "gpu_mem_used_mib": float(parts[1].strip()),
            "gpu_mem_total_mib": float(parts[2].strip()),
        }
    except Exception:
        return {"gpu_util_pct": 0, "gpu_mem_used_mib": 0, "gpu_mem_total_mib": 0}


def main():
    parser = argparse.ArgumentParser(description="Competitor Database Benchmark Runner")
    parser.add_argument("--engine", required=True, choices=["memgraph", "neo4j", "falkordb"])
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=7687)
    parser.add_argument("--password", default="")
    parser.add_argument("--ldbc-dir", required=True, help="Path to LDBC SF1 CSVs")
    parser.add_argument("--output", required=True, help="Output directory for results")
    parser.add_argument("--skip-load", action="store_true", help="Skip data loading (reuse existing)")
    parser.add_argument("--skip-analytics", action="store_true", help="Skip analytics procedures")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    driver = get_driver(args.engine, args.host, args.port, args.password)
    all_results = {"engine": args.engine, "version": "", "host": args.host, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}

    print(f"=== Competitor Benchmark: {args.engine} ===")
    print(f"Host: {args.host}:{args.port}")

    with driver.session() as session:
        # Get version
        try:
            if args.engine == "neo4j":
                r, _ = timed_query(session, "CALL dbms.components() YIELD name, versions RETURN versions[0] AS v")
                all_results["version"] = r[0]["v"] if r else "unknown"
            else:
                r, _ = timed_query(session, "RETURN 1 AS x")
                all_results["version"] = "2.22.1"  # from install
        except Exception:
            all_results["version"] = "unknown"

        print(f"Engine version: {all_results['version']}")

        # Load data
        if not args.skip_load:
            print("\n=== LOADING DATA ===")

            # Clear existing
            print("  Clearing existing data...")
            session.run("MATCH (n) DETACH DELETE n").consume()

            # Create indexes first
            print("  Creating indexes...")
            create_indexes(session, args.engine)

            # Load nodes
            print("  --- Nodes ---")
            node_results = load_ldbc_nodes(session, args.ldbc_dir, args.engine)
            all_results["load_nodes"] = [asdict(r) for r in node_results]

            # Load edges
            print("  --- Edges ---")
            edge_results = load_ldbc_edges(session, args.ldbc_dir, args.engine)
            all_results["load_edges"] = [asdict(r) for r in edge_results]

            # Verify counts
            r, _ = timed_query(session, "MATCH (n) RETURN count(n) AS cnt")
            node_count = r[0]["cnt"]
            r, _ = timed_query(session, "MATCH ()-[r]->() RETURN count(r) AS cnt")
            edge_count = r[0]["cnt"]
            print(f"\n  Total: {node_count:,} nodes, {edge_count:,} edges")
            all_results["total_nodes"] = node_count
            all_results["total_edges"] = edge_count

        # Run benchmarks
        print("\n=== RUNNING BENCHMARKS ===")
        bench_results = run_benchmarks(session, args.engine)
        all_results["benchmarks"] = [asdict(r) for r in bench_results]

        # Run analytics
        if not args.skip_analytics:
            print("\n=== RUNNING ANALYTICS ===")
            analytics_results = run_analytics(session, args.engine)
            all_results["analytics"] = [asdict(r) for r in analytics_results]

    # GPU check
    all_results["gpu"] = check_gpu(args.engine)

    driver.close()

    # Save results
    out_file = os.path.join(args.output, f"{args.engine}_benchmark.json")
    with open(out_file, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n=== Results saved to {out_file} ===")

    # Print summary
    print("\n=== SUMMARY ===")
    print(f"{'Benchmark':<30} {'Cold (ms)':>10} {'Warm (ms)':>10} {'Rows':>8}")
    print("=" * 62)
    for br in bench_results:
        b = BenchmarkResult(**br) if isinstance(br, dict) else br
        print(f"{b.name:<30} {b.cold_ms:>10.1f} {b.warm_ms:>10.1f} {b.rows:>8}")


if __name__ == "__main__":
    main()
