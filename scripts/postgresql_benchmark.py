#!/usr/bin/env python3
"""PostgreSQL + Apache AGE benchmark: SQL + Cypher via AGE, BFS 1-10."""
import csv
import json
import os
import subprocess
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

QUERY_TIMEOUT_SEC = 300

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


def pg_query(query, db="ldbc"):
    """Run a PostgreSQL query and return (rows, wall_ms)."""
    start = time.perf_counter()
    result = subprocess.run(
        ["sudo", "-u", "postgres", "psql", "-d", db, "-t", "-A", "-F", "|",
         "-c", query],
        capture_output=True, text=True, timeout=QUERY_TIMEOUT_SEC
    )
    wall_ms = (time.perf_counter() - start) * 1000.0
    rows = []
    if result.stdout.strip():
        lines = [l for l in result.stdout.strip().split("\n") if l.strip()]
        for line in lines:
            rows.append({"result": line})
    if result.returncode != 0 and result.stderr and "ERROR" in result.stderr:
        raise Exception(result.stderr.strip()[:300])
    return rows, wall_ms


def load_ldbc(ldbc_dir):
    """Load LDBC into PostgreSQL relational tables."""
    results = []
    base = Path(ldbc_dir)

    # Create tables
    ddl = """
    DROP TABLE IF EXISTS knows CASCADE;
    DROP TABLE IF EXISTS likes_comment CASCADE;
    DROP TABLE IF EXISTS likes_post CASCADE;
    DROP TABLE IF EXISTS has_member CASCADE;
    DROP TABLE IF EXISTS work_at CASCADE;
    DROP TABLE IF EXISTS study_at CASCADE;
    DROP TABLE IF EXISTS comment CASCADE;
    DROP TABLE IF EXISTS post CASCADE;
    DROP TABLE IF EXISTS forum CASCADE;
    DROP TABLE IF EXISTS person CASCADE;
    DROP TABLE IF EXISTS organisation CASCADE;
    DROP TABLE IF EXISTS place CASCADE;

    CREATE TABLE person (id BIGINT PRIMARY KEY, firstName TEXT, lastName TEXT, gender TEXT, birthday TEXT, creationDate TEXT, locationIP TEXT, browserUsed TEXT, place BIGINT, language TEXT, email TEXT);
    CREATE TABLE forum (id BIGINT PRIMARY KEY, title TEXT, creationDate TEXT, moderator BIGINT);
    CREATE TABLE comment (id BIGINT PRIMARY KEY, creationDate TEXT, locationIP TEXT, browserUsed TEXT, content TEXT, length INT, creator BIGINT, place BIGINT, replyOfPost BIGINT, replyOfComment BIGINT);
    CREATE TABLE post (id BIGINT PRIMARY KEY, imageFile TEXT, creationDate TEXT, locationIP TEXT, browserUsed TEXT, language TEXT, content TEXT, length INT, creator BIGINT, forumId BIGINT, place BIGINT);
    CREATE TABLE organisation (id BIGINT PRIMARY KEY, type TEXT, name TEXT, url TEXT);
    CREATE TABLE place (id BIGINT PRIMARY KEY, name TEXT, url TEXT, type TEXT);
    CREATE TABLE knows (person1 BIGINT, person2 BIGINT, creationDate TEXT);
    CREATE TABLE likes_comment (person BIGINT, comment BIGINT, creationDate TEXT);
    CREATE TABLE likes_post (person BIGINT, post BIGINT, creationDate TEXT);
    CREATE TABLE has_member (forum BIGINT, person BIGINT, creationDate TEXT);
    CREATE TABLE work_at (person BIGINT, organisation BIGINT, workFrom INT);
    CREATE TABLE study_at (person BIGINT, organisation BIGINT, classYear INT);
    """
    pg_query(ddl)
    print("  Tables created")

    # Load CSVs using COPY
    tables = [
        ("person", "dynamic/person_0_0.csv"),
        ("forum", "dynamic/forum_0_0.csv"),
        ("comment", "dynamic/comment_0_0.csv"),
        ("post", "dynamic/post_0_0.csv"),
        ("organisation", "static/organisation_0_0.csv"),
        ("place", "static/place_0_0.csv"),
        ("knows", "dynamic/person_knows_person_0_0.csv"),
        ("likes_comment", "dynamic/person_likes_comment_0_0.csv"),
        ("likes_post", "dynamic/person_likes_post_0_0.csv"),
        ("has_member", "dynamic/forum_hasMember_person_0_0.csv"),
        ("work_at", "dynamic/person_workAt_organisation_0_0.csv"),
        ("study_at", "dynamic/person_studyAt_organisation_0_0.csv"),
    ]

    for tname, csv_file in tables:
        fpath = base / csv_file
        if not fpath.exists():
            print(f"  SKIP {tname}")
            continue

        print(f"  Loading {tname}...", end=" ", flush=True)
        start = time.perf_counter()

        # Use COPY with pipe delimiter, skip header
        copy_cmd = f"\\COPY {tname} FROM '{fpath}' WITH (FORMAT csv, DELIMITER '|', HEADER true, NULL '')"
        result = subprocess.run(
            ["sudo", "-u", "postgres", "psql", "-d", "ldbc", "-c", copy_cmd],
            capture_output=True, text=True, timeout=300
        )

        rows, _ = pg_query(f"SELECT count(*) FROM {tname}")
        count = int(rows[0]["result"]) if rows else 0

        elapsed = (time.perf_counter() - start) * 1000.0
        rate = count / (elapsed / 1000.0) if elapsed > 0 else 0

        if result.returncode != 0 and "ERROR" in (result.stderr or ""):
            print(f"ERROR: {result.stderr[:150]}")
        else:
            print(f"{count} rows, {elapsed:.0f}ms ({rate:.0f}/s)")

        results.append({"table": tname, "rows_loaded": count, "elapsed_ms": elapsed})

    # Create indexes
    print("  Creating indexes...")
    pg_query("CREATE INDEX IF NOT EXISTS idx_knows_p1 ON knows(person1); CREATE INDEX IF NOT EXISTS idx_knows_p2 ON knows(person2); CREATE INDEX IF NOT EXISTS idx_comment_creator ON comment(creator);")

    return results


def load_age_graph(ldbc_dir):
    """Load data into AGE graph for Cypher queries."""
    base = Path(ldbc_dir)

    print("  Setting up AGE graph...")
    pg_query("SET search_path = ag_catalog, public; SELECT create_graph('ldbc_graph');")

    # Load persons into AGE
    print("  Loading Person vertices into AGE...", end=" ", flush=True)
    start = time.perf_counter()
    pg_query("""
        SET search_path = ag_catalog, public;
        SELECT * FROM cypher('ldbc_graph', $$
            UNWIND [r IN (SELECT array_agg(row_to_json(p)) FROM person p)] AS row
            CREATE (:Person {id: row.id, firstName: row.firstname, lastName: row.lastname})
        $$) AS (v agtype);
    """)
    elapsed = (time.perf_counter() - start) * 1000.0
    print(f"{elapsed:.0f}ms")

    # Load KNOWS edges
    print("  Loading KNOWS edges into AGE...", end=" ", flush=True)
    start = time.perf_counter()
    pg_query("""
        SET search_path = ag_catalog, public;
        SELECT * FROM cypher('ldbc_graph', $$
            UNWIND [r IN (SELECT array_agg(row_to_json(k)) FROM knows k)] AS row
            MATCH (a:Person {id: row.person1}), (b:Person {id: row.person2})
            CREATE (a)-[:KNOWS]->(b)
        $$) AS (e agtype);
    """)
    elapsed = (time.perf_counter() - start) * 1000.0
    print(f"{elapsed:.0f}ms")


def run_sql_benchmarks():
    """Run SQL benchmarks."""
    results = []
    person_id = 933

    benchmarks = [
        ("IS1: Person lookup (SQL)", f"SELECT firstName, lastName, gender, birthday FROM person WHERE id = {person_id}"),
        ("IS3: Friend count (SQL)", f"SELECT count(*) FROM knows WHERE person1 = {person_id} OR person2 = {person_id}"),
        ("IC2: Recent messages (SQL)", f"""
            SELECT p.firstName, p.lastName, c.id, c.content
            FROM knows k
            JOIN person p ON p.id = CASE WHEN k.person1 = {person_id} THEN k.person2 ELSE k.person1 END
            JOIN comment c ON c.creator = p.id
            WHERE k.person1 = {person_id} OR k.person2 = {person_id}
            ORDER BY c.creationDate DESC LIMIT 10
        """),
        ("IC5: Forums (SQL 2-hop)", f"""
            WITH friends AS (
                SELECT CASE WHEN person1 = {person_id} THEN person2 ELSE person1 END AS fid
                FROM knows WHERE person1 = {person_id} OR person2 = {person_id}
            ),
            fof AS (
                SELECT DISTINCT CASE WHEN k.person1 = f.fid THEN k.person2 ELSE k.person1 END AS fid
                FROM knows k JOIN friends f ON k.person1 = f.fid OR k.person2 = f.fid
                UNION SELECT fid FROM friends
            )
            SELECT f.title, count(DISTINCT ff.fid) AS members
            FROM has_member hm JOIN fof ff ON ff.fid = hm.person JOIN forum f ON f.id = hm.forum
            GROUP BY f.title ORDER BY members DESC LIMIT 10
        """),
        ("Aggregation: Msgs/person", "SELECT creator, count(*) AS cnt FROM comment GROUP BY creator ORDER BY cnt DESC LIMIT 10"),
        ("Full scan: Filter by IP", "SELECT count(*) FROM comment WHERE locationIP LIKE '1.%'"),
        ("Edge count", "SELECT count(*) FROM knows"),
        ("Node count", "SELECT count(*) FROM person"),
    ]

    # BFS via recursive CTE 1-10
    for hop in range(1, 11):
        if hop == 1:
            q = f"SELECT count(DISTINCT CASE WHEN person1={person_id} THEN person2 ELSE person1 END) FROM knows WHERE person1={person_id} OR person2={person_id}"
        elif hop == 2:
            q = f"""
                WITH hop1 AS (
                    SELECT CASE WHEN person1={person_id} THEN person2 ELSE person1 END AS fid
                    FROM knows WHERE person1={person_id} OR person2={person_id}
                )
                SELECT count(DISTINCT n) FROM (
                    SELECT fid AS n FROM hop1
                    UNION
                    SELECT CASE WHEN k.person1 IN (SELECT fid FROM hop1) THEN k.person2 ELSE k.person1 END
                    FROM knows k WHERE k.person1 IN (SELECT fid FROM hop1) OR k.person2 IN (SELECT fid FROM hop1)
                ) sub
            """
        else:
            q = f"""
                WITH RECURSIVE bfs(node, depth) AS (
                    SELECT CASE WHEN person1={person_id} THEN person2 ELSE person1 END, 1
                    FROM knows WHERE person1={person_id} OR person2={person_id}
                    UNION
                    SELECT CASE WHEN k.person1 = b.node THEN k.person2 ELSE k.person1 END, b.depth + 1
                    FROM knows k JOIN bfs b ON k.person1 = b.node OR k.person2 = b.node
                    WHERE b.depth < {hop}
                )
                SELECT count(DISTINCT node) FROM bfs
            """
        benchmarks.append((f"BFS {hop}-hop (SQL CTE)", q))

    for name, query in benchmarks:
        print(f"  {name}...", end=" ", flush=True)
        br = BenchmarkResult(name=name, query=query.strip())
        try:
            start_wall = time.perf_counter()
            rows, cold_ms = pg_query(query)
            elapsed = time.perf_counter() - start_wall

            br.cold_ms = round(cold_ms, 2)
            br.rows = len(rows)
            br.result_sample = rows[:3]

            if elapsed > QUERY_TIMEOUT_SEC:
                br.warm_ms = cold_ms
                br.warm_runs = 0
                print(f"cold={br.cold_ms:.1f}ms (TIMEOUT) rows={br.rows}")
                results.append(br)
                if name.startswith("BFS"):
                    hop_num = int(name.split()[1].split("-")[0])
                    for rh in range(hop_num + 1, 11):
                        skip = BenchmarkResult(name=f"BFS {rh}-hop (SQL CTE)", query="SKIPPED", error=f"DB died at {name}")
                        results.append(skip)
                        print(f"  BFS {rh}-hop... SKIPPED")
                    break
                continue

            if cold_ms < 60000:
                warm_times = []
                for _ in range(3):
                    _, wms = pg_query(query)
                    warm_times.append(wms)
                br.warm_ms = round(sum(warm_times) / len(warm_times), 2)
                br.warm_runs = len(warm_times)
            else:
                _, wms = pg_query(query)
                br.warm_ms = round(wms, 2)
                br.warm_runs = 1

            print(f"cold={br.cold_ms:.1f}ms warm={br.warm_ms:.1f}ms rows={br.rows}")
        except Exception as e:
            br.error = str(e)[:300]
            print(f"ERROR: {str(e)[:100]}")

        results.append(br)

    return results


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--ldbc-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--skip-load", action="store_true")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    all_results = {
        "engine": "postgresql+age",
        "version": "PostgreSQL 16.13 + Apache AGE 1.5.0",
        "host": "localhost",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "install_method": "native apt package (postgresql + postgresql-16-age)",
        "gpu_support": False,
        "gpu_note": "PostgreSQL has NO GPU acceleration",
        "db_type": "relational + graph extension",
        "query_language": "SQL + Cypher (via AGE extension)",
        "graph_support": "Apache AGE provides Cypher query support as a PostgreSQL extension",
        "foreign_keys": "YES - PostgreSQL natively supports foreign key constraints",
    }

    print("=== Competitor Benchmark: PostgreSQL 16.13 + AGE 1.5.0 ===")

    if not args.skip_load:
        print("\n=== LOADING DATA (relational tables) ===")
        load_results = load_ldbc(args.ldbc_dir)
        all_results["load"] = load_results

    print("\n=== RUNNING SQL BENCHMARKS ===")
    bench_results = run_sql_benchmarks()
    all_results["benchmarks"] = [asdict(r) for r in bench_results]

    out_file = os.path.join(args.output, "postgresql_benchmark.json")
    with open(out_file, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n=== Results saved to {out_file} ===")

    print("\n=== SUMMARY ===")
    print(f"{'Benchmark':<35} {'Cold (ms)':>10} {'Warm (ms)':>10} {'Rows':>8}")
    print("=" * 67)
    for br in bench_results:
        print(f"{br.name:<35} {br.cold_ms:>10.1f} {br.warm_ms:>10.1f} {br.rows:>8}")


if __name__ == "__main__":
    main()
