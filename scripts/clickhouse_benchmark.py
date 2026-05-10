#!/usr/bin/env python3
"""ClickHouse benchmark: load LDBC SF1 as relational tables, run SQL benchmarks."""
import csv
import json
import os
import subprocess
import time
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


QUERY_TIMEOUT_SEC = 300
LDBC_DIR = ""


def ch_query(query):
    """Run a ClickHouse query via clickhouse-client and return (rows, wall_ms)."""
    start = time.perf_counter()
    try:
        result = subprocess.run(
            ["clickhouse-client", "--query", query, "--format", "JSONEachRow"],
            capture_output=True, text=True, timeout=QUERY_TIMEOUT_SEC
        )
        wall_ms = (time.perf_counter() - start) * 1000.0
        rows = []
        if result.stdout.strip():
            for line in result.stdout.strip().split("\n"):
                rows.append(json.loads(line))
        if result.returncode != 0 and result.stderr:
            raise Exception(result.stderr.strip()[:300])
        return rows, wall_ms
    except subprocess.TimeoutExpired:
        wall_ms = (time.perf_counter() - start) * 1000.0
        return [], wall_ms


def load_ldbc_tables(ldbc_dir):
    """Create ClickHouse tables and load LDBC CSV data."""
    results = []
    base = Path(ldbc_dir)

    # Create database
    subprocess.run(["clickhouse-client", "--query", "CREATE DATABASE IF NOT EXISTS ldbc"], check=True)

    tables = {
        "person": {
            "file": "dynamic/person_0_0.csv",
            "ddl": """CREATE TABLE IF NOT EXISTS ldbc.person (
                id Int64, firstName String, lastName String, gender String,
                birthday String, creationDate String, locationIP String,
                browserUsed String, place Int64, language String, email String
            ) ENGINE = MergeTree() ORDER BY id""",
        },
        "comment": {
            "file": "dynamic/comment_0_0.csv",
            "ddl": """CREATE TABLE IF NOT EXISTS ldbc.comment (
                id Int64, creationDate String, locationIP String, browserUsed String,
                content String, length Int32, creator Int64, place Int64,
                replyOfPost Int64, replyOfComment Int64
            ) ENGINE = MergeTree() ORDER BY id""",
        },
        "post": {
            "file": "dynamic/post_0_0.csv",
            "ddl": """CREATE TABLE IF NOT EXISTS ldbc.post (
                id Int64, imageFile String, creationDate String, locationIP String,
                browserUsed String, language String, content String, length Int32,
                creator Int64, forumId Int64, place Int64
            ) ENGINE = MergeTree() ORDER BY id""",
        },
        "forum": {
            "file": "dynamic/forum_0_0.csv",
            "ddl": """CREATE TABLE IF NOT EXISTS ldbc.forum (
                id Int64, title String, creationDate String, moderator Int64
            ) ENGINE = MergeTree() ORDER BY id""",
        },
        "knows": {
            "file": "dynamic/person_knows_person_0_0.csv",
            "ddl": """CREATE TABLE IF NOT EXISTS ldbc.knows (
                person1 Int64, person2 Int64, creationDate String
            ) ENGINE = MergeTree() ORDER BY (person1, person2)""",
        },
        "likes_comment": {
            "file": "dynamic/person_likes_comment_0_0.csv",
            "ddl": """CREATE TABLE IF NOT EXISTS ldbc.likes_comment (
                person Int64, comment Int64, creationDate String
            ) ENGINE = MergeTree() ORDER BY (person, comment)""",
        },
        "likes_post": {
            "file": "dynamic/person_likes_post_0_0.csv",
            "ddl": """CREATE TABLE IF NOT EXISTS ldbc.likes_post (
                person Int64, post Int64, creationDate String
            ) ENGINE = MergeTree() ORDER BY (person, post)""",
        },
        "has_member": {
            "file": "dynamic/forum_hasMember_person_0_0.csv",
            "ddl": """CREATE TABLE IF NOT EXISTS ldbc.has_member (
                forum Int64, person Int64, creationDate String
            ) ENGINE = MergeTree() ORDER BY (forum, person)""",
        },
        "work_at": {
            "file": "dynamic/person_workAt_organisation_0_0.csv",
            "ddl": """CREATE TABLE IF NOT EXISTS ldbc.work_at (
                person Int64, organisation Int64, workFrom Int32
            ) ENGINE = MergeTree() ORDER BY (person, organisation)""",
        },
        "organisation": {
            "file": "static/organisation_0_0.csv",
            "ddl": """CREATE TABLE IF NOT EXISTS ldbc.organisation (
                id Int64, type String, name String, url String
            ) ENGINE = MergeTree() ORDER BY id""",
        },
        "place": {
            "file": "static/place_0_0.csv",
            "ddl": """CREATE TABLE IF NOT EXISTS ldbc.place (
                id Int64, name String, url String, type String
            ) ENGINE = MergeTree() ORDER BY id""",
        },
    }

    for tname, tinfo in tables.items():
        # Create table
        subprocess.run(["clickhouse-client", "--query", tinfo["ddl"]], check=True)

        fpath = base / tinfo["file"]
        if not fpath.exists():
            print(f"  SKIP {tname} (file not found)")
            continue

        print(f"  Loading {tname}...", end=" ", flush=True)
        start = time.perf_counter()

        # Load pipe-delimited CSV using clickhouse-client
        # LDBC uses | delimiter, not comma
        with open(fpath, "r") as f:
            header = f.readline()  # skip header
            result = subprocess.run(
                ["clickhouse-client",
                 "--format_csv_delimiter=|",
                 "--query", f"INSERT INTO ldbc.{tname} FORMAT CSV"],
                stdin=f, capture_output=True, text=True
            )

        # Count rows
        rows, _ = ch_query(f"SELECT count() AS cnt FROM ldbc.{tname}")
        count = rows[0]["cnt"] if rows else 0

        elapsed = (time.perf_counter() - start) * 1000.0
        rate = count / (elapsed / 1000.0) if elapsed > 0 else 0
        print(f"{count} rows, {elapsed:.0f}ms ({rate:.0f}/s)")

        if result.returncode != 0:
            print(f"    WARNING: {result.stderr[:200]}")

        results.append({"table": tname, "rows_loaded": count, "elapsed_ms": elapsed})

    return results


def run_benchmarks():
    """Run SQL analytical benchmarks against ClickHouse."""
    results = []
    person_id = 933

    benchmarks = [
        ("IS1: Person lookup (SQL)", f"SELECT firstName, lastName, gender, birthday FROM ldbc.person WHERE id = {person_id}"),
        ("IS3: Friend count (SQL JOIN)", f"SELECT count() AS cnt FROM ldbc.knows WHERE person1 = {person_id} OR person2 = {person_id}"),
        ("IC2: Recent messages (SQL JOIN)", f"""
            SELECT p.firstName, p.lastName, c.id, c.content
            FROM ldbc.knows k
            JOIN ldbc.person p ON p.id = CASE WHEN k.person1 = {person_id} THEN k.person2 ELSE k.person1 END
            JOIN ldbc.comment c ON c.creator = p.id
            WHERE k.person1 = {person_id} OR k.person2 = {person_id}
            ORDER BY c.creationDate DESC LIMIT 10
        """),
        ("IC5: Forums via friends (SQL 2-hop JOIN)", f"""
            WITH friends AS (
                SELECT CASE WHEN person1 = {person_id} THEN person2 ELSE person1 END AS fid
                FROM ldbc.knows WHERE person1 = {person_id} OR person2 = {person_id}
            ),
            fof AS (
                SELECT DISTINCT CASE WHEN k.person1 = f.fid THEN k.person2 ELSE k.person1 END AS fid
                FROM ldbc.knows k JOIN friends f ON k.person1 = f.fid OR k.person2 = f.fid
                UNION ALL SELECT fid FROM friends
            )
            SELECT f.title, count(DISTINCT ff.fid) AS members
            FROM ldbc.has_member hm
            JOIN fof ff ON ff.fid = hm.person
            JOIN ldbc.forum f ON f.id = hm.forum
            GROUP BY f.title ORDER BY members DESC LIMIT 10
        """),
        ("Aggregation: Messages per person", "SELECT creator, count() AS msg_count FROM ldbc.comment GROUP BY creator ORDER BY msg_count DESC LIMIT 10"),
        ("Full scan: Filter comments by IP", "SELECT count() AS cnt FROM ldbc.comment WHERE locationIP LIKE '1.%'"),
        ("Edge count (SQL)", "SELECT count() FROM ldbc.knows"),
        ("Node count (SQL)", "SELECT count() FROM ldbc.person"),
        # Recursive BFS using ClickHouse - limited support
        ("BFS 1-hop (SQL)", f"SELECT count(DISTINCT CASE WHEN person1 = {person_id} THEN person2 ELSE person1 END) AS cnt FROM ldbc.knows WHERE person1 = {person_id} OR person2 = {person_id}"),
        ("BFS 2-hop (SQL)", f"""
            WITH hop1 AS (
                SELECT CASE WHEN person1 = {person_id} THEN person2 ELSE person1 END AS fid
                FROM ldbc.knows WHERE person1 = {person_id} OR person2 = {person_id}
            )
            SELECT count(DISTINCT CASE WHEN k.person1 IN (SELECT fid FROM hop1) THEN k.person2 ELSE k.person1 END) AS cnt
            FROM ldbc.knows k WHERE k.person1 IN (SELECT fid FROM hop1) OR k.person2 IN (SELECT fid FROM hop1)
        """),
    ]

    for name, query in benchmarks:
        print(f"  {name}...", end=" ", flush=True)
        br = BenchmarkResult(name=name, query=query.strip())
        try:
            rows, cold_ms = ch_query(query)
            br.cold_ms = round(cold_ms, 2)
            br.rows = len(rows)
            br.result_sample = rows[:3]

            warm_times = []
            for _ in range(3):
                _, wms = ch_query(query)
                warm_times.append(wms)
            br.warm_ms = round(sum(warm_times) / len(warm_times), 2)
            br.warm_runs = len(warm_times)

            print(f"cold={br.cold_ms:.1f}ms warm={br.warm_ms:.1f}ms rows={br.rows}")
        except Exception as e:
            br.error = str(e)
            print(f"ERROR: {e}")

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
        "engine": "clickhouse",
        "version": "26.3.9.8",
        "host": "localhost",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "install_method": "native apt package",
        "gpu_support": False,
        "gpu_note": "ClickHouse has NO GPU acceleration",
        "db_type": "analytical/columnar",
        "query_language": "SQL",
        "graph_support": "NO native graph support. No Cypher, no graph traversal operators. BFS requires manual recursive CTEs.",
    }

    print("=== Competitor Benchmark: ClickHouse 26.3.9 ===")

    if not args.skip_load:
        print("\n=== LOADING DATA (as relational tables) ===")
        load_results = load_ldbc_tables(args.ldbc_dir)
        all_results["load"] = load_results

    print("\n=== RUNNING SQL BENCHMARKS ===")
    bench_results = run_benchmarks()
    all_results["benchmarks"] = [asdict(r) for r in bench_results]

    out_file = os.path.join(args.output, "clickhouse_benchmark.json")
    with open(out_file, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n=== Results saved to {out_file} ===")

    print("\n=== SUMMARY ===")
    print(f"{'Benchmark':<40} {'Cold (ms)':>10} {'Warm (ms)':>10} {'Rows':>8}")
    print("=" * 72)
    for br in bench_results:
        print(f"{br.name:<40} {br.cold_ms:>10.1f} {br.warm_ms:>10.1f} {br.rows:>8}")


if __name__ == "__main__":
    main()
