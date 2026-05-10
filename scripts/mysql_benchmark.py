#!/usr/bin/env python3
"""MySQL benchmark: load LDBC SF1, run SQL benchmarks + recursive CTE BFS 1-10."""
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


def mysql_query(query, db="ldbc"):
    start = time.perf_counter()
    result = subprocess.run(
        ["mysql", "-u", "root", "-D", db, "-N", "-B", "-e", query],
        capture_output=True, text=True, timeout=QUERY_TIMEOUT_SEC
    )
    wall_ms = (time.perf_counter() - start) * 1000.0
    rows = []
    if result.stdout.strip():
        for line in result.stdout.strip().split("\n"):
            rows.append({"result": line})
    if result.returncode != 0 and result.stderr:
        raise Exception(result.stderr.strip()[:300])
    return rows, wall_ms


def load_ldbc(ldbc_dir):
    results = []
    base = Path(ldbc_dir)

    ddl = """
    DROP TABLE IF EXISTS knows;
    DROP TABLE IF EXISTS likes_comment;
    DROP TABLE IF EXISTS likes_post;
    DROP TABLE IF EXISTS has_member;
    DROP TABLE IF EXISTS work_at;
    DROP TABLE IF EXISTS study_at;
    DROP TABLE IF EXISTS comment_tbl;
    DROP TABLE IF EXISTS post;
    DROP TABLE IF EXISTS forum;
    DROP TABLE IF EXISTS person;
    DROP TABLE IF EXISTS organisation;
    DROP TABLE IF EXISTS place;

    CREATE TABLE person (id BIGINT PRIMARY KEY, firstName VARCHAR(255), lastName VARCHAR(255), gender VARCHAR(20), birthday VARCHAR(50), creationDate VARCHAR(50), locationIP VARCHAR(50), browserUsed VARCHAR(50), place BIGINT, language TEXT, email TEXT);
    CREATE TABLE forum (id BIGINT PRIMARY KEY, title TEXT, creationDate VARCHAR(50), moderator BIGINT);
    CREATE TABLE comment_tbl (id BIGINT PRIMARY KEY, creationDate VARCHAR(50), locationIP VARCHAR(50), browserUsed VARCHAR(50), content TEXT, length INT, creator BIGINT, place BIGINT, replyOfPost BIGINT, replyOfComment BIGINT);
    CREATE TABLE post (id BIGINT PRIMARY KEY, imageFile TEXT, creationDate VARCHAR(50), locationIP VARCHAR(50), browserUsed VARCHAR(50), language VARCHAR(50), content TEXT, length INT, creator BIGINT, forumId BIGINT, place BIGINT);
    CREATE TABLE organisation (id BIGINT PRIMARY KEY, type VARCHAR(50), name VARCHAR(255), url TEXT);
    CREATE TABLE place (id BIGINT PRIMARY KEY, name VARCHAR(255), url TEXT, type VARCHAR(50));
    CREATE TABLE knows (person1 BIGINT, person2 BIGINT, creationDate VARCHAR(50), INDEX(person1), INDEX(person2));
    CREATE TABLE likes_comment (person BIGINT, comment_id BIGINT, creationDate VARCHAR(50));
    CREATE TABLE likes_post (person BIGINT, post BIGINT, creationDate VARCHAR(50));
    CREATE TABLE has_member (forum BIGINT, person BIGINT, creationDate VARCHAR(50));
    CREATE TABLE work_at (person BIGINT, organisation BIGINT, workFrom INT);
    CREATE TABLE study_at (person BIGINT, organisation BIGINT, classYear INT);
    """
    mysql_query(ddl)
    print("  Tables created")

    csv_map = [
        ("person", "dynamic/person_0_0.csv"),
        ("forum", "dynamic/forum_0_0.csv"),
        ("comment_tbl", "dynamic/comment_0_0.csv"),
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

    for tname, csv_file in csv_map:
        fpath = base / csv_file
        if not fpath.exists():
            print(f"  SKIP {tname}")
            continue

        print(f"  Loading {tname}...", end=" ", flush=True)
        start = time.perf_counter()

        load_q = f"LOAD DATA LOCAL INFILE '{fpath}' INTO TABLE {tname} FIELDS TERMINATED BY '|' LINES TERMINATED BY '\\n' IGNORE 1 ROWS"
        result = subprocess.run(
            ["mysql", "-u", "root", "-D", "ldbc", "--local-infile=1", "-e", load_q],
            capture_output=True, text=True, timeout=300
        )

        rows, _ = mysql_query(f"SELECT count(*) FROM {tname}")
        count = int(rows[0]["result"]) if rows else 0

        elapsed = (time.perf_counter() - start) * 1000.0
        rate = count / (elapsed / 1000.0) if elapsed > 0 else 0

        if result.returncode != 0:
            print(f"ERROR: {result.stderr[:150]}")
        else:
            print(f"{count} rows, {elapsed:.0f}ms ({rate:.0f}/s)")

        results.append({"table": tname, "rows_loaded": count, "elapsed_ms": elapsed})

    # Index on comment creator
    mysql_query("CREATE INDEX idx_comment_creator ON comment_tbl(creator)")
    return results


def run_benchmarks():
    results = []
    person_id = 933

    benchmarks = [
        ("IS1: Person lookup (SQL)", f"SELECT firstName, lastName, gender, birthday FROM person WHERE id = {person_id}"),
        ("IS3: Friend count (SQL)", f"SELECT count(*) FROM knows WHERE person1 = {person_id} OR person2 = {person_id}"),
        ("IC2: Recent messages (SQL)", f"""
            SELECT p.firstName, p.lastName, c.id, c.content
            FROM knows k
            JOIN person p ON p.id = CASE WHEN k.person1 = {person_id} THEN k.person2 ELSE k.person1 END
            JOIN comment_tbl c ON c.creator = p.id
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
        ("Aggregation: Msgs/person", "SELECT creator, count(*) AS cnt FROM comment_tbl GROUP BY creator ORDER BY cnt DESC LIMIT 10"),
        ("Full scan: Filter by IP", "SELECT count(*) FROM comment_tbl WHERE locationIP LIKE '1.%'"),
        ("Edge count", "SELECT count(*) FROM knows"),
        ("Node count", "SELECT count(*) FROM person"),
    ]

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
                    UNION ALL
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
            rows, cold_ms = mysql_query(query)
            elapsed = time.perf_counter() - start_wall

            br.cold_ms = round(cold_ms, 2)
            br.rows = len(rows)
            br.result_sample = rows[:3]

            if elapsed > QUERY_TIMEOUT_SEC:
                br.warm_ms = cold_ms
                br.warm_runs = 0
                print(f"cold={br.cold_ms:.1f}ms (TIMEOUT)")
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
                    _, wms = mysql_query(query)
                    warm_times.append(wms)
                br.warm_ms = round(sum(warm_times) / len(warm_times), 2)
                br.warm_runs = len(warm_times)
            else:
                _, wms = mysql_query(query)
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

    # Create database
    subprocess.run(["mysql", "-u", "root", "-e", "CREATE DATABASE IF NOT EXISTS ldbc"], check=True)

    all_results = {
        "engine": "mysql",
        "version": "",
        "host": "localhost",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "install_method": "native apt package",
        "gpu_support": False,
        "gpu_note": "MySQL has NO GPU acceleration",
        "db_type": "relational (OLTP)",
        "query_language": "SQL only",
        "graph_support": "NO — MySQL has no graph query support, no Cypher, no path operators. BFS requires recursive CTEs (added in MySQL 8.0).",
        "foreign_keys": "YES — MySQL supports foreign key constraints via InnoDB",
    }

    # Get version
    rows, _ = mysql_query("SELECT version()")
    all_results["version"] = rows[0]["result"] if rows else "unknown"

    print(f"=== Competitor Benchmark: MySQL {all_results['version']} ===")

    if not args.skip_load:
        print("\n=== LOADING DATA ===")
        load_results = load_ldbc(args.ldbc_dir)
        all_results["load"] = load_results

    print("\n=== RUNNING SQL BENCHMARKS ===")
    bench_results = run_benchmarks()
    all_results["benchmarks"] = [asdict(r) for r in bench_results]

    out_file = os.path.join(args.output, "mysql_benchmark.json")
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
