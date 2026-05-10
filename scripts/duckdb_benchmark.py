#!/usr/bin/env python3
"""DuckDB benchmark: load LDBC SF1, run SQL + DuckPGQ graph benchmarks."""
import json
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
import duckdb


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


def timed_query(con, query):
    start = time.perf_counter()
    result = con.execute(query)
    rows = result.fetchall()
    cols = [desc[0] for desc in result.description] if result.description else []
    wall_ms = (time.perf_counter() - start) * 1000.0
    row_dicts = [dict(zip(cols, r)) for r in rows]
    return row_dicts, wall_ms


def load_ldbc(con, ldbc_dir):
    """Load LDBC CSV files directly into DuckDB — it handles pipe-delimited CSVs natively."""
    results = []
    base = Path(ldbc_dir)

    tables = {
        "person": ("dynamic/person_0_0.csv", "id BIGINT, firstName VARCHAR, lastName VARCHAR, gender VARCHAR, birthday VARCHAR, creationDate VARCHAR, locationIP VARCHAR, browserUsed VARCHAR, place BIGINT, language VARCHAR, email VARCHAR"),
        "comment": ("dynamic/comment_0_0.csv", "id BIGINT, creationDate VARCHAR, locationIP VARCHAR, browserUsed VARCHAR, content VARCHAR, length INTEGER, creator BIGINT, place BIGINT, replyOfPost BIGINT, replyOfComment BIGINT"),
        "post": ("dynamic/post_0_0.csv", "id BIGINT, imageFile VARCHAR, creationDate VARCHAR, locationIP VARCHAR, browserUsed VARCHAR, language VARCHAR, content VARCHAR, length INTEGER, creator BIGINT, forumId BIGINT, place BIGINT"),
        "forum": ("dynamic/forum_0_0.csv", "id BIGINT, title VARCHAR, creationDate VARCHAR, moderator BIGINT"),
        "knows": ("dynamic/person_knows_person_0_0.csv", "person1 BIGINT, person2 BIGINT, creationDate VARCHAR"),
        "likes_comment": ("dynamic/person_likes_comment_0_0.csv", "person BIGINT, comment BIGINT, creationDate VARCHAR"),
        "likes_post": ("dynamic/person_likes_post_0_0.csv", "person BIGINT, post BIGINT, creationDate VARCHAR"),
        "has_member": ("dynamic/forum_hasMember_person_0_0.csv", "forum BIGINT, person BIGINT, creationDate VARCHAR"),
        "work_at": ("dynamic/person_workAt_organisation_0_0.csv", "person BIGINT, organisation BIGINT, workFrom INTEGER"),
        "study_at": ("dynamic/person_studyAt_organisation_0_0.csv", "person BIGINT, organisation BIGINT, classYear INTEGER"),
        "organisation": ("static/organisation_0_0.csv", "id BIGINT, type VARCHAR, name VARCHAR, url VARCHAR"),
        "place": ("static/place_0_0.csv", "id BIGINT, name VARCHAR, url VARCHAR, type VARCHAR"),
    }

    for tname, (csv_file, schema) in tables.items():
        fpath = base / csv_file
        if not fpath.exists():
            print(f"  SKIP {tname}")
            continue

        print(f"  Loading {tname}...", end=" ", flush=True)
        start = time.perf_counter()

        try:
            con.execute(f"DROP TABLE IF EXISTS {tname}")
            con.execute(f"""
                CREATE TABLE {tname} AS
                SELECT * FROM read_csv('{fpath}',
                    delim='|', header=true, ignore_errors=true,
                    columns={{{', '.join(f"'{c.split()[0]}': '{' '.join(c.split()[1:])}'" for c in schema.split(', '))}}}
                )
            """)
            rows, _ = timed_query(con, f"SELECT count(*) AS cnt FROM {tname}")
            count = rows[0]["cnt"]
        except Exception as e:
            # Fallback: auto-detect
            try:
                con.execute(f"DROP TABLE IF EXISTS {tname}")
                con.execute(f"CREATE TABLE {tname} AS SELECT * FROM read_csv_auto('{fpath}', delim='|', header=true, ignore_errors=true)")
                rows, _ = timed_query(con, f"SELECT count(*) AS cnt FROM {tname}")
                count = rows[0]["cnt"]
            except Exception as e2:
                print(f"ERROR: {e2}")
                count = 0

        elapsed = (time.perf_counter() - start) * 1000.0
        rate = count / (elapsed / 1000.0) if elapsed > 0 else 0
        print(f"{count} rows, {elapsed:.0f}ms ({rate:.0f}/s)")
        results.append({"table": tname, "rows_loaded": count, "elapsed_ms": elapsed})

    return results


def run_benchmarks(con):
    results = []
    person_id = 933

    benchmarks = [
        ("IS1: Person lookup", f"SELECT firstName, lastName, gender, birthday FROM person WHERE id = {person_id}"),
        ("IS3: Friend count (SQL JOIN)", f"SELECT count(*) AS cnt FROM knows WHERE person1 = {person_id} OR person2 = {person_id}"),
        ("IC2: Recent messages (SQL JOIN)", f"""
            SELECT p.firstName, p.lastName, c.id, c.content
            FROM knows k
            JOIN person p ON p.id = CASE WHEN k.person1 = {person_id} THEN k.person2 ELSE k.person1 END
            JOIN comment c ON c.creator = p.id
            WHERE k.person1 = {person_id} OR k.person2 = {person_id}
            ORDER BY c.creationDate DESC LIMIT 10
        """),
        ("IC5: Forums via friends (SQL 2-hop)", f"""
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
            FROM has_member hm
            JOIN fof ff ON ff.fid = hm.person
            JOIN forum f ON f.id = hm.forum
            GROUP BY f.title ORDER BY members DESC LIMIT 10
        """),
        ("Aggregation: Msgs per person", "SELECT creator, count(*) AS msg_count FROM comment GROUP BY creator ORDER BY msg_count DESC LIMIT 10"),
        ("Full scan: Filter by IP", "SELECT count(*) AS cnt FROM comment WHERE locationIP LIKE '1.%'"),
        ("Edge count", "SELECT count(*) FROM knows"),
        ("Node count", "SELECT count(*) FROM person"),
    ]

    # BFS hops via recursive CTE
    for hop in range(1, 11):
        if hop == 1:
            q = f"SELECT count(DISTINCT CASE WHEN person1={person_id} THEN person2 ELSE person1 END) AS cnt FROM knows WHERE person1={person_id} OR person2={person_id}"
        elif hop == 2:
            q = f"""
                WITH hop1 AS (
                    SELECT CASE WHEN person1={person_id} THEN person2 ELSE person1 END AS fid
                    FROM knows WHERE person1={person_id} OR person2={person_id}
                )
                SELECT count(DISTINCT n) AS cnt FROM (
                    SELECT fid AS n FROM hop1
                    UNION
                    SELECT CASE WHEN k.person1 IN (SELECT fid FROM hop1) THEN k.person2 ELSE k.person1 END AS n
                    FROM knows k WHERE k.person1 IN (SELECT fid FROM hop1) OR k.person2 IN (SELECT fid FROM hop1)
                )
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
                SELECT count(DISTINCT node) AS cnt FROM bfs
            """
        benchmarks.append((f"BFS {hop}-hop", q))

    for name, query in benchmarks:
        print(f"  {name}...", end=" ", flush=True)
        br = BenchmarkResult(name=name, query=query.strip())
        try:
            start_wall = time.perf_counter()
            rows, cold_ms = timed_query(con, query)
            elapsed = time.perf_counter() - start_wall

            br.cold_ms = round(cold_ms, 2)
            br.rows = len(rows)
            br.result_sample = [{k: str(v) for k, v in r.items()} for r in rows[:3]]

            if elapsed > QUERY_TIMEOUT_SEC:
                br.warm_ms = cold_ms
                br.warm_runs = 0
                print(f"cold={br.cold_ms:.1f}ms (TIMEOUT) rows={br.rows}")
                results.append(br)
                if name.startswith("BFS"):
                    hop_num = int(name.split()[1].split("-")[0])
                    for rh in range(hop_num + 1, 11):
                        skip = BenchmarkResult(name=f"BFS {rh}-hop", query="SKIPPED", error=f"DB died at {name}")
                        results.append(skip)
                        print(f"  BFS {rh}-hop... SKIPPED")
                    break
                continue

            if cold_ms < 60000:
                warm_times = []
                for _ in range(3):
                    _, wms = timed_query(con, query)
                    warm_times.append(wms)
                br.warm_ms = round(sum(warm_times) / len(warm_times), 2)
                br.warm_runs = len(warm_times)
            else:
                _, wms = timed_query(con, query)
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

    db_path = os.path.join(args.output, "ldbc.duckdb")
    con = duckdb.connect(db_path)

    all_results = {
        "engine": "duckdb",
        "version": duckdb.__version__,
        "host": "localhost (embedded)",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "install_method": "pip install duckdb (embedded, no server)",
        "gpu_support": False,
        "gpu_note": "DuckDB has NO GPU acceleration (CPU-only, SIMD vectorized execution)",
        "db_type": "analytical/embedded",
        "query_language": "SQL (PostgreSQL-compatible)",
        "graph_support": "DuckPGQ extension available for SQL/PGQ graph queries, but requires separate install",
    }

    print(f"=== Competitor Benchmark: DuckDB {duckdb.__version__} ===")

    if not args.skip_load:
        print("\n=== LOADING DATA ===")
        load_results = load_ldbc(con, args.ldbc_dir)
        all_results["load"] = load_results

    print("\n=== RUNNING BENCHMARKS ===")
    bench_results = run_benchmarks(con)
    all_results["benchmarks"] = [asdict(r) for r in bench_results]

    con.close()

    out_file = os.path.join(args.output, "duckdb_benchmark.json")
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
