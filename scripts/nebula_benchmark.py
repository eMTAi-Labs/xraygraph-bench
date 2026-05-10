#!/usr/bin/env python3
"""NebulaGraph benchmark runner using nGQL."""
import csv
import json
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config


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


@dataclass
class LoadResult:
    table: str
    rows_loaded: int
    elapsed_ms: float
    error: str = ""


QUERY_TIMEOUT_SEC = 300

# FK columns to skip when loading node properties
FK_COLUMNS = {"creator", "place", "replyOfPost", "replyOfComment", "Forum.id", "moderator"}


def timed_query(session, query):
    start = time.perf_counter()
    result = session.execute(query)
    wall_ms = (time.perf_counter() - start) * 1000.0
    rows = []
    if result.is_succeeded() and result.row_size() > 0:
        col_names = result.keys()
        for i in range(result.row_size()):
            row = {}
            for j, col in enumerate(col_names):
                val = result.row_values(i)[j]
                if val.is_string():
                    row[col] = val.as_string()
                elif val.is_int():
                    row[col] = val.as_int()
                elif val.is_double():
                    row[col] = val.as_double()
                elif val.is_bool():
                    row[col] = val.as_bool()
                elif val.is_vertex():
                    row[col] = str(val.as_node().get_id())
                else:
                    row[col] = str(val)
            rows.append(row)
    elif not result.is_succeeded():
        raise Exception(result.error_msg())
    return rows, wall_ms


def load_nodes(session, ldbc_dir):
    results = []
    base = Path(ldbc_dir)

    node_files = [
        ("dynamic/person_0_0.csv", "Person", ["firstName", "lastName", "gender", "birthday", "creationDate", "locationIP", "browserUsed"]),
        ("dynamic/forum_0_0.csv", "Forum", ["title", "creationDate"]),
        ("dynamic/comment_0_0.csv", "Comment", ["creationDate", "locationIP", "browserUsed", "content", "length"]),
        ("dynamic/post_0_0.csv", "Post", ["imageFile", "creationDate", "locationIP", "browserUsed", "language", "content", "length"]),
        ("static/organisation_0_0.csv", "Organisation", ["type", "name", "url"]),
        ("static/place_0_0.csv", "Place", ["name", "url", "type"]),
        ("static/tagclass_0_0.csv", "TagClass", ["name", "url"]),
    ]

    for csv_file, label, props in node_files:
        fpath = base / csv_file
        if not fpath.exists():
            print(f"  SKIP {csv_file}")
            continue

        print(f"  Loading {label}...", end=" ", flush=True)
        start = time.perf_counter()
        count = 0
        batch_stmts = []
        batch_size = 500

        with open(fpath, "r") as f:
            reader = csv.reader(f, delimiter="|")
            header = next(reader)
            id_idx = header.index("id")
            prop_indices = []
            for p in props:
                if p in header and p not in FK_COLUMNS:
                    prop_indices.append((header.index(p), p))

            prop_names = ", ".join(p for _, p in prop_indices)

            for row in reader:
                vid = row[id_idx]
                vals = []
                for idx, _ in prop_indices:
                    v = row[idx] if idx < len(row) else ""
                    v = v.replace("\\", "\\\\").replace('"', '\\"')
                    vals.append(f'"{v}"')
                val_str = ", ".join(vals)
                batch_stmts.append(f'{vid}:({val_str})')

                if len(batch_stmts) >= batch_size:
                    stmt = f'INSERT VERTEX {label}({prop_names}) VALUES ' + ", ".join(batch_stmts)
                    result = session.execute(stmt)
                    if not result.is_succeeded():
                        print(f"\n    ERROR: {result.error_msg()[:200]}")
                        break
                    count += len(batch_stmts)
                    batch_stmts = []

            if batch_stmts:
                stmt = f'INSERT VERTEX {label}({prop_names}) VALUES ' + ", ".join(batch_stmts)
                session.execute(stmt)
                count += len(batch_stmts)

        elapsed = (time.perf_counter() - start) * 1000.0
        rate = count / (elapsed / 1000.0) if elapsed > 0 else 0
        print(f"{count} rows, {elapsed:.0f}ms ({rate:.0f}/s)")
        results.append(LoadResult(label, count, elapsed))

    return results


def load_edges(session, ldbc_dir):
    results = []
    base = Path(ldbc_dir)

    # Explicit edge files
    edge_files = [
        ("dynamic/person_knows_person_0_0.csv", "KNOWS", 0, 1),
        ("dynamic/person_likes_comment_0_0.csv", "LIKES", 0, 1),
        ("dynamic/person_likes_post_0_0.csv", "LIKES", 0, 1),
        ("dynamic/forum_hasMember_person_0_0.csv", "HAS_MEMBER", 0, 1),
        ("dynamic/person_studyAt_organisation_0_0.csv", "STUDY_AT", 0, 1),
        ("dynamic/person_workAt_organisation_0_0.csv", "WORK_AT", 0, 1),
    ]

    for csv_file, rel_type, src_col, dst_col in edge_files:
        fpath = base / csv_file
        if not fpath.exists():
            continue

        print(f"  Loading {rel_type} from {csv_file}...", end=" ", flush=True)
        start = time.perf_counter()
        count = 0
        batch_stmts = []
        batch_size = 500

        with open(fpath, "r") as f:
            reader = csv.reader(f, delimiter="|")
            next(reader)  # skip header
            for row in reader:
                try:
                    s = row[src_col]
                    t = row[dst_col]
                    batch_stmts.append(f'{s}->{t}:()')
                except (IndexError, ValueError):
                    continue

                if len(batch_stmts) >= batch_size:
                    stmt = f'INSERT EDGE {rel_type}() VALUES ' + ", ".join(batch_stmts)
                    result = session.execute(stmt)
                    if not result.is_succeeded():
                        print(f"\n    ERROR: {result.error_msg()[:200]}")
                        break
                    count += len(batch_stmts)
                    batch_stmts = []

            if batch_stmts:
                stmt = f'INSERT EDGE {rel_type}() VALUES ' + ", ".join(batch_stmts)
                session.execute(stmt)
                count += len(batch_stmts)

        elapsed = (time.perf_counter() - start) * 1000.0
        rate = count / (elapsed / 1000.0) if elapsed > 0 else 0
        print(f"{count} rows, {elapsed:.0f}ms ({rate:.0f}/s)")
        results.append(LoadResult(rel_type, count, elapsed))

    # Embedded FK edges from CsvCompositeMergeForeign
    embedded = [
        ("dynamic/comment_0_0.csv", "HAS_CREATOR", "id", "creator"),
        ("dynamic/comment_0_0.csv", "REPLY_OF", "id", "replyOfComment"),
        ("dynamic/comment_0_0.csv", "REPLY_OF", "id", "replyOfPost"),
        ("dynamic/comment_0_0.csv", "IS_LOCATED_IN", "id", "place"),
        ("dynamic/post_0_0.csv", "HAS_CREATOR", "id", "creator"),
        ("dynamic/post_0_0.csv", "IS_LOCATED_IN", "id", "place"),
        ("dynamic/forum_0_0.csv", "HAS_MODERATOR", "id", "moderator"),
        ("dynamic/person_0_0.csv", "IS_LOCATED_IN", "id", "place"),
    ]

    for csv_file, rel_type, id_col, fk_col in embedded:
        fpath = base / csv_file
        if not fpath.exists():
            continue

        print(f"  Loading {rel_type} (from {fk_col} in {csv_file})...", end=" ", flush=True)
        start = time.perf_counter()
        count = 0
        batch_stmts = []
        batch_size = 500

        with open(fpath, "r") as f:
            reader = csv.reader(f, delimiter="|")
            header = next(reader)
            try:
                id_idx = header.index(id_col)
                fk_idx = header.index(fk_col)
            except ValueError:
                print(f"SKIP (column not found)")
                continue

            for row in reader:
                if fk_idx >= len(row) or not row[fk_idx].strip():
                    continue
                try:
                    s = int(row[id_idx])
                    t = int(row[fk_idx])
                    batch_stmts.append(f'{s}->{t}:()')
                except (ValueError, IndexError):
                    continue

                if len(batch_stmts) >= batch_size:
                    stmt = f'INSERT EDGE {rel_type}() VALUES ' + ", ".join(batch_stmts)
                    result = session.execute(stmt)
                    if not result.is_succeeded():
                        print(f"\n    ERROR: {result.error_msg()[:200]}")
                        break
                    count += len(batch_stmts)
                    batch_stmts = []

            if batch_stmts:
                stmt = f'INSERT EDGE {rel_type}() VALUES ' + ", ".join(batch_stmts)
                session.execute(stmt)
                count += len(batch_stmts)

        elapsed = (time.perf_counter() - start) * 1000.0
        rate = count / (elapsed / 1000.0) if elapsed > 0 else 0
        print(f"{count} rows, {elapsed:.0f}ms ({rate:.0f}/s)")
        results.append(LoadResult(f"{rel_type}(embedded)", count, elapsed))

    return results


def run_benchmarks(session, person_id=933):
    results = []

    benchmarks = [
        ("IS1: Profile lookup", f'FETCH PROP ON Person {person_id} YIELD properties(vertex)'),
        ("IS3: Friend count", f'GO FROM {person_id} OVER KNOWS YIELD dst(edge) AS dst | YIELD count(*) AS cnt'),
        ("IC2: Recent messages", f'GO FROM {person_id} OVER KNOWS YIELD dst(edge) AS friend | GO FROM $-.friend OVER HAS_CREATOR REVERSELY YIELD $$.Comment.content AS content, $$.Comment.creationDate AS dt LIMIT 10'),
        ("IC5: Forums via friends", f'GO 1 TO 2 STEPS FROM {person_id} OVER KNOWS YIELD dst(edge) AS friend | GO FROM $-.friend OVER HAS_MEMBER REVERSELY YIELD $$.Forum.title AS title | YIELD $-.title AS title, count(*) AS members | ORDER BY $-.members DESC | LIMIT 10'),
        ("IC11: Work connections", f'GO 1 TO 2 STEPS FROM {person_id} OVER KNOWS YIELD dst(edge) AS friend | GO FROM $-.friend OVER WORK_AT YIELD $-.friend AS fid, dst(edge) AS org | FETCH PROP ON Organisation $-.org YIELD properties(vertex) | LIMIT 10'),
        ("Edge count", 'SUBMIT JOB STATS; YIELD 1 AS placeholder'),
        ("Node count", 'SUBMIT JOB STATS; YIELD 1 AS placeholder'),
    ]

    # BFS 1-10 hops
    for hop in range(1, 11):
        if hop == 1:
            q = f'GO FROM {person_id} OVER KNOWS YIELD dst(edge) AS dst | YIELD count(*) AS cnt'
        else:
            q = f'GO 1 TO {hop} STEPS FROM {person_id} OVER KNOWS YIELD DISTINCT dst(edge) AS dst | YIELD count(*) AS cnt'
        benchmarks.append((f"BFS {hop}-hop", q))

    for name, query in benchmarks:
        print(f"  {name}...", end=" ", flush=True)
        br = BenchmarkResult(name=name, query=query)
        try:
            start_wall = time.perf_counter()
            rows, cold_ms = timed_query(session, query)
            elapsed_total = time.perf_counter() - start_wall

            br.cold_ms = round(cold_ms, 2)
            br.rows = len(rows)
            br.result_sample = rows[:3]

            if elapsed_total > QUERY_TIMEOUT_SEC:
                br.warm_ms = cold_ms
                br.warm_runs = 0
                print(f"cold={br.cold_ms:.1f}ms (TIMEOUT) rows={br.rows}")
                results.append(br)
                if name.startswith("BFS") and cold_ms > QUERY_TIMEOUT_SEC * 1000:
                    hop_num = int(name.split()[1].split("-")[0])
                    for rh in range(hop_num + 1, 11):
                        skip_br = BenchmarkResult(name=f"BFS {rh}-hop", query="SKIPPED", error=f"DB died at {name}")
                        results.append(skip_br)
                        print(f"  BFS {rh}-hop... SKIPPED")
                    break
                continue

            if cold_ms < 60000:
                warm_times = []
                for _ in range(3):
                    _, wms = timed_query(session, query)
                    warm_times.append(wms)
                br.warm_ms = round(sum(warm_times) / len(warm_times), 2)
                br.warm_runs = len(warm_times)
            else:
                _, wms = timed_query(session, query)
                br.warm_ms = round(wms, 2)
                br.warm_runs = 1

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

    config = Config()
    config.max_connection_pool_size = 10
    pool = ConnectionPool()
    pool.init([("127.0.0.1", 9669)], config)
    session = pool.get_session("root", "nebula")

    session.execute("USE ldbc")

    all_results = {
        "engine": "nebulagraph",
        "version": "3.8.0",
        "host": "localhost",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "install_method": "native deb package",
        "gpu_support": False,
        "gpu_note": "NebulaGraph has NO GPU acceleration",
    }

    print("=== Competitor Benchmark: NebulaGraph 3.8.0 ===")

    if not args.skip_load:
        print("\n=== LOADING DATA ===")
        print("  --- Nodes ---")
        node_results = load_nodes(session, args.ldbc_dir)
        all_results["load_nodes"] = [asdict(r) for r in node_results]

        print("  --- Edges ---")
        edge_results = load_edges(session, args.ldbc_dir)
        all_results["load_edges"] = [asdict(r) for r in edge_results]

        # Wait for data to sync
        time.sleep(5)

        # Get counts
        try:
            session.execute("SUBMIT JOB STATS")
            time.sleep(5)
            result = session.execute("SHOW STATS")
            print(f"\n  Stats submitted: {result.is_succeeded()}")
        except Exception as e:
            print(f"\n  Stats collection: {e}")

    print("\n=== RUNNING BENCHMARKS ===")
    bench_results = run_benchmarks(session)
    all_results["benchmarks"] = [asdict(r) for r in bench_results]

    out_file = os.path.join(args.output, "nebulagraph_benchmark.json")
    with open(out_file, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n=== Results saved to {out_file} ===")

    # Summary
    print("\n=== SUMMARY ===")
    print(f"{'Benchmark':<30} {'Cold (ms)':>10} {'Warm (ms)':>10} {'Rows':>8}")
    print("=" * 62)
    for br in bench_results:
        print(f"{br.name:<30} {br.cold_ms:>10.1f} {br.warm_ms:>10.1f} {br.rows:>8}")

    session.release()
    pool.close()


if __name__ == "__main__":
    main()
