#!/usr/bin/env python3
"""TPC-H SF1 benchmark runner — runs against PostgreSQL, MySQL, DuckDB, ClickHouse.
Loads TPC-H SF1 data, runs key TPC-H queries, records results."""
import json
import os
import subprocess
import time
from dataclasses import asdict, dataclass, field

QUERY_TIMEOUT_SEC = 300
TPCH_DIR = "/opt/tpch-dbgen"

@dataclass
class BenchmarkResult:
    name: str
    query: str
    cold_ms: float = 0.0
    warm_ms: float = 0.0
    warm_runs: int = 0
    rows: int = 0
    error: str = ""


# Key TPC-H queries (simplified for benchmark, standard parameterization)
TPCH_QUERIES = {
    "Q1": """SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc, count(*) as count_order
        FROM lineitem WHERE l_shipdate <= '1998-09-01'
        GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus""",

    "Q3": """SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority
        FROM customer, orders, lineitem
        WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey
        AND o_orderdate < '1995-03-15' AND l_shipdate > '1995-03-15'
        GROUP BY l_orderkey, o_orderdate, o_shippriority
        ORDER BY revenue DESC, o_orderdate LIMIT 10""",

    "Q5": """SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
        FROM customer, orders, lineitem, supplier, nation, region
        WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey
        AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
        AND r_name = 'ASIA' AND o_orderdate >= '1994-01-01' AND o_orderdate < '1995-01-01'
        GROUP BY n_name ORDER BY revenue DESC""",

    "Q6": """SELECT sum(l_extendedprice * l_discount) as revenue
        FROM lineitem
        WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
        AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24""",

    "Q10": """SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal, n_name, c_address, c_phone, c_comment
        FROM customer, orders, lineitem, nation
        WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
        AND o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01'
        AND l_returnflag = 'R' AND c_nationkey = n_nationkey
        GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
        ORDER BY revenue DESC LIMIT 20""",

    "Q12": """SELECT l_shipmode,
        sum(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) as high_line_count,
        sum(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) as low_line_count
        FROM orders, lineitem
        WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP')
        AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
        AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'
        GROUP BY l_shipmode ORDER BY l_shipmode""",

    "Q14": """SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
        FROM lineitem, part
        WHERE l_partkey = p_partkey AND l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'""",

    "Q19": """SELECT sum(l_extendedprice* (1 - l_discount)) as revenue
        FROM lineitem, part
        WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 1 AND l_quantity <= 11 AND p_size BETWEEN 1 AND 5
        AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')""",
}


def run_duckdb(tpch_dir, output_dir):
    """Run TPC-H on DuckDB using built-in generator."""
    import duckdb
    con = duckdb.connect(os.path.join(output_dir, "tpch.duckdb"))

    print("  Loading TPC-H SF1 via built-in generator...")
    start = time.perf_counter()
    con.execute("INSTALL tpch; LOAD tpch; CALL dbgen(sf=1);")
    load_ms = (time.perf_counter() - start) * 1000.0
    rows = con.execute("SELECT count(*) FROM lineitem").fetchone()[0]
    print(f"  Loaded {rows} lineitem rows in {load_ms:.0f}ms")

    results = []
    for qname, query in TPCH_QUERIES.items():
        print(f"  {qname}...", end=" ", flush=True)
        br = BenchmarkResult(name=f"TPC-H {qname}", query=query)
        try:
            start = time.perf_counter()
            r = con.execute(query).fetchall()
            cold_ms = (time.perf_counter() - start) * 1000.0
            br.cold_ms = round(cold_ms, 2)
            br.rows = len(r)

            warm_times = []
            for _ in range(3):
                s = time.perf_counter()
                con.execute(query).fetchall()
                warm_times.append((time.perf_counter() - s) * 1000.0)
            br.warm_ms = round(sum(warm_times) / len(warm_times), 2)
            br.warm_runs = 3
            print(f"cold={br.cold_ms:.1f}ms warm={br.warm_ms:.1f}ms rows={br.rows}")
        except Exception as e:
            br.error = str(e)[:200]
            print(f"ERROR: {str(e)[:80]}")
        results.append(br)

    con.close()
    return {"engine": "duckdb", "version": duckdb.__version__, "load_ms": load_ms, "lineitem_rows": rows, "benchmarks": [asdict(r) for r in results]}


def run_pg(tpch_dir, output_dir):
    """Run TPC-H on PostgreSQL."""
    def pg(q):
        start = time.perf_counter()
        r = subprocess.run(["sudo", "-u", "postgres", "psql", "-d", "tpch", "-t", "-A", "-c", q],
                           capture_output=True, text=True, timeout=QUERY_TIMEOUT_SEC)
        ms = (time.perf_counter() - start) * 1000.0
        rows = [l for l in r.stdout.strip().split("\n") if l.strip()] if r.stdout.strip() else []
        if r.returncode != 0 and "ERROR" in (r.stderr or ""):
            raise Exception(r.stderr[:200])
        return rows, ms

    # Create and load
    print("  Creating TPC-H database...")
    subprocess.run(["sudo", "-u", "postgres", "psql", "-c", "DROP DATABASE IF EXISTS tpch; CREATE DATABASE tpch;"], capture_output=True)

    # Load DDL and data
    print("  Loading TPC-H SF1 via COPY...")
    start = time.perf_counter()
    ddl = open(os.path.join(tpch_dir, "dss.ddl")).read()
    subprocess.run(["sudo", "-u", "postgres", "psql", "-d", "tpch", "-c", ddl], capture_output=True)

    tables = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]
    for t in tables:
        fpath = os.path.join(tpch_dir, f"{t}.tbl")
        # Remove trailing pipe
        subprocess.run(["sudo", "-u", "postgres", "psql", "-d", "tpch", "-c",
                        f"\\COPY {t} FROM '{fpath}' WITH (FORMAT csv, DELIMITER '|')"], capture_output=True)
    load_ms = (time.perf_counter() - start) * 1000.0

    rows, _ = pg("SELECT count(*) FROM lineitem")
    lineitem_count = int(rows[0]) if rows else 0
    print(f"  Loaded {lineitem_count} lineitem rows in {load_ms:.0f}ms")

    results = []
    for qname, query in TPCH_QUERIES.items():
        print(f"  {qname}...", end=" ", flush=True)
        br = BenchmarkResult(name=f"TPC-H {qname}", query=query)
        try:
            rows, cold_ms = pg(query)
            br.cold_ms = round(cold_ms, 2)
            br.rows = len(rows)
            warm_times = []
            for _ in range(3):
                _, wms = pg(query)
                warm_times.append(wms)
            br.warm_ms = round(sum(warm_times) / len(warm_times), 2)
            br.warm_runs = 3
            print(f"cold={br.cold_ms:.1f}ms warm={br.warm_ms:.1f}ms rows={br.rows}")
        except Exception as e:
            br.error = str(e)[:200]
            print(f"ERROR: {str(e)[:80]}")
        results.append(br)

    return {"engine": "postgresql", "version": "16.13", "load_ms": load_ms, "lineitem_rows": lineitem_count, "benchmarks": [asdict(r) for r in results]}


def run_mysql(tpch_dir, output_dir):
    """Run TPC-H on MySQL."""
    def mq(q, db="tpch"):
        start = time.perf_counter()
        r = subprocess.run(["mysql", "-u", "root", "-D", db, "-N", "-B", "-e", q],
                           capture_output=True, text=True, timeout=QUERY_TIMEOUT_SEC)
        ms = (time.perf_counter() - start) * 1000.0
        rows = [l for l in r.stdout.strip().split("\n") if l.strip()] if r.stdout.strip() else []
        if r.returncode != 0 and r.stderr:
            raise Exception(r.stderr[:200])
        return rows, ms

    print("  Creating TPC-H database...")
    subprocess.run(["mysql", "-u", "root", "-e", "DROP DATABASE IF EXISTS tpch; CREATE DATABASE tpch;"], capture_output=True)

    # Create tables (simplified DDL for MySQL)
    mysql_ddl = """
    CREATE TABLE region (r_regionkey INT, r_name CHAR(25), r_comment VARCHAR(152));
    CREATE TABLE nation (n_nationkey INT, n_name CHAR(25), n_regionkey INT, n_comment VARCHAR(152));
    CREATE TABLE supplier (s_suppkey INT, s_name CHAR(25), s_address VARCHAR(40), s_nationkey INT, s_phone CHAR(15), s_acctbal DECIMAL(15,2), s_comment VARCHAR(101));
    CREATE TABLE customer (c_custkey INT, c_name VARCHAR(25), c_address VARCHAR(40), c_nationkey INT, c_phone CHAR(15), c_acctbal DECIMAL(15,2), c_mktsegment CHAR(10), c_comment VARCHAR(117));
    CREATE TABLE part (p_partkey INT, p_name VARCHAR(55), p_mfgr CHAR(25), p_brand CHAR(10), p_type VARCHAR(25), p_size INT, p_container CHAR(10), p_retailprice DECIMAL(15,2), p_comment VARCHAR(23));
    CREATE TABLE partsupp (ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost DECIMAL(15,2), ps_comment VARCHAR(199));
    CREATE TABLE orders (o_orderkey INT, o_custkey INT, o_orderstatus CHAR(1), o_totalprice DECIMAL(15,2), o_orderdate DATE, o_orderpriority CHAR(15), o_clerk CHAR(15), o_shippriority INT, o_comment VARCHAR(79));
    CREATE TABLE lineitem (l_orderkey INT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), l_tax DECIMAL(15,2), l_returnflag CHAR(1), l_linestatus CHAR(1), l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct CHAR(25), l_shipmode CHAR(10), l_comment VARCHAR(44));
    """
    mq(mysql_ddl)

    print("  Loading TPC-H SF1 via LOAD DATA...")
    start = time.perf_counter()
    tables = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]
    for t in tables:
        fpath = os.path.join(tpch_dir, f"{t}.tbl")
        subprocess.run(["mysql", "-u", "root", "-D", "tpch", "--local-infile=1", "-e",
                        f"LOAD DATA LOCAL INFILE '{fpath}' INTO TABLE {t} FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\\n'"],
                       capture_output=True, timeout=300)
    load_ms = (time.perf_counter() - start) * 1000.0

    rows, _ = mq("SELECT count(*) FROM lineitem")
    lineitem_count = int(rows[0]) if rows else 0
    print(f"  Loaded {lineitem_count} lineitem rows in {load_ms:.0f}ms")

    results = []
    for qname, query in TPCH_QUERIES.items():
        print(f"  {qname}...", end=" ", flush=True)
        br = BenchmarkResult(name=f"TPC-H {qname}", query=query)
        try:
            rows, cold_ms = mq(query)
            br.cold_ms = round(cold_ms, 2)
            br.rows = len(rows)
            warm_times = []
            for _ in range(3):
                _, wms = mq(query)
                warm_times.append(wms)
            br.warm_ms = round(sum(warm_times) / len(warm_times), 2)
            br.warm_runs = 3
            print(f"cold={br.cold_ms:.1f}ms warm={br.warm_ms:.1f}ms rows={br.rows}")
        except Exception as e:
            br.error = str(e)[:200]
            print(f"ERROR: {str(e)[:80]}")
        results.append(br)

    return {"engine": "mysql", "version": "8.0.45", "load_ms": load_ms, "lineitem_rows": lineitem_count, "benchmarks": [asdict(r) for r in results]}


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--engines", default="duckdb,postgresql,mysql", help="Comma-separated list of engines")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    engines = args.engines.split(",")
    all_results = {"timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "benchmark": "TPC-H SF1", "engines": {}}

    print("=== TPC-H SF1 Benchmark Suite ===\n")

    if "duckdb" in engines:
        print("--- DuckDB ---")
        all_results["engines"]["duckdb"] = run_duckdb(TPCH_DIR, args.output)
        print()

    if "postgresql" in engines:
        print("--- PostgreSQL ---")
        all_results["engines"]["postgresql"] = run_pg(TPCH_DIR, args.output)
        print()

    if "mysql" in engines:
        print("--- MySQL ---")
        all_results["engines"]["mysql"] = run_mysql(TPCH_DIR, args.output)
        print()

    out_file = os.path.join(args.output, "tpch_results.json")
    with open(out_file, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n=== Results saved to {out_file} ===")

    # Comparison table
    print("\n=== TPC-H COMPARISON ===")
    print(f"{'Query':<12}", end="")
    for eng in all_results["engines"]:
        print(f"{'  ' + eng + ' (ms)':>20}", end="")
    print()
    print("=" * (12 + 20 * len(all_results["engines"])))

    query_names = list(TPCH_QUERIES.keys())
    for qn in query_names:
        print(f"{qn:<12}", end="")
        for eng, data in all_results["engines"].items():
            found = [b for b in data["benchmarks"] if b["name"] == f"TPC-H {qn}"]
            if found:
                print(f"{found[0]['warm_ms']:>20.1f}", end="")
            else:
                print(f"{'N/A':>20}", end="")
        print()


if __name__ == "__main__":
    main()
