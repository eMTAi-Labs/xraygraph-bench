#!/usr/bin/env python3
"""SSB (Star Schema Benchmark) SF1 — runs against DuckDB, PostgreSQL, MySQL."""
import json
import os
import subprocess
import time
from dataclasses import asdict, dataclass, field

QUERY_TIMEOUT_SEC = 300
SSB_DIR = "/opt/ssb-dbgen/build"

@dataclass
class BenchmarkResult:
    name: str
    query: str
    cold_ms: float = 0.0
    warm_ms: float = 0.0
    warm_runs: int = 0
    rows: int = 0
    error: str = ""

# SSB queries (13 queries in 4 flights)
SSB_QUERIES = {
    "Q1.1": "SELECT sum(lo_extendedprice * lo_discount) AS revenue FROM lineorder, date_ WHERE lo_orderdate = d_datekey AND d_year = 1993 AND lo_discount BETWEEN 1 AND 3 AND lo_quantity < 25",
    "Q1.2": "SELECT sum(lo_extendedprice * lo_discount) AS revenue FROM lineorder, date_ WHERE lo_orderdate = d_datekey AND d_yearmonthnum = 199401 AND lo_discount BETWEEN 4 AND 6 AND lo_quantity BETWEEN 26 AND 35",
    "Q1.3": "SELECT sum(lo_extendedprice * lo_discount) AS revenue FROM lineorder, date_ WHERE lo_orderdate = d_datekey AND d_weeknuminyear = 6 AND d_year = 1994 AND lo_discount BETWEEN 5 AND 7 AND lo_quantity BETWEEN 26 AND 35",
    "Q2.1": "SELECT sum(lo_revenue), d_year, p_brand1 FROM lineorder, date_, part, supplier WHERE lo_orderdate = d_datekey AND lo_partkey = p_partkey AND lo_suppkey = s_suppkey AND p_category = 'MFGR#12' AND s_region = 'AMERICA' GROUP BY d_year, p_brand1 ORDER BY d_year, p_brand1",
    "Q2.2": "SELECT sum(lo_revenue), d_year, p_brand1 FROM lineorder, date_, part, supplier WHERE lo_orderdate = d_datekey AND lo_partkey = p_partkey AND lo_suppkey = s_suppkey AND p_brand1 BETWEEN 'MFGR#2221' AND 'MFGR#2228' AND s_region = 'ASIA' GROUP BY d_year, p_brand1 ORDER BY d_year, p_brand1",
    "Q3.1": "SELECT c_nation, s_nation, d_year, sum(lo_revenue) AS revenue FROM lineorder, date_, customer, supplier WHERE lo_orderdate = d_datekey AND lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND c_region = 'ASIA' AND s_region = 'ASIA' AND d_year >= 1992 AND d_year <= 1997 GROUP BY c_nation, s_nation, d_year ORDER BY d_year, revenue DESC",
    "Q3.2": "SELECT c_city, s_city, d_year, sum(lo_revenue) AS revenue FROM lineorder, date_, customer, supplier WHERE lo_orderdate = d_datekey AND lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND c_nation = 'UNITED STATES' AND s_nation = 'UNITED STATES' AND d_year >= 1992 AND d_year <= 1997 GROUP BY c_city, s_city, d_year ORDER BY d_year, revenue DESC",
    "Q4.1": "SELECT d_year, c_nation, sum(lo_revenue - lo_supplycost) AS profit FROM lineorder, date_, customer, supplier, part WHERE lo_orderdate = d_datekey AND lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_partkey = p_partkey AND c_region = 'AMERICA' AND s_region = 'AMERICA' AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2') GROUP BY d_year, c_nation ORDER BY d_year, c_nation",
    "Q4.2": "SELECT d_year, s_nation, p_category, sum(lo_revenue - lo_supplycost) AS profit FROM lineorder, date_, customer, supplier, part WHERE lo_orderdate = d_datekey AND lo_custkey = c_custkey AND lo_suppkey = s_suppkey AND lo_partkey = p_partkey AND c_region = 'AMERICA' AND s_region = 'AMERICA' AND (d_year = 1997 OR d_year = 1998) AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2') GROUP BY d_year, s_nation, p_category ORDER BY d_year, s_nation, p_category",
}


def run_duckdb(output_dir):
    import duckdb
    con = duckdb.connect(os.path.join(output_dir, "ssb.duckdb"))

    print("  Loading SSB SF1...")
    start = time.perf_counter()
    # Load SSB tables from CSV
    for tbl, fname in [("customer", "customer.tbl"), ("date_", "date.tbl"), ("lineorder", "lineorder.tbl"), ("part", "part.tbl"), ("supplier", "supplier.tbl")]:
        fpath = os.path.join(SSB_DIR, fname)
        con.execute(f"CREATE OR REPLACE TABLE {tbl} AS SELECT * FROM read_csv_auto('{fpath}', delim='|', header=false, ignore_errors=true)")
    load_ms = (time.perf_counter() - start) * 1000.0
    rows = con.execute("SELECT count(*) FROM lineorder").fetchone()[0]
    print(f"  Loaded {rows} lineorder rows in {load_ms:.0f}ms")

    # Fix column names for SSB (auto-detected as column0, column1, etc.)
    # Rename to match SSB schema
    try:
        con.execute("ALTER TABLE lineorder RENAME COLUMN column0 TO lo_orderkey")
        con.execute("ALTER TABLE lineorder RENAME COLUMN column1 TO lo_linenumber")
        con.execute("ALTER TABLE lineorder RENAME COLUMN column2 TO lo_custkey")
        con.execute("ALTER TABLE lineorder RENAME COLUMN column3 TO lo_partkey")
        con.execute("ALTER TABLE lineorder RENAME COLUMN column4 TO lo_suppkey")
        con.execute("ALTER TABLE lineorder RENAME COLUMN column5 TO lo_orderdate")
        con.execute("ALTER TABLE lineorder RENAME COLUMN column6 TO lo_orderpriority")
        con.execute("ALTER TABLE lineorder RENAME COLUMN column7 TO lo_shippriority")
        con.execute("ALTER TABLE lineorder RENAME COLUMN column8 TO lo_quantity")
        con.execute("ALTER TABLE lineorder RENAME COLUMN column9 TO lo_extendedprice")
        con.execute("ALTER TABLE lineorder RENAME COLUMN column10 TO lo_ordtotalprice")
        con.execute("ALTER TABLE lineorder RENAME COLUMN column11 TO lo_discount")
        con.execute("ALTER TABLE lineorder RENAME COLUMN column12 TO lo_revenue")
        con.execute("ALTER TABLE lineorder RENAME COLUMN column13 TO lo_supplycost")
        con.execute("ALTER TABLE lineorder RENAME COLUMN column14 TO lo_tax")
        con.execute("ALTER TABLE lineorder RENAME COLUMN column15 TO lo_commitdate")
        con.execute("ALTER TABLE lineorder RENAME COLUMN column16 TO lo_shipmode")
    except Exception:
        pass  # columns might already be named

    # Rename date columns
    date_cols = ["d_datekey","d_date","d_dayofweek","d_month","d_year","d_yearmonthnum","d_yearmonth","d_daynuminweek","d_daynuminmonth","d_daynuminyear","d_monthnuminyear","d_weeknuminyear","d_sellingseason","d_lastdayinweekfl","d_lastdayinmonthfl","d_holidayfl","d_weekdayfl"]
    try:
        for i, c in enumerate(date_cols):
            con.execute(f"ALTER TABLE date_ RENAME COLUMN column{i} TO {c}")
    except Exception:
        pass

    # Customer columns
    cust_cols = ["c_custkey","c_name","c_address","c_city","c_nation","c_region","c_phone","c_mktsegment"]
    try:
        for i, c in enumerate(cust_cols):
            con.execute(f"ALTER TABLE customer RENAME COLUMN column{i} TO {c}")
    except Exception:
        pass

    # Supplier columns
    supp_cols = ["s_suppkey","s_name","s_address","s_city","s_nation","s_region","s_phone"]
    try:
        for i, c in enumerate(supp_cols):
            con.execute(f"ALTER TABLE supplier RENAME COLUMN column{i} TO {c}")
    except Exception:
        pass

    # Part columns
    part_cols = ["p_partkey","p_name","p_mfgr","p_category","p_brand1","p_color","p_type","p_size","p_container"]
    try:
        for i, c in enumerate(part_cols):
            con.execute(f"ALTER TABLE part RENAME COLUMN column{i} TO {c}")
    except Exception:
        pass

    results = []
    for qname, query in SSB_QUERIES.items():
        print(f"  {qname}...", end=" ", flush=True)
        br = BenchmarkResult(name=f"SSB {qname}", query=query)
        try:
            s = time.perf_counter()
            r = con.execute(query).fetchall()
            cold_ms = (time.perf_counter() - s) * 1000.0
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
    return {"engine": "duckdb", "load_ms": load_ms, "lineorder_rows": rows, "benchmarks": [asdict(r) for r in results]}


def run_mysql(output_dir):
    def mq(q, db="ssb"):
        start = time.perf_counter()
        r = subprocess.run(["mysql", "-u", "root", "-D", db, "-N", "-B", "-e", q],
                           capture_output=True, text=True, timeout=QUERY_TIMEOUT_SEC)
        ms = (time.perf_counter() - start) * 1000.0
        rows = [l for l in r.stdout.strip().split("\n") if l.strip()] if r.stdout.strip() else []
        if r.returncode != 0 and r.stderr:
            raise Exception(r.stderr[:200])
        return rows, ms

    subprocess.run(["mysql", "-u", "root", "-e", "DROP DATABASE IF EXISTS ssb; CREATE DATABASE ssb;"], capture_output=True)

    # Create SSB tables
    ddl = """
    CREATE TABLE customer (c_custkey INT, c_name VARCHAR(25), c_address VARCHAR(25), c_city VARCHAR(10), c_nation VARCHAR(15), c_region VARCHAR(12), c_phone VARCHAR(15), c_mktsegment VARCHAR(10));
    CREATE TABLE date_ (d_datekey INT, d_date VARCHAR(18), d_dayofweek VARCHAR(9), d_month VARCHAR(9), d_year INT, d_yearmonthnum INT, d_yearmonth VARCHAR(7), d_daynuminweek INT, d_daynuminmonth INT, d_daynuminyear INT, d_monthnuminyear INT, d_weeknuminyear INT, d_sellingseason VARCHAR(12), d_lastdayinweekfl INT, d_lastdayinmonthfl INT, d_holidayfl INT, d_weekdayfl INT);
    CREATE TABLE supplier (s_suppkey INT, s_name VARCHAR(25), s_address VARCHAR(25), s_city VARCHAR(10), s_nation VARCHAR(15), s_region VARCHAR(12), s_phone VARCHAR(15));
    CREATE TABLE part (p_partkey INT, p_name VARCHAR(22), p_mfgr VARCHAR(6), p_category VARCHAR(7), p_brand1 VARCHAR(9), p_color VARCHAR(11), p_type VARCHAR(25), p_size INT, p_container VARCHAR(10));
    CREATE TABLE lineorder (lo_orderkey INT, lo_linenumber INT, lo_custkey INT, lo_partkey INT, lo_suppkey INT, lo_orderdate INT, lo_orderpriority VARCHAR(15), lo_shippriority CHAR(1), lo_quantity INT, lo_extendedprice INT, lo_ordtotalprice INT, lo_discount INT, lo_revenue INT, lo_supplycost INT, lo_tax INT, lo_commitdate INT, lo_shipmode VARCHAR(10));
    """
    mq(ddl)

    print("  Loading SSB SF1...")
    start = time.perf_counter()
    for tbl, fname in [("customer", "customer.tbl"), ("date_", "date.tbl"), ("lineorder", "lineorder.tbl"), ("part", "part.tbl"), ("supplier", "supplier.tbl")]:
        fpath = os.path.join(SSB_DIR, fname)
        subprocess.run(["mysql", "-u", "root", "-D", "ssb", "--local-infile=1", "-e",
                        f"LOAD DATA LOCAL INFILE '{fpath}' INTO TABLE {tbl} FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\\n'"],
                       capture_output=True, timeout=300)
    load_ms = (time.perf_counter() - start) * 1000.0
    rows, _ = mq("SELECT count(*) FROM lineorder")
    lo_count = int(rows[0]) if rows else 0
    print(f"  Loaded {lo_count} lineorder rows in {load_ms:.0f}ms")

    results = []
    for qname, query in SSB_QUERIES.items():
        print(f"  {qname}...", end=" ", flush=True)
        br = BenchmarkResult(name=f"SSB {qname}", query=query)
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

    return {"engine": "mysql", "load_ms": load_ms, "lineorder_rows": lo_count, "benchmarks": [asdict(r) for r in results]}


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--engines", default="duckdb,mysql")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    engines = args.engines.split(",")
    all_results = {"benchmark": "SSB SF1", "engines": {}}

    print("=== SSB SF1 Benchmark Suite ===\n")

    if "duckdb" in engines:
        print("--- DuckDB ---")
        all_results["engines"]["duckdb"] = run_duckdb(args.output)
        print()

    if "mysql" in engines:
        print("--- MySQL ---")
        all_results["engines"]["mysql"] = run_mysql(args.output)
        print()

    out_file = os.path.join(args.output, "ssb_results.json")
    with open(out_file, "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    print(f"\n=== SSB COMPARISON ===")
    print(f"{'Query':<12}", end="")
    for eng in all_results["engines"]:
        print(f"{'  ' + eng + ' (ms)':>20}", end="")
    print()
    print("=" * (12 + 20 * len(all_results["engines"])))
    for qn in SSB_QUERIES:
        print(f"{qn:<12}", end="")
        for eng, data in all_results["engines"].items():
            found = [b for b in data["benchmarks"] if b["name"] == f"SSB {qn}"]
            if found:
                print(f"{found[0]['warm_ms']:>20.1f}", end="")
            else:
                print(f"{'N/A':>20}", end="")
        print()


if __name__ == "__main__":
    main()
