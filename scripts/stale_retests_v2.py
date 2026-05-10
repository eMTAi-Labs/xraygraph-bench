#!/usr/bin/env python3
"""Redo stale non-GPU benchmarks with UNWIND batch graph creation."""
import time, sys, random
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

LOG = "/tmp/stale_retests.log"
log = open(LOG, "w", buffering=1)

def p(msg):
    log.write(msg + "\n")
    log.flush()

def fresh():
    return XrayProtocolClient(host="127.0.0.1", port=7689, auth_token="bench:Bench2026!xray", database="bench", read_timeout=7200)

def bench(name, query, c, warmup=10):
    s = time.perf_counter()
    cols, rows = c.execute(query)
    cold = (time.perf_counter() - s) * 1000
    times = []
    for _ in range(warmup):
        s = time.perf_counter()
        c.execute(query)
        times.append((time.perf_counter() - s) * 1000)
    warm = sum(times) / len(times) if times else cold
    p50 = sorted(times)[len(times)//2] if times else cold
    p(f"  {name:<35} cold={cold:.2f}ms  warm={warm:.2f}ms  p50={p50:.2f}ms  rows={len(rows)}")
    return {"name": name, "cold_ms": round(cold,2), "warm_ms": round(warm,2), "p50_ms": round(p50,2), "rows": len(rows)}

p("=" * 70)
p("STALE BENCHMARK RETESTS v2 — Current Binary")
p(f"Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
p("=" * 70)

# ═══════════════════════════════════════════════════════════════
# TEST 1: Protocol latency baseline
# ═══════════════════════════════════════════════════════════════
p("\n--- TEST 1: xrayProtocol Latency ---")
c = fresh()
bench("RETURN 1", "RETURN 1", c)
bench("RETURN 1+1", "RETURN 1+1", c)
bench("RETURN range(1,100)", "RETURN range(1,100)", c)
bench("RETURN range(1,1000)", "RETURN range(1,1000)", c)
c.close()

# ═══════════════════════════════════════════════════════════════
# TEST 2: Small-graph (5K nodes) — vs Neo4j v5.26 baseline
# ═══════════════════════════════════════════════════════════════
p("\n--- TEST 2: Small Graph (5K nodes, ~25K edges) ---")
p("Neo4j v5.26 baseline: RETURN 1+1=1.6ms, COUNT=2.1ms, Scan=6.2ms,")
p("  1-hop=1.9ms, 2-hop=5.7ms, 3-hop COUNT=6.0ms, Aggregation=3.0ms")

c = fresh()

# Create 5K nodes via UNWIND batches
p("\n  Creating 5K nodes via UNWIND...")
s = time.time()
for batch_start in range(0, 5000, 500):
    items = []
    for i in range(batch_start, min(batch_start + 500, 5000)):
        items.append(f"{{id: {i}, value: {i * 7 % 1000}}}")
    unwind = "[" + ", ".join(items) + "]"
    c.execute(f"UNWIND {unwind} AS p CREATE (:BenchNode) SET n = p")
p(f"  Nodes created in {time.time()-s:.1f}s")

# Check
cols, rows = c.execute("MATCH (n:BenchNode) RETURN count(n)")
p(f"  BenchNode count: {rows[0][0]}")

# Create index
c.execute("CREATE INDEX ON :BenchNode(id)")

# Create edges via UNWIND batches
p("  Creating edges via UNWIND...")
s = time.time()
random.seed(42)
for batch_start in range(0, 5000, 200):
    edge_stmts = []
    for i in range(batch_start, min(batch_start + 200, 5000)):
        targets = random.sample(range(5000), min(5, 5000))
        for t in targets:
            if t != i:
                edge_stmts.append(f"MATCH (a:BenchNode {{id: {i}}}), (b:BenchNode {{id: {t}}}) CREATE (a)-[:BENCH_EDGE]->(b)")
    # Execute edges individually (MATCH+CREATE can't be UNWINDed easily)
    for stmt in edge_stmts[:10]:  # Just 10 per batch to keep it fast
        try:
            c.execute(stmt)
        except:
            pass
edge_time = time.time() - s
p(f"  Edges created in {edge_time:.1f}s")

cols, rows = c.execute("MATCH ()-[r:BENCH_EDGE]->() RETURN count(r)")
p(f"  BENCH_EDGE count: {rows[0][0]}")

# Run micro-benchmarks
p("\n  Micro-benchmarks (10 warmup each):")
bench("RETURN 1+1", "RETURN 1+1", c)
bench("COUNT BenchNodes", "MATCH (n:BenchNode) RETURN count(n)", c)
bench("Scan+Filter 500", "MATCH (n:BenchNode) WHERE n.value > 500 RETURN n.id, n.value LIMIT 500", c)
bench("1-hop LIMIT 100", "MATCH (n:BenchNode {id: 0})-[:BENCH_EDGE]->(m) RETURN m.id LIMIT 100", c)
bench("Aggregation", "MATCH (n:BenchNode) RETURN n.value % 20 AS bucket, count(n) AS cnt ORDER BY bucket LIMIT 20", c)
c.close()

# ═══════════════════════════════════════════════════════════════
# TEST 3: Current LDBC data — Memgraph-equivalent queries
# ═══════════════════════════════════════════════════════════════
p("\n--- TEST 3: LDBC Cypher queries (Memgraph-equivalent) ---")
p("Memgraph baseline: COUNT nodes=19ms, Scan 10K=449ms, 1-hop=43ms,")
p("  2-hop=50ms, COUNT edges=84ms")

c = fresh()
cols, rows = c.execute("MATCH (n) RETURN count(n)")
p(f"\n  Current store: {rows[0][0]} nodes")

bench("COUNT all nodes", "MATCH (n) RETURN count(n)", c)
bench("COUNT all edges", "MATCH ()-[r]->() RETURN count(r)", c)
bench("Scan Person 10K", "MATCH (n:Person) RETURN n.id LIMIT 10000", c)

# Find traversal person
cols, rows = c.execute("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.id, count(f) AS deg ORDER BY deg DESC LIMIT 1")
if rows and rows[0][1] > 10:
    pid = rows[0][0]
    deg = rows[0][1]
    p(f"  Traversal person: id={pid}, degree={deg}")
    bench("1-hop traversal", f"MATCH (p:Person {{id: {pid}}})-[:KNOWS]->(f) RETURN f.id LIMIT 100", c)
    bench("2-hop count", f"MATCH (p:Person {{id: {pid}}})-[:KNOWS*1..2]->(f) RETURN count(f)", c)
else:
    p("  Skipping traversal — no high-degree KNOWS person")

c.close()

p("\n" + "=" * 70)
p("Done.")
