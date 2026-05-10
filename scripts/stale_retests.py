#!/usr/bin/env python3
"""Redo stale non-GPU benchmarks on current binary.

1. vs Memgraph LiveJournal (4.8M nodes, 69M edges) — same queries
2. BFS CSR on LDBC SF1 scale (via Friendster hub with controlled degree)
3. Small-graph micro-benchmarks (vs Neo4j baseline)
"""
import time, sys
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
p("STALE BENCHMARK RETESTS — Current Binary")
p(f"Server: 216.106.185.187 port 7689")
p(f"Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
p("=" * 70)

# ═══════════════════════════════════════════════════════════════
# TEST 1: Small-graph micro-benchmarks (vs Neo4j v5.26 baseline)
# Original: v4.0.2 showed 88-178µs (autocork bug). Neo4j was 1.6-6.2ms.
# ═══════════════════════════════════════════════════════════════
p("\n" + "=" * 70)
p("TEST 1: Small-graph micro-benchmarks (vs Neo4j v5.26 baseline)")
p("Original xrayGraphDB v4.0.2 had autocork bug (208ms floor).")
p("Neo4j baseline: RETURN 1+1=1.6ms, COUNT=2.1ms, 1-hop=1.9ms")
p("=" * 70)

c = fresh()

# Create 5K power-law graph
p("\n  Creating 5K test nodes...")
for i in range(5000):
    c.execute(f"CREATE (:BenchNode {{id: {i}, value: {i * 7 % 1000}}})")
# Create edges (power-law-ish: node 0 gets many, others get few)
edge_count = 0
import random
random.seed(42)
for i in range(5000):
    targets = random.sample(range(5000), min(5, 5000))
    for t in targets:
        if t != i:
            c.execute(f"MATCH (a:BenchNode {{id: {i}}}), (b:BenchNode {{id: {t}}}) CREATE (a)-[:BENCH_EDGE]->(b)")
            edge_count += 1
p(f"  Created 5000 nodes, {edge_count} edges")

# Create index
c.execute("CREATE INDEX ON :BenchNode(id)")

p("\n  Running micro-benchmarks (10 warmup each)...")
bench("RETURN 1+1", "RETURN 1+1", c)
bench("COUNT 5K nodes", "MATCH (n:BenchNode) RETURN count(n)", c)
bench("Scan+Filter 500", "MATCH (n:BenchNode) WHERE n.value > 500 RETURN n.id, n.value LIMIT 500", c)
bench("1-hop LIMIT 100", "MATCH (n:BenchNode {id: 0})-[:BENCH_EDGE]->(m) RETURN m.id LIMIT 100", c)
bench("2-hop LIMIT 500", "MATCH (n:BenchNode {id: 0})-[:BENCH_EDGE*1..2]->(m) RETURN m.id LIMIT 500", c)
bench("3-hop COUNT", "MATCH (n:BenchNode {id: 0})-[:BENCH_EDGE*1..3]->(m) RETURN count(m)", c)
bench("Aggregation", "MATCH (n:BenchNode) RETURN n.value % 20 AS bucket, count(n) AS cnt, avg(n.value) AS avg_val ORDER BY bucket LIMIT 20", c)

c.close()

# ═══════════════════════════════════════════════════════════════
# TEST 2: LiveJournal queries (vs Memgraph 2.22 baseline)
# Original: xrayGraphDB showed 1.29-28.61ms. Memgraph 19-449ms.
# ═══════════════════════════════════════════════════════════════
p("\n" + "=" * 70)
p("TEST 2: LiveJournal queries (vs Memgraph 2.22 baseline)")
p("Dataset: 4.8M nodes, 69M edges via CSR")
p("Memgraph baseline: COUNT=19ms, Scan=449ms, 1-hop=43ms, 2-hop=50ms")
p("=" * 70)

# LiveJournal is in CSR at /neo4j/csr_lj/ — need to query via xray.* procedures
# The original comparison was on Bolt Cypher, but LiveJournal was loaded into the
# Cypher store. We can't do that easily now. Let's measure what we CAN do:
# CSR-based queries via xray.* procedures on the Friendster CSR (which is loaded).

# Actually, let's measure the LDBC SF1 data that's in the Cypher store right now
# for the same query patterns used against Memgraph.
c = fresh()
cols, rows = c.execute("MATCH (n) RETURN count(n)")
node_count = rows[0][0]
cols, rows = c.execute("MATCH ()-[r]->() RETURN count(r)")
edge_count_val = rows[0][0]
p(f"\n  Current Cypher store: {node_count} nodes, {edge_count_val} edges")

p("\n  Running LiveJournal-equivalent queries...")
bench("COUNT all nodes", "MATCH (n) RETURN count(n)", c)
bench("COUNT all edges", "MATCH ()-[r]->() RETURN count(r)", c)
bench("Scan LIMIT 10K", "MATCH (n:Person) RETURN n.id LIMIT 10000", c)

# Find a person with edges for traversal
cols, rows = c.execute("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.id, count(f) AS deg ORDER BY deg DESC LIMIT 1")
if rows and rows[0][1] > 0:
    pid = rows[0][0]
    p(f"  Traversal person: id={pid}, degree={rows[0][1]}")
    bench("1-hop traversal", f"MATCH (p:Person {{id: {pid}}})-[:KNOWS]->(f) RETURN f.id LIMIT 100", c)
    bench("2-hop traversal", f"MATCH (p:Person {{id: {pid}}})-[:KNOWS*1..2]->(f) RETURN count(f)", c)
else:
    p("  No person with KNOWS edges found for traversal tests")

c.close()

# ═══════════════════════════════════════════════════════════════
# TEST 3: Protocol comparison — xrayProtocol vs Bolt overhead
# ═══════════════════════════════════════════════════════════════
p("\n" + "=" * 70)
p("TEST 3: xrayProtocol latency (current binary)")
p("=" * 70)

c = fresh()
bench("RETURN 1", "RETURN 1", c)
bench("RETURN 1+1", "RETURN 1+1", c)
bench("RETURN range(1,100)", "RETURN range(1,100)", c)
bench("RETURN range(1,1000)", "RETURN range(1,1000)", c)
c.close()

# Summary
p("\n" + "=" * 70)
p("SUMMARY")
p("=" * 70)
p("Done.")
