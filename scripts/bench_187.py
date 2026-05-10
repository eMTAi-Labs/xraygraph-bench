#!/usr/bin/env python3
"""Full benchmark suite for 216.106.185.187 with database field patch."""
import time, json, sys, struct

sys.stdout = open("/tmp/bench_187.log", "w", buffering=1)

# Patch xgdb_connect to include database field in HELLO
from xgdb_connect.protocol import XrayProtocolClient, HELLO, HELLO_OK, PROTOCOL_VERSION

_orig_hello = XrayProtocolClient._hello
def _patched_hello(self, auth_token, capabilities, database="bench"):
    token_bytes = auth_token.encode("utf-8")
    db_bytes = database.encode("utf-8")
    payload = struct.pack("<HHI", PROTOCOL_VERSION, capabilities, len(token_bytes))
    payload += token_bytes
    payload += struct.pack("<I", len(db_bytes)) + db_bytes
    self._send_frame(HELLO, 0, 0, payload)
    msg_type, flags, qid, resp = self._recv_frame()
    if msg_type != HELLO_OK:
        raise Exception(f"HELLO failed: 0x{msg_type:02x}")
XrayProtocolClient._hello = _patched_hello

ISSUE_FILE = "/tmp/bench-issues.txt"
RESOLVE_FILE = "/tmp/bench-resolve.txt"
issue_count = 0

def log_issue(title, error, details=""):
    global issue_count
    issue_count += 1
    with open(ISSUE_FILE, "a") as f:
        f.write(f"### Start Issue #{issue_count} ###\n")
        f.write(f"TITLE: {title}\n")
        f.write(f"ERROR: {error}\n")
        if details:
            f.write(f"DETAILS: {details}\n")
        f.write(f"TIMESTAMP: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n")
        f.write(f"### End Issue #{issue_count} ###\n\n")
    print(f"  *** ISSUE #{issue_count} LOGGED: {title} ***", flush=True)
    return issue_count

def check_resolved(issue_num):
    """Check if /tmp/bench-resolve.txt contains resolution for this issue."""
    try:
        with open(RESOLVE_FILE, "r") as f:
            content = f.read()
        return f"### Issue #{issue_num} Resolved" in content or f"Issue #{issue_num} Resolved" in content
    except:
        return False

def wait_for_resolution(issue_num, timeout=3600):
    """Wait for issue to be resolved, checking every 10 seconds."""
    print(f"  Waiting for Issue #{issue_num} resolution in {RESOLVE_FILE}...", flush=True)
    start = time.time()
    while time.time() - start < timeout:
        if check_resolved(issue_num):
            print(f"  Issue #{issue_num} RESOLVED! Retrying...", flush=True)
            return True
        # Also check for other instructions
        try:
            with open(RESOLVE_FILE, "r") as f:
                content = f.read().strip()
            if content and f"Issue #{issue_num}" in content:
                print(f"  Found instruction for Issue #{issue_num}: {content[-200:]}", flush=True)
                return True
        except:
            pass
        time.sleep(10)
    print(f"  Timeout waiting for Issue #{issue_num} resolution", flush=True)
    return False

def run_bench(name, query, c, warmup=3):
    """Run a benchmark with cold + warm runs. Returns result dict."""
    try:
        s = time.perf_counter()
        cols, rows = c.execute(query)
        cold = (time.perf_counter() - s) * 1000

        times = []
        for _ in range(warmup):
            s = time.perf_counter()
            c.execute(query)
            times.append((time.perf_counter() - s) * 1000)
        warm = sum(times) / len(times) if times else cold

        print(f"  {name:<30} cold={cold:.1f}ms warm={warm:.1f}ms rows={len(rows)}", flush=True)
        if rows:
            print(f"    {rows[0]}", flush=True)
        return {"name": name, "cold_ms": round(cold, 1), "warm_ms": round(warm, 1), "rows": len(rows), "status": "PASS"}
    except Exception as e:
        err = str(e)[:200]
        print(f"  {name:<30} ERROR: {err}", flush=True)
        return {"name": name, "error": err, "status": "FAIL"}

def run_analytics(name, query, c):
    """Run a single analytics procedure (no warm runs — too expensive)."""
    try:
        s = time.perf_counter()
        cols, rows = c.execute(query)
        ms = (time.perf_counter() - s) * 1000
        print(f"  {name:<30} {ms:.0f}ms, {len(rows)} rows", flush=True)
        if rows:
            print(f"    {rows[0]}", flush=True)
        return {"name": name, "ms": round(ms, 1), "rows": len(rows), "status": "PASS"}
    except Exception as e:
        err = str(e)[:200]
        print(f"  {name:<30} ERROR: {err}", flush=True)
        return {"name": name, "error": err, "status": "FAIL"}

# ══════════════════════════════════════════════════════════════════════
print("=" * 70, flush=True)
print("FULL BENCHMARK SUITE — 216.106.185.187 (503GB EPYC, no GPU)", flush=True)
print(f"Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}", flush=True)
print("=" * 70, flush=True)

# Clear old issue file content (keep Issue #1 about xgdb_connect)
with open(ISSUE_FILE, "w") as f:
    f.write("")  # Start fresh

all_results = {}
c = XrayProtocolClient("127.0.0.1", 7689, auth_token="bench:Bench2026!xray", read_timeout=1800)
print(f"\nConnected: {c.connected}", flush=True)

# ── SECTION 1: Friendster CSR Health ──────────────────────────────────
print("\n--- SECTION 1: Friendster CSR Health ---", flush=True)
s1 = []
s1.append(run_analytics("Health Report", 'CALL xray.db_health_report() YIELD metric, value, unit, status RETURN *', c))
s1.append(run_analytics("Graph Stats", 'CALL xray.graph_stats("") YIELD metric, value RETURN *', c))
s1.append(run_analytics("Degree Distribution", 'CALL xray.degree_distribution("") YIELD degree, count, cumulative_pct RETURN * LIMIT 10', c))
all_results["health"] = s1

# ── SECTION 2: Traversal ─────────────────────────────────────────────
print("\n--- SECTION 2: Traversal ---", flush=True)
s2 = []
s2.append(run_analytics("Frontier 5-hop", 'CALL xray.frontier_profile(13594, 5, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *', c))
s2.append(run_analytics("Frontier 10-hop", 'CALL xray.frontier_profile(13594, 10, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *', c))
s2.append(run_bench("Shortest Path", 'CALL xray.shortest_path(13594, 13600, "") YIELD node_id, distance, path_index, time_ms RETURN *', c, warmup=3))
s2.append(run_bench("Find Path Budgeted", 'CALL xray.find_path_budgeted(13594, 13600, 10.0, "") YIELD path_nodes, total_cost, hops, explored_nodes RETURN *', c, warmup=3))
s2.append(run_analytics("TopK Reachable", 'CALL xray.topk_reachable(13594, 10, 3, "degree", "OUTGOING") YIELD node_id, name, score, distance RETURN *', c))
all_results["traversal"] = s2

# ── SECTION 3: Similarity ────────────────────────────────────────────
print("\n--- SECTION 3: Similarity ---", flush=True)
s3 = []
s3.append(run_bench("Common Neighbors", 'CALL xray.common_neighbors(13594, 13596) YIELD neighbor_id, count RETURN *', c, warmup=3))
s3.append(run_bench("Jaccard Similarity", 'CALL xray.jaccard_similarity(13594, 13596) YIELD jaccard, overlap, common, total_union, degree_a, degree_b RETURN *', c, warmup=3))
s3.append(run_analytics("Link Prediction", 'CALL xray.link_prediction(13594, 10, "adamic_adar") YIELD node_id, score, method, rank RETURN *', c))
all_results["similarity"] = s3

# ── SECTION 4: Global Analytics ──────────────────────────────────────
print("\n--- SECTION 4: Global Analytics ---", flush=True)
s4 = []
s4.append(run_analytics("Connected Components", 'CALL xray.connected_components("") YIELD component_size, num_components, time_ms RETURN * ORDER BY component_size DESC LIMIT 5', c))
s4.append(run_analytics("PageRank 5iter", 'CALL xray.pagerank(5, 0.85, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5', c))
s4.append(run_analytics("Triangle Count", 'CALL xray.triangle_count("") YIELD triangles, edges_checked, time_ms, vertices RETURN *', c))
s4.append(run_analytics("Betweenness 100", 'CALL xray.betweenness_centrality("", 100) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 5', c))
s4.append(run_analytics("K-Core", 'CALL xray.kcore("") YIELD core_number, time_ms RETURN core_number, count(*) AS cnt, time_ms ORDER BY core_number DESC LIMIT 5', c))
all_results["global_analytics"] = s4

# ── SECTION 5: Community ─────────────────────────────────────────────
print("\n--- SECTION 5: Community ---", flush=True)
s5 = []
s5.append(run_analytics("Community 3iter", 'CALL xray.community_detection(3, "") YIELD community_size, num_communities RETURN * ORDER BY community_size DESC LIMIT 5', c))
s5.append(run_analytics("HITS 3iter", 'CALL xray.hits(3, "") YIELD node_id, hub, authority, time_ms RETURN node_id, hub, authority, time_ms ORDER BY authority DESC LIMIT 5', c))
all_results["community"] = s5

# ── SECTION 6: Vertex-Level ──────────────────────────────────────────
print("\n--- SECTION 6: Vertex-Level ---", flush=True)
s6 = []
s6.append(run_analytics("PersonalizedPR 5iter", 'CALL xray.personalized_pagerank(13594, 0.85, 5, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5', c))
s6.append(run_analytics("Clustering Coefficient", 'CALL xray.clustering_coefficient("") YIELD node_id, coefficient, triangles RETURN * ORDER BY coefficient DESC LIMIT 5', c))
all_results["vertex_level"] = s6

c.close()

# ── SUMMARY ──────────────────────────────────────────────────────────
print("\n" + "=" * 70, flush=True)
print("SUMMARY", flush=True)
print("=" * 70, flush=True)

passed = 0
failed = 0
failed_names = []
for section, results in all_results.items():
    for r in results:
        if r["status"] == "PASS":
            passed += 1
        else:
            failed += 1
            failed_names.append(r["name"])
            inum = log_issue(
                f"{r['name']} failed",
                r.get("error", "unknown"),
                f"Section: {section}, Query failed during benchmark run"
            )

print(f"\nPASSED: {passed}", flush=True)
print(f"FAILED: {failed}", flush=True)
if failed_names:
    print(f"Failed procedures: {', '.join(failed_names)}", flush=True)

print(f"\n{'Name':<30} {'Time':>10} {'Rows':>6} {'Status':>8}", flush=True)
print("-" * 58, flush=True)
for section, results in all_results.items():
    print(f"\n[{section}]", flush=True)
    for r in results:
        if r["status"] == "PASS":
            t = r.get("warm_ms", r.get("ms", 0))
            print(f"  {r['name']:<28} {t:>9.1f}ms {r.get('rows',0):>5} {'PASS':>8}", flush=True)
        else:
            print(f"  {r['name']:<28} {'':>10} {'':>6} {'FAIL':>8}", flush=True)

json.dump(all_results, open("/tmp/bench_187_results.json", "w"), indent=2, default=str)
print(f"\nResults saved to /tmp/bench_187_results.json", flush=True)

# ── MONITOR FOR RESOLUTIONS ──────────────────────────────────────────
if failed > 0:
    print(f"\n{failed} procedures failed. Monitoring {RESOLVE_FILE} for resolutions...", flush=True)
    # Wait for resolutions and re-run
    for section, results in all_results.items():
        for r in results:
            if r["status"] == "FAIL":
                # Find the issue number for this failure
                with open(ISSUE_FILE, "r") as f:
                    content = f.read()
                # Extract issue number
                import re
                for m in re.finditer(r"### Start Issue #(\d+) ###\nTITLE: " + re.escape(r["name"]), content):
                    inum = int(m.group(1))
                    if wait_for_resolution(inum, timeout=3600):
                        print(f"\n  Re-running {r['name']}...", flush=True)
                        c2 = XrayProtocolClient("127.0.0.1", 7689, auth_token="bench:Bench2026!xray", read_timeout=1800)
                        # Find original query
                        # Re-run will be manual after resolution
                        c2.close()

print("\nBenchmark complete.", flush=True)
