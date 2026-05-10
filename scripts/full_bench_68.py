#!/usr/bin/env python3
"""Full benchmark suite for .68 GPU server — LDBC load + queries + BFS + Friendster analytics + GPU monitoring."""
import time, sys, os, csv, subprocess
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

AUTH = "admin:0Z8hGUrSojkVy41AhQPV1Wl2"
DB = "xraygraphdb"
LOG = "/tmp/full_bench_68.log"
log = open(LOG, "w", buffering=1)

def p(msg):
    log.write(msg + "\n")
    log.flush()

def fresh():
    return XrayProtocolClient(host="127.0.0.1", port=7689, auth_token=AUTH, database=DB, read_timeout=7200)

def gpu():
    try:
        r = subprocess.run(["nvidia-smi", "--query-gpu=utilization.gpu,memory.used", "--format=csv,noheader"], capture_output=True, text=True, timeout=5)
        return r.stdout.strip()
    except:
        return "n/a"

def bench(name, query, c, warmup=10):
    s = time.perf_counter()
    cols, rows = c.execute(query)
    cold = (time.perf_counter() - s) * 1000
    times = []
    for _ in range(warmup):
        s = time.perf_counter()
        c.execute(query)
        times.append((time.perf_counter() - s) * 1000)
    warm = sum(times)/len(times) if times else cold
    p50 = sorted(times)[len(times)//2] if times else cold
    p(f"  {name:<40} cold={cold:.2f}  warm={warm:.2f}  p50={p50:.2f}ms  rows={len(rows)}")
    if rows: p(f"    {rows[0]}")

def run_once(name, query, c):
    g1 = gpu()
    s = time.perf_counter()
    try:
        cols, rows = c.execute(query)
        ms = (time.perf_counter() - s) * 1000
        g2 = gpu()
        p(f"  {name:<40} {ms:.0f}ms  rows={len(rows)}  GPU:{g2}")
        if rows: p(f"    {rows[0]}")
    except Exception as e:
        ms = (time.perf_counter() - s) * 1000
        p(f"  {name:<40} ERROR ({ms:.0f}ms): {str(e)[:100]}")

p("=" * 70)
p(f"FULL BENCHMARK — 66.163.122.68 (62GB, 28C Xeon 1.7GHz, T1000 8GB)")
p(f"Binary: af9396a0c29abed6a06a2bc85bec59a5")
p(f"Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
p("=" * 70)

# Protocol latency
p("\n--- Protocol Latency ---")
c = fresh()
bench("RETURN 1", "RETURN 1", c)
bench("RETURN 1+1", "RETURN 1+1", c)
c.close()

# LDBC Load
p("\n--- LDBC SF1 Load ---")
BASE = "/neo4j/datasets_ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter"
DYNAMIC = os.path.join(BASE, "dynamic")
STATIC = os.path.join(BASE, "static")
FK_COLUMNS = {"creator", "place", "replyOfPost", "replyOfComment", "Forum.id", "moderator"}

def load_csv(path):
    with open(path, "r") as f:
        reader = csv.DictReader(f, delimiter="|")
        return list(reader), reader.fieldnames

def escape(v):
    v = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{v}'"

if os.path.exists(BASE):
    c = fresh()
    t0 = time.time()
    for folder, filename, label in [
        (STATIC, "organisation_0_0.csv", "Organisation"), (STATIC, "place_0_0.csv", "Place"),
        (STATIC, "tag_0_0.csv", "Tag"), (STATIC, "tagclass_0_0.csv", "TagClass"),
        (DYNAMIC, "person_0_0.csv", "Person"), (DYNAMIC, "comment_0_0.csv", "Comment"),
        (DYNAMIC, "post_0_0.csv", "Post"), (DYNAMIC, "forum_0_0.csv", "Forum"),
    ]:
        path = os.path.join(folder, filename)
        rows, fields = load_csv(path)
        prop_fields = [f for f in fields if f not in FK_COLUMNS and f != "id"]
        count = 0
        s = time.time()
        for i in range(0, len(rows), 2000):
            batch = rows[i:i+2000]
            items = ["{" + ", ".join([f"id: {int(r['id'])}"] + [f"{f}: {escape(r[f])}" for f in prop_fields if f in r and r[f]]) + "}" for r in batch]
            try:
                c.execute(f"UNWIND [{', '.join(items)}] AS p CREATE (n:{label}) SET n = p")
                count += len(batch)
            except:
                for r in batch:
                    try:
                        props = [f"id: {int(r['id'])}"] + [f"{f}: {escape(r[f])}" for f in prop_fields if f in r and r[f]]
                        c.execute(f"CREATE (:{label} {{{', '.join(props)}}})")
                        count += 1
                    except: pass
        p(f"  {label:<15} {count:>10,} nodes  {time.time()-s:.1f}s")

    for label in ["Person","Comment","Post","Forum","Organisation","Place","Tag","TagClass"]:
        try: c.execute(f"CREATE INDEX ON :{label}(id)")
        except: pass

    c2 = fresh()
    gid_maps = {}
    for label in ["Person","Comment","Post","Forum","Organisation","Place","Tag","TagClass"]:
        cols, rows = c2.execute(f"MATCH (n:{label}) RETURN n.id, id(n)")
        gid_maps[label] = {int(r[0]): int(r[1]) for r in rows}

    for folder, filename, src_label, rel_type, dst_label in [
        (DYNAMIC, "person_knows_person_0_0.csv", "Person", "KNOWS", "Person"),
        (DYNAMIC, "person_hasInterest_tag_0_0.csv", "Person", "HAS_INTEREST", "Tag"),
        (DYNAMIC, "person_studyAt_organisation_0_0.csv", "Person", "STUDY_AT", "Organisation"),
        (DYNAMIC, "person_workAt_organisation_0_0.csv", "Person", "WORK_AT", "Organisation"),
        (DYNAMIC, "person_likes_comment_0_0.csv", "Person", "LIKES_COMMENT", "Comment"),
        (DYNAMIC, "person_likes_post_0_0.csv", "Person", "LIKES_POST", "Post"),
        (DYNAMIC, "forum_hasMember_person_0_0.csv", "Forum", "HAS_MEMBER", "Person"),
        (DYNAMIC, "forum_hasTag_tag_0_0.csv", "Forum", "HAS_TAG", "Tag"),
        (DYNAMIC, "comment_hasTag_tag_0_0.csv", "Comment", "HAS_TAG_C", "Tag"),
        (DYNAMIC, "post_hasTag_tag_0_0.csv", "Post", "HAS_TAG_P", "Tag"),
    ]:
        path = os.path.join(folder, filename)
        rows, fields = load_csv(path)
        sc, dc = fields[0], fields[1]
        sg, dg = [], []
        for r in rows:
            sr = r[sc].split("|")[0] if "|" in r[sc] else r[sc]
            dr = r[dc].split("|")[0] if "|" in r[dc] else r[dc]
            try:
                si, di = int(sr), int(dr)
                if si in gid_maps[src_label] and di in gid_maps[dst_label]:
                    sg.append(gid_maps[src_label][si]); dg.append(gid_maps[dst_label][di])
            except: pass
        s = time.time()
        cnt = 0
        for i in range(0, len(sg), 50000):
            cnt += c2.bulk_insert_edges_gid(rel_type, sg[i:i+50000], dg[i:i+50000])
        p(f"  {rel_type:<15} {cnt:>10,} edges  {time.time()-s:.1f}s  ({cnt/max(time.time()-s,0.001):,.0f}/s)")
    p(f"\n  Load time: {time.time()-t0:.1f}s")
    c.close(); c2.close()
else:
    p("  LDBC data not found")

# LDBC Queries
p("\n--- LDBC Queries ---")
c = fresh()
cols, rows = c.execute("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.id, p.firstName, count(f) AS deg ORDER BY abs(count(f)-96) LIMIT 1")
if rows and rows[0][2] > 5:
    pid = rows[0][0]
    p(f"  Person: {pid} ({rows[0][1]}), KNOWS={rows[0][2]}")
else:
    cols, rows = c.execute("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.id, count(f) AS deg ORDER BY deg DESC LIMIT 1")
    pid = rows[0][0] if rows else 933
    p(f"  Person: {pid}, KNOWS={rows[0][1] if rows else 0}")

bench("IS1: Profile", f"MATCH (p:Person {{id: {pid}}}) RETURN p.id, p.firstName, p.lastName", c)
bench("IS3: Friend count", f"MATCH (p:Person {{id: {pid}}})-[:KNOWS]-(f) RETURN count(f)", c)
bench("IC5: Forums", f"MATCH (p:Person {{id: {pid}}})-[:KNOWS]-(f)<-[:HAS_MEMBER]-(forum) RETURN forum.id LIMIT 10", c)
bench("IC11: Work", f"MATCH (p:Person {{id: {pid}}})-[:KNOWS]-(f)-[:WORK_AT]->(org) RETURN f.firstName, org.name LIMIT 10", c)
bench("Edge count", "MATCH ()-[r]->() RETURN count(r)", c)
bench("Node count", "MATCH (n) RETURN count(n)", c)
c.close()

# BFS Hops
p("\n--- BFS Hops (Cypher) ---")
for hops in range(1, 8):
    c = fresh()
    s = time.perf_counter()
    try:
        cols, rows = c.execute(f"MATCH (p:Person {{id: {pid}}})-[:KNOWS*1..{hops}]-(f) RETURN count(f)")
        ms = (time.perf_counter() - s) * 1000
        cnt = rows[0][0] if rows else 0
        p(f"  BFS {hops}-hop: {ms:.1f}ms  paths={cnt}")
        if ms > 600000:
            p("  STOPPING"); c.close(); break
    except Exception as e:
        ms = (time.perf_counter() - s) * 1000
        p(f"  BFS {hops}-hop: ERROR ({ms:.0f}ms)"); c.close(); break
    c.close()

# SF1 Analytics with GPU monitoring
p("\n--- SF1 Analytics (GPU monitoring) ---")
for name, q in [
    ("PageRank 5iter", 'CALL xray.pagerank(5, 0.85, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5'),
    ("Triangle Count", 'CALL xray.triangle_count("") YIELD triangles, edges_checked, time_ms, vertices RETURN *'),
    ("Connected Components", 'CALL xray.connected_components("") YIELD component_size, num_components, time_ms RETURN component_size, num_components, time_ms ORDER BY component_size DESC LIMIT 5'),
    ("Community 3iter", 'CALL xray.community_detection(3, "") YIELD community_size, num_communities RETURN community_size, num_communities ORDER BY community_size DESC LIMIT 5'),
    ("K-Core", 'CALL xray.kcore("") YIELD core_number, time_ms RETURN core_number, time_ms ORDER BY core_number DESC LIMIT 5'),
    ("HITS 3iter", 'CALL xray.hits(3, "") YIELD node_id, hub, authority, time_ms RETURN node_id, hub, authority, time_ms ORDER BY authority DESC LIMIT 5'),
    ("Betweenness 50", 'CALL xray.betweenness_centrality("", 50) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 5'),
]:
    c = fresh()
    run_once(name, q, c)
    c.close()

# Friendster Analytics with GPU monitoring
p("\n--- Friendster Analytics (GPU monitoring) ---")
for name, q in [
    ("F: Triangle Count", 'CALL xray.triangle_count("") YIELD triangles, edges_checked, time_ms, vertices RETURN *'),
    ("F: PersonalizedPR 5iter", 'CALL xray.personalized_pagerank(81306110, 0.85, 5, "") YIELD node_id, rank, time_ms RETURN node_id, rank, time_ms ORDER BY rank DESC LIMIT 5'),
    ("F: Clustering Coefficient", 'CALL xray.clustering_coefficient("") YIELD node_id, coefficient, triangles RETURN node_id, coefficient, triangles ORDER BY coefficient DESC LIMIT 5'),
    ("F: BC COLD e=0.05 b=1", 'CALL xray.betweenness_pair_sampled(0.05, 0.05, "", 1, 1) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10'),
    ("F: BC WARM e=0.05 b=1", 'CALL xray.betweenness_pair_sampled(0.05, 0.05, "", 1, 1) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10'),
    ("F: BC WARM e=0.10 b=1", 'CALL xray.betweenness_pair_sampled(0.10, 0.05, "", 1, 1) YIELD node_id, centrality, time_ms RETURN node_id, centrality, time_ms ORDER BY centrality DESC LIMIT 10'),
]:
    c = fresh()
    run_once(name, q, c)
    c.close()

# Friendster traversal
p("\n--- Friendster Traversal ---")
c = fresh()
bench("Shortest Path", 'CALL xray.shortest_path(81306110, 20676652, "") YIELD node_id, distance, path_index, time_ms RETURN *', c, warmup=5)
bench("Jaccard Similarity", 'CALL xray.jaccard_similarity(81306110, 20676652) YIELD jaccard, overlap, common, total_union, degree_a, degree_b RETURN *', c, warmup=5)
run_once("Link Prediction", 'CALL xray.link_prediction(81306110, 10, "adamic_adar") YIELD node_id, score, method, rank RETURN *', c)
c.close()

# CSR BFS
p("\n--- CSR BFS on Friendster ---")
for hops in range(1, 11):
    c = fresh()
    run_once(f"CSR BFS {hops}-hop", f'CALL xray.frontier_profile(81306110, {hops}, "OUTGOING") YIELD hop, frontier_size, cumulative_nodes, new_edges, avg_degree, max_degree RETURN *', c)
    c.close()

p(f"\nGPU final state: {gpu()}")
p("\n" + "=" * 70)
p("Benchmark complete.")
