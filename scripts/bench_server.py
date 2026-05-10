#!/usr/bin/env python3
"""Generic benchmark script — pass AUTH, PORT, SERVER_NAME as env vars."""
import time, sys, os
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

AUTH = os.environ.get("BENCH_AUTH", "admin:xraygraphdb")
PORT = int(os.environ.get("BENCH_PORT", "7689"))
NAME = os.environ.get("BENCH_NAME", "unknown")
LOG = os.environ.get("BENCH_LOG", "/tmp/bench_server.log")
log = open(LOG, "w", buffering=1)

def p(msg):
    log.write(msg + "\n")
    log.flush()

def fresh():
    return XrayProtocolClient(host="127.0.0.1", port=PORT, auth_token=AUTH, database="xraygraphdb", read_timeout=7200)

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
    p(f"  {name:<35} cold={cold:.2f}  warm={warm:.2f}  p50={p50:.2f}ms  rows={len(rows)}")
    if rows: p(f"    {rows[0]}")

def run_once(name, query, c):
    s = time.perf_counter()
    try:
        cols, rows = c.execute(query)
        ms = (time.perf_counter() - s) * 1000
        p(f"  {name:<35} {ms:.0f}ms  rows={len(rows)}")
        if rows: p(f"    {rows[0]}")
    except Exception as e:
        ms = (time.perf_counter() - s) * 1000
        p(f"  {name:<35} ERROR ({ms:.0f}ms): {str(e)[:100]}")

p("=" * 70)
p(f"BENCHMARK — {NAME}")
p(f"Port: {PORT}, Timestamp: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
p("=" * 70)

# Protocol latency
p("\n--- Protocol Latency ---")
c = fresh()
bench("RETURN 1", "RETURN 1", c)
bench("RETURN 1+1", "RETURN 1+1", c)
bench("RETURN range(1,100)", "RETURN range(1,100)", c)
c.close()

# LDBC Load
p("\n--- LDBC SF1 Load ---")
import csv

BASE = None
for candidate in [
    "/neo4j/datasets_ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter",
    "/opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter",
]:
    if os.path.exists(candidate):
        BASE = candidate
        break

if BASE:
    def load_csv(path):
        with open(path, "r") as f:
            reader = csv.DictReader(f, delimiter="|")
            return list(reader), reader.fieldnames

    def escape(v):
        v = str(v).replace("\\", "\\\\").replace("'", "\\'")
        return f"'{v}'"

    FK_COLUMNS = {"creator", "place", "replyOfPost", "replyOfComment", "Forum.id", "moderator"}
    DYNAMIC = os.path.join(BASE, "dynamic")
    STATIC = os.path.join(BASE, "static")

    c = fresh()
    t0 = time.time()
    for folder, filename, label in [
        (STATIC, "organisation_0_0.csv", "Organisation"),
        (STATIC, "place_0_0.csv", "Place"),
        (STATIC, "tag_0_0.csv", "Tag"),
        (STATIC, "tagclass_0_0.csv", "TagClass"),
        (DYNAMIC, "person_0_0.csv", "Person"),
        (DYNAMIC, "comment_0_0.csv", "Comment"),
        (DYNAMIC, "post_0_0.csv", "Post"),
        (DYNAMIC, "forum_0_0.csv", "Forum"),
    ]:
        path = os.path.join(folder, filename)
        rows, fields = load_csv(path)
        prop_fields = [f for f in fields if f not in FK_COLUMNS and f != "id"]
        count = 0
        s = time.time()
        for i in range(0, len(rows), 2000):
            batch = rows[i:i+2000]
            items = []
            for row in batch:
                props = [f"id: {int(row['id'])}"]
                for f in prop_fields:
                    if f in row and row[f]:
                        props.append(f"{f}: {escape(row[f])}")
                items.append("{" + ", ".join(props) + "}")
            try:
                c.execute(f"UNWIND [{', '.join(items)}] AS p CREATE (n:{label}) SET n = p")
                count += len(batch)
            except:
                for row in batch:
                    try:
                        props = [f"id: {int(row['id'])}"]
                        for f in prop_fields:
                            if f in row and row[f]:
                                props.append(f"{f}: {escape(row[f])}")
                        c.execute(f"CREATE (:{label} {{{', '.join(props)}}})")
                        count += 1
                    except: pass
        p(f"  {label:<15} {count:>10,} nodes  {time.time()-s:.1f}s")

    for label in ["Person","Comment","Post","Forum","Organisation","Place","Tag","TagClass"]:
        try: c.execute(f"CREATE INDEX ON :{label}(id)")
        except: pass

    # GID edges
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
        src_col, dst_col = fields[0], fields[1]
        sg, dg = [], []
        for row in rows:
            sr = row[src_col].split("|")[0] if "|" in row[src_col] else row[src_col]
            dr = row[dst_col].split("|")[0] if "|" in row[dst_col] else row[dst_col]
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

    load_time = time.time() - t0
    cols, rows = c2.execute("MATCH (n) RETURN count(n)")
    cols, rows2 = c2.execute("MATCH ()-[r]->() RETURN count(r)")
    p(f"\n  Total: {rows[0][0]} nodes, {rows2[0][0]} edges in {load_time:.1f}s")
    c.close(); c2.close()
else:
    p("  LDBC data not found — skipping load")

# LDBC queries
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

# BFS
p("\n--- BFS Hops ---")
for hops in range(1, 8):
    c = fresh()
    s = time.perf_counter()
    try:
        cols, rows = c.execute(f"MATCH (p:Person {{id: {pid}}})-[:KNOWS*1..{hops}]-(f) RETURN count(f)")
        ms = (time.perf_counter() - s) * 1000
        cnt = rows[0][0] if rows else 0
        p(f"  BFS {hops}-hop: {ms:.1f}ms  paths={cnt}")
        if ms > 600000:
            p(f"  STOPPING"); c.close(); break
    except Exception as e:
        ms = (time.perf_counter() - s) * 1000
        p(f"  BFS {hops}-hop: ERROR ({ms:.0f}ms)"); c.close(); break
    c.close()

# Analytics on Cypher store
p("\n--- Analytics (Cypher store) ---")
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

p("\n" + "=" * 70)
p("Benchmark complete.")
