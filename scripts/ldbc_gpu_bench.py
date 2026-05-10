#!/usr/bin/env python3
"""LDBC load + queries + BFS on GPU server."""
import time, sys, csv, os
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

AUTH = "admin:xraygraphdb"
DB = "xraygraphdb"
BASE = "/neo4j/datasets_ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter"
DYN = os.path.join(BASE, "dynamic")
STA = os.path.join(BASE, "static")
FK = {"creator", "place", "replyOfPost", "replyOfComment", "Forum.id", "moderator"}

def fresh():
    return XrayProtocolClient(host="127.0.0.1", port=7689, auth_token=AUTH, database=DB, read_timeout=7200)

def escape(v):
    v = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return "'" + v + "'"

def load_csv(path):
    with open(path, "r") as f:
        reader = csv.DictReader(f, delimiter="|")
        return list(reader), reader.fieldnames

print("=== LDBC Load ===")
c = fresh()
t0 = time.time()
for folder, fn, label in [
    (STA, "organisation_0_0.csv", "Organisation"), (STA, "place_0_0.csv", "Place"),
    (STA, "tag_0_0.csv", "Tag"), (STA, "tagclass_0_0.csv", "TagClass"),
    (DYN, "person_0_0.csv", "Person"), (DYN, "comment_0_0.csv", "Comment"),
    (DYN, "post_0_0.csv", "Post"), (DYN, "forum_0_0.csv", "Forum"),
]:
    rows, fields = load_csv(os.path.join(folder, fn))
    pf = [f for f in fields if f not in FK and f != "id"]
    cnt = 0
    s = time.time()
    for i in range(0, len(rows), 2000):
        batch = rows[i:i+2000]
        items = []
        for r in batch:
            props = ["id: " + str(int(r["id"]))]
            for f in pf:
                if f in r and r[f]:
                    props.append(f + ": " + escape(r[f]))
            items.append("{" + ", ".join(props) + "}")
        try:
            c.execute("UNWIND [" + ", ".join(items) + "] AS p CREATE (n:" + label + ") SET n = p")
            cnt += len(batch)
        except:
            for r in batch:
                try:
                    props = ["id: " + str(int(r["id"]))]
                    for f in pf:
                        if f in r and r[f]:
                            props.append(f + ": " + escape(r[f]))
                    c.execute("CREATE (:" + label + " {" + ", ".join(props) + "})")
                    cnt += 1
                except:
                    pass
    print("  " + label.ljust(15) + str(cnt).rjust(10) + " nodes  " + str(round(time.time()-s, 1)) + "s")

for lb in ["Person", "Comment", "Post", "Forum", "Organisation", "Place", "Tag", "TagClass"]:
    try:
        c.execute("CREATE INDEX ON :" + lb + "(id)")
    except:
        pass

c2 = fresh()
gm = {}
for lb in ["Person", "Comment", "Post", "Forum", "Organisation", "Place", "Tag", "TagClass"]:
    cols, rows = c2.execute("MATCH (n:" + lb + ") RETURN n.id, id(n)")
    gm[lb] = {int(r[0]): int(r[1]) for r in rows}

for folder, fn, sl, rt, dl in [
    (DYN, "person_knows_person_0_0.csv", "Person", "KNOWS", "Person"),
    (DYN, "person_hasInterest_tag_0_0.csv", "Person", "HAS_INTEREST", "Tag"),
    (DYN, "person_studyAt_organisation_0_0.csv", "Person", "STUDY_AT", "Organisation"),
    (DYN, "person_workAt_organisation_0_0.csv", "Person", "WORK_AT", "Organisation"),
    (DYN, "person_likes_comment_0_0.csv", "Person", "LIKES_COMMENT", "Comment"),
    (DYN, "person_likes_post_0_0.csv", "Person", "LIKES_POST", "Post"),
    (DYN, "forum_hasMember_person_0_0.csv", "Forum", "HAS_MEMBER", "Person"),
    (DYN, "forum_hasTag_tag_0_0.csv", "Forum", "HAS_TAG", "Tag"),
    (DYN, "comment_hasTag_tag_0_0.csv", "Comment", "HAS_TAG_C", "Tag"),
    (DYN, "post_hasTag_tag_0_0.csv", "Post", "HAS_TAG_P", "Tag"),
]:
    rows, fields = load_csv(os.path.join(folder, fn))
    sc, dc = fields[0], fields[1]
    sg, dg = [], []
    for r in rows:
        sr = r[sc].split("|")[0] if "|" in r[sc] else r[sc]
        dr = r[dc].split("|")[0] if "|" in r[dc] else r[dc]
        try:
            si, di = int(sr), int(dr)
            if si in gm[sl] and di in gm[dl]:
                sg.append(gm[sl][si])
                dg.append(gm[dl][di])
        except:
            pass
    s = time.time()
    cnt = 0
    for i in range(0, len(sg), 50000):
        cnt += c2.bulk_insert_edges_gid(rt, sg[i:i+50000], dg[i:i+50000])
    elapsed = time.time() - s
    rate = int(cnt / max(elapsed, 0.001))
    print("  " + rt.ljust(15) + str(cnt).rjust(10) + " edges  " + str(round(elapsed, 1)) + "s  (" + str(rate) + "/s)")

lt = time.time() - t0
cols, r1 = c2.execute("MATCH (n) RETURN count(n)")
cols, r2 = c2.execute("MATCH ()-[r]->() RETURN count(r)")
print("  Total: " + str(r1[0][0]) + " nodes, " + str(r2[0][0]) + " edges in " + str(round(lt, 1)) + "s")
c.close()
c2.close()

# LDBC Queries
print("\n=== LDBC Queries ===")
c = fresh()
cols, rows = c.execute("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.id, p.firstName, count(f) AS deg ORDER BY abs(count(f)-96) LIMIT 1")
if rows and rows[0][2] > 5:
    pid = rows[0][0]
    print("Person: " + str(pid) + " (" + str(rows[0][1]) + "), KNOWS=" + str(rows[0][2]))
else:
    cols, rows = c.execute("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.id, count(f) AS deg ORDER BY deg DESC LIMIT 1")
    pid = rows[0][0] if rows else 933
    print("Person: " + str(pid) + ", KNOWS=" + str(rows[0][1] if rows else 0))

queries = [
    ("IS1", "MATCH (p:Person {id: " + str(pid) + "}) RETURN p.id, p.firstName, p.lastName"),
    ("IS3", "MATCH (p:Person {id: " + str(pid) + "})-[:KNOWS]-(f) RETURN count(f)"),
    ("IC5", "MATCH (p:Person {id: " + str(pid) + "})-[:KNOWS]-(f)<-[:HAS_MEMBER]-(forum) RETURN forum.id LIMIT 10"),
    ("IC11", "MATCH (p:Person {id: " + str(pid) + "})-[:KNOWS]-(f)-[:WORK_AT]->(org) RETURN f.firstName, org.name LIMIT 10"),
    ("Edge count", "MATCH ()-[r]->() RETURN count(r)"),
    ("Node count", "MATCH (n) RETURN count(n)"),
]

for name, q in queries:
    s = time.perf_counter()
    cols, rows = c.execute(q)
    cold = (time.perf_counter() - s) * 1000
    times = []
    for _ in range(10):
        s = time.perf_counter()
        c.execute(q)
        times.append((time.perf_counter() - s) * 1000)
    warm = sum(times) / len(times)
    p50 = sorted(times)[5]
    print("  " + name.ljust(15) + "cold=" + str(round(cold, 2)) + "  warm=" + str(round(warm, 2)) + "  p50=" + str(round(p50, 2)) + "ms  rows=" + str(len(rows)))
c.close()

# BFS
print("\n=== BFS Hops ===")
for hops in range(1, 8):
    c = fresh()
    s = time.perf_counter()
    try:
        cols, rows = c.execute("MATCH (p:Person {id: " + str(pid) + "})-[:KNOWS*1.." + str(hops) + "]-(f) RETURN count(f)")
        ms = (time.perf_counter() - s) * 1000
        cnt = rows[0][0] if rows else 0
        print("  BFS " + str(hops) + "-hop: " + str(round(ms, 1)) + "ms  paths=" + str(cnt))
        if ms > 600000:
            c.close()
            break
    except Exception as e:
        ms = (time.perf_counter() - s) * 1000
        print("  BFS " + str(hops) + "-hop: ERROR (" + str(int(ms)) + "ms)")
        c.close()
        break
    c.close()

print("\nLDBC COMPLETE")
