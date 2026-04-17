#!/usr/bin/env python3
"""Load LDBC SNB data into xrayGraphDB via Bolt UNWIND batches.

LOAD CSV is not available in v4.9.2, so we read CSVs in Python
and send UNWIND batches over Bolt.

Usage:
    python3 ldbc_load_bolt.py --sf sf1 [--host localhost] [--port 7687]
"""
import argparse
import csv
import os
import time
from neo4j import GraphDatabase


def load_csv(path, delimiter="|"):
    """Read a CSV file and return list of dicts."""
    with open(path, "r") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return list(reader)


def load_csv_positional(path, delimiter="|"):
    """Read a CSV without headers, return list of tuples."""
    with open(path, "r") as f:
        reader = csv.reader(f, delimiter=delimiter)
        next(reader)  # skip header
        return [row for row in reader]


def batch_create(session, query, data, batch_size=3000, label=""):
    """Execute UNWIND batches with retry on connection failure."""
    t0 = time.time()
    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        for attempt in range(2):
            try:
                session.run(query, {"batch": batch}).consume()
                break
            except Exception as e:
                if attempt == 0:
                    print(f"    RETRY {label} at {total}: {str(e)[:60]}", flush=True)
                    time.sleep(2)
                else:
                    raise
        total += len(batch)
        if total % 50000 < batch_size and total > batch_size:
            elapsed = time.time() - t0
            rate = total / elapsed if elapsed > 0 else 0
            print(f"    {label}: {total:>10,} / {len(data):>10,}  ({rate:,.0f}/s)", flush=True)
    elapsed = time.time() - t0
    rate = len(data) / elapsed if elapsed > 0 else 0
    print(f"  OK  {label:50s} {len(data):>10,} rows  {elapsed:7.1f}s  ({rate:,.0f}/s)", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sf", required=True, choices=["sf1", "sf10"])
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=7687)
    parser.add_argument("--drop", action="store_true")
    args = parser.parse_args()

    base = f"/opt/ldbc-snb/{args.sf}/social_network-{args.sf}-CsvCompositeMergeForeign-LongDateFormatter"
    driver = GraphDatabase.driver(f"bolt://{args.host}:{args.port}")
    t_start = time.time()

    with driver.session() as s:
        s.run("RETURN 1").consume()
        print(f"Connected to xrayGraphDB at {args.host}:{args.port}")

        if args.drop:
            print("\n=== DROP ALL ===")
            s.run("MATCH (n) DETACH DELETE n").consume()
            print("  Dropped.")

        # === INDEXES ===
        print("\n=== INDEXES ===")
        for label in ["Place", "Organisation", "TagClass", "Tag", "Person", "Forum", "Comment", "Post"]:
            try:
                s.run(f"CREATE INDEX ON :{label}(id)").consume()
                print(f"  Index :{label}(id)")
            except Exception:
                pass

        # === STATIC NODES ===
        print("\n=== STATIC NODES ===")

        rows = load_csv(f"{base}/static/place_0_0.csv")
        data = [{"id": int(r["id"]), "name": r["name"], "url": r["url"],
                 "type": r["type"], "isPartOf": int(r["isPartOf"]) if r.get("isPartOf") else None}
                for r in rows]
        batch_create(s, "UNWIND $batch AS p CREATE (:Place {id: p.id, name: p.name, url: p.url, type: p.type, isPartOf: p.isPartOf})",
                     data, label="Place")

        rows = load_csv(f"{base}/static/tagclass_0_0.csv")
        data = [{"id": int(r["id"]), "name": r["name"], "url": r["url"],
                 "sub": int(r["isSubclassOf"]) if r.get("isSubclassOf") else None}
                for r in rows]
        batch_create(s, "UNWIND $batch AS p CREATE (:TagClass {id: p.id, name: p.name, url: p.url, isSubclassOf: p.sub})",
                     data, label="TagClass")

        rows = load_csv(f"{base}/static/tag_0_0.csv")
        data = [{"id": int(r["id"]), "name": r["name"], "url": r["url"],
                 "hasType": int(r["hasType"])} for r in rows]
        batch_create(s, "UNWIND $batch AS p CREATE (:Tag {id: p.id, name: p.name, url: p.url, hasType: p.hasType})",
                     data, label="Tag")

        rows = load_csv(f"{base}/static/organisation_0_0.csv")
        data = [{"id": int(r["id"]), "type": r["type"], "name": r["name"],
                 "url": r["url"], "place": int(r["place"])} for r in rows]
        batch_create(s, "UNWIND $batch AS p CREATE (:Organisation {id: p.id, type: p.type, name: p.name, url: p.url, place: p.place})",
                     data, label="Organisation")

        # === DYNAMIC NODES ===
        print("\n=== DYNAMIC NODES ===")

        rows = load_csv(f"{base}/dynamic/person_0_0.csv")
        data = [{"id": int(r["id"]), "fn": r["firstName"], "ln": r["lastName"],
                 "g": r["gender"], "bd": int(r["birthday"]), "cd": int(r["creationDate"]),
                 "ip": r["locationIP"], "br": r["browserUsed"], "pl": int(r["place"]),
                 "lang": r.get("language", ""), "email": r.get("email", "")} for r in rows]
        batch_create(s, """UNWIND $batch AS p CREATE (:Person {id: p.id, firstName: p.fn, lastName: p.ln,
            gender: p.g, birthday: p.bd, creationDate: p.cd, locationIP: p.ip, browserUsed: p.br,
            place: p.pl, language: p.lang, email: p.email})""",
                     data, label="Person")

        rows = load_csv(f"{base}/dynamic/forum_0_0.csv")
        data = [{"id": int(r["id"]), "title": r["title"], "cd": int(r["creationDate"]),
                 "mod": int(r["moderator"]) if r.get("moderator") else None} for r in rows]
        batch_create(s, "UNWIND $batch AS p CREATE (:Forum {id: p.id, title: p.title, creationDate: p.cd, moderator: p.mod})",
                     data, label="Forum")

        print("  Loading Post — capped at 400K (v4.9.2 crashes at ~1M total nodes)...")
        rows = load_csv(f"{base}/dynamic/post_0_0.csv")[:400000]
        data = [{"id": int(r["id"]),
                 "cd": int(r["creationDate"]), "ip": r["locationIP"], "br": r["browserUsed"],
                 "lang": r.get("language", ""),
                 "len": int(r["length"]), "cr": int(r["creator"]),
                 "fid": int(r["Forum.id"]) if r.get("Forum.id") else None,
                 "pl": int(r["place"])} for r in rows]
        batch_create(s, """UNWIND $batch AS p CREATE (:Post:Message {id: p.id,
            creationDate: p.cd, locationIP: p.ip, browserUsed: p.br, language: p.lang,
            length: p.len, creator: p.cr, forumId: p.fid, place: p.pl})""",
                     data, batch_size=1000, label="Post")

        print("  Loading Comment — capped at 400K (v4.9.2 crashes at ~1M total nodes)...")
        rows = load_csv(f"{base}/dynamic/comment_0_0.csv")[:400000]
        data = [{"id": int(r["id"]), "cd": int(r["creationDate"]), "ip": r["locationIP"],
                 "br": r["browserUsed"],
                 "len": int(r["length"]), "cr": int(r["creator"]), "pl": int(r["place"]),
                 "rp": int(r["replyOfPost"]) if r.get("replyOfPost") else None,
                 "rc": int(r["replyOfComment"]) if r.get("replyOfComment") else None}
                for r in rows]
        batch_create(s, """UNWIND $batch AS p CREATE (:Comment:Message {id: p.id,
            creationDate: p.cd, locationIP: p.ip, browserUsed: p.br,
            length: p.len, creator: p.cr, place: p.pl, replyOfPost: p.rp, replyOfComment: p.rc})""",
                     data, batch_size=1000, label="Comment")

        # === RELATIONSHIPS FROM CSVs ===
        print("\n=== RELATIONSHIPS ===")

        # KNOWS (duplicate header — positional)
        rows = load_csv_positional(f"{base}/dynamic/person_knows_person_0_0.csv")
        data = [{"a": int(r[0]), "b": int(r[1]), "cd": int(r[2])} for r in rows]
        batch_create(s, "UNWIND $batch AS p MATCH (a:Person {id: p.a}), (b:Person {id: p.b}) CREATE (a)-[:KNOWS {creationDate: p.cd}]->(b)",
                     data, batch_size=1000, label="KNOWS")

        rows = load_csv(f"{base}/dynamic/person_hasInterest_tag_0_0.csv")
        data = [{"p": int(r["Person.id"]), "t": int(r["Tag.id"])} for r in rows]
        batch_create(s, "UNWIND $batch AS p MATCH (a:Person {id: p.p}), (b:Tag {id: p.t}) CREATE (a)-[:HAS_INTEREST]->(b)",
                     data, batch_size=2000, label="HAS_INTEREST")

        rows = load_csv(f"{base}/dynamic/person_studyAt_organisation_0_0.csv")
        data = [{"p": int(r["Person.id"]), "o": int(r["Organisation.id"]), "y": int(r["classYear"])} for r in rows]
        batch_create(s, "UNWIND $batch AS p MATCH (a:Person {id: p.p}), (b:Organisation {id: p.o}) CREATE (a)-[:STUDY_AT {classYear: p.y}]->(b)",
                     data, label="STUDY_AT")

        rows = load_csv(f"{base}/dynamic/person_workAt_organisation_0_0.csv")
        data = [{"p": int(r["Person.id"]), "o": int(r["Organisation.id"]), "y": int(r["workFrom"])} for r in rows]
        batch_create(s, "UNWIND $batch AS p MATCH (a:Person {id: p.p}), (b:Organisation {id: p.o}) CREATE (a)-[:WORK_AT {workFrom: p.y}]->(b)",
                     data, label="WORK_AT")

        rows = load_csv(f"{base}/dynamic/person_likes_post_0_0.csv")
        data = [{"p": int(r["Person.id"]), "m": int(r["Post.id"]), "cd": int(r["creationDate"])} for r in rows]
        batch_create(s, "UNWIND $batch AS p MATCH (a:Person {id: p.p}), (b:Post {id: p.m}) CREATE (a)-[:LIKES {creationDate: p.cd}]->(b)",
                     data, batch_size=1000, label="LIKES_POST")

        rows = load_csv(f"{base}/dynamic/person_likes_comment_0_0.csv")
        data = [{"p": int(r["Person.id"]), "c": int(r["Comment.id"]), "cd": int(r["creationDate"])} for r in rows]
        batch_create(s, "UNWIND $batch AS p MATCH (a:Person {id: p.p}), (b:Comment {id: p.c}) CREATE (a)-[:LIKES {creationDate: p.cd}]->(b)",
                     data, batch_size=1000, label="LIKES_COMMENT")

        rows = load_csv(f"{base}/dynamic/forum_hasMember_person_0_0.csv")
        data = [{"f": int(r["Forum.id"]), "p": int(r["Person.id"]), "jd": int(r["joinDate"])} for r in rows]
        batch_create(s, "UNWIND $batch AS p MATCH (a:Forum {id: p.f}), (b:Person {id: p.p}) CREATE (a)-[:HAS_MEMBER {joinDate: p.jd}]->(b)",
                     data, batch_size=1000, label="HAS_MEMBER")

        rows = load_csv(f"{base}/dynamic/forum_hasTag_tag_0_0.csv")
        data = [{"f": int(r["Forum.id"]), "t": int(r["Tag.id"])} for r in rows]
        batch_create(s, "UNWIND $batch AS p MATCH (a:Forum {id: p.f}), (b:Tag {id: p.t}) CREATE (a)-[:HAS_TAG]->(b)",
                     data, batch_size=2000, label="FORUM_HAS_TAG")

        rows = load_csv(f"{base}/dynamic/post_hasTag_tag_0_0.csv")
        data = [{"p": int(r["Post.id"]), "t": int(r["Tag.id"])} for r in rows]
        batch_create(s, "UNWIND $batch AS p MATCH (a:Post {id: p.p}), (b:Tag {id: p.t}) CREATE (a)-[:HAS_TAG]->(b)",
                     data, batch_size=2000, label="POST_HAS_TAG")

        rows = load_csv(f"{base}/dynamic/comment_hasTag_tag_0_0.csv")
        data = [{"c": int(r["Comment.id"]), "t": int(r["Tag.id"])} for r in rows]
        batch_create(s, "UNWIND $batch AS p MATCH (a:Comment {id: p.c}), (b:Tag {id: p.t}) CREATE (a)-[:HAS_TAG]->(b)",
                     data, batch_size=1000, label="COMMENT_HAS_TAG")

        # === EMBEDDED FK RELATIONSHIPS ===
        print("\n=== FK RELATIONSHIPS ===")

        for label, query in [
            ("Post HAS_CREATOR", "MATCH (p:Post) WHERE p.creator IS NOT NULL MATCH (per:Person {id: p.creator}) CREATE (p)-[:HAS_CREATOR]->(per)"),
            ("Comment HAS_CREATOR", "MATCH (c:Comment) WHERE c.creator IS NOT NULL MATCH (per:Person {id: c.creator}) CREATE (c)-[:HAS_CREATOR]->(per)"),
            ("Forum CONTAINER_OF", "MATCH (p:Post) WHERE p.forumId IS NOT NULL MATCH (f:Forum {id: p.forumId}) CREATE (f)-[:CONTAINER_OF]->(p)"),
            ("Post IS_LOCATED_IN", "MATCH (p:Post) WHERE p.place IS NOT NULL MATCH (pl:Place {id: p.place}) CREATE (p)-[:IS_LOCATED_IN]->(pl)"),
            ("Comment IS_LOCATED_IN", "MATCH (c:Comment) WHERE c.place IS NOT NULL MATCH (pl:Place {id: c.place}) CREATE (c)-[:IS_LOCATED_IN]->(pl)"),
            ("Comment REPLY_OF Post", "MATCH (c:Comment) WHERE c.replyOfPost IS NOT NULL AND c.replyOfPost <> 0 MATCH (p:Post {id: c.replyOfPost}) CREATE (c)-[:REPLY_OF]->(p)"),
            ("Comment REPLY_OF Comment", "MATCH (c:Comment) WHERE c.replyOfComment IS NOT NULL AND c.replyOfComment <> 0 MATCH (parent:Comment {id: c.replyOfComment}) CREATE (c)-[:REPLY_OF]->(parent)"),
            ("Person IS_LOCATED_IN", "MATCH (per:Person) MATCH (p:Place {id: per.place}) CREATE (per)-[:IS_LOCATED_IN]->(p)"),
            ("Forum HAS_MODERATOR", "MATCH (f:Forum) WHERE f.moderator IS NOT NULL MATCH (p:Person {id: f.moderator}) CREATE (f)-[:HAS_MODERATOR]->(p)"),
            ("Place IS_PART_OF", "MATCH (p:Place) WHERE p.isPartOf IS NOT NULL MATCH (parent:Place {id: p.isPartOf}) CREATE (p)-[:IS_PART_OF]->(parent)"),
            ("Tag HAS_TYPE", "MATCH (t:Tag) MATCH (tc:TagClass {id: t.hasType}) CREATE (t)-[:HAS_TYPE]->(tc)"),
            ("TagClass IS_SUBCLASS_OF", "MATCH (tc:TagClass) WHERE tc.isSubclassOf IS NOT NULL MATCH (parent:TagClass {id: tc.isSubclassOf}) CREATE (tc)-[:IS_SUBCLASS_OF]->(parent)"),
            ("Org IS_LOCATED_IN", "MATCH (o:Organisation) MATCH (p:Place {id: o.place}) CREATE (o)-[:IS_LOCATED_IN]->(p)"),
        ]:
            t0 = time.time()
            try:
                s.run(query).consume()
                print(f"  OK  {label:40s} {time.time()-t0:7.1f}s", flush=True)
            except Exception as e:
                print(f"  ERR {label:40s} {str(e)[:80]}", flush=True)

        # === VERIFY ===
        print("\n=== VERIFICATION ===")
        r = s.run("MATCH (n) RETURN labels(n)[0] AS label, count(n) AS c ORDER BY label")
        for rec in r:
            print(f"  {rec['label']:20s} {rec['c']:>10,}")
        r = s.run("MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS c ORDER BY type")
        for rec in r:
            print(f"  {rec['type']:20s} {rec['c']:>10,}")

    driver.close()
    total = time.time() - t_start
    print(f"\n=== COMPLETE in {total:.1f}s ({total/60:.1f} min) ===")


if __name__ == "__main__":
    main()
