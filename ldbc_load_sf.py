#!/usr/bin/env python3
"""Load LDBC SNB data into xrayGraphDB using LOAD CSV.

Usage:
    python3 ldbc_load_sf.py --sf sf1 [--host localhost] [--port 7687]
    python3 ldbc_load_sf.py --sf sf10 [--host localhost] [--port 7687]
"""
import argparse
import time
import sys
from neo4j import GraphDatabase


def run(session, query, label=""):
    """Execute a query and print timing."""
    t0 = time.time()
    try:
        result = session.run(query)
        summary = result.consume()
        elapsed = time.time() - t0
        counters = summary.counters if hasattr(summary, 'counters') else None
        info = ""
        if counters:
            parts = []
            if counters.nodes_created: parts.append(f"{counters.nodes_created} nodes")
            if counters.relationships_created: parts.append(f"{counters.relationships_created} rels")
            if counters.indexes_added: parts.append(f"{counters.indexes_added} indexes")
            info = " | " + ", ".join(parts) if parts else ""
        print(f"  OK  {label:50s} {elapsed:7.1f}s{info}", flush=True)
        return True
    except Exception as e:
        elapsed = time.time() - t0
        msg = str(e)[:120]
        print(f"  ERR {label:50s} {elapsed:7.1f}s | {msg}", flush=True)
        return False


def main():
    parser = argparse.ArgumentParser(description="Load LDBC SNB into xrayGraphDB")
    parser.add_argument("--sf", required=True, choices=["sf1", "sf10"], help="Scale factor")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=7687)
    parser.add_argument("--drop", action="store_true", help="Drop all data first")
    args = parser.parse_args()

    sf = args.sf
    base = f"/opt/ldbc-snb/{sf}_csv"

    driver = GraphDatabase.driver(f"bolt://{args.host}:{args.port}")
    total_start = time.time()

    with driver.session() as s:
        # Verify connection
        s.run("RETURN 1").consume()
        print(f"Connected to xrayGraphDB at {args.host}:{args.port}")

        if args.drop:
            print("\n=== DROPPING ALL DATA ===")
            run(s, "MATCH (n) DETACH DELETE n", "Drop all")

        # ---- PHASE 1: INDEXES ----
        print("\n=== PHASE 1: INDEXES ===")
        for label in ["Place", "Organisation", "TagClass", "Tag", "Person", "Forum", "Comment", "Post", "Message"]:
            run(s, f"CREATE INDEX ON :{label}(id)", f"Index :{label}(id)")

        # ---- PHASE 2: STATIC NODES ----
        print("\n=== PHASE 2: STATIC NODES ===")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/static/place_0_0.csv' AS row             CREATE (:Place {{id: toInteger(row.id), name: row.name, url: row.url, type: row.type, isPartOf: toInteger(row.isPartOf)}})
        """, "Place nodes")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/static/tagclass_0_0.csv' AS row             CREATE (:TagClass {{id: toInteger(row.id), name: row.name, url: row.url, isSubclassOf: toInteger(row.isSubclassOf)}})
        """, "TagClass nodes")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/static/tag_0_0.csv' AS row             CREATE (:Tag {{id: toInteger(row.id), name: row.name, url: row.url, hasType: toInteger(row.hasType)}})
        """, "Tag nodes")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/static/organisation_0_0.csv' AS row             CREATE (:Organisation {{id: toInteger(row.id), type: row.type, name: row.name, url: row.url, place: toInteger(row.place)}})
        """, "Organisation nodes")

        # ---- PHASE 3: DYNAMIC NODES ----
        print("\n=== PHASE 3: DYNAMIC NODES ===")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/dynamic/person_0_0.csv' AS row             CREATE (:Person {{id: toInteger(row.id), firstName: row.firstName, lastName: row.lastName,
                gender: row.gender, birthday: toInteger(row.birthday), creationDate: toInteger(row.creationDate),
                locationIP: row.locationIP, browserUsed: row.browserUsed, place: toInteger(row.place),
                language: row.language, email: row.email}})
        """, "Person nodes")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/dynamic/forum_0_0.csv' AS row             CREATE (:Forum {{id: toInteger(row.id), title: row.title, creationDate: toInteger(row.creationDate),
                moderator: toInteger(row.moderator)}})
        """, "Forum nodes")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/dynamic/post_0_0.csv' AS row             CREATE (:Post:Message {{id: toInteger(row.id), imageFile: row.imageFile,
                creationDate: toInteger(row.creationDate), locationIP: row.locationIP,
                browserUsed: row.browserUsed, language: row.language, content: row.content,
                length: toInteger(row.length), creator: toInteger(row.creator),
                forumId: toInteger(row.`Forum.id`), place: toInteger(row.place)}})
        """, "Post nodes (1M+)")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/dynamic/comment_0_0.csv' AS row             CREATE (:Comment:Message {{id: toInteger(row.id), creationDate: toInteger(row.creationDate),
                locationIP: row.locationIP, browserUsed: row.browserUsed, content: row.content,
                length: toInteger(row.length), creator: toInteger(row.creator),
                place: toInteger(row.place), replyOfPost: toInteger(row.replyOfPost),
                replyOfComment: toInteger(row.replyOfComment)}})
        """, "Comment nodes (2M+)")

        # ---- PHASE 4: RELATIONSHIPS FROM SEPARATE CSVs ----
        print("\n=== PHASE 4: RELATIONSHIP CSVs ===")

        # person_knows_person has duplicate headers — use positional access
        run(s, f"""
            LOAD CSV FROM 'file://{base}/dynamic/person_knows_person_0_0.csv' AS row             WITH row SKIP 1
            MATCH (a:Person {{id: toInteger(row[0])}}), (b:Person {{id: toInteger(row[1])}})
            CREATE (a)-[:KNOWS {{creationDate: toInteger(row[2])}}]->(b)
        """, "KNOWS relationships (180K)")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/dynamic/person_hasInterest_tag_0_0.csv' AS row             MATCH (p:Person {{id: toInteger(row.`Person.id`)}}), (t:Tag {{id: toInteger(row.`Tag.id`)}})
            CREATE (p)-[:HAS_INTEREST]->(t)
        """, "HAS_INTEREST relationships (229K)")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/dynamic/person_studyAt_organisation_0_0.csv' AS row             MATCH (p:Person {{id: toInteger(row.`Person.id`)}}), (o:Organisation {{id: toInteger(row.`Organisation.id`)}})
            CREATE (p)-[:STUDY_AT {{classYear: toInteger(row.classYear)}}]->(o)
        """, "STUDY_AT relationships (8K)")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/dynamic/person_workAt_organisation_0_0.csv' AS row             MATCH (p:Person {{id: toInteger(row.`Person.id`)}}), (o:Organisation {{id: toInteger(row.`Organisation.id`)}})
            CREATE (p)-[:WORK_AT {{workFrom: toInteger(row.workFrom)}}]->(o)
        """, "WORK_AT relationships (22K)")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/dynamic/person_likes_post_0_0.csv' AS row             MATCH (p:Person {{id: toInteger(row.`Person.id`)}}), (m:Post {{id: toInteger(row.`Post.id`)}})
            CREATE (p)-[:LIKES {{creationDate: toInteger(row.creationDate)}}]->(m)
        """, "LIKES Post relationships (752K)")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/dynamic/person_likes_comment_0_0.csv' AS row             MATCH (p:Person {{id: toInteger(row.`Person.id`)}}), (c:Comment {{id: toInteger(row.`Comment.id`)}})
            CREATE (p)-[:LIKES {{creationDate: toInteger(row.creationDate)}}]->(c)
        """, "LIKES Comment relationships (1.4M)")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/dynamic/forum_hasMember_person_0_0.csv' AS row             MATCH (f:Forum {{id: toInteger(row.`Forum.id`)}}), (p:Person {{id: toInteger(row.`Person.id`)}})
            CREATE (f)-[:HAS_MEMBER {{joinDate: toInteger(row.joinDate)}}]->(p)
        """, "HAS_MEMBER relationships (1.6M)")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/dynamic/forum_hasTag_tag_0_0.csv' AS row             MATCH (f:Forum {{id: toInteger(row.`Forum.id`)}}), (t:Tag {{id: toInteger(row.`Tag.id`)}})
            CREATE (f)-[:HAS_TAG]->(t)
        """, "Forum HAS_TAG relationships (310K)")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/dynamic/post_hasTag_tag_0_0.csv' AS row             MATCH (p:Post {{id: toInteger(row.`Post.id`)}}), (t:Tag {{id: toInteger(row.`Tag.id`)}})
            CREATE (p)-[:HAS_TAG]->(t)
        """, "Post HAS_TAG relationships (713K)")

        run(s, f"""
            LOAD CSV WITH HEADERS FROM 'file://{base}/dynamic/comment_hasTag_tag_0_0.csv' AS row             MATCH (c:Comment {{id: toInteger(row.`Comment.id`)}}), (t:Tag {{id: toInteger(row.`Tag.id`)}})
            CREATE (c)-[:HAS_TAG]->(t)
        """, "Comment HAS_TAG relationships (2.7M)")

        # ---- PHASE 5: RELATIONSHIPS FROM EMBEDDED FOREIGN KEYS ----
        print("\n=== PHASE 5: EMBEDDED FK RELATIONSHIPS ===")

        run(s, """
            MATCH (p:Post) WHERE p.creator IS NOT NULL
            MATCH (per:Person {id: p.creator})
            CREATE (p)-[:HAS_CREATOR]->(per)
        """, "Post HAS_CREATOR (1M)")

        run(s, """
            MATCH (c:Comment) WHERE c.creator IS NOT NULL
            MATCH (per:Person {id: c.creator})
            CREATE (c)-[:HAS_CREATOR]->(per)
        """, "Comment HAS_CREATOR (2M)")

        run(s, """
            MATCH (p:Post) WHERE p.forumId IS NOT NULL
            MATCH (f:Forum {id: p.forumId})
            CREATE (f)-[:CONTAINER_OF]->(p)
        """, "Forum CONTAINER_OF Post (1M)")

        run(s, """
            MATCH (p:Post) WHERE p.place IS NOT NULL
            MATCH (pl:Place {id: p.place})
            CREATE (p)-[:IS_LOCATED_IN]->(pl)
        """, "Post IS_LOCATED_IN (1M)")

        run(s, """
            MATCH (c:Comment) WHERE c.place IS NOT NULL
            MATCH (pl:Place {id: c.place})
            CREATE (c)-[:IS_LOCATED_IN]->(pl)
        """, "Comment IS_LOCATED_IN (2M)")

        run(s, """
            MATCH (c:Comment) WHERE c.replyOfPost IS NOT NULL AND c.replyOfPost <> 0
            MATCH (p:Post {id: c.replyOfPost})
            CREATE (c)-[:REPLY_OF]->(p)
        """, "Comment REPLY_OF Post")

        run(s, """
            MATCH (c:Comment) WHERE c.replyOfComment IS NOT NULL AND c.replyOfComment <> 0
            MATCH (parent:Comment {id: c.replyOfComment})
            CREATE (c)-[:REPLY_OF]->(parent)
        """, "Comment REPLY_OF Comment")

        run(s, """
            MATCH (per:Person)
            MATCH (p:Place {id: per.place})
            CREATE (per)-[:IS_LOCATED_IN]->(p)
        """, "Person IS_LOCATED_IN Place")

        run(s, """
            MATCH (f:Forum) WHERE f.moderator IS NOT NULL
            MATCH (p:Person {id: f.moderator})
            CREATE (f)-[:HAS_MODERATOR]->(p)
        """, "Forum HAS_MODERATOR Person")

        run(s, """
            MATCH (p:Place) WHERE p.isPartOf IS NOT NULL
            MATCH (parent:Place {id: p.isPartOf})
            CREATE (p)-[:IS_PART_OF]->(parent)
        """, "Place IS_PART_OF Place")

        run(s, """
            MATCH (t:Tag)
            MATCH (tc:TagClass {id: t.hasType})
            CREATE (t)-[:HAS_TYPE]->(tc)
        """, "Tag HAS_TYPE TagClass")

        run(s, """
            MATCH (tc:TagClass) WHERE tc.isSubclassOf IS NOT NULL
            MATCH (parent:TagClass {id: tc.isSubclassOf})
            CREATE (tc)-[:IS_SUBCLASS_OF]->(parent)
        """, "TagClass IS_SUBCLASS_OF")

        run(s, """
            MATCH (o:Organisation)
            MATCH (p:Place {id: o.place})
            CREATE (o)-[:IS_LOCATED_IN]->(p)
        """, "Organisation IS_LOCATED_IN Place")

        # ---- PHASE 6: VERIFICATION ----
        print("\n=== PHASE 6: VERIFICATION ===")

        result = s.run("MATCH (n) RETURN labels(n)[0] AS label, count(n) AS count ORDER BY label")
        print("  Node counts:")
        for rec in result:
            print(f"    {rec['label']:20s} {rec['count']:>10,}")

        result = s.run("MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count ORDER BY type")
        print("  Relationship counts:")
        for rec in result:
            print(f"    {rec['type']:20s} {rec['count']:>10,}")

    driver.close()
    total_elapsed = time.time() - total_start
    print(f"\n=== COMPLETE in {total_elapsed:.1f}s ({total_elapsed/60:.1f} min) ===")


if __name__ == "__main__":
    main()
