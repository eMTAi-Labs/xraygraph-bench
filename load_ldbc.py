#!/usr/bin/env python3
"""
LDBC SNB data loader for xrayGraphDB.

Loads LDBC Social Network Benchmark data (CsvCompositeMergeForeign-LongDateFormatter
format) into xrayGraphDB via the Bolt protocol using batched UNWIND operations.

Usage:
    python3 load_ldbc.py --scale-factor sf1 --host localhost --port 7687
    python3 load_ldbc.py --scale-factor sf10 --host localhost --port 7687 --user neo4j --password neo4j
    python3 load_ldbc.py --scale-factor sf1 --create-indexes-only  # just create indexes via CLI
"""

import argparse
import csv
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, SessionExpired, TransientError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

NODE_BATCH_SIZE = 3000
REL_BATCH_SIZE = 1000   # smaller for relationship ops (they do MATCH lookups)
LOG_EVERY = 50_000

BASE_DIR_TEMPLATE = (
    "/opt/ldbc-snb/{sf}/"
    "social_network-{sf}-CsvCompositeMergeForeign-LongDateFormatter"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ldbc_loader")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Load LDBC SNB data into xrayGraphDB")
    p.add_argument(
        "--scale-factor", "-sf",
        choices=["sf1", "sf10"],
        default="sf1",
        help="Scale factor dataset to load (default: sf1)",
    )
    p.add_argument("--host", default="localhost", help="xrayGraphDB host")
    p.add_argument("--port", type=int, default=7687, help="Bolt port")
    p.add_argument("--user", default="xrayadmin", help="Database user")
    p.add_argument("--password", default="xrayadmin", help="Database password")
    p.add_argument("--node-batch-size", type=int, default=NODE_BATCH_SIZE,
                   help="UNWIND batch size for node creation")
    p.add_argument("--rel-batch-size", type=int, default=REL_BATCH_SIZE,
                   help="UNWIND batch size for relationship creation")
    p.add_argument("--drop-existing", action="store_true",
                   help="Drop all existing data first")
    p.add_argument("--create-indexes-only", action="store_true",
                   help="Only create indexes (via mgconsole CLI), then exit")
    p.add_argument("--skip-indexes", action="store_true",
                   help="Skip index creation (use if indexes already exist)")
    return p.parse_args()


def read_csv(filepath):
    """Yield dicts from a pipe-delimited CSV with header row."""
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="|")
        headers = next(reader)
        for row in reader:
            if len(row) < len(headers):
                row.extend([""] * (len(headers) - len(row)))
            yield dict(zip(headers, row))


def safe_int(val, default=None):
    """Convert a string to int, returning default for empty/invalid."""
    if val is None or val == "":
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def safe_str(val, default=""):
    """Return string or default for None."""
    if val is None:
        return default
    return val


def batched(iterable, n):
    """Yield successive batches of size n from iterable."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == n:
            yield batch
            batch = []
    if batch:
        yield batch


def run_batched(session, cypher, rows_iter, batch_size, label, log_every=LOG_EVERY):
    """Execute a Cypher UNWIND in batches with retry logic. Returns total count."""
    total = 0
    t0 = time.time()
    for batch in batched(rows_iter, batch_size):
        retries = 0
        while retries < 2:
            try:
                session.run(cypher, rows=batch)
                break
            except (ServiceUnavailable, SessionExpired, TransientError) as e:
                retries += 1
                if retries >= 2:
                    log.error("  %s: Failed after 2 retries at row %d: %s", label, total, e)
                    raise
                log.warning("  %s: Transient error at row %d, retry %d: %s",
                            label, total, retries, e)
                time.sleep(2)
        total += len(batch)
        if total % log_every < batch_size:
            elapsed = time.time() - t0
            rate = total / elapsed if elapsed > 0 else 0
            log.info("  %s: %d rows (%.1f rows/s)", label, total, rate)
    elapsed = time.time() - t0
    rate = total / elapsed if elapsed > 0 else 0
    log.info("  %s: DONE -- %d total in %.1fs (%.0f rows/s)", label, total, elapsed, rate)
    return total


# ---------------------------------------------------------------------------
# Schema: indexes
# ---------------------------------------------------------------------------

INDEX_STMTS = [
    "CREATE INDEX ON :Person(id);",
    "CREATE INDEX ON :Comment(id);",
    "CREATE INDEX ON :Post(id);",
    "CREATE INDEX ON :Forum(id);",
    "CREATE INDEX ON :Organisation(id);",
    "CREATE INDEX ON :Place(id);",
    "CREATE INDEX ON :Tag(id);",
    "CREATE INDEX ON :TagClass(id);",
]


def create_indexes_via_bolt(session):
    """Try creating indexes via Bolt protocol (may fail if user lacks DDL privileges)."""
    log.info("Attempting to create indexes via Bolt...")
    success = 0
    for stmt in INDEX_STMTS:
        cypher = stmt.rstrip(";")
        try:
            session.run(cypher)
            log.info("  OK: %s", cypher)
            success += 1
        except Exception as e:
            log.warning("  Failed: %s -> %s", cypher, str(e)[:120])
    return success


def create_indexes_via_cli(host, port):
    """Create indexes using mgconsole CLI (requires local access to xrayGraphDB host)."""
    log.info("Creating indexes via mgconsole CLI...")

    # Find mgconsole binary
    mgconsole = None
    for path in ["/usr/bin/mgconsole", "/usr/local/bin/mgconsole",
                 "/usr/lib/xraygraphdb/mgconsole"]:
        if os.path.isfile(path):
            mgconsole = path
            break
    if mgconsole is None:
        # Try finding it
        try:
            result = subprocess.run(["which", "mgconsole"], capture_output=True, text=True)
            if result.returncode == 0:
                mgconsole = result.stdout.strip()
        except Exception:
            pass

    if mgconsole is None:
        log.warning("mgconsole not found. Trying direct Cypher via echo+pipe...")
        # Fall back to using the xraygraphdb binary itself or netcat
        for stmt in INDEX_STMTS:
            cmd = f'echo "{stmt}" | mgconsole --host {host} --port {port}'
            try:
                r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
                if r.returncode == 0:
                    log.info("  OK: %s", stmt)
                else:
                    log.warning("  Failed: %s -> %s", stmt, r.stderr.strip()[:120])
            except Exception as e:
                log.warning("  Error: %s -> %s", stmt, e)
        return

    log.info("Using mgconsole at: %s", mgconsole)
    for stmt in INDEX_STMTS:
        try:
            result = subprocess.run(
                [mgconsole, f"--host={host}", f"--port={port}"],
                input=stmt,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode == 0:
                log.info("  OK: %s", stmt)
            else:
                log.warning("  Failed: %s -> %s", stmt, result.stderr.strip()[:120])
        except Exception as e:
            log.warning("  Error: %s -> %s", stmt, e)


def drop_all(session):
    log.info("Dropping all existing data (batch DETACH DELETE)...")
    t0 = time.time()
    total_del = 0
    empty_rounds = 0
    while empty_rounds < 3:
        result = session.run(
            "MATCH (n) WITH n LIMIT 10000 DETACH DELETE n RETURN count(n) AS cnt"
        )
        cnt = result.single()["cnt"]
        if cnt == 0:
            empty_rounds += 1
            continue
        empty_rounds = 0
        total_del += cnt
        if total_del % 50000 < 10000:
            log.info("  Deleted so far: %d (%.1fs)", total_del, time.time() - t0)
    log.info("  Drop complete: %d nodes removed in %.1fs", total_del, time.time() - t0)


# ---------------------------------------------------------------------------
# Node loaders
# ---------------------------------------------------------------------------

def load_places(session, base_dir, batch_size):
    """Load Place nodes (static). isPartOf FK handled as relationship later."""
    log.info("Loading Place nodes...")
    filepath = os.path.join(base_dir, "static", "place_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            yield {
                "id": int(r["id"]),
                "name": r["name"],
                "url": r["url"],
                "type": r["type"],
            }

    cypher = """
    UNWIND $rows AS r
    CREATE (p:Place {
        id: r.id, name: r.name, url: r.url, type: r.type
    })
    """
    return run_batched(session, cypher, rows(), batch_size, "Place")


def load_place_is_part_of(session, base_dir, batch_size):
    """Load IS_PART_OF relationships between Place nodes."""
    log.info("Loading Place IS_PART_OF relationships...")
    filepath = os.path.join(base_dir, "static", "place_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            parent = safe_int(r.get("isPartOf"))
            if parent is not None:
                yield {"child_id": int(r["id"]), "parent_id": parent}

    cypher = """
    UNWIND $rows AS r
    MATCH (c:Place {id: r.child_id})
    MATCH (p:Place {id: r.parent_id})
    CREATE (c)-[:IS_PART_OF]->(p)
    """
    return run_batched(session, cypher, rows(), batch_size, "Place-IS_PART_OF")


def load_tag_classes(session, base_dir, batch_size):
    """Load TagClass nodes (static)."""
    log.info("Loading TagClass nodes...")
    filepath = os.path.join(base_dir, "static", "tagclass_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            yield {
                "id": int(r["id"]),
                "name": r["name"],
                "url": r["url"],
            }

    cypher = """
    UNWIND $rows AS r
    CREATE (tc:TagClass {id: r.id, name: r.name, url: r.url})
    """
    return run_batched(session, cypher, rows(), batch_size, "TagClass")


def load_tagclass_is_subclass_of(session, base_dir, batch_size):
    """Load IS_SUBCLASS_OF relationships between TagClass nodes."""
    log.info("Loading TagClass IS_SUBCLASS_OF relationships...")
    filepath = os.path.join(base_dir, "static", "tagclass_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            parent = safe_int(r.get("isSubclassOf"))
            if parent is not None:
                yield {"child_id": int(r["id"]), "parent_id": parent}

    cypher = """
    UNWIND $rows AS r
    MATCH (c:TagClass {id: r.child_id})
    MATCH (p:TagClass {id: r.parent_id})
    CREATE (c)-[:IS_SUBCLASS_OF]->(p)
    """
    return run_batched(session, cypher, rows(), batch_size, "TagClass-IS_SUBCLASS_OF")


def load_tags(session, base_dir, batch_size):
    """Load Tag nodes (static)."""
    log.info("Loading Tag nodes...")
    filepath = os.path.join(base_dir, "static", "tag_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            yield {
                "id": int(r["id"]),
                "name": r["name"],
                "url": r["url"],
            }

    cypher = """
    UNWIND $rows AS r
    CREATE (t:Tag {id: r.id, name: r.name, url: r.url})
    """
    return run_batched(session, cypher, rows(), batch_size, "Tag")


def load_tag_has_type(session, base_dir, batch_size):
    """Load HAS_TYPE relationships from Tag to TagClass."""
    log.info("Loading Tag HAS_TYPE relationships...")
    filepath = os.path.join(base_dir, "static", "tag_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            tc = safe_int(r.get("hasType"))
            if tc is not None:
                yield {"tag_id": int(r["id"]), "tc_id": tc}

    cypher = """
    UNWIND $rows AS r
    MATCH (t:Tag {id: r.tag_id})
    MATCH (tc:TagClass {id: r.tc_id})
    CREATE (t)-[:HAS_TYPE]->(tc)
    """
    return run_batched(session, cypher, rows(), batch_size, "Tag-HAS_TYPE")


def load_organisations(session, base_dir, batch_size):
    """Load Organisation nodes (static)."""
    log.info("Loading Organisation nodes...")
    filepath = os.path.join(base_dir, "static", "organisation_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            yield {
                "id": int(r["id"]),
                "type": r["type"],
                "name": r["name"],
                "url": r["url"],
            }

    cypher = """
    UNWIND $rows AS r
    CREATE (o:Organisation {id: r.id, type: r.type, name: r.name, url: r.url})
    """
    return run_batched(session, cypher, rows(), batch_size, "Organisation")


def load_organisation_is_located_in(session, base_dir, batch_size):
    """Load IS_LOCATED_IN from Organisation to Place."""
    log.info("Loading Organisation IS_LOCATED_IN relationships...")
    filepath = os.path.join(base_dir, "static", "organisation_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            place = safe_int(r.get("place"))
            if place is not None:
                yield {"org_id": int(r["id"]), "place_id": place}

    cypher = """
    UNWIND $rows AS r
    MATCH (o:Organisation {id: r.org_id})
    MATCH (p:Place {id: r.place_id})
    CREATE (o)-[:IS_LOCATED_IN]->(p)
    """
    return run_batched(session, cypher, rows(), batch_size, "Org-IS_LOCATED_IN")


def load_persons(session, base_dir, batch_size):
    """Load Person nodes (dynamic)."""
    log.info("Loading Person nodes...")
    filepath = os.path.join(base_dir, "dynamic", "person_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            langs = [l.strip() for l in r.get("language", "").split(";") if l.strip()]
            emails = [e.strip() for e in r.get("email", "").split(";") if e.strip()]
            yield {
                "id": int(r["id"]),
                "firstName": r["firstName"],
                "lastName": r["lastName"],
                "gender": r["gender"],
                "birthday": safe_int(r.get("birthday")),
                "creationDate": safe_int(r.get("creationDate")),
                "locationIP": r.get("locationIP", ""),
                "browserUsed": r.get("browserUsed", ""),
                "languages": langs,
                "emails": emails,
            }

    cypher = """
    UNWIND $rows AS r
    CREATE (p:Person {
        id: r.id, firstName: r.firstName, lastName: r.lastName,
        gender: r.gender, birthday: r.birthday, creationDate: r.creationDate,
        locationIP: r.locationIP, browserUsed: r.browserUsed,
        languages: r.languages, emails: r.emails
    })
    """
    return run_batched(session, cypher, rows(), batch_size, "Person")


def load_person_is_located_in(session, base_dir, batch_size):
    """Load IS_LOCATED_IN from Person to Place."""
    log.info("Loading Person IS_LOCATED_IN relationships...")
    filepath = os.path.join(base_dir, "dynamic", "person_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            place = safe_int(r.get("place"))
            if place is not None:
                yield {"person_id": int(r["id"]), "place_id": place}

    cypher = """
    UNWIND $rows AS r
    MATCH (p:Person {id: r.person_id})
    MATCH (pl:Place {id: r.place_id})
    CREATE (p)-[:IS_LOCATED_IN]->(pl)
    """
    return run_batched(session, cypher, rows(), batch_size, "Person-IS_LOCATED_IN")


def load_forums(session, base_dir, batch_size):
    """Load Forum nodes (dynamic)."""
    log.info("Loading Forum nodes...")
    filepath = os.path.join(base_dir, "dynamic", "forum_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            yield {
                "id": int(r["id"]),
                "title": r["title"],
                "creationDate": safe_int(r.get("creationDate")),
            }

    cypher = """
    UNWIND $rows AS r
    CREATE (f:Forum {id: r.id, title: r.title, creationDate: r.creationDate})
    """
    return run_batched(session, cypher, rows(), batch_size, "Forum")


def load_forum_has_moderator(session, base_dir, batch_size):
    """Load HAS_MODERATOR from Forum to Person."""
    log.info("Loading Forum HAS_MODERATOR relationships...")
    filepath = os.path.join(base_dir, "dynamic", "forum_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            mod = safe_int(r.get("moderator"))
            if mod is not None:
                yield {"forum_id": int(r["id"]), "person_id": mod}

    cypher = """
    UNWIND $rows AS r
    MATCH (f:Forum {id: r.forum_id})
    MATCH (p:Person {id: r.person_id})
    CREATE (f)-[:HAS_MODERATOR]->(p)
    """
    return run_batched(session, cypher, rows(), batch_size, "Forum-HAS_MODERATOR")


def load_posts(session, base_dir, batch_size):
    """Load Post nodes (dynamic)."""
    log.info("Loading Post nodes...")
    filepath = os.path.join(base_dir, "dynamic", "post_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            yield {
                "id": int(r["id"]),
                "imageFile": safe_str(r.get("imageFile")),
                "creationDate": safe_int(r.get("creationDate")),
                "locationIP": safe_str(r.get("locationIP")),
                "browserUsed": safe_str(r.get("browserUsed")),
                "language": safe_str(r.get("language")),
                "content": safe_str(r.get("content")),
                "length": safe_int(r.get("length"), 0),
            }

    cypher = """
    UNWIND $rows AS r
    CREATE (p:Post {
        id: r.id, imageFile: r.imageFile, creationDate: r.creationDate,
        locationIP: r.locationIP, browserUsed: r.browserUsed,
        language: r.language, content: r.content, length: r.length
    })
    """
    return run_batched(session, cypher, rows(), batch_size, "Post")


def load_post_relationships(session, base_dir, batch_size):
    """Load HAS_CREATOR, CONTAINER_OF, IS_LOCATED_IN for Posts."""
    filepath = os.path.join(base_dir, "dynamic", "post_0_0.csv")

    # HAS_CREATOR (Post -> Person)
    log.info("Loading Post HAS_CREATOR relationships...")
    def creator_rows():
        for r in read_csv(filepath):
            creator = safe_int(r.get("creator"))
            if creator is not None:
                yield {"post_id": int(r["id"]), "person_id": creator}

    cypher_creator = """
    UNWIND $rows AS r
    MATCH (post:Post {id: r.post_id})
    MATCH (p:Person {id: r.person_id})
    CREATE (post)-[:HAS_CREATOR]->(p)
    """
    c1 = run_batched(session, cypher_creator, creator_rows(), batch_size, "Post-HAS_CREATOR")

    # CONTAINER_OF (Forum -> Post)
    log.info("Loading Forum CONTAINER_OF Post relationships...")
    def container_rows():
        for r in read_csv(filepath):
            forum = safe_int(r.get("Forum.id"))
            if forum is not None:
                yield {"post_id": int(r["id"]), "forum_id": forum}

    cypher_container = """
    UNWIND $rows AS r
    MATCH (f:Forum {id: r.forum_id})
    MATCH (post:Post {id: r.post_id})
    CREATE (f)-[:CONTAINER_OF]->(post)
    """
    c2 = run_batched(session, cypher_container, container_rows(), batch_size, "Forum-CONTAINER_OF")

    # IS_LOCATED_IN (Post -> Place)
    log.info("Loading Post IS_LOCATED_IN relationships...")
    def place_rows():
        for r in read_csv(filepath):
            place = safe_int(r.get("place"))
            if place is not None:
                yield {"post_id": int(r["id"]), "place_id": place}

    cypher_place = """
    UNWIND $rows AS r
    MATCH (post:Post {id: r.post_id})
    MATCH (pl:Place {id: r.place_id})
    CREATE (post)-[:IS_LOCATED_IN]->(pl)
    """
    c3 = run_batched(session, cypher_place, place_rows(), batch_size, "Post-IS_LOCATED_IN")
    return c1 + c2 + c3


def load_comments(session, base_dir, batch_size):
    """Load Comment nodes (dynamic)."""
    log.info("Loading Comment nodes...")
    filepath = os.path.join(base_dir, "dynamic", "comment_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            yield {
                "id": int(r["id"]),
                "creationDate": safe_int(r.get("creationDate")),
                "locationIP": safe_str(r.get("locationIP")),
                "browserUsed": safe_str(r.get("browserUsed")),
                "content": safe_str(r.get("content")),
                "length": safe_int(r.get("length"), 0),
            }

    cypher = """
    UNWIND $rows AS r
    CREATE (c:Comment {
        id: r.id, creationDate: r.creationDate,
        locationIP: r.locationIP, browserUsed: r.browserUsed,
        content: r.content, length: r.length
    })
    """
    return run_batched(session, cypher, rows(), batch_size, "Comment")


def load_comment_relationships(session, base_dir, batch_size):
    """Load HAS_CREATOR, IS_LOCATED_IN, REPLY_OF for Comments."""
    filepath = os.path.join(base_dir, "dynamic", "comment_0_0.csv")

    # HAS_CREATOR (Comment -> Person)
    log.info("Loading Comment HAS_CREATOR relationships...")
    def creator_rows():
        for r in read_csv(filepath):
            creator = safe_int(r.get("creator"))
            if creator is not None:
                yield {"comment_id": int(r["id"]), "person_id": creator}

    cypher_creator = """
    UNWIND $rows AS r
    MATCH (c:Comment {id: r.comment_id})
    MATCH (p:Person {id: r.person_id})
    CREATE (c)-[:HAS_CREATOR]->(p)
    """
    c1 = run_batched(session, cypher_creator, creator_rows(), batch_size, "Comment-HAS_CREATOR")

    # IS_LOCATED_IN (Comment -> Place)
    log.info("Loading Comment IS_LOCATED_IN relationships...")
    def place_rows():
        for r in read_csv(filepath):
            place = safe_int(r.get("place"))
            if place is not None:
                yield {"comment_id": int(r["id"]), "place_id": place}

    cypher_place = """
    UNWIND $rows AS r
    MATCH (c:Comment {id: r.comment_id})
    MATCH (pl:Place {id: r.place_id})
    CREATE (c)-[:IS_LOCATED_IN]->(pl)
    """
    c2 = run_batched(session, cypher_place, place_rows(), batch_size, "Comment-IS_LOCATED_IN")

    # REPLY_OF to Post
    log.info("Loading Comment REPLY_OF Post relationships...")
    def reply_post_rows():
        for r in read_csv(filepath):
            post = safe_int(r.get("replyOfPost"))
            if post is not None:
                yield {"comment_id": int(r["id"]), "post_id": post}

    cypher_reply_post = """
    UNWIND $rows AS r
    MATCH (c:Comment {id: r.comment_id})
    MATCH (p:Post {id: r.post_id})
    CREATE (c)-[:REPLY_OF]->(p)
    """
    c3 = run_batched(session, cypher_reply_post, reply_post_rows(), batch_size, "Comment-REPLY_OF-Post")

    # REPLY_OF to Comment
    log.info("Loading Comment REPLY_OF Comment relationships...")
    def reply_comment_rows():
        for r in read_csv(filepath):
            parent = safe_int(r.get("replyOfComment"))
            if parent is not None:
                yield {"comment_id": int(r["id"]), "parent_id": parent}

    cypher_reply_comment = """
    UNWIND $rows AS r
    MATCH (c:Comment {id: r.comment_id})
    MATCH (p:Comment {id: r.parent_id})
    CREATE (c)-[:REPLY_OF]->(p)
    """
    c4 = run_batched(session, cypher_reply_comment, reply_comment_rows(), batch_size, "Comment-REPLY_OF-Comment")
    return c1 + c2 + c3 + c4


# ---------------------------------------------------------------------------
# Edge-only relationship loaders (from separate CSV files)
# ---------------------------------------------------------------------------

def load_person_knows_person(session, base_dir, batch_size):
    """Load KNOWS relationships."""
    log.info("Loading KNOWS relationships...")
    filepath = os.path.join(base_dir, "dynamic", "person_knows_person_0_0.csv")

    # This file has duplicate header "Person.id|Person.id|creationDate"
    # so we use positional reading instead of dict-based
    def rows():
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter="|")
            next(reader)  # skip header
            for row in reader:
                if len(row) >= 3:
                    yield {
                        "person1": int(row[0]),
                        "person2": int(row[1]),
                        "creationDate": safe_int(row[2]),
                    }

    cypher = """
    UNWIND $rows AS r
    MATCH (a:Person {id: r.person1})
    MATCH (b:Person {id: r.person2})
    CREATE (a)-[:KNOWS {creationDate: r.creationDate}]->(b)
    """
    return run_batched(session, cypher, rows(), batch_size, "KNOWS")


def load_person_likes_post(session, base_dir, batch_size):
    """Load LIKES relationships (Person -> Post)."""
    log.info("Loading Person LIKES Post relationships...")
    filepath = os.path.join(base_dir, "dynamic", "person_likes_post_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            yield {
                "person_id": int(r["Person.id"]),
                "post_id": int(r["Post.id"]),
                "creationDate": safe_int(r.get("creationDate")),
            }

    cypher = """
    UNWIND $rows AS r
    MATCH (p:Person {id: r.person_id})
    MATCH (post:Post {id: r.post_id})
    CREATE (p)-[:LIKES {creationDate: r.creationDate}]->(post)
    """
    return run_batched(session, cypher, rows(), batch_size, "LIKES-Post")


def load_person_likes_comment(session, base_dir, batch_size):
    """Load LIKES relationships (Person -> Comment)."""
    log.info("Loading Person LIKES Comment relationships...")
    filepath = os.path.join(base_dir, "dynamic", "person_likes_comment_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            yield {
                "person_id": int(r["Person.id"]),
                "comment_id": int(r["Comment.id"]),
                "creationDate": safe_int(r.get("creationDate")),
            }

    cypher = """
    UNWIND $rows AS r
    MATCH (p:Person {id: r.person_id})
    MATCH (c:Comment {id: r.comment_id})
    CREATE (p)-[:LIKES {creationDate: r.creationDate}]->(c)
    """
    return run_batched(session, cypher, rows(), batch_size, "LIKES-Comment")


def load_person_study_at(session, base_dir, batch_size):
    """Load STUDY_AT relationships."""
    log.info("Loading STUDY_AT relationships...")
    filepath = os.path.join(base_dir, "dynamic", "person_studyAt_organisation_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            yield {
                "person_id": int(r["Person.id"]),
                "org_id": int(r["Organisation.id"]),
                "classYear": safe_int(r.get("classYear")),
            }

    cypher = """
    UNWIND $rows AS r
    MATCH (p:Person {id: r.person_id})
    MATCH (o:Organisation {id: r.org_id})
    CREATE (p)-[:STUDY_AT {classYear: r.classYear}]->(o)
    """
    return run_batched(session, cypher, rows(), batch_size, "STUDY_AT")


def load_person_work_at(session, base_dir, batch_size):
    """Load WORK_AT relationships."""
    log.info("Loading WORK_AT relationships...")
    filepath = os.path.join(base_dir, "dynamic", "person_workAt_organisation_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            yield {
                "person_id": int(r["Person.id"]),
                "org_id": int(r["Organisation.id"]),
                "workFrom": safe_int(r.get("workFrom")),
            }

    cypher = """
    UNWIND $rows AS r
    MATCH (p:Person {id: r.person_id})
    MATCH (o:Organisation {id: r.org_id})
    CREATE (p)-[:WORK_AT {workFrom: r.workFrom}]->(o)
    """
    return run_batched(session, cypher, rows(), batch_size, "WORK_AT")


def load_person_has_interest(session, base_dir, batch_size):
    """Load HAS_INTEREST relationships (Person -> Tag)."""
    log.info("Loading HAS_INTEREST relationships...")
    filepath = os.path.join(base_dir, "dynamic", "person_hasInterest_tag_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            yield {
                "person_id": int(r["Person.id"]),
                "tag_id": int(r["Tag.id"]),
            }

    cypher = """
    UNWIND $rows AS r
    MATCH (p:Person {id: r.person_id})
    MATCH (t:Tag {id: r.tag_id})
    CREATE (p)-[:HAS_INTEREST]->(t)
    """
    return run_batched(session, cypher, rows(), batch_size, "HAS_INTEREST")


def load_forum_has_member(session, base_dir, batch_size):
    """Load HAS_MEMBER relationships (Forum -> Person)."""
    log.info("Loading HAS_MEMBER relationships...")
    filepath = os.path.join(base_dir, "dynamic", "forum_hasMember_person_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            yield {
                "forum_id": int(r["Forum.id"]),
                "person_id": int(r["Person.id"]),
                "joinDate": safe_int(r.get("joinDate")),
            }

    cypher = """
    UNWIND $rows AS r
    MATCH (f:Forum {id: r.forum_id})
    MATCH (p:Person {id: r.person_id})
    CREATE (f)-[:HAS_MEMBER {joinDate: r.joinDate}]->(p)
    """
    return run_batched(session, cypher, rows(), batch_size, "HAS_MEMBER")


def load_forum_has_tag(session, base_dir, batch_size):
    """Load HAS_TAG relationships (Forum -> Tag)."""
    log.info("Loading Forum HAS_TAG relationships...")
    filepath = os.path.join(base_dir, "dynamic", "forum_hasTag_tag_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            yield {
                "forum_id": int(r["Forum.id"]),
                "tag_id": int(r["Tag.id"]),
            }

    cypher = """
    UNWIND $rows AS r
    MATCH (f:Forum {id: r.forum_id})
    MATCH (t:Tag {id: r.tag_id})
    CREATE (f)-[:HAS_TAG]->(t)
    """
    return run_batched(session, cypher, rows(), batch_size, "Forum-HAS_TAG")


def load_comment_has_tag(session, base_dir, batch_size):
    """Load HAS_TAG relationships (Comment -> Tag)."""
    log.info("Loading Comment HAS_TAG relationships...")
    filepath = os.path.join(base_dir, "dynamic", "comment_hasTag_tag_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            yield {
                "comment_id": int(r["Comment.id"]),
                "tag_id": int(r["Tag.id"]),
            }

    cypher = """
    UNWIND $rows AS r
    MATCH (c:Comment {id: r.comment_id})
    MATCH (t:Tag {id: r.tag_id})
    CREATE (c)-[:HAS_TAG]->(t)
    """
    return run_batched(session, cypher, rows(), batch_size, "Comment-HAS_TAG")


def load_post_has_tag(session, base_dir, batch_size):
    """Load HAS_TAG relationships (Post -> Tag)."""
    log.info("Loading Post HAS_TAG relationships...")
    filepath = os.path.join(base_dir, "dynamic", "post_hasTag_tag_0_0.csv")

    def rows():
        for r in read_csv(filepath):
            yield {
                "post_id": int(r["Post.id"]),
                "tag_id": int(r["Tag.id"]),
            }

    cypher = """
    UNWIND $rows AS r
    MATCH (post:Post {id: r.post_id})
    MATCH (t:Tag {id: r.tag_id})
    CREATE (post)-[:HAS_TAG]->(t)
    """
    return run_batched(session, cypher, rows(), batch_size, "Post-HAS_TAG")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    sf = args.scale_factor
    base_dir = BASE_DIR_TEMPLATE.format(sf=sf)
    bolt_uri = f"bolt://{args.host}:{args.port}"

    log.info("=" * 70)
    log.info("LDBC SNB Loader for xrayGraphDB")
    log.info("  Scale factor      : %s", sf)
    log.info("  Data dir          : %s", base_dir)
    log.info("  Bolt URI          : %s", bolt_uri)
    log.info("  Node batch size   : %d", args.node_batch_size)
    log.info("  Rel batch size    : %d", args.rel_batch_size)
    log.info("=" * 70)

    # Handle --create-indexes-only mode
    if args.create_indexes_only:
        create_indexes_via_cli(args.host, args.port)
        # Also try via Bolt in case CLI fails
        driver = GraphDatabase.driver(bolt_uri, auth=(args.user, args.password))
        with driver.session() as session:
            create_indexes_via_bolt(session)
        driver.close()
        return

    # Validate data directory
    if not os.path.isdir(base_dir):
        log.error("Data directory not found: %s", base_dir)
        sys.exit(1)

    driver = GraphDatabase.driver(bolt_uri, auth=(args.user, args.password))

    # Verify connectivity
    try:
        with driver.session() as s:
            s.run("RETURN 1").single()
        log.info("Connected to xrayGraphDB successfully.")
    except Exception as e:
        log.error("Cannot connect to xrayGraphDB at %s: %s", bolt_uri, e)
        sys.exit(1)

    t_start = time.time()
    total_nodes = 0
    total_rels = 0

    with driver.session() as session:
        # Optionally drop existing data
        if args.drop_existing:
            drop_all(session)

        # Create indexes BEFORE loading data (may fail if user lacks DDL)
        if not args.skip_indexes:
            idx_count = create_indexes_via_bolt(session)
            if idx_count == 0:
                log.warning("No indexes created via Bolt. Trying CLI fallback...")
                create_indexes_via_cli(args.host, args.port)

        # ---- PHASE 1: Static nodes ----
        log.info("")
        log.info("=" * 40 + " PHASE 1: Static Nodes " + "=" * 40)
        total_nodes += load_places(session, base_dir, args.node_batch_size)
        total_nodes += load_tag_classes(session, base_dir, args.node_batch_size)
        total_nodes += load_tags(session, base_dir, args.node_batch_size)
        total_nodes += load_organisations(session, base_dir, args.node_batch_size)

        # Static relationships (embedded FKs)
        log.info("")
        log.info("=" * 40 + " PHASE 2: Static Rels " + "=" * 40)
        total_rels += load_place_is_part_of(session, base_dir, args.rel_batch_size)
        total_rels += load_tagclass_is_subclass_of(session, base_dir, args.rel_batch_size)
        total_rels += load_tag_has_type(session, base_dir, args.rel_batch_size)
        total_rels += load_organisation_is_located_in(session, base_dir, args.rel_batch_size)

        # ---- PHASE 3: Dynamic nodes ----
        log.info("")
        log.info("=" * 40 + " PHASE 3: Dynamic Nodes " + "=" * 40)
        total_nodes += load_persons(session, base_dir, args.node_batch_size)
        total_rels += load_person_is_located_in(session, base_dir, args.rel_batch_size)
        total_nodes += load_forums(session, base_dir, args.node_batch_size)
        total_rels += load_forum_has_moderator(session, base_dir, args.rel_batch_size)
        total_nodes += load_posts(session, base_dir, args.node_batch_size)
        total_rels += load_post_relationships(session, base_dir, args.rel_batch_size)
        total_nodes += load_comments(session, base_dir, args.node_batch_size)
        total_rels += load_comment_relationships(session, base_dir, args.rel_batch_size)

        # ---- PHASE 4: Edge-only relationship files ----
        log.info("")
        log.info("=" * 40 + " PHASE 4: Relationship Files " + "=" * 40)
        total_rels += load_person_knows_person(session, base_dir, args.rel_batch_size)
        total_rels += load_person_has_interest(session, base_dir, args.rel_batch_size)
        total_rels += load_person_study_at(session, base_dir, args.rel_batch_size)
        total_rels += load_person_work_at(session, base_dir, args.rel_batch_size)
        total_rels += load_person_likes_post(session, base_dir, args.rel_batch_size)
        total_rels += load_person_likes_comment(session, base_dir, args.rel_batch_size)
        total_rels += load_forum_has_member(session, base_dir, args.rel_batch_size)
        total_rels += load_forum_has_tag(session, base_dir, args.rel_batch_size)
        total_rels += load_comment_has_tag(session, base_dir, args.rel_batch_size)
        total_rels += load_post_has_tag(session, base_dir, args.rel_batch_size)

    # ---- Verification ----
    log.info("")
    log.info("=" * 40 + " VERIFICATION " + "=" * 40)
    with driver.session() as session:
        # Count nodes by label
        node_labels = ["Person", "Comment", "Post", "Forum",
                       "Organisation", "Place", "Tag", "TagClass"]
        db_node_total = 0
        for label in node_labels:
            r = session.run(f"MATCH (n:{label}) RETURN count(n) AS cnt")
            cnt = r.single()["cnt"]
            db_node_total += cnt
            log.info("  :%s nodes: %d", label, cnt)

        # Count relationship types
        all_rel_types = ["KNOWS", "LIKES", "HAS_TAG", "HAS_MEMBER",
                         "HAS_CREATOR", "REPLY_OF", "CONTAINER_OF",
                         "IS_LOCATED_IN", "IS_PART_OF", "STUDY_AT",
                         "WORK_AT", "HAS_MODERATOR", "HAS_INTEREST",
                         "HAS_TYPE", "IS_SUBCLASS_OF"]
        db_rel_total = 0
        for rt in all_rel_types:
            r = session.run(f"MATCH ()-[r:{rt}]->() RETURN count(r) AS cnt")
            cnt = r.single()["cnt"]
            db_rel_total += cnt
            log.info("  :%s rels: %d", rt, cnt)

    driver.close()

    t_total = time.time() - t_start
    log.info("")
    log.info("=" * 70)
    log.info("LOAD COMPLETE")
    log.info("  Nodes loaded (from CSV)    : %d", total_nodes)
    log.info("  Rels loaded (from CSV)     : %d", total_rels)
    log.info("  DB node count (verified)   : %d", db_node_total)
    log.info("  DB rel count (verified)    : %d", db_rel_total)
    log.info("  Total time                 : %.1fs (%.1f min)", t_total, t_total / 60)
    log.info("  Overall rate               : %.0f records/s",
             (total_nodes + total_rels) / t_total if t_total > 0 else 0)
    log.info("=" * 70)


if __name__ == "__main__":
    main()
