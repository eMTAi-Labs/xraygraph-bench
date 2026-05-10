#!/usr/bin/env python3
"""Load LDBC SNB data into xrayGraphDB via xrayProtocol BULK_UPSERT/INSERT.

136K edges/sec vs 800/s Cypher UNWIND. Cuts load from hours to minutes.

Usage:
    python3 ldbc_load_bulk.py --sf sf1 [--host localhost] [--port 7689]
"""
import argparse
import csv
import time
import sys

sys.path.insert(0, "/opt/xraygraph-bench")
from tools.xraybench.adapters.xray_protocol import XrayProtocolClient


def load_csv(path, delimiter="|"):
    with open(path, "r") as f:
        return list(csv.DictReader(f, delimiter=delimiter))


def load_csv_positional(path, delimiter="|"):
    with open(path, "r") as f:
        reader = csv.reader(f, delimiter=delimiter)
        next(reader)
        return [row for row in reader]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sf", required=True, choices=["sf1", "sf10"])
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=7689)
    args = parser.parse_args()

    base = f"/opt/ldbc-snb/{args.sf}/social_network-{args.sf}-CsvCompositeMergeForeign-LongDateFormatter"

    client = XrayProtocolClient(args.host, args.port, timeout=300.0)
    ver, caps, info = client.connect()
    print(f"Connected: {info} (v{ver}, caps={caps:#x})")

    t_start = time.time()

    # Start bulk session
    client.bulk_begin()
    print("Bulk session started")

    # === NODES ===
    batch_size = 3000

    node_files = [
        ("static/place_0_0.csv", "Place", "id",
         lambda r: {"id": int(r["id"]), "name": r["name"], "url": r["url"], "type": r["type"]}),
        ("static/tagclass_0_0.csv", "TagClass", "id",
         lambda r: {"id": int(r["id"]), "name": r["name"], "url": r["url"]}),
        ("static/tag_0_0.csv", "Tag", "id",
         lambda r: {"id": int(r["id"]), "name": r["name"], "url": r["url"]}),
        ("static/organisation_0_0.csv", "Organisation", "id",
         lambda r: {"id": int(r["id"]), "type": r["type"], "name": r["name"], "url": r["url"]}),
        ("dynamic/person_0_0.csv", "Person", "id",
         lambda r: {"id": int(r["id"]), "firstName": r["firstName"], "lastName": r["lastName"],
                     "gender": r["gender"], "birthday": int(r["birthday"]),
                     "creationDate": int(r["creationDate"]), "locationIP": r["locationIP"],
                     "browserUsed": r["browserUsed"]}),
        ("dynamic/forum_0_0.csv", "Forum", "id",
         lambda r: {"id": int(r["id"]), "title": r["title"], "creationDate": int(r["creationDate"])}),
        ("dynamic/post_0_0.csv", "Post", "id",
         lambda r: {"id": int(r["id"]), "creationDate": int(r["creationDate"]),
                     "locationIP": r["locationIP"], "browserUsed": r["browserUsed"],
                     "length": int(r["length"]), "creator": int(r["creator"]),
                     "place": int(r["place"])}),
        ("dynamic/comment_0_0.csv", "Comment", "id",
         lambda r: {"id": int(r["id"]), "creationDate": int(r["creationDate"]),
                     "locationIP": r["locationIP"], "browserUsed": r["browserUsed"],
                     "length": int(r["length"]), "creator": int(r["creator"]),
                     "place": int(r["place"])}),
    ]

    for csv_file, label, key, transform in node_files:
        t0 = time.time()
        rows = load_csv(f"{base}/{csv_file}")
        data = [transform(r) for r in rows]
        total = 0
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            n, ms = client.bulk_upsert_nodes(label, key, batch)
            total += n
            if total % 50000 < batch_size and total > batch_size:
                elapsed = time.time() - t0
                print(f"    {label}: {total:>10,} / {len(data):>10,}  ({total/elapsed:,.0f}/s)", flush=True)
        elapsed = time.time() - t0
        rate = len(data) / elapsed if elapsed > 0 else 0
        print(f"  OK  {label:20s} {len(data):>10,} rows  {elapsed:7.1f}s  ({rate:,.0f}/s)", flush=True)

    # === EDGES from CSV files ===
    print("\n=== RELATIONSHIPS ===")

    edge_files = [
        ("dynamic/person_knows_person_0_0.csv", True,  # positional
         lambda r: {"from": r[0], "to": r[1], "type": "KNOWS", "creationDate": r[2]}),
        ("dynamic/person_hasInterest_tag_0_0.csv", False,
         lambda r: {"from": r["Person.id"], "to": r["Tag.id"], "type": "HAS_INTEREST"}),
        ("dynamic/person_studyAt_organisation_0_0.csv", False,
         lambda r: {"from": r["Person.id"], "to": r["Organisation.id"], "type": "STUDY_AT",
                     "classYear": r["classYear"]}),
        ("dynamic/person_workAt_organisation_0_0.csv", False,
         lambda r: {"from": r["Person.id"], "to": r["Organisation.id"], "type": "WORK_AT",
                     "workFrom": r["workFrom"]}),
        ("dynamic/person_likes_post_0_0.csv", False,
         lambda r: {"from": r["Person.id"], "to": r["Post.id"], "type": "LIKES",
                     "creationDate": r["creationDate"]}),
        ("dynamic/person_likes_comment_0_0.csv", False,
         lambda r: {"from": r["Person.id"], "to": r["Comment.id"], "type": "LIKES",
                     "creationDate": r["creationDate"]}),
        ("dynamic/forum_hasMember_person_0_0.csv", False,
         lambda r: {"from": r["Forum.id"], "to": r["Person.id"], "type": "HAS_MEMBER",
                     "joinDate": r["joinDate"]}),
        ("dynamic/forum_hasTag_tag_0_0.csv", False,
         lambda r: {"from": r["Forum.id"], "to": r["Tag.id"], "type": "HAS_TAG"}),
        ("dynamic/post_hasTag_tag_0_0.csv", False,
         lambda r: {"from": r["Post.id"], "to": r["Tag.id"], "type": "HAS_TAG"}),
        ("dynamic/comment_hasTag_tag_0_0.csv", False,
         lambda r: {"from": r["Comment.id"], "to": r["Tag.id"], "type": "HAS_TAG"}),
    ]

    edge_batch = 2000
    for csv_file, positional, transform in edge_files:
        t0 = time.time()
        label = csv_file.split("/")[-1].replace("_0_0.csv", "")
        if positional:
            raw = load_csv_positional(f"{base}/{csv_file}")
        else:
            raw = load_csv(f"{base}/{csv_file}")
        data = [transform(r) for r in raw]

        # Detect property names (beyond from/to/type)
        prop_names = [k for k in data[0].keys() if k not in ("from", "to", "type")] if data else []

        total = 0
        for i in range(0, len(data), edge_batch):
            batch = data[i:i + edge_batch]
            e, ms = client.bulk_insert_edges(batch, prop_names)
            total += e
            if total % 50000 < edge_batch and total > edge_batch:
                elapsed = time.time() - t0
                print(f"    {label}: {total:>10,} / {len(data):>10,}  ({total/elapsed:,.0f}/s)", flush=True)
        elapsed = time.time() - t0
        rate = len(data) / elapsed if elapsed > 0 else 0
        print(f"  OK  {label:30s} {len(data):>10,} rows  {elapsed:7.1f}s  ({rate:,.0f}/s)", flush=True)

    # Commit
    print("\n=== COMMIT ===")
    n, e, ms = client.bulk_commit()
    print(f"  Committed: {n} nodes, {e} edges in {ms}ms")

    # Verify via Cypher query
    print("\n=== VERIFICATION ===")
    cols, rows, _ = client.execute("MATCH (n) RETURN labels(n)[0] AS label, count(n) AS c ORDER BY label")
    print("  Nodes:")
    for row in rows:
        print(f"    {row.get('label', '?'):20s} {row.get('c', 0):>10,}")

    cols, rows, _ = client.execute("MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS c ORDER BY type")
    print("  Relationships:")
    for row in rows:
        print(f"    {row.get('type', '?'):20s} {row.get('c', 0):>10,}")

    client.close()
    total_time = time.time() - t_start
    print(f"\n=== COMPLETE in {total_time:.1f}s ({total_time/60:.1f} min) ===")


if __name__ == "__main__":
    main()
