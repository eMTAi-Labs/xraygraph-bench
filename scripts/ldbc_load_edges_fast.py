#!/usr/bin/env python3
"""Fast LDBC edge loading via bulk_insert_edges_gid.

Nodes must already be loaded. This script:
1. Builds id->GID mapping by querying all nodes
2. Loads edges via bulk_insert_edges_gid (724K/s vs 1.4K/s Cypher MATCH)
"""
import csv, time, sys, os
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

BASE = "/neo4j/datasets_ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter"
DYNAMIC = os.path.join(BASE, "dynamic")

def load_csv(path):
    with open(path, "r") as f:
        reader = csv.DictReader(f, delimiter="|")
        return list(reader), reader.fieldnames

def main():
    c = XrayProtocolClient(host="127.0.0.1", port=7689,
                           auth_token="bench:Bench2026!xray",
                           database="bench", read_timeout=600)
    print(f"Connected: {c.connected}")
    t_start = time.time()

    # Step 1: Build id -> GID mapping for each label
    print("\n=== Building ID -> GID mappings ===")
    gid_maps = {}
    for label in ["Person", "Comment", "Post", "Forum", "Organisation", "Place", "Tag", "TagClass"]:
        s = time.time()
        cols, rows = c.execute(f"MATCH (n:{label}) RETURN n.id, id(n)")
        mapping = {}
        for row in rows:
            mapping[int(row[0])] = int(row[1])
        gid_maps[label] = mapping
        print(f"  {label:<15} {len(mapping):>10,} nodes  ({time.time()-s:.1f}s)")

    # Step 2: Load edges via GID fast path
    print("\n=== Loading edges via bulk_insert_edges_gid ===")
    total_edges = 0

    edge_files = [
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
    ]

    for folder, filename, src_label, rel_type, dst_label in edge_files:
        path = os.path.join(folder, filename)
        rows, fields = load_csv(path)
        src_col = fields[0]
        dst_col = fields[1]
        src_map = gid_maps[src_label]
        dst_map = gid_maps[dst_label]

        src_gids = []
        dst_gids = []
        skipped = 0
        for row in rows:
            src_raw = row[src_col].split("|")[0] if "|" in row[src_col] else row[src_col]
            dst_raw = row[dst_col].split("|")[0] if "|" in row[dst_col] else row[dst_col]
            try:
                src_id = int(src_raw)
                dst_id = int(dst_raw)
            except ValueError:
                skipped += 1
                continue
            if src_id in src_map and dst_id in dst_map:
                src_gids.append(src_map[src_id])
                dst_gids.append(dst_map[dst_id])
            else:
                skipped += 1

        # Insert in batches
        batch_size = 50000
        count = 0
        s = time.time()
        for i in range(0, len(src_gids), batch_size):
            batch_src = src_gids[i:i+batch_size]
            batch_dst = dst_gids[i:i+batch_size]
            n = c.bulk_insert_edges_gid(rel_type, batch_src, batch_dst)
            count += n

        elapsed = time.time() - s
        total_edges += count
        rate = count / max(elapsed, 0.001)
        skip_str = f" (skipped {skipped})" if skipped else ""
        print(f"  {rel_type:<15} {count:>10,} edges  {elapsed:.1f}s  ({rate:,.0f}/s){skip_str}")

    # === VERIFICATION ===
    print(f"\n=== VERIFICATION ===")
    cols, rows = c.execute("MATCH (n) RETURN count(n)")
    print(f"  Total nodes: {rows[0][0]}")
    cols, rows = c.execute("MATCH ()-[r]->() RETURN count(r)")
    print(f"  Total edges: {rows[0][0]}")

    c.close()
    total_time = time.time() - t_start
    print(f"\n=== COMPLETE: {total_edges:,} edges in {total_time:.1f}s ({total_time/60:.1f} min) ===")

if __name__ == "__main__":
    main()
