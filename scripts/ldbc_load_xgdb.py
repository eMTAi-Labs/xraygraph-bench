#!/usr/bin/env python3
"""Load LDBC SNB SF1 into xrayGraphDB via Cypher UNWIND batches.

Uses xgdb_connect on port 7689.
"""
import csv, time, sys, os, json
sys.stdout.reconfigure(line_buffering=True)

from xgdb_connect.protocol import XrayProtocolClient

BASE = "/neo4j/datasets_ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter"
DYNAMIC = os.path.join(BASE, "dynamic")
STATIC = os.path.join(BASE, "static")

FK_COLUMNS = {"creator", "place", "replyOfPost", "replyOfComment", "Forum.id", "moderator"}

def load_csv(path):
    with open(path, "r") as f:
        reader = csv.DictReader(f, delimiter="|")
        return list(reader), reader.fieldnames

def escape(v):
    if v is None: return "null"
    v = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{v}'"

def main():
    c = XrayProtocolClient(host="127.0.0.1", port=7689,
                           auth_token="bench:Bench2026!xray",
                           database="bench", read_timeout=600)
    print(f"Connected: {c.connected}")
    t_start = time.time()
    total_nodes = 0
    total_edges = 0

    # === NODES ===
    node_files = [
        (STATIC,  "organisation_0_0.csv", "Organisation"),
        (STATIC,  "place_0_0.csv", "Place"),
        (STATIC,  "tag_0_0.csv", "Tag"),
        (STATIC,  "tagclass_0_0.csv", "TagClass"),
        (DYNAMIC, "person_0_0.csv", "Person"),
        (DYNAMIC, "comment_0_0.csv", "Comment"),
        (DYNAMIC, "post_0_0.csv", "Post"),
        (DYNAMIC, "forum_0_0.csv", "Forum"),
    ]

    for folder, filename, label in node_files:
        path = os.path.join(folder, filename)
        rows, fields = load_csv(path)
        prop_fields = [f for f in fields if f not in FK_COLUMNS and f != "id"]

        batch_size = 2000
        count = 0
        s = time.time()
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            # Build UNWIND list as inline Cypher
            items = []
            for row in batch:
                props = [f"id: {int(row['id'])}"]
                for f in prop_fields:
                    if f in row and row[f]:
                        props.append(f"{f}: {escape(row[f])}")
                items.append("{" + ", ".join(props) + "}")
            unwind_list = "[" + ", ".join(items) + "]"
            q = f"UNWIND {unwind_list} AS p CREATE (n:{label}) SET n = p"
            try:
                c.execute(q)
                count += len(batch)
            except Exception as e:
                print(f"    Batch error at {i}: {str(e)[:100]}")
                # Retry one by one
                for row in batch:
                    try:
                        props = [f"id: {int(row['id'])}"]
                        for f in prop_fields:
                            if f in row and row[f]:
                                props.append(f"{f}: {escape(row[f])}")
                        c.execute(f"CREATE (:{label} {{{', '.join(props)}}})")
                        count += 1
                    except:
                        pass

        elapsed = time.time() - s
        total_nodes += count
        print(f"  {label:<15} {count:>10,} nodes  {elapsed:.1f}s  ({count/max(elapsed,0.1):.0f}/s)")

    # Create indexes for MATCH lookups
    print("\n  Creating indexes...")
    for label in ["Person", "Comment", "Post", "Forum", "Organisation", "Place", "Tag", "TagClass"]:
        try:
            c.execute(f"CREATE INDEX ON :{label}(id)")
        except:
            pass

    # === EDGES ===
    print()
    edge_files = [
        (DYNAMIC, "person_knows_person_0_0.csv", "Person", "KNOWS", "Person"),
        (DYNAMIC, "person_hasInterest_tag_0_0.csv", "Person", "HAS_INTEREST", "Tag"),
        (DYNAMIC, "person_studyAt_organisation_0_0.csv", "Person", "STUDY_AT", "Organisation"),
        (DYNAMIC, "person_workAt_organisation_0_0.csv", "Person", "WORK_AT", "Organisation"),
        (DYNAMIC, "person_likes_comment_0_0.csv", "Person", "LIKES", "Comment"),
        (DYNAMIC, "person_likes_post_0_0.csv", "Person", "LIKES", "Post"),
        (DYNAMIC, "forum_hasMember_person_0_0.csv", "Forum", "HAS_MEMBER", "Person"),
        (DYNAMIC, "forum_hasTag_tag_0_0.csv", "Forum", "HAS_TAG", "Tag"),
        (DYNAMIC, "comment_hasTag_tag_0_0.csv", "Comment", "HAS_TAG", "Tag"),
        (DYNAMIC, "post_hasTag_tag_0_0.csv", "Post", "HAS_TAG", "Tag"),
    ]

    for folder, filename, src_label, rel_type, dst_label in edge_files:
        path = os.path.join(folder, filename)
        rows, fields = load_csv(path)
        src_col = fields[0]
        dst_col = fields[1]
        prop_fields = [f for f in fields[2:] if f and f.strip()]

        batch_size = 1000
        count = 0
        errors = 0
        s = time.time()
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            for row in batch:
                src_raw = row[src_col].split("|")[0] if "|" in row[src_col] else row[src_col]
                dst_raw = row[dst_col].split("|")[0] if "|" in row[dst_col] else row[dst_col]
                try:
                    src_id = int(src_raw)
                    dst_id = int(dst_raw)
                except ValueError:
                    errors += 1
                    continue

                props = ""
                if prop_fields:
                    prop_parts = []
                    for f in prop_fields:
                        if f in row and row[f]:
                            prop_parts.append(f"{f}: {escape(row[f])}")
                    if prop_parts:
                        props = " {" + ", ".join(prop_parts) + "}"

                q = f"MATCH (a:{src_label} {{id: {src_id}}}), (b:{dst_label} {{id: {dst_id}}}) CREATE (a)-[:{rel_type}{props}]->(b)"
                try:
                    c.execute(q)
                    count += 1
                except Exception as e:
                    errors += 1
                    if errors <= 3:
                        print(f"    Edge error: {str(e)[:80]}")

        elapsed = time.time() - s
        total_edges += count
        rate = count / max(elapsed, 0.1)
        print(f"  {rel_type:<15} ({src_label}->{dst_label}): {count:>10,} edges  {elapsed:.1f}s  ({rate:.0f}/s)")

    # === VERIFICATION ===
    print(f"\n=== VERIFICATION ===")
    cols, rows = c.execute("MATCH (n) RETURN count(n)")
    print(f"  Total nodes: {rows[0][0]}")
    cols, rows = c.execute("MATCH ()-[r]->() RETURN count(r)")
    print(f"  Total edges: {rows[0][0]}")

    c.close()
    total_time = time.time() - t_start
    print(f"\n=== COMPLETE: {total_nodes:,} nodes + {total_edges:,} edges in {total_time:.1f}s ({total_time/60:.1f} min) ===")

if __name__ == "__main__":
    main()
