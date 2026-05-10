#!/usr/bin/env python3
"""Test xray.* GPU procedure signatures."""
from neo4j import GraphDatabase
import time

d = GraphDatabase.driver("bolt://localhost:7687")

tests = [
    ("pagerank with args", 'CALL xray.pagerank(20, 0.85, "") YIELD node_id, rank RETURN node_id, rank LIMIT 3'),
    ("pagerank zero-arg", "CALL xray.pagerank() YIELD node_id, rank RETURN node_id, rank LIMIT 3"),
    ("triangle with arg", 'CALL xray.triangle_count("") YIELD node_id, triangles RETURN sum(triangles) AS total'),
    ("triangle zero-arg", "CALL xray.triangle_count() YIELD node_id, triangles RETURN sum(triangles) AS total"),
    ("community with args", 'CALL xray.community_detection(20, "") YIELD node_id, community RETURN community, count(*) AS sz ORDER BY sz DESC LIMIT 3'),
    ("community zero-arg", "CALL xray.community_detection() YIELD node_id, community RETURN community, count(*) AS sz ORDER BY sz DESC LIMIT 3"),
    ("betweenness with args", 'CALL xray.betweenness_centrality("", 50) YIELD node_id, centrality RETURN node_id, centrality ORDER BY centrality DESC LIMIT 3'),
    ("betweenness zero-arg", "CALL xray.betweenness_centrality() YIELD node_id, centrality RETURN node_id, centrality ORDER BY centrality DESC LIMIT 3"),
]

for name, q in tests:
    try:
        with d.session() as s:
            start = time.perf_counter()
            r = s.run(q)
            rows = [dict(rec) for rec in r]
            ms = (time.perf_counter() - start) * 1000.0
            print(f"OK rows={len(rows)} {ms:.1f}ms: {name}")
            if rows:
                for row in rows[:2]:
                    print(f"   {row}")
    except Exception as e:
        print(f"ERR: {name} -> {str(e)[:150]}")

d.close()
