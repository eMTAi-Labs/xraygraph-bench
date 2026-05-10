#!/usr/bin/env python3
"""Quick retest of IS3/IC2/BFS/Edge count after fixes."""
import time
from xgdb_connect.protocol import XrayProtocolClient

c = XrayProtocolClient("127.0.0.1", 7689, auth_token="bench:Bench2026!xray", read_timeout=60)
print(f"Connected: {c.connected}", flush=True)

queries = [
    ("IS1: Profile", 'MATCH (p:Person {id: 933}) RETURN p.firstName, p.lastName'),
    ("IS3: Friends", 'MATCH (p:Person {id: 933})-[:KNOWS]-(f) RETURN count(f) AS cnt'),
    ("IC2: Messages", 'MATCH (p:Person {id: 933})-[:KNOWS]-(f:Person)<-[:HAS_CREATOR]-(m) RETURN f.firstName, m.id ORDER BY m.creationDate DESC LIMIT 10'),
    ("IC5: Forums", 'MATCH (p:Person {id: 933})-[:KNOWS*1..2]-(f:Person)<-[:HAS_MEMBER]-(forum:Forum) RETURN forum.title, count(DISTINCT f) AS members ORDER BY members DESC LIMIT 10'),
    ("Edge count", 'MATCH ()-[r]->() RETURN count(r) AS cnt'),
    ("Node count", 'MATCH (n) RETURN count(n) AS cnt'),
    ("BFS 1-hop", 'MATCH (p:Person {id: 933})-[:KNOWS]-(f) RETURN count(f) AS cnt'),
    ("BFS 2-hop", 'MATCH (p:Person {id: 933})-[:KNOWS*1..2]-(f) RETURN count(DISTINCT f) AS cnt'),
    ("BFS 3-hop", 'MATCH (p:Person {id: 933})-[:KNOWS*1..3]-(f) RETURN count(DISTINCT f) AS cnt'),
    ("BFS 4-hop", 'MATCH (p:Person {id: 933})-[:KNOWS*1..4]-(f) RETURN count(DISTINCT f) AS cnt'),
    ("BFS 5-hop", 'MATCH (p:Person {id: 933})-[:KNOWS*1..5]-(f) RETURN count(DISTINCT f) AS cnt'),
    ("BFS 6-hop", 'MATCH (p:Person {id: 933})-[:KNOWS*1..6]-(f) RETURN count(DISTINCT f) AS cnt'),
    ("BFS 7-hop", 'MATCH (p:Person {id: 933})-[:KNOWS*1..7]-(f) RETURN count(DISTINCT f) AS cnt'),
    ("BFS 8-hop", 'MATCH (p:Person {id: 933})-[:KNOWS*1..8]-(f) RETURN count(DISTINCT f) AS cnt'),
    ("BFS 9-hop", 'MATCH (p:Person {id: 933})-[:KNOWS*1..9]-(f) RETURN count(DISTINCT f) AS cnt'),
    ("BFS 10-hop", 'MATCH (p:Person {id: 933})-[:KNOWS*1..10]-(f) RETURN count(DISTINCT f) AS cnt'),
]

for name, q in queries:
    s = time.perf_counter()
    cols, rows = c.execute(q)
    cold = (time.perf_counter() - s) * 1000
    times = []
    for _ in range(3):
        s = time.perf_counter()
        c.execute(q)
        times.append((time.perf_counter() - s) * 1000)
    warm = sum(times) / len(times)
    print(f"{name:<20} cold={cold:.1f}ms warm={warm:.1f}ms rows={len(rows)}", flush=True)

c.close()
