#!/usr/bin/env python3
"""GPU analytics benchmark — takes port as argument."""
from neo4j import GraphDatabase
import time, json, subprocess, threading, sys

port = int(sys.argv[1]) if len(sys.argv) > 1 else 7687
output = sys.argv[2] if len(sys.argv) > 2 else "/opt/xraybench-results/gpu_analytics.json"

def gpu_monitor(stop_event, readings):
    while not stop_event.is_set():
        try:
            out = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used",
                 "--format=csv,noheader,nounits"], text=True, timeout=2).strip()
            parts = out.split(",")
            readings.append((float(parts[0].strip()), float(parts[1].strip())))
        except Exception:
            pass
        stop_event.wait(0.1)

d = GraphDatabase.driver(f"bolt://localhost:{port}")
results = []

analytics = [
    ("PageRank (20 iter, d=0.85)", 'CALL xray.pagerank(20, 0.85, "") YIELD node_id, rank RETURN node_id, rank ORDER BY rank DESC LIMIT 10'),
    ("Triangle Count", 'CALL xray.triangle_count("") YIELD node_id, triangles RETURN sum(triangles) AS total'),
    ("Community Detection (20 iter)", 'CALL xray.community_detection(20, "") YIELD node_id, community RETURN community, count(*) AS sz ORDER BY sz DESC LIMIT 10'),
    ("Betweenness Centrality (50)", 'CALL xray.betweenness_centrality("", 50) YIELD node_id, centrality RETURN node_id, centrality ORDER BY centrality DESC LIMIT 10'),
]

for name, query in analytics:
    print(f"  {name}...", end=" ", flush=True)
    stop = threading.Event()
    readings = []
    monitor = threading.Thread(target=gpu_monitor, args=(stop, readings), daemon=True)
    monitor.start()
    try:
        with d.session() as s:
            start = time.perf_counter()
            r = s.run(query)
            rows = [dict(rec) for rec in r]
            cold_ms = (time.perf_counter() - start) * 1000.0
        stop.set()
        monitor.join(timeout=2)
        peak_gpu = max([r[0] for r in readings]) if readings else 0
        peak_vram = max([r[1] for r in readings]) if readings else 0
        with d.session() as s:
            start = time.perf_counter()
            r = s.run(query)
            _ = [dict(rec) for rec in r]
            warm_ms = (time.perf_counter() - start) * 1000.0
        sample = [{k: str(v)[:60] for k, v in row.items()} for row in rows[:3]]
        print(f"cold={cold_ms:.1f}ms warm={warm_ms:.1f}ms rows={len(rows)} GPU={peak_gpu:.0f}% VRAM={peak_vram:.0f}MiB")
        results.append({"name": name, "cold_ms": round(cold_ms,1), "warm_ms": round(warm_ms,1),
                        "rows": len(rows), "gpu_peak_pct": peak_gpu, "vram_peak_mib": peak_vram, "sample": sample})
    except Exception as e:
        stop.set()
        print(f"ERROR: {str(e)[:120]}")
        results.append({"name": name, "error": str(e)[:300]})

d.close()
json.dump(results, open(output, "w"), indent=2)
print(f"\nSaved to {output}")
