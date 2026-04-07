# BUG: Connection drops under sustained sequential write load

**Date:** 2026-04-06
**Severity:** High — prevents bulk data ingestion, benchmark data loading fails
**Affects:** Both xrayProtocol (port 7689) and Bolt (port 7687)
**Server:** xrayGraphDB v4.0.2 on <SERVER_HOST> (503GB RAM, Ubuntu)
**Reporter:** xraygraph-bench automated benchmark suite

---

## Summary

xrayGraphDB connections (both xrayProtocol and Bolt) drop silently after ~5,000–7,000 sequential write operations. The server closes the TCP connection without sending an error frame. The client receives either `BrokenPipeError` (xrayProtocol) or `OSError: No data` (Bolt). Neo4j 5.26 on the same server handles identical workloads without connection loss.

---

## Reproduction Steps

### xrayProtocol reproduction (port 7689)

```python
from tools.xraybench.adapters.xray_protocol import XrayProtocolClient

client = XrayProtocolClient('<SERVER_HOST>', 7689, timeout=60)
client.connect('admin', '<password>')

# Create 5000 nodes (succeeds)
for batch in range(0, 5000, 200):
    creates = ",".join(f"(:N {{id:{i}}})" for i in range(batch, min(batch+200, 5000)))
    client.execute(f"CREATE {creates}")

client.execute("CREATE INDEX ON :N(id)")

# Create edges one by one — connection drops at ~7,000 operations
for i, (src, tgt) in enumerate(edges[:10000]):
    client.execute(
        f"MATCH (a:N {{id:{src}}}), (b:N {{id:{tgt}}}) CREATE (a)-[:E]->(b)"
    )
    # Dies at i ≈ 7,210 with BrokenPipeError: [Errno 32] Broken pipe
```

### Bolt reproduction (port 7687)

```python
import neo4j

driver = neo4j.GraphDatabase.driver(
    'bolt://<SERVER_HOST>:7687', auth=('admin', '<password>')
)

with driver.session() as s:
    # Create nodes (succeeds)
    for b in range(0, 5000, 500):
        s.run(f"CREATE {','.join(f'(:N {{id:{i}}})' for i in range(b, min(b+500,5000)))}")

    s.run("CREATE INDEX ON :N(id)")

    # UNWIND batch edge creation — connection drops mid-batch
    for b in range(0, 10000, 200):
        chunk = edges[b:b+200]
        s.run(
            "UNWIND $e AS r MATCH (a:N {id:r.s}),(b:N {id:r.t}) CREATE (a)-[:E]->(b)",
            e=[{"s": e[0], "t": e[1]} for e in chunk]
        )
    # Dies with: neo4j.exceptions.ServiceUnavailable:
    #   Failed to read from defunct connection — OSError('No data')
```

---

## Observed Behavior

### xrayProtocol path

| Phase | Operations | Duration | Result |
|-------|-----------|----------|--------|
| Node creation (batch CREATE) | 25 batches × 200 nodes | ~15s | SUCCESS |
| Index creation | 1 statement | <1s | SUCCESS |
| Edge creation (individual MATCH+CREATE) | 7,210 of 10,000 | ~1,246s (~20 min) | **FAIL — BrokenPipeError** |

**Error:**
```
BrokenPipeError: [Errno 32] Broken pipe
```
at `socket.sendall()` in `_send_frame()`. The server closed the TCP connection without sending an ERROR frame (0x07) or any notification. The client's next `sendall()` hits a closed socket.

### Bolt path

| Phase | Operations | Duration | Result |
|-------|-----------|----------|--------|
| Node creation (batch CREATE) | 10 batches × 500 nodes | ~5s | SUCCESS |
| Index creation | 1 statement | <1s | SUCCESS |
| Edge creation (UNWIND batches of 200) | ~25 of 50 batches | ~170s (~3 min) | **FAIL — ServiceUnavailable** |

**Error:**
```
neo4j.exceptions.ServiceUnavailable: Failed to read from defunct connection
  IPv4Address(('<SERVER_HOST>', 7687)) — OSError('No data')
```

The server stopped responding mid-UNWIND batch. The neo4j driver detected a defunct connection and raised `ServiceUnavailable`.

### First occurrence (earlier in session)

A third failure was observed during the initial data loading attempt using Bolt with larger UNWIND batches:

| Phase | Operations | Result |
|-------|-----------|--------|
| 55,071 node creation | Batch CREATE × 200 | SUCCESS in 15s |
| Edge creation via individual MATCH+CREATE | ~8,451 of 55,068 | Connection lost, required full server restart |

After this failure, the xrayGraphDB service required a manual restart (`systemctl restart xraygraphdb`). The server did not recover automatically.

---

## Expected Behavior

1. **No silent connection drops.** If the server needs to terminate a connection due to resource pressure, it should send an ERROR frame (xrayProtocol 0x07, severity=FATAL) or Bolt FAILURE message before closing.

2. **UNWIND batch operations should not kill the connection.** Neo4j 5.26 on the same server (same hardware, same OS, same network) handles identical UNWIND batches of 200 edges without any connection issues. The workload is: `UNWIND $edges AS e MATCH (a:N {id: e.s}), (b:N {id: e.t}) CREATE (a)-[:E]->(b)` with 200 edge pairs per batch.

3. **Individual MATCH+CREATE in a loop should be sustainable.** While not optimal, 10K sequential simple queries should not crash the connection. Each query is independent, stateless, and completes in <100ms.

---

## Control: Neo4j 5.26 on Same Server

To verify this is not a network or OS issue, Neo4j 5.26.24 was started on the same server (<SERVER_HOST>) in a Docker container with 4GB heap:

```
docker run -d --name neo4j-bench \
  -p 17474:7474 -p 17687:7687 \
  -e NEO4J_AUTH=neo4j/***REDACTED*** \
  -e NEO4J_server_memory_heap_initial__size=4g \
  -e NEO4J_server_memory_heap_max__size=4g \
  neo4j:5
```

**Neo4j result:** Loaded 5,000 nodes and 10,000 edges (50 UNWIND batches × 200 edges) in 14 seconds with zero connection issues. The identical edge data, identical batch sizes, identical network path.

---

## Environment

| Component | Value |
|-----------|-------|
| Server OS | Ubuntu (kernel not checked) |
| Server RAM | 503 GB |
| xrayGraphDB version | v4.0.2 |
| xrayGraphDB service | systemd `xraygraphdb.service` |
| xrayGraphDB Bolt port | 7687 |
| xrayGraphDB xrayProtocol port | 7689 |
| Neo4j version (control) | 5.26.24 |
| Neo4j port (control) | 17687 |
| Client OS | macOS Darwin 24.5.0 arm64 |
| Client Python | 3.12.8 |
| neo4j Python driver | 5.x (pip) |
| xrayProtocol client | xraybench custom (tools/xraybench/adapters/xray_protocol.py) |
| Network | Remote (client → server over internet, ~88ms RTT) |

---

## Possible Causes

### 1. PMR (Polymorphic Memory Resource) lifetime exhaustion

The xrayProtocol spec documents that Bolt connections crash at ~298 sequential connections due to PMR lifetime issues. While xrayProtocol was designed to avoid this, the connection drop at ~7K sequential queries on xrayProtocol suggests a similar resource accumulation issue — possibly per-query allocations that aren't fully freed.

### 2. Per-connection query count limit

The server may have an undocumented limit on the number of queries per connection. The drops consistently occur in the 5,000–8,000 range across both protocols.

### 3. Memory pressure from MATCH+CREATE pattern

Each `MATCH (a:N {id: X}), (b:N {id: Y}) CREATE (a)-[:E]->(b)` requires an index lookup + edge creation. Accumulated transaction state or WAL entries from thousands of individual transactions may exceed an internal threshold.

### 4. TCP keepalive or idle timeout interaction

Although the connection is actively sending queries (not idle), the server may have a connection lifetime timer that fires after ~20 minutes regardless of activity. The xrayProtocol path died at ~20 minutes; the Bolt path died at ~3 minutes (different timeout?).

---

## Impact

- **Benchmark data loading is blocked.** Cannot load meaningful graph data (>5K edges) into xrayGraphDB for comparative benchmarking.
- **Production bulk ingestion affected.** Any ETL or migration tool doing sequential writes would hit this.
- **Silent data loss risk.** If this occurs during a user's bulk import, they get no warning — the connection just dies, and they may not realize some data wasn't loaded.

---

## Recommended Investigation

1. Check `journalctl -u xraygraphdb` for logs at the time of connection drop — is there an OOM, assertion failure, or segfault?
2. Monitor RSS memory of the xraygraphdb process during sustained writes — does it grow unboundedly?
3. Check if `--bolt-session-inactivity-timeout` or any similar flag is set to a low value
4. Test with the C++ `xgconsole` client doing the same sequential writes — does it also drop?
5. Add server-side connection lifetime logging — when a connection is terminated, log why (timeout, OOM, error, client disconnect)

---

## Workaround

None currently. The benchmark suite falls back to testing with whatever data was successfully loaded before the drop, but this limits the graph size and reduces the validity of traversal benchmarks.

Potential workaround (untested): reconnect after every N queries (e.g., 2,000) and resume loading. This would require the benchmark data loader to be idempotent (check before create).
