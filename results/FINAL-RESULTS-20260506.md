# Final Benchmark Results — May 6, 2026

## Server Configurations

| Server | CPU | RAM | GPU | Role |
|--------|-----|-----|-----|------|
| .187 (Production) | 64-core AMD EPYC @ 2.9GHz | 503GB | None | Primary benchmarks |
| .29 (Docker) | Production host | - | None | Docker overhead comparison |
| .68 (Budget GPU) | 28-core Xeon E5-2650L @ 1.7GHz | 62GB | T1000 8GB | Budget hardware + GPU |
| Competitor | 44-core Xeon Gold 6152 @ 2.1GHz | 187GB | Tesla T4 | All competitor databases |

## LDBC SF1 Interactive Queries (warm ms)

| Query | xrayGraphDB | Memgraph 2.22 | Neo4j 2026 | NebulaGraph 3.8 | DuckDB 1.5 | PostgreSQL 16 | MySQL 8.0 |
|-------|-------------|---------------|------------|-----------------|------------|---------------|-----------|
| IS1: Profile | 0.7 | 1.1 | 2.4 | 1.6 | 0.7 | 55 | 7.7 |
| IS3: Friends | 0.9 | 1.1 | 2.0 | 2.2 | 1.9 | 47 | 8.3 |
| IC5: Forums | 1.1 | 1,078 | 707 | 1,428 | 78 | 3,017 | 1,959 |
| IC11: Work | 1.0 | 2.7 | 3.5 | 80.8 | N/A | N/A | N/A |
| Edge count | 0.5 | 731 | 1.5 | 1.7 | 0.5 | 62 | 68 |
| Node count | 0.5 | 470 | 1.2 | 1.3 | 0.4 | 49 | 9.2 |

## Friendster Analytics (65.6M vertices, 3.6B undirected edges)

| Procedure | .187 Production | .68 Budget | Any Competitor |
|-----------|-----------------|------------|----------------|
| BC Pair-Sampled (ε=0.05, warm) | **979ms** | 2.8s | impossible |
| BC Pair-Sampled (ε=0.10, warm) | **517ms** | 1.4s | impossible |
| Shortest Path (hub-to-hub) | **226ms** | 439ms | impossible |
| Jaccard Similarity | **2.0ms** | 2.2ms | impossible |
| Link Prediction | **1ms** | 3ms | impossible |
| Clustering Coefficient | 2.9s | 1.4s | impossible |
| Triangle Count | 143s | 537s | impossible |
| Connected Components | 100s | OOM (62GB) | impossible |
| PageRank 5iter | 161s | OOM (62GB) | impossible |
| K-Core | 147s | OOM (62GB) | impossible |
| Community 3iter | 155s | OOM (62GB) | impossible |
| HITS 3iter | 302s | OOM (62GB) | impossible |
| PersonalizedPR 5iter | 79s | 210s | impossible |

Triangle count verified: 4,173,724,142 (exact SNAP match).

## CSR BFS on Friendster (3.6B edges, .187)

| Hop | Time | Rows |
|-----|------|------|
| 1 | 19ms | 2 |
| 2 | 11ms | 3 |
| 3 | 25ms | 4 |
| 4 | 7.9s | 5 |
| 5 | 11.1s | 6 |
| 6 | 13.1s | 7 |
| 7 | 13.0s | 8 |
| 8 | 13.3s | 9 |
| 9 | 13.5s | 10 |
| 10 | 13.1s | 11 |

## GPU Analytics (SF1, T1000 8GB on .68)

| Procedure | Time |
|-----------|------|
| PageRank 5iter | 8.8s |
| Triangle Count | 1.9s |
| Community 3iter | 7.9s |
| Connected Components | 9.1s |
| K-Core | 7.3s |
| HITS 3iter | 9.4s |
| Betweenness 50 | 8.4s |

## Docker Performance (.29, zero overhead)

| Metric | Docker | Bare-Metal |
|--------|--------|------------|
| RETURN 1 p50 | 0.24ms | 0.47ms |
| IS3: Friends | 0.39ms | 0.9ms |
| Edge count | 0.31ms | 0.5ms |
| Edge load rate | 1.28-1.66M/s | 261-598K/s |

## Data Loading

| Database | Rate | Notes |
|----------|------|-------|
| xrayGraphDB CSR | 1.17M edges/s | Friendster in 9.6 min |
| xrayGraphDB GID | 261-598K/s | LDBC SF1 edges |
| DuckDB | 1-5M rows/s | Embedded CSV |
| PostgreSQL | 270K-1.2M/s | COPY |
| MySQL | 100-266K/s | LOAD DATA |
| NebulaGraph | 30-44K/s | nGQL INSERT |
| Memgraph | 8-26K/s | Bolt UNWIND |
| Neo4j | 12-14K/s | Bolt UNWIND |

## Protocol Latency

| Query | xrayProtocol p50 |
|-------|-----------------|
| RETURN 1 | 0.47ms |
| RETURN 1+1 | 0.43ms |
