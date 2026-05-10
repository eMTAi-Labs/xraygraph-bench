# Roadmap

This document tracks what is scaffolded, what is stubbed, and what is planned.

## Scaffolded and specified (current state)

- Repository structure and documentation
- 5 benchmark families with 18 benchmark specs
- JSON schemas for results, benchmark specs, and dataset manifests
- Python CLI runner with `list`, `validate`, `run`, and `load-test` commands
- Adapter interface and skeletons for memgraph, neo4j, xraygraphdb
- Load testing module with throughput, saturation, mixed, and stability profiles
- Synthetic graph generators (uniform, power-law, hub, chain)
- SNAP and OGB dataset integration documentation
- CI workflows for schema validation and linting

## Stubbed (interface defined, implementation incomplete)

- Memgraph adapter: connection and query execution
- Neo4j adapter: connection and query execution
- xrayGraphDB adapter: connection, query execution, and metric collection
- Dataset download and ingestion for SNAP and OGB
- Correctness oracle evaluation beyond row-count checks

## Planned work

### Near-term

- Full xrayGraphDB adapter implementation with compile_ms extraction
- SNAP dataset automated download and Cypher ingestion scripts
- Result comparison tooling (diff two result sets)
- HTML report generation from result JSON

### Medium-term

- OGB dataset ingestion with property mapping
- Additional core-executor benchmarks (index-only access, property-heavy scan)
- Additional graph-breaker benchmarks (path enumeration, shortest path)
- Containerized benchmark environment (Docker Compose)
- Automated nightly benchmark runs via CI

### Long-term

- Result database for historical tracking
- Interactive result visualization dashboard
- Community-contributed benchmark specs
- Additional adapters (TigerGraph, ArangoDB, DGraph)
- LDBC SNB workload integration
- Distributed execution benchmarks
