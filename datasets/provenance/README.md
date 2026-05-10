# Provenance datasets

Data lineage and artifact provenance directed acyclic graphs (DAGs).

## Graph structure

Provenance graphs model the flow of data through processing pipelines:

- **Nodes:**
  - `Artifact` -- data artifacts (tables, files, models, reports) with name,
    type, and creation timestamp
  - `Process` -- transformations that consume and produce artifacts, with
    engine type and configuration

- **Edges:**
  - `CONSUMED_BY` -- artifact consumed by a process
  - `PRODUCED_BY` -- process produced an artifact

## Characteristics

Provenance graphs have specific structural properties:

- **DAG structure:** Provenance flows in one direction. No cycles (a
  well-formed provenance graph is a DAG).
- **Variable fan-out:** Some processes produce many artifacts (e.g., a
  partitioning step); others produce one.
- **Variable fan-in:** Some artifacts depend on many inputs (e.g., a join);
  others depend on one.
- **Depth:** Real pipelines can be 5-20+ stages deep from raw source to
  final report.
- **Temporal ordering:** Artifacts have creation timestamps that respect
  the DAG ordering.

## Synthetic generation

The `provenance_dag` generator produces synthetic provenance graphs with:
- Configurable artifact and process counts
- Configurable average inputs/outputs per process
- Configurable maximum pipeline depth
- Deterministic generation from seed

## Real-world options

For benchmarks requiring real provenance data:

- **OpenLineage** compatible datasets from data engineering pipelines
- **W3C PROV** formatted provenance records
- **Apache Atlas** lineage exports

Real-world provenance ingestion requires format-specific import tooling
that is not yet implemented in the benchmark runner.
