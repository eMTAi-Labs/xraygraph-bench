# Datasets

This directory contains dataset documentation and manifests for use with
xraygraph-bench benchmarks.

## Categories

| Category | Description |
|----------|-------------|
| `synthetic/` | Programmatically generated graphs for controlled experiments |
| `code-graph/` | Graphs shaped like code: functions, files, classes, dependencies |
| `provenance/` | Data lineage and artifact provenance DAGs |
| `snap/` | Stanford Network Analysis Platform real-world graph datasets |
| `ogb/` | Open Graph Benchmark datasets |

## Dataset storage

Dataset files (CSV, Parquet, edge lists) are not committed to this
repository. They are generated or downloaded on demand by the benchmark
runner.

The `.gitignore` excludes data files while preserving README and manifest
files.

## Adding a dataset

1. Create a directory under the appropriate category
2. Add a `manifest.yaml` following `schemas/dataset.manifest.schema.json`
3. For synthetic datasets, add a generator in `tools/xraybench/generators/`
4. For external datasets, document the download and ingestion procedure

See [CONTRIBUTING.md](../CONTRIBUTING.md) for details.
