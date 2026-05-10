# Synthetic datasets

Programmatically generated graphs for controlled benchmark experiments.

## Available generators

Generators are implemented in `tools/xraybench/generators/synthetic.py`.

### uniform_nodes

Generates a flat collection of nodes with uniformly distributed property
values. Used by core-executor benchmarks that do not require graph
structure.

- **Node labels:** `Node`
- **Properties:** configurable (typically id, value, category)
- **Values:** uniformly distributed in their respective ranges
- **Nullable support:** configurable fraction of null values

### uniform_directed_graph

Generates a directed graph with approximately uniform out-degree.

- **Node labels:** `Node`
- **Edge type:** `EDGE`
- **Properties:** configurable per node
- **Degree distribution:** approximately uniform with configurable mean

### power_law_graph

Generates a graph with power-law degree distribution using the
Barabasi-Albert preferential attachment model.

- **Node labels:** `Node`
- **Edge type:** `EDGE`
- **Degree distribution:** power-law with configurable exponent
- **Useful for:** BFS and traversal benchmarks where irregular fan-out matters

### hub_graph

Generates a hub-and-spoke graph with a small number of high-degree hub
nodes connected to many spoke nodes.

- **Node labels:** `Hub`, `Spoke`
- **Edge type:** configurable (default `CONNECTED_TO`)
- **Hub count:** configurable
- **Spokes per hub:** configurable (typically 10K-1M)
- **Useful for:** expand-high-fanout benchmarks

### community_graph

Generates a graph with community structure: dense intra-community edges
and sparse inter-community edges.

- **Node labels:** `Node` (with `community` property)
- **Edge type:** `EDGE`
- **Community count:** configurable
- **Density:** separate intra and inter-community edge density
- **Useful for:** multi-hop traversal benchmarks

### categorical_nodes

Generates nodes with categorical properties for aggregation benchmarks.

- **Node labels:** `Node`
- **Properties:** configurable categories with controlled cardinality
- **Useful for:** aggregate-groupby benchmarks

## Determinism

All generators accept a `seed` parameter for deterministic output. Given
the same seed and parameters, the same graph is produced. Seeds are recorded
in result metadata for reproducibility.

## Output format

Generators produce Cypher CREATE statements by default. Alternative output
formats (CSV, edge list) are available via the `--format` flag.
