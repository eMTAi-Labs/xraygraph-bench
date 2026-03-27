# Benchmarks

This directory contains benchmark suite definitions organized by family.

## Structure

```
benchmarks/
  suites/
    core-executor/       Pure executor workloads (scan, filter, aggregate, sort)
    graph-breakers/      Traversal-heavy workloads that stress expansion
    end-to-end/          Realistic application-level graph workloads
    public-compare/      Portable workloads for cross-engine comparison
    compile/             Compilation and caching behavior
```

## Benchmark specs

Each benchmark is defined by a `benchmark.yaml` file conforming to
`schemas/benchmark.spec.schema.json`. A spec includes:

- What the benchmark measures
- Which dataset it requires
- Tunable parameters with defaults and ranges
- A correctness oracle
- Required result metrics

## Running benchmarks

```bash
# List all available benchmarks
xraybench list

# Validate a spec
xraybench validate benchmarks/suites/core-executor/scan-filter-project/benchmark.yaml

# Run a benchmark
xraybench run benchmarks/suites/core-executor/scan-filter-project/benchmark.yaml --engine xraygraphdb
```

## Adding benchmarks

See [CONTRIBUTING.md](../CONTRIBUTING.md) for instructions on adding new
benchmarks to the suite.
