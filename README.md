# xraygraph-bench

A reproducible benchmark suite for evaluating graph database execution engines,
with emphasis on traversal-heavy, analytical, and hybrid workloads.

## Why this benchmark exists

Most graph database benchmarks measure end-to-end query time on simple
workloads. They rarely expose the differences that matter when choosing or
tuning an engine:

- **Execution model** -- iterator vs compiled vs vectorized execution produce
  dramatically different profiles on the same query.
- **Traversal under irregular fan-out** -- a graph with skewed degree
  distribution punishes naive implementations in ways uniform benchmarks hide.
- **Cold vs warm execution** -- first-run compile cost is real work that
  vanishes in warm-cache benchmarks.
- **Compilation overhead** -- engines that compile queries pay upfront cost
  that amortizes differently depending on workload.
- **Fallback and deoptimization** -- compiled engines sometimes fall back to
  interpreted paths; benchmarks should make this visible.
- **Materialization boundaries** -- operators that force full materialization
  (sorts, aggregations after expansions) create measurable breaks in
  streaming execution.

xraygraph-bench is designed to make these differences visible and measurable.

## Benchmark families

The suite organizes benchmarks into eight families, each targeting a distinct
aspect of engine behavior.

| Family | Purpose | Tier |
|--------|---------|------|
| **core-executor** | Scan, filter, projection, aggregation, sort -- pure executor overhead | A |
| **graph-breakers** | Expand, BFS, multi-hop traversal -- irregular graph work that breaks streaming | A |
| **end-to-end** | Lineage, dependency analysis, impact analysis -- realistic graph workloads | A |
| **public-compare** | Portable workloads suitable for fair cross-engine comparison | A |
| **compile** | Cold vs warm, cache reuse, deopt/fallback -- compilation-aware measurement | A/B |
| **gfql** | GFQL dataframe-style queries -- transpilation overhead, native query language | B |
| **emergent-edge** | Workload-learned optimization -- learning curves, invalidation, cache tiers | B |
| **hybrid-vector** | Vector similarity + graph traversal pipelines -- handoff cost, constrained ANN | B |

**Tier A** = portable cross-engine comparison (same protocol, same query).
**Tier B** = engine-native advantage (native protocol, engine-specific features).
Tier A and Tier B results are never mixed in the same comparison.

See [docs/benchmark-categories.md](docs/benchmark-categories.md) for detailed
descriptions. See [METHODOLOGY.md](METHODOLOGY.md) for the fairness contract,
benchmark classes, engine mode declarations, and reproducibility requirements.

## Design principles

1. **Separate cold and warm.** Every benchmark produces both cold-run and
   warm-run timings. These are different measurements, not noise.
2. **Correctness is non-negotiable.** Every benchmark defines a correctness
   oracle. A fast wrong answer is not a result.
3. **Adapters, not assumptions.** The runner uses a pluggable adapter model.
   Any engine that implements the adapter interface can participate.
4. **Reproducibility over convenience.** Deterministic dataset generation,
   environment capture, and run metadata are built into the result schema.
5. **Compile time is real work.** Compilation cost is measured and reported
   separately, not hidden inside warm-run averages.

## Repository layout

```
xraygraph-bench/
  docs/                 Design documents and methodology
  schemas/              JSON schemas for results, benchmark specs, datasets
  benchmarks/           Benchmark suite definitions (YAML specs)
  datasets/             Dataset documentation and manifests
  examples/             Sample result and benchmark files
  tools/xraybench/      Python CLI runner, adapters, and generators
  .github/workflows/    CI validation and linting
```

## Result representation

Benchmark results follow a structured JSON schema
([schemas/result.schema.json](schemas/result.schema.json)) that captures:

- Timing: `cold_ms`, `warm_ms`, `compile_ms`
- Cardinality: `rows_in`, `rows_out`
- Execution detail: `segments`, `breakers`, `cache_hit`, `fallback`, `deopt`
- Correctness: pass/fail with optional detail
- Concurrency: `qps`, latency percentiles (`p50`, `p95`, `p99`)
- Environment: host, engine version, timestamp

See [docs/result-schema.md](docs/result-schema.md) for field-level
documentation.

## Quick start

```bash
# Install the runner
pip install -e tools/

# List available benchmarks
xraybench list

# Validate a benchmark spec
xraybench validate benchmarks/suites/core-executor/scan-filter-project/benchmark.yaml

# Run a benchmark
xraybench run benchmarks/suites/core-executor/scan-filter-project/benchmark.yaml --engine xraygraphdb

# Run a load test
xraybench load-test --engine xraygraphdb --profile mixed --clients 32
```

## Current status

**Scaffolded and specified:**
- 5 benchmark families with 18 benchmark specs
- JSON schemas for results, benchmark specs, and dataset manifests
- Python CLI runner with adapter model
- Load testing module (throughput, saturation, mixed, stability)
- Dataset integration documentation (SNAP, OGB, synthetic)

**Not yet implemented:**
- Full adapter implementations (xrayGraphDB adapter is most complete)
- Automated dataset ingestion pipelines
- Result visualization and comparison tooling
- CI-driven benchmark execution

See [docs/roadmap.md](docs/roadmap.md) for planned work.

## License

Apache License 2.0. See [LICENSE](LICENSE).
