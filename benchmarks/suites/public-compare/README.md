# public-compare

Portable benchmarks designed for fair cross-engine comparison.

## Purpose

These benchmarks use conservative query patterns that map cleanly to
multiple query languages and execution models. They are intended for
public sharing and comparison between engines.

## Design constraints

- Query patterns must be expressible in standard Cypher without
  engine-specific extensions
- Dataset generation must be fully deterministic
- Correctness oracles must be unambiguous
- No engine-specific tuning is assumed in the benchmark definition

## What this exposes

- Baseline traversal performance across engines
- Reachability query scaling with hop count and source cardinality
- Simple pattern matching efficiency
- Engine overhead on well-understood workloads

## Benchmarks

| Benchmark | Description |
|-----------|-------------|
| `portable-reachability` | Bounded reachability from seed nodes on a standard graph |

## Publishing results

Results from public-compare benchmarks are suitable for cross-engine
comparison when accompanied by:

1. Exact engine versions
2. Hardware specification
3. Default vs tuned configuration documentation
4. Both cold and warm run numbers
5. Correctness validation status

See [docs/reproducibility.md](../../../docs/reproducibility.md) for the
full methodology.
