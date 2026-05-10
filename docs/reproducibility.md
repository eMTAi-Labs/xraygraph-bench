# Reproducibility

Benchmark results are only useful if they can be reproduced. This document
describes the methodology xraygraph-bench uses to ensure reproducibility.

## Deterministic dataset generation

Synthetic datasets are generated from a fixed seed. The seed is recorded in
the dataset manifest and in the result schema. Given the same seed and
generator version, the same graph is produced.

For external datasets (SNAP, OGB), the dataset version and download URL are
recorded in the manifest. The runner verifies checksums after download.

## Environment capture

Every result record includes a `host` object with:
- Operating system and version
- CPU model
- Number of cores
- Total system memory

This allows consumers of benchmark results to account for hardware
differences when comparing results across environments.

## Run metadata

Each result includes:
- `timestamp` -- when the run completed
- `engine_version` -- exact engine version
- `dataset_version` -- dataset version or generation seed
- `parameters` -- all benchmark parameter values used

## Cold/warm execution methodology

### Cold run procedure

1. Load the dataset into the engine
2. Clear all query plan caches (`adapter.clear_caches()`)
3. Execute the query once
4. Record the wall-clock time as `cold_ms`

If the engine supports it, the adapter also records `compile_ms` separately.

### Warm run procedure

1. After the cold run, execute the query N additional times (default: 10)
2. Discard the first warm run if desired (configurable)
3. Record the arithmetic mean of the remaining runs as `warm_ms`

The cold and warm procedures are always run in sequence within a single
benchmark execution. The dataset is not reloaded between cold and warm runs.

### Cache clearing

Cache clearing is engine-specific. Adapters must implement `clear_caches()`
to remove:
- Query plan caches
- Compiled query caches
- Any engine-specific execution caches

Buffer pool / page cache clearing is not required by default, since it would
measure OS-level I/O rather than engine behavior. If a benchmark requires
buffer pool clearing, it specifies this in its parameters.

## Publishing fair results

When publishing benchmark results for comparison:

1. **Use the same hardware** for all engines being compared, or clearly
   document hardware differences.
2. **Use the same dataset** and parameters for all engines.
3. **Report both cold and warm** numbers. Do not cherry-pick whichever
   looks better.
4. **Include correctness** status. A fast incorrect result is not a result.
5. **Document engine configuration.** Default configurations are acceptable
   for initial comparison, but tuned configurations must be documented.
6. **Report engine versions** exactly. Minor versions can have significant
   performance differences.
7. **Run multiple iterations** and report variance. A single run is not
   sufficient for comparison.

## Validation

The runner validates results against the benchmark's correctness oracle
before recording timing. This ensures that performance numbers are never
reported for incorrect results.

Use `xraybench validate` to check benchmark specs and result files against
their respective schemas before publishing.
