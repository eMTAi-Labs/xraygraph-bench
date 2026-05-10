# Contributing to xraygraph-bench

Contributions are welcome. This document explains how to add benchmarks,
datasets, and adapters to the suite.

## Adding a benchmark

1. Choose the appropriate family directory under `benchmarks/suites/`:
   - `core-executor` -- pure executor workloads (scan, filter, aggregate, sort)
   - `graph-breakers` -- traversal-heavy workloads that stress expansion
   - `end-to-end` -- realistic application-level graph workloads
   - `public-compare` -- portable workloads for cross-engine comparison
   - `compile` -- compilation and cache behavior

2. Create a new directory under the family:
   ```
   benchmarks/suites/<family>/<benchmark-name>/benchmark.yaml
   ```

3. Write the benchmark spec following `schemas/benchmark.spec.schema.json`.
   Every spec must include:
   - `name` -- unique identifier
   - `family` -- the parent family
   - `description` -- what the benchmark measures and why
   - `dataset` -- which dataset is required
   - `parameters` -- tunable knobs with defaults and ranges
   - `correctness_oracle` -- how to verify the result is correct
   - `metrics` -- which result fields are required

4. Validate your spec:
   ```bash
   xraybench validate benchmarks/suites/<family>/<name>/benchmark.yaml
   ```

5. If you add a new benchmark family, update `docs/benchmark-categories.md`.

### Style rules for benchmark definitions

- Use clear, descriptive names. `scan-filter-project` not `bench1`.
- Parameters should have documented defaults and sensible ranges.
- The correctness oracle must be specific enough to catch wrong results.
- Description should explain what the benchmark isolates, not just what it runs.
- Do not include engine-specific query syntax in the spec. Adapters translate.

## Adding a dataset

1. Create a directory under `datasets/`:
   ```
   datasets/<category>/<dataset-name>/
   ```

2. Add a `manifest.yaml` following `schemas/dataset.manifest.schema.json`.
   Include:
   - `name`, `version`, `source`
   - `format` (csv, parquet, edge-list)
   - `node_count`, `edge_count`
   - `labels` and `edge_types`
   - `description`

3. For synthetic datasets, add a generator function in
   `tools/xraybench/generators/synthetic.py`.

4. For external datasets (SNAP, OGB), document the download procedure and
   ingestion steps in the dataset README.

## Adding an adapter

1. Create a new file under `tools/xraybench/adapters/`:
   ```
   tools/xraybench/adapters/<engine>.py
   ```

2. Implement the `BaseAdapter` interface defined in
   `tools/xraybench/adapters/base.py`:
   - `connect()` -- establish connection to the engine
   - `load_dataset(manifest)` -- ingest a dataset
   - `execute(query, params)` -- run a query and return raw results
   - `collect_metrics()` -- gather engine-specific metrics
   - `validate_correctness(result, oracle)` -- check result against oracle
   - `close()` -- clean up resources

3. Register the adapter in `tools/xraybench/adapters/__init__.py`.

4. Document any engine-specific setup requirements in the adapter file's
   docstring.

See [docs/adapter-model.md](docs/adapter-model.md) for the full interface
specification.

## Validating result JSON

Result files must conform to `schemas/result.schema.json`:

```bash
xraybench validate --schema result path/to/result.json
```

See `examples/sample-result.json` for a complete example.

## Expectations for contributions

- **Neutrality.** Benchmarks must not be rigged for or against any engine.
  If a benchmark inherently favors a particular execution model, document that
  explicitly.
- **Reproducibility.** Any contributed benchmark must be runnable from a
  deterministic starting state. Document dataset generation, environment
  requirements, and cache-clearing procedures.
- **Correctness.** Every benchmark must have a correctness oracle. Speed
  without correctness is not a valid result.
- **Clarity.** Benchmark descriptions should be understandable to someone
  who has not read the implementation.

## Code style

- Python code follows PEP 8.
- YAML files use 2-space indentation.
- JSON schemas use draft-07.
- Commit messages follow conventional commits (`feat:`, `fix:`, `docs:`).
