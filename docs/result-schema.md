# Result schema

Every benchmark run produces a result object conforming to
`schemas/result.schema.json`. This document describes each field and how
adapters should populate them.

## Identification fields

| Field | Type | Description |
|-------|------|-------------|
| `benchmark` | string | Benchmark name matching the spec (e.g., `scan-filter-project`) |
| `engine` | string | Engine identifier (e.g., `xraygraphdb`, `memgraph`, `neo4j`) |
| `engine_version` | string | Exact engine version string |
| `dataset` | string | Dataset name used for this run |
| `dataset_version` | string | Dataset version or generation seed |
| `timestamp` | string | ISO 8601 timestamp of run completion |
| `host` | object | Host environment details (os, cpu, memory, cores) |

## Timing fields

| Field | Type | Description |
|-------|------|-------------|
| `cold_ms` | number | First-run latency including any compilation |
| `warm_ms` | number | Average latency of subsequent warm runs |
| `compile_ms` | number or null | Compilation time if separable, null if not applicable |

**How to populate:**
- `cold_ms`: Execute the query once after clearing plan caches. Measure
  wall-clock time from submission to result receipt.
- `warm_ms`: Execute the query N additional times (default: 10). Report the
  arithmetic mean.
- `compile_ms`: If the engine exposes compilation timing separately (e.g.,
  via EXPLAIN ANALYZE or profiling hooks), report it. If the engine does not
  separate compilation from execution, set to `null`.

## Cardinality fields

| Field | Type | Description |
|-------|------|-------------|
| `rows_in` | integer | Number of input rows/nodes scanned |
| `rows_out` | integer | Number of result rows returned |

These fields help distinguish workloads where the engine reads many rows but
returns few (high selectivity) from workloads with full materialization.

## Execution detail fields

| Field | Type | Description |
|-------|------|-------------|
| `query_shape` | string | Canonical query shape or template identifier |
| `segments` | integer or null | Number of execution segments/pipeline stages |
| `breakers` | array of string | Operators that force full materialization (e.g., Sort, Aggregate, Distinct) |
| `buffer_repr` | string or null | Buffer representation format if applicable |
| `cache_hit` | boolean | Whether the query plan was served from plan cache |
| `fallback` | boolean | Whether the engine fell back to an alternate execution path |
| `deopt` | boolean | Whether a compiled plan was deoptimized during execution |

**Notes on breakers:**
Not all engines expose materialization boundaries. If the engine does not
report this information, set `breakers` to an empty array and document the
limitation in `notes`.

## Correctness field

| Field | Type | Description |
|-------|------|-------------|
| `correctness` | object | `{"passed": true/false, "detail": "..."}` |

The runner validates results against the benchmark's correctness oracle
before recording timing. A failed correctness check should still produce a
result record with `correctness.passed: false` so that the failure is
tracked, not silently dropped.

## Concurrency fields

These fields are populated during load tests. For single-query benchmarks,
they may be omitted.

| Field | Type | Description |
|-------|------|-------------|
| `concurrency` | integer | Number of concurrent clients |
| `qps` | number | Queries per second sustained |
| `latency_p50` | number | 50th percentile latency in ms |
| `latency_p95` | number | 95th percentile latency in ms |
| `latency_p99` | number | 99th percentile latency in ms |
| `error_rate` | number | Fraction of queries that errored (0.0 to 1.0) |

## Environment fields

| Field | Type | Description |
|-------|------|-------------|
| `host.os` | string | Operating system and version |
| `host.cpu` | string | CPU model |
| `host.cores` | integer | Number of CPU cores |
| `host.memory_gb` | number | Total system memory in GB |

## Metadata fields

| Field | Type | Description |
|-------|------|-------------|
| `parameters` | object | Benchmark parameter values used for this run |
| `notes` | string | Free-form notes about the run |

## Extensibility

The schema uses `additionalProperties: true` at the top level to allow
engine-specific fields. Adapters may add custom fields prefixed with the
engine name (e.g., `xraygraphdb_plan_stages`). These fields are not
validated by the base schema but should be documented in the adapter.
