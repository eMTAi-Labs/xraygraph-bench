# Adapter model

xraygraph-bench uses a pluggable adapter architecture to support multiple
graph database engines. Each adapter translates between the benchmark
runner's abstract operations and the engine's specific API.

## Interface

Every adapter must implement the `BaseAdapter` abstract class defined in
`tools/xraybench/adapters/base.py`.

### Required methods

```python
class BaseAdapter(ABC):

    @abstractmethod
    def connect(self, config: dict) -> None:
        """Establish a connection to the engine.

        config contains engine-specific connection parameters
        (host, port, credentials, database name, etc.)
        """

    @abstractmethod
    def close(self) -> None:
        """Clean up connections and resources."""

    @abstractmethod
    def load_dataset(self, manifest: DatasetManifest) -> LoadResult:
        """Ingest a dataset into the engine.

        Returns a LoadResult with node_count, edge_count, and load_time_ms.
        """

    @abstractmethod
    def execute(self, query: str, params: dict | None = None) -> ExecuteResult:
        """Execute a query and return results with timing.

        Must return:
        - rows: list of result rows
        - wall_ms: wall-clock execution time
        - compile_ms: compilation time if separable, else None
        - metadata: engine-specific execution metadata
        """

    @abstractmethod
    def clear_caches(self) -> None:
        """Clear query plan caches and any execution caches.

        Called before cold-run measurements.
        """

    @abstractmethod
    def collect_metrics(self) -> dict:
        """Gather engine-specific metrics after execution.

        May include memory usage, cache statistics, plan details, etc.
        """

    @abstractmethod
    def validate_correctness(
        self, result: ExecuteResult, oracle: dict
    ) -> CorrectnessResult:
        """Check query results against the benchmark's correctness oracle.

        Oracle format varies by benchmark but typically specifies:
        - expected_row_count
        - expected_columns
        - row_checksum
        - structural properties
        """
```

### Optional methods

```python
    def warmup(self, query: str, params: dict | None = None) -> None:
        """Pre-warm caches without recording results.

        Default implementation calls execute() and discards the result.
        """

    def engine_version(self) -> str:
        """Return the engine version string.

        Default returns 'unknown'.
        """
```

## Lifecycle

The benchmark runner calls adapter methods in this order:

1. `connect(config)` -- establish connection
2. `load_dataset(manifest)` -- ingest the benchmark's dataset
3. `clear_caches()` -- prepare for cold run
4. `execute(query)` -- cold-run measurement
5. `execute(query)` (N times) -- warm-run measurements
6. `collect_metrics()` -- gather post-execution metrics
7. `validate_correctness(result, oracle)` -- check correctness
8. `close()` -- clean up

For load tests, steps 4-6 are replaced by concurrent execution across
multiple threads/connections.

## Registration

Adapters are registered in `tools/xraybench/adapters/__init__.py`:

```python
ADAPTERS = {
    "memgraph": MemgraphAdapter,
    "neo4j": Neo4jAdapter,
    "xraygraphdb": XrayGraphDBAdapter,
}
```

The CLI resolves adapter names to classes at runtime.

## Configuration

Each adapter receives a configuration dictionary. The runner loads adapter
config from:

1. CLI flags (`--host`, `--port`, `--engine-config`)
2. Environment variables (`XRAYBENCH_ENGINE_HOST`, etc.)
3. A config file if specified (`--config`)

Adapters should document their required and optional configuration keys in
their module docstring.

## Writing a new adapter

1. Create `tools/xraybench/adapters/<engine_name>.py`
2. Subclass `BaseAdapter` and implement all required methods
3. Add the adapter to the `ADAPTERS` dict in `__init__.py`
4. Test with `xraybench run <benchmark> --engine <engine_name>`

See the existing adapter files for reference implementations.
