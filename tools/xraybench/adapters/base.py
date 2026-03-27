"""Abstract base adapter for graph database engines."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from ..models import (
    CorrectnessResult,
    DatasetManifest,
    DatasetSpec,
    ExecuteResult,
    LoadResult,
)


class BaseAdapter(ABC):
    """Abstract interface for graph database engine adapters.

    Every adapter must implement these methods to participate in
    xraygraph-bench benchmark execution.
    """

    @abstractmethod
    def connect(self, config: dict[str, Any]) -> None:
        """Establish a connection to the engine.

        Args:
            config: Engine-specific connection parameters. Common keys:
                - host: hostname or IP
                - port: connection port
                - username: authentication user
                - password: authentication password
                - database: database name
        """

    @abstractmethod
    def close(self) -> None:
        """Clean up connections and resources."""

    @abstractmethod
    def load_dataset(self, dataset: DatasetSpec | DatasetManifest) -> LoadResult:
        """Ingest a dataset into the engine.

        Args:
            dataset: Dataset specification or manifest.

        Returns:
            LoadResult with node_count, edge_count, and load_time_ms.
        """

    @abstractmethod
    def execute(self, query: str, params: dict[str, Any] | None = None) -> ExecuteResult:
        """Execute a query and return results with timing.

        Args:
            query: Query string (typically Cypher).
            params: Optional query parameters.

        Returns:
            ExecuteResult with rows, wall_ms, compile_ms, and metadata.
        """

    @abstractmethod
    def clear_caches(self) -> None:
        """Clear query plan caches and execution caches.

        Called before cold-run measurements to ensure the query is
        compiled and planned from scratch.
        """

    @abstractmethod
    def collect_metrics(self) -> dict[str, Any]:
        """Gather engine-specific metrics after execution.

        Returns:
            Dictionary with available metrics. Standard keys:
            - segments: int -- number of execution pipeline stages
            - breakers: list[str] -- materialization boundary operators
            - buffer_repr: str -- buffer representation format
            - cache_hit: bool -- plan cache hit
            - fallback: bool -- execution fallback occurred
            - deopt: bool -- deoptimization occurred
        """

    @abstractmethod
    def validate_correctness(
        self, result: ExecuteResult, oracle: dict[str, Any]
    ) -> CorrectnessResult:
        """Check query results against the benchmark's correctness oracle.

        Args:
            result: The execution result to validate.
            oracle: Correctness oracle specification from the benchmark spec.

        Returns:
            CorrectnessResult with passed status and detail message.
        """

    def warmup(self, query: str, params: dict[str, Any] | None = None) -> None:
        """Pre-warm caches without recording results.

        Default implementation executes the query and discards the result.
        Override for engine-specific warmup procedures.
        """
        self.execute(query, params)

    def engine_version(self) -> str:
        """Return the engine version string.

        Override to query the engine for its version.
        """
        return "unknown"
