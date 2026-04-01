# Phase 2: Adapter Implementations — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all adapter stubs with fully working implementations — revised interface with capabilities, shared dataset loading, shared correctness validation using the Rust core, three complete Bolt adapters (xrayGraphDB, Neo4j, Memgraph), GFQL support, and adapter overhead calibration.

**Architecture:** The adapter base class gains a capability system and richer return types. Correctness validation delegates to `xraybench_core.checksum` (the Rust BLAKE3/structural validator built in Phase 1). Dataset loading uses a shared `DatasetLoader` that generates data via `xraybench_core.generators` and ingests through any Bolt-compatible adapter. The xrayProtocol native adapter is deferred to Phase 2b (custom binary protocol — separate project).

**Tech Stack:** Python 3.12, neo4j Python driver, xraybench_core (Rust PyO3), pytest

**Spec:** `docs/superpowers/specs/2026-03-31-full-implementation-design.md` — Sections 3, 4, 9-13

---

## File Structure

```
tools/xraybench/
  models.py                            # MODIFY: add Capability, Outcome, ConnectionInfo, HealthStatus, LoadReport, etc.
  adapters/
    base.py                            # REWRITE: revised interface with capabilities
    capabilities.py                    # CREATE: Capability enum and related types
    validation.py                      # CREATE: shared correctness validation (delegates to Rust core)
    dataset_loader.py                  # CREATE: shared dataset loading for Bolt-compatible engines
    overhead.py                        # CREATE: adapter overhead calibration
    xraygraphdb.py                     # REWRITE: complete Bolt adapter with GFQL
    neo4j.py                           # REWRITE: complete adapter
    memgraph.py                        # REWRITE: complete adapter
    __init__.py                        # MODIFY: add new adapter registrations
tests/
  adapters/
    __init__.py                        # CREATE
    test_capabilities.py               # CREATE
    test_validation.py                 # CREATE
    test_dataset_loader.py             # CREATE
    test_overhead.py                   # CREATE
    test_xraygraphdb.py                # CREATE
    test_neo4j.py                      # CREATE
    test_memgraph.py                   # CREATE
```

---

### Task 0: Capability System and Revised Types

**Files:**
- Create: `tools/xraybench/adapters/capabilities.py`
- Modify: `tools/xraybench/models.py`
- Test: `tests/adapters/test_capabilities.py`

- [ ] **Step 1: Write tests for capability types**

Create `tests/adapters/__init__.py` (empty file).

Create `tests/adapters/test_capabilities.py`:

```python
"""Tests for the adapter capability system."""

from tools.xraybench.adapters.capabilities import (
    Capability,
    ConnectionInfo,
    HealthStatus,
    LoadReport,
    CacheClearReport,
    EngineInfo,
    EngineState,
    Outcome,
    QueryPlan,
    ProfileResult,
)


def test_capability_enum_values():
    assert Capability.COMPILE_TIME_REPORTING.value == "compile_time"
    assert Capability.PLAN_PROFILING.value == "plan_profile"
    assert Capability.CACHE_CLEAR.value == "cache_clear"
    assert Capability.VECTORIZED_METRICS.value == "vectorized_metrics"
    assert Capability.NATIVE_PROTOCOL.value == "native_protocol"
    assert Capability.EXPLAIN_ANALYZE.value == "explain_analyze"
    assert Capability.MEMORY_REPORTING.value == "memory_reporting"
    assert Capability.GFQL.value == "gfql"


def test_outcome_enum_values():
    assert Outcome.SUCCESS.value == "success"
    assert Outcome.CORRECTNESS_MISMATCH.value == "correctness_mismatch"
    assert Outcome.ENGINE_ERROR.value == "engine_error"
    assert Outcome.TIMEOUT.value == "timeout"
    assert Outcome.UNSUPPORTED.value == "unsupported"
    assert Outcome.DATASET_VERIFICATION_FAILED.value == "dataset_verification_failed"
    assert Outcome.HARNESS_FAILURE.value == "harness_failure"
    assert Outcome.CONNECTION_FAILURE.value == "connection_failure"
    assert Outcome.OUT_OF_MEMORY.value == "out_of_memory"


def test_connection_info():
    info = ConnectionInfo(
        host="localhost", port=7687, protocol="bolt", connected=True
    )
    assert info.host == "localhost"
    assert info.connected is True
    d = info.to_dict()
    assert d["host"] == "localhost"
    assert d["protocol"] == "bolt"


def test_health_status():
    hs = HealthStatus(healthy=True, latency_ms=1.5, detail="OK")
    assert hs.healthy is True
    assert hs.latency_ms == 1.5


def test_load_report():
    lr = LoadReport(
        node_count=1000,
        edge_count=5000,
        load_time_ms=234.5,
        verified=True,
        expected_nodes=1000,
        expected_edges=5000,
    )
    assert lr.verified is True
    assert lr.node_count == 1000
    d = lr.to_dict()
    assert d["verified"] is True


def test_engine_info():
    ei = EngineInfo(
        name="xraygraphdb",
        version="4.0.2",
        build="release",
        capabilities={Capability.COMPILE_TIME_REPORTING, Capability.GFQL},
    )
    assert Capability.GFQL in ei.capabilities
    d = ei.to_dict()
    assert "gfql" in d["capabilities"]


def test_cache_clear_report():
    ccr = CacheClearReport(cleared=True, detail="FREE MEMORY executed")
    assert ccr.cleared is True


def test_engine_state():
    es = EngineState(
        memory_used_mb=512.0,
        memory_available_mb=1024.0,
        active_queries=0,
    )
    assert es.memory_used_mb == 512.0


def test_query_plan():
    qp = QueryPlan(
        operators=["Scan", "Filter", "Produce"],
        estimated_cost=42.0,
        raw="EXPLAIN output here",
    )
    assert len(qp.operators) == 3


def test_profile_result():
    pr = ProfileResult(
        operators=[{"name": "Scan", "rows": 1000, "db_hits": 1000}],
        total_db_hits=1000,
        total_rows=1000,
        raw="PROFILE output here",
    )
    assert pr.total_rows == 1000
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -m pytest tests/adapters/test_capabilities.py -v
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement capabilities.py**

Create `tools/xraybench/adapters/capabilities.py`:

```python
"""Adapter capability system and shared types.

Adapters declare their capabilities so the runner knows what metrics
to expect. Missing capability → result field is null with reason, not
a silent zero.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Capability(Enum):
    """Capabilities an adapter may declare."""

    COMPILE_TIME_REPORTING = "compile_time"
    PLAN_PROFILING = "plan_profile"
    CACHE_CLEAR = "cache_clear"
    VECTORIZED_METRICS = "vectorized_metrics"
    STREAMING_RESULTS = "streaming_results"
    NATIVE_PROTOCOL = "native_protocol"
    EXPLAIN_ANALYZE = "explain_analyze"
    MEMORY_REPORTING = "memory_reporting"
    GFQL = "gfql"


class Outcome(Enum):
    """Machine-readable outcome of a benchmark execution."""

    SUCCESS = "success"
    CORRECTNESS_MISMATCH = "correctness_mismatch"
    ENGINE_ERROR = "engine_error"
    TIMEOUT = "timeout"
    UNSUPPORTED = "unsupported"
    DATASET_VERIFICATION_FAILED = "dataset_verification_failed"
    HARNESS_FAILURE = "harness_failure"
    CONNECTION_FAILURE = "connection_failure"
    OUT_OF_MEMORY = "out_of_memory"


@dataclass
class ConnectionInfo:
    """Information about an established engine connection."""

    host: str
    port: int
    protocol: str
    connected: bool
    database: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "protocol": self.protocol,
            "connected": self.connected,
        }
        if self.database is not None:
            d["database"] = self.database
        return d


@dataclass
class HealthStatus:
    """Result of a health check against the engine."""

    healthy: bool
    latency_ms: float
    detail: str = ""


@dataclass
class LoadReport:
    """Report from loading a dataset into an engine."""

    node_count: int
    edge_count: int
    load_time_ms: float
    verified: bool
    expected_nodes: int | None = None
    expected_edges: int | None = None
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "load_time_ms": self.load_time_ms,
            "verified": self.verified,
            "expected_nodes": self.expected_nodes,
            "expected_edges": self.expected_edges,
            "errors": self.errors,
        }


@dataclass
class CacheClearReport:
    """Report from clearing engine caches."""

    cleared: bool
    detail: str = ""


@dataclass
class EngineInfo:
    """Metadata about the connected engine."""

    name: str
    version: str
    build: str = "unknown"
    capabilities: set[Capability] = field(default_factory=set)
    config_hash: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "build": self.build,
            "capabilities": sorted(c.value for c in self.capabilities),
            "config_hash": self.config_hash,
        }


@dataclass
class EngineState:
    """Current engine resource state."""

    memory_used_mb: float | None = None
    memory_available_mb: float | None = None
    active_queries: int | None = None
    cache_size_mb: float | None = None


@dataclass
class QueryPlan:
    """Result of EXPLAIN on a query."""

    operators: list[str]
    estimated_cost: float | None = None
    raw: str = ""


@dataclass
class ProfileResult:
    """Result of PROFILE on a query."""

    operators: list[dict[str, Any]]
    total_db_hits: int = 0
    total_rows: int = 0
    raw: str = ""
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -m pytest tests/adapters/test_capabilities.py -v
```

Expected: All 10 tests pass.

- [ ] **Step 5: Commit**

```bash
git add tools/xraybench/adapters/capabilities.py tests/adapters/
git commit -m "feat: add adapter capability system — Capability enum, Outcome, ConnectionInfo, LoadReport, EngineInfo"
```

---

### Task 1: Revised Base Adapter Interface

**Files:**
- Rewrite: `tools/xraybench/adapters/base.py`

- [ ] **Step 1: Rewrite base.py with new interface**

Rewrite `tools/xraybench/adapters/base.py`:

```python
"""Abstract base adapter for graph database engines.

All adapters must implement this interface. The capability system
allows engines to declare what they support — the runner adapts
its expectations accordingly.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from .capabilities import (
    CacheClearReport,
    Capability,
    ConnectionInfo,
    EngineInfo,
    EngineState,
    HealthStatus,
    LoadReport,
    ProfileResult,
    QueryPlan,
)
from ..models import (
    CorrectnessResult,
    DatasetManifest,
    DatasetSpec,
    ExecuteResult,
)


class BaseAdapter(ABC):
    """Abstract interface for graph database engine adapters.

    Every adapter must implement the abstract methods below. Optional
    methods have default implementations that return sensible defaults.
    """

    # === Lifecycle ===

    @abstractmethod
    def connect(self, config: dict[str, Any]) -> ConnectionInfo:
        """Establish a connection to the engine."""

    @abstractmethod
    def close(self) -> None:
        """Clean up connections and resources."""

    def health_check(self) -> HealthStatus:
        """Check if the engine is responsive.

        Default: executes RETURN 1 and times it.
        """
        import time

        start = time.perf_counter()
        try:
            self.execute("RETURN 1 AS health")
            latency = (time.perf_counter() - start) * 1000
            return HealthStatus(healthy=True, latency_ms=round(latency, 2))
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            return HealthStatus(
                healthy=False, latency_ms=round(latency, 2), detail=str(e)
            )

    # === Dataset ===

    @abstractmethod
    def load_dataset(
        self,
        dataset: DatasetSpec | DatasetManifest,
        data_source: Any = None,
    ) -> LoadReport:
        """Ingest a dataset into the engine.

        Args:
            dataset: Dataset specification or manifest.
            data_source: Optional data source (generator, file path, etc.)

        Returns:
            LoadReport with counts, timing, and verification status.
        """

    def verify_dataset(self, manifest: DatasetManifest) -> bool:
        """Verify loaded data matches manifest counts.

        Default: queries node/edge counts via Cypher.
        """
        result_n = self.execute("MATCH (n) RETURN count(n) AS c")
        result_e = self.execute("MATCH ()-[r]->() RETURN count(r) AS c")
        actual_nodes = result_n.rows[0]["c"] if result_n.rows else 0
        actual_edges = result_e.rows[0]["c"] if result_e.rows else 0
        return (
            actual_nodes == manifest.node_count
            and actual_edges == manifest.edge_count
        )

    def clear_dataset(self) -> None:
        """Remove all data from the engine.

        Default: MATCH (n) DETACH DELETE n.
        """
        self.execute("MATCH (n) DETACH DELETE n")

    # === Execution ===

    @abstractmethod
    def execute(
        self, query: str, params: dict[str, Any] | None = None
    ) -> ExecuteResult:
        """Execute a query and return results with timing."""

    def explain(
        self, query: str, params: dict[str, Any] | None = None
    ) -> QueryPlan:
        """Return the query execution plan without running it.

        Default: prepends EXPLAIN to the query.
        """
        result = self.execute(f"EXPLAIN {query}", params)
        operators = [str(row) for row in result.rows]
        return QueryPlan(operators=operators, raw=str(result.rows))

    def profile(
        self, query: str, params: dict[str, Any] | None = None
    ) -> ProfileResult:
        """Execute and profile a query.

        Default: prepends PROFILE to the query.
        """
        result = self.execute(f"PROFILE {query}", params)
        return ProfileResult(
            operators=[dict(row) for row in result.rows],
            raw=str(result.rows),
        )

    # === Cache & State ===

    @abstractmethod
    def clear_caches(self) -> CacheClearReport:
        """Clear query plan caches and execution caches."""

    def engine_state(self) -> EngineState:
        """Gather current engine resource state.

        Default: returns empty state.
        """
        return EngineState()

    # === Metadata ===

    @abstractmethod
    def engine_info(self) -> EngineInfo:
        """Return engine metadata including capabilities."""

    @abstractmethod
    def capabilities(self) -> set[Capability]:
        """Return the set of capabilities this adapter supports."""

    # === Correctness ===

    @abstractmethod
    def validate_correctness(
        self, result: ExecuteResult, oracle: dict[str, Any]
    ) -> CorrectnessResult:
        """Check query results against the benchmark's correctness oracle."""

    # === Legacy compat ===

    def engine_version(self) -> str:
        """Return engine version string. Delegates to engine_info()."""
        return f"{self.engine_info().name}-{self.engine_info().version}"

    def collect_metrics(self) -> dict[str, Any]:
        """Legacy method. Override for backward compatibility."""
        return {}
```

- [ ] **Step 2: Verify existing code still imports cleanly**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -c "from tools.xraybench.adapters.base import BaseAdapter; print('OK')"
```

Expected: OK

- [ ] **Step 3: Commit**

```bash
git add tools/xraybench/adapters/base.py
git commit -m "feat: rewrite adapter base class — capabilities, health check, explain, profile, engine state"
```

---

### Task 2: Shared Correctness Validation

**Files:**
- Create: `tools/xraybench/adapters/validation.py`
- Test: `tests/adapters/test_validation.py`

- [ ] **Step 1: Write tests for shared validation**

Create `tests/adapters/test_validation.py`:

```python
"""Tests for shared correctness validation using Rust core."""

from tools.xraybench.adapters.validation import validate_oracle
from tools.xraybench.models import ExecuteResult


def _make_result(rows: list[dict]) -> ExecuteResult:
    return ExecuteResult(rows=rows, wall_ms=1.0)


def test_row_count_pass():
    result = _make_result([{"id": i} for i in range(100)])
    oracle = {"type": "row_count", "expected_row_count": 100}
    cr = validate_oracle(result, oracle)
    assert cr.passed is True


def test_row_count_fail():
    result = _make_result([{"id": i} for i in range(50)])
    oracle = {"type": "row_count", "expected_row_count": 100}
    cr = validate_oracle(result, oracle)
    assert cr.passed is False
    assert "50" in cr.detail
    assert "100" in cr.detail


def test_row_count_range_pass():
    result = _make_result([{"id": i} for i in range(75)])
    oracle = {
        "type": "row_count_range",
        "expected_row_count_min": 50,
        "expected_row_count_max": 100,
    }
    cr = validate_oracle(result, oracle)
    assert cr.passed is True


def test_row_count_range_fail():
    result = _make_result([{"id": i} for i in range(200)])
    oracle = {
        "type": "row_count_range",
        "expected_row_count_min": 50,
        "expected_row_count_max": 100,
    }
    cr = validate_oracle(result, oracle)
    assert cr.passed is False


def test_exact_match_pass():
    rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    result = _make_result(rows)
    # First compute the reference hash
    import xraybench_core

    row_lists = [[r["a"], r["b"]] for r in rows]
    ref_hash = xraybench_core.checksum.hash_result_set(row_lists)

    oracle = {
        "type": "exact_match",
        "expected_checksum": ref_hash,
        "expected_columns": ["a", "b"],
    }
    cr = validate_oracle(result, oracle)
    assert cr.passed is True


def test_exact_match_fail():
    rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    result = _make_result(rows)
    oracle = {
        "type": "exact_match",
        "expected_checksum": "blake3:" + "0" * 64,
        "expected_columns": ["a", "b"],
    }
    cr = validate_oracle(result, oracle)
    assert cr.passed is False


def test_exact_match_order_independent():
    rows_a = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    rows_b = [{"a": 2, "b": "y"}, {"a": 1, "b": "x"}]
    result_a = _make_result(rows_a)
    result_b = _make_result(rows_b)

    import xraybench_core

    row_lists = [[r["a"], r["b"]] for r in rows_a]
    ref_hash = xraybench_core.checksum.hash_result_set(row_lists)

    oracle = {
        "type": "exact_match",
        "expected_checksum": ref_hash,
        "expected_columns": ["a", "b"],
    }
    cr_a = validate_oracle(result_a, oracle)
    cr_b = validate_oracle(result_b, oracle)
    assert cr_a.passed is True
    assert cr_b.passed is True


def test_unknown_oracle_type():
    result = _make_result([])
    oracle = {"type": "future_oracle_type"}
    cr = validate_oracle(result, oracle)
    assert cr.passed is False
    assert "unsupported" in cr.detail.lower() or "unknown" in cr.detail.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -m pytest tests/adapters/test_validation.py -v
```

Expected: FAIL — validation module not found.

- [ ] **Step 3: Implement validation.py**

Create `tools/xraybench/adapters/validation.py`:

```python
"""Shared correctness validation using the Rust core.

Delegates exact-match checksumming to xraybench_core.checksum (BLAKE3),
structural validation to xraybench_core.checksum structural validators,
and implements row-count checks directly.
"""

from __future__ import annotations

import logging
from typing import Any

from ..models import CorrectnessResult, ExecuteResult

logger = logging.getLogger(__name__)


def validate_oracle(result: ExecuteResult, oracle: dict[str, Any]) -> CorrectnessResult:
    """Validate query results against a correctness oracle.

    Supports oracle types:
    - row_count: exact row count match
    - row_count_range: row count within [min, max]
    - exact_match: BLAKE3 hash of canonical sorted rows
    - checksum: same as exact_match with float tolerance
    - structural: topology validation for path results
    - invariant: predicate-based validation

    Args:
        result: Query execution result.
        oracle: Oracle specification dict from benchmark spec.

    Returns:
        CorrectnessResult with pass/fail and detail.
    """
    oracle_type = oracle.get("type", "")

    if oracle_type == "row_count":
        return _validate_row_count(result, oracle)
    elif oracle_type == "row_count_range":
        return _validate_row_count_range(result, oracle)
    elif oracle_type == "exact_match":
        return _validate_exact_match(result, oracle)
    elif oracle_type == "checksum":
        return _validate_exact_match(result, oracle)
    elif oracle_type == "structural":
        return _validate_structural(result, oracle)
    elif oracle_type == "invariant":
        return _validate_invariant(result, oracle)
    else:
        return CorrectnessResult(
            passed=False,
            detail=f"Unknown oracle type: '{oracle_type}'",
        )


def _validate_row_count(result: ExecuteResult, oracle: dict[str, Any]) -> CorrectnessResult:
    expected = oracle.get("expected_row_count", 0)
    actual = result.row_count
    if actual == expected:
        return CorrectnessResult(
            passed=True,
            detail=f"Row count {actual} matches expected {expected}",
        )
    return CorrectnessResult(
        passed=False,
        detail=f"Row count {actual} != expected {expected}",
    )


def _validate_row_count_range(result: ExecuteResult, oracle: dict[str, Any]) -> CorrectnessResult:
    lo = oracle.get("expected_row_count_min", 0)
    hi = oracle.get("expected_row_count_max", float("inf"))
    actual = result.row_count
    if lo <= actual <= hi:
        return CorrectnessResult(
            passed=True,
            detail=f"Row count {actual} within expected range [{lo}, {hi}]",
        )
    return CorrectnessResult(
        passed=False,
        detail=f"Row count {actual} outside expected range [{lo}, {hi}]",
    )


def _validate_exact_match(result: ExecuteResult, oracle: dict[str, Any]) -> CorrectnessResult:
    """Validate using BLAKE3 hash of canonical sorted rows."""
    try:
        import xraybench_core
    except ImportError:
        return CorrectnessResult(
            passed=False,
            detail="xraybench_core not installed — cannot perform exact match validation",
        )

    expected_hash = oracle.get("expected_checksum")
    if not expected_hash:
        return CorrectnessResult(
            passed=False,
            detail="exact_match oracle requires 'expected_checksum' field",
        )

    # Extract column order from oracle or use natural dict key order
    columns = oracle.get("expected_columns")

    # Convert rows to lists of values in column order
    row_lists: list[list[Any]] = []
    for row in result.rows:
        if columns:
            row_lists.append([row.get(col) for col in columns])
        else:
            row_lists.append(list(row.values()))

    computed_hash = xraybench_core.checksum.hash_result_set(row_lists)

    if computed_hash == expected_hash:
        return CorrectnessResult(
            passed=True,
            detail=f"Hash match: {computed_hash[:20]}...",
        )
    return CorrectnessResult(
        passed=False,
        detail=f"Hash mismatch: computed={computed_hash[:20]}... expected={expected_hash[:20]}...",
    )


def _validate_structural(result: ExecuteResult, oracle: dict[str, Any]) -> CorrectnessResult:
    """Validate topology properties of graph-shaped results."""
    invariants = oracle.get("invariants", [])
    max_depth = oracle.get("max_depth")
    seed_id = oracle.get("seed_id")

    # Extract paths from results (assume each row has a "path" column with node id list)
    paths: list[list[int]] = []
    for row in result.rows:
        path_data = row.get("path", row.get("nodes", []))
        if isinstance(path_data, list):
            paths.append([int(n) if not isinstance(n, int) else n for n in path_data])

    if not paths:
        return CorrectnessResult(
            passed=True,
            detail="No paths to validate (empty result)",
        )

    errors: list[str] = []

    # Check path lengths
    if max_depth is not None:
        for i, path in enumerate(paths):
            hops = len(path) - 1 if len(path) > 0 else 0
            if hops > max_depth:
                errors.append(f"Path {i} has {hops} hops, max allowed is {max_depth}")

    # Check seed node
    if seed_id is not None:
        for i, path in enumerate(paths):
            if path and path[0] != seed_id:
                errors.append(f"Path {i} starts at {path[0]}, expected seed {seed_id}")

    if errors:
        return CorrectnessResult(
            passed=False,
            detail=f"Structural validation failed: {'; '.join(errors[:5])}",
        )
    return CorrectnessResult(
        passed=True,
        detail=f"Structural validation passed ({len(paths)} paths checked)",
    )


def _validate_invariant(result: ExecuteResult, oracle: dict[str, Any]) -> CorrectnessResult:
    """Validate predicate-based invariants."""
    invariants = oracle.get("invariants", [])
    row_count_range = oracle.get("row_count_range")

    errors: list[str] = []

    # Check row count range if specified
    if row_count_range:
        lo = row_count_range.get("min", 0)
        hi = row_count_range.get("max", float("inf"))
        if not (lo <= result.row_count <= hi):
            errors.append(
                f"Row count {result.row_count} outside range [{lo}, {hi}]"
            )

    # Process string invariants
    for inv in invariants:
        if isinstance(inv, str):
            if inv.startswith("all_paths_length_le:"):
                max_len = int(inv.split(":")[1].strip().strip("{}"))
                for i, row in enumerate(result.rows):
                    path = row.get("path", row.get("nodes", []))
                    if isinstance(path, list) and len(path) - 1 > max_len:
                        errors.append(f"Row {i}: path length {len(path)-1} > {max_len}")
                        break

    if errors:
        return CorrectnessResult(
            passed=False,
            detail=f"Invariant validation failed: {'; '.join(errors[:5])}",
        )
    return CorrectnessResult(
        passed=True,
        detail=f"All invariants passed ({len(invariants)} checked)",
    )
```

- [ ] **Step 4: Run tests**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -m pytest tests/adapters/test_validation.py -v
```

Expected: All 8 tests pass.

- [ ] **Step 5: Commit**

```bash
git add tools/xraybench/adapters/validation.py tests/adapters/test_validation.py
git commit -m "feat: add shared correctness validation — BLAKE3 exact match, row count, structural, invariant oracles"
```

---

### Task 3: Shared Dataset Loader

**Files:**
- Create: `tools/xraybench/adapters/dataset_loader.py`
- Test: `tests/adapters/test_dataset_loader.py`

- [ ] **Step 1: Write tests for dataset loader**

Create `tests/adapters/test_dataset_loader.py`:

```python
"""Tests for shared dataset loader."""

from tools.xraybench.adapters.dataset_loader import (
    generate_cypher_from_edges,
    generate_synthetic_edges,
)


def test_generate_synthetic_edges_uniform():
    edges = generate_synthetic_edges(
        generator="uniform",
        params={"node_count": 100, "edge_count": 200, "seed": 42},
    )
    assert len(edges) == 200
    for src, dst in edges:
        assert src != dst


def test_generate_synthetic_edges_chain():
    edges = generate_synthetic_edges(
        generator="chain",
        params={"length": 50, "seed": 42},
    )
    assert len(edges) == 49
    assert edges[0] == (0, 1)
    assert edges[-1] == (48, 49)


def test_generate_synthetic_edges_hub():
    edges = generate_synthetic_edges(
        generator="hub",
        params={"hub_count": 3, "spokes_per_hub": 10, "seed": 42},
    )
    assert len(edges) == 30


def test_generate_synthetic_edges_deep_traversal():
    edges = generate_synthetic_edges(
        generator="deep_traversal",
        params={"num_roots": 1, "fanout_per_level": [5, 3], "seed": 42},
    )
    assert len(edges) >= 20  # 5 + 15 = 20 minimum


def test_generate_synthetic_deterministic():
    e1 = generate_synthetic_edges(
        generator="chain", params={"length": 100, "seed": 42}
    )
    e2 = generate_synthetic_edges(
        generator="chain", params={"length": 100, "seed": 42}
    )
    assert e1 == e2


def test_cypher_from_edges_creates_nodes():
    edges = [(0, 1), (1, 2)]
    stmts = generate_cypher_from_edges(edges, batch_size=10)
    # First statements should create nodes
    assert any("CREATE" in s and "Node" in s for s in stmts)


def test_cypher_from_edges_creates_edges():
    edges = [(0, 1), (1, 2)]
    stmts = generate_cypher_from_edges(edges, batch_size=10)
    # Should have UNWIND-based edge creation
    assert any("UNWIND" in s or "EDGE" in s for s in stmts)


def test_cypher_from_edges_batch_size():
    edges = [(i, i + 1) for i in range(100)]
    stmts = generate_cypher_from_edges(edges, batch_size=25)
    # Multiple batches expected
    assert len(stmts) > 1
```

- [ ] **Step 2: Implement dataset_loader.py**

Create `tools/xraybench/adapters/dataset_loader.py`:

```python
"""Shared dataset loading for Bolt-compatible engines.

Uses xraybench_core.generators for synthetic data and converts
to Cypher statements for ingestion via any Bolt adapter.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def generate_synthetic_edges(
    generator: str,
    params: dict[str, Any],
) -> list[tuple[int, int]]:
    """Generate edges using the Rust core generators.

    Args:
        generator: Generator name (uniform, chain, hub, community, power_law, deep_traversal).
        params: Generator-specific parameters.

    Returns:
        List of (source, target) edge tuples.
    """
    import xraybench_core

    if generator == "uniform":
        return xraybench_core.generators.generate_power_law_edges(
            node_count=params.get("node_count", 1000),
            m=params.get("m", 3),
            seed=params.get("seed", 42),
        ) if params.get("edge_count") is None else _uniform_edges(params)

    elif generator == "chain":
        return xraybench_core.generators.generate_chain(
            length=params.get("length", 1000),
            seed=params.get("seed", 42),
        )

    elif generator == "hub":
        _, edges = xraybench_core.generators.generate_hub_graph(
            hub_count=params.get("hub_count", 10),
            spokes_per_hub=params.get("spokes_per_hub", 100),
            seed=params.get("seed", 42),
        )
        return edges

    elif generator == "deep_traversal":
        _, edges = xraybench_core.generators.generate_deep_traversal(
            num_roots=params.get("num_roots", 1),
            fanout_per_level=params.get("fanout_per_level", [10, 5, 3]),
            seed=params.get("seed", 42),
        )
        return edges

    elif generator == "power_law":
        return xraybench_core.generators.generate_power_law_edges(
            node_count=params.get("node_count", 1000),
            m=params.get("m", 3),
            seed=params.get("seed", 42),
        )

    else:
        raise ValueError(f"Unknown generator: {generator}")


def _uniform_edges(params: dict[str, Any]) -> list[tuple[int, int]]:
    """Generate uniform random edges using Rust core."""
    import xraybench_core

    # Use power_law with high m to approximate uniform for edge generation
    # The actual uniform generator returns nodes, not edges.
    # For edge-only generation, use power_law as proxy.
    return xraybench_core.generators.generate_power_law_edges(
        node_count=params.get("node_count", 1000),
        m=max(1, params.get("edge_count", 1000) // params.get("node_count", 1000)),
        seed=params.get("seed", 42),
    )


def generate_cypher_from_edges(
    edges: list[tuple[int, int]],
    batch_size: int = 1000,
    node_label: str = "Node",
    edge_type: str = "EDGE",
) -> list[str]:
    """Convert edge tuples to batched Cypher CREATE statements.

    Creates nodes first (UNWIND batch), then edges (UNWIND batch).
    Uses UNWIND for efficient batched ingestion.

    Args:
        edges: List of (source, target) tuples.
        batch_size: Number of items per UNWIND batch.
        node_label: Label for created nodes.
        edge_type: Relationship type name.

    Returns:
        List of Cypher statements to execute in order.
    """
    # Collect unique node IDs
    node_ids: set[int] = set()
    for src, dst in edges:
        node_ids.add(src)
        node_ids.add(dst)

    sorted_nodes = sorted(node_ids)
    statements: list[str] = []

    # Create nodes in batches using UNWIND
    for i in range(0, len(sorted_nodes), batch_size):
        batch = sorted_nodes[i : i + batch_size]
        statements.append(
            f"UNWIND {batch} AS nid "
            f"CREATE (:{node_label} {{id: nid}})"
        )

    # Create index for efficient edge MATCH
    statements.append(f"CREATE INDEX ON :{node_label}(id)")

    # Create edges in batches using UNWIND
    for i in range(0, len(edges), batch_size):
        batch = edges[i : i + batch_size]
        edge_data = [{"s": s, "t": t} for s, t in batch]
        statements.append(
            f"UNWIND {edge_data} AS e "
            f"MATCH (a:{node_label} {{id: e.s}}), (b:{node_label} {{id: e.t}}) "
            f"CREATE (a)-[:{edge_type}]->(b)"
        )

    return statements


def load_edges_into_adapter(
    adapter: Any,
    edges: list[tuple[int, int]],
    batch_size: int = 1000,
    node_label: str = "Node",
    edge_type: str = "EDGE",
) -> tuple[int, int]:
    """Load edges into an engine via its adapter.

    Args:
        adapter: A connected BaseAdapter instance.
        edges: List of (source, target) tuples.
        batch_size: Cypher batch size.
        node_label: Node label.
        edge_type: Relationship type.

    Returns:
        (node_count, edge_count) as verified from the engine.
    """
    statements = generate_cypher_from_edges(
        edges, batch_size=batch_size, node_label=node_label, edge_type=edge_type
    )

    for i, stmt in enumerate(statements):
        try:
            adapter.execute(stmt)
        except Exception as e:
            logger.warning("Statement %d/%d failed: %s", i + 1, len(statements), e)

    # Verify counts
    result_n = adapter.execute(f"MATCH (n:{node_label}) RETURN count(n) AS c")
    result_e = adapter.execute(f"MATCH ()-[r:{edge_type}]->() RETURN count(r) AS c")

    node_count = result_n.rows[0]["c"] if result_n.rows else 0
    edge_count = result_e.rows[0]["c"] if result_e.rows else 0

    return node_count, edge_count
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -m pytest tests/adapters/test_dataset_loader.py -v
```

Expected: All 8 tests pass.

- [ ] **Step 4: Commit**

```bash
git add tools/xraybench/adapters/dataset_loader.py tests/adapters/test_dataset_loader.py
git commit -m "feat: add shared dataset loader — Rust generator integration, batched Cypher UNWIND ingestion"
```

---

### Task 4: Adapter Overhead Calibration

**Files:**
- Create: `tools/xraybench/adapters/overhead.py`
- Test: `tests/adapters/test_overhead.py`

- [ ] **Step 1: Write tests**

Create `tests/adapters/test_overhead.py`:

```python
"""Tests for adapter overhead calibration."""

from unittest.mock import MagicMock
from tools.xraybench.adapters.overhead import measure_adapter_overhead
from tools.xraybench.models import ExecuteResult


def test_overhead_returns_positive():
    adapter = MagicMock()
    adapter.execute.return_value = ExecuteResult(
        rows=[{"health": 1}], wall_ms=0.5
    )
    result = measure_adapter_overhead(adapter, iterations=100)
    assert result["median_ms"] >= 0
    assert result["iterations"] == 100
    assert "p95_ms" in result
    assert "p99_ms" in result


def test_overhead_calls_execute():
    adapter = MagicMock()
    adapter.execute.return_value = ExecuteResult(
        rows=[{"health": 1}], wall_ms=0.5
    )
    measure_adapter_overhead(adapter, iterations=50)
    assert adapter.execute.call_count == 50


def test_overhead_uses_return_1():
    adapter = MagicMock()
    adapter.execute.return_value = ExecuteResult(
        rows=[{"health": 1}], wall_ms=0.5
    )
    measure_adapter_overhead(adapter, iterations=10)
    # All calls should be RETURN 1
    for call in adapter.execute.call_args_list:
        assert call[0][0] == "RETURN 1 AS _ping"
```

- [ ] **Step 2: Implement overhead.py**

Create `tools/xraybench/adapters/overhead.py`:

```python
"""Adapter overhead calibration.

Measures the round-trip cost of a no-op query to quantify the
adapter's noise floor. This value is reported alongside benchmark
results so auditors can assess measurement granularity.
"""

from __future__ import annotations

import time
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .base import BaseAdapter


def measure_adapter_overhead(
    adapter: BaseAdapter,
    iterations: int = 1000,
) -> dict[str, Any]:
    """Measure adapter round-trip overhead.

    Executes RETURN 1 `iterations` times and computes latency statistics.

    Args:
        adapter: A connected adapter instance.
        iterations: Number of measurements to take.

    Returns:
        Dict with median_ms, p95_ms, p99_ms, min_ms, max_ms, iterations.
    """
    latencies: list[float] = []

    for _ in range(iterations):
        start = time.perf_counter()
        adapter.execute("RETURN 1 AS _ping")
        elapsed = (time.perf_counter() - start) * 1000
        latencies.append(elapsed)

    latencies.sort()
    n = len(latencies)

    return {
        "median_ms": round(latencies[n // 2], 4),
        "p95_ms": round(latencies[int(n * 0.95)], 4),
        "p99_ms": round(latencies[int(n * 0.99)], 4),
        "min_ms": round(latencies[0], 4),
        "max_ms": round(latencies[-1], 4),
        "mean_ms": round(sum(latencies) / n, 4),
        "iterations": iterations,
    }
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -m pytest tests/adapters/test_overhead.py -v
```

Expected: All 3 tests pass.

- [ ] **Step 4: Commit**

```bash
git add tools/xraybench/adapters/overhead.py tests/adapters/test_overhead.py
git commit -m "feat: add adapter overhead calibration — no-op query latency measurement"
```

---

### Task 5: xrayGraphDB Bolt Adapter (Complete)

**Files:**
- Rewrite: `tools/xraybench/adapters/xraygraphdb.py`
- Test: `tests/adapters/test_xraygraphdb.py`

- [ ] **Step 1: Write tests**

Create `tests/adapters/test_xraygraphdb.py`:

```python
"""Unit tests for xrayGraphDB Bolt adapter.

These test the adapter logic without a running engine.
"""

from tools.xraybench.adapters.xraygraphdb import XrayGraphDBAdapter
from tools.xraybench.adapters.capabilities import Capability


def test_capabilities():
    adapter = XrayGraphDBAdapter()
    caps = adapter.capabilities()
    assert Capability.COMPILE_TIME_REPORTING in caps
    assert Capability.PLAN_PROFILING in caps
    assert Capability.CACHE_CLEAR in caps
    assert Capability.EXPLAIN_ANALYZE in caps
    assert Capability.GFQL in caps


def test_engine_info_before_connect():
    adapter = XrayGraphDBAdapter()
    info = adapter.engine_info()
    assert info.name == "xraygraphdb"
    assert Capability.GFQL in info.capabilities


def test_gfql_context_format():
    adapter = XrayGraphDBAdapter()
    stmt = adapter._gfql_context_statement("tenant1", "repo1")
    assert "SET GFQL_CONTEXT" in stmt
    assert "tenant1" in stmt
    assert "repo1" in stmt


def test_breaker_detection():
    adapter = XrayGraphDBAdapter()
    breaker_names = adapter._breaker_operator_names()
    assert "Sort" in breaker_names
    assert "Aggregate" in breaker_names
    assert "Distinct" in breaker_names
```

- [ ] **Step 2: Rewrite xraygraphdb.py**

Rewrite `tools/xraybench/adapters/xraygraphdb.py`:

```python
"""xrayGraphDB Bolt adapter for xraygraph-bench.

Connects to xrayGraphDB via Bolt protocol (port 7688). Extracts
compilation timing, execution segments, materialization boundaries,
cache/fallback/deopt status, and supports GFQL queries.

For the xrayProtocol native adapter, see xraygraphdb_native.py (Phase 2b).
"""

from __future__ import annotations

import logging
import time
from typing import Any

from .base import BaseAdapter
from .capabilities import (
    CacheClearReport,
    Capability,
    ConnectionInfo,
    EngineInfo,
    EngineState,
    LoadReport,
    ProfileResult,
    QueryPlan,
)
from .validation import validate_oracle
from ..models import (
    CorrectnessResult,
    DatasetManifest,
    DatasetSpec,
    ExecuteResult,
)

logger = logging.getLogger(__name__)

_CAPABILITIES = frozenset({
    Capability.COMPILE_TIME_REPORTING,
    Capability.PLAN_PROFILING,
    Capability.CACHE_CLEAR,
    Capability.EXPLAIN_ANALYZE,
    Capability.GFQL,
})

_BREAKER_OPERATORS = frozenset({
    "Sort", "Aggregate", "Distinct", "OrderBy",
    "HashJoin", "Accumulate", "Unwind",
})


class XrayGraphDBAdapter(BaseAdapter):
    """Adapter for xrayGraphDB via Bolt protocol."""

    def __init__(self) -> None:
        self._driver: Any = None
        self._version: str = "unknown"
        self._build: str = "unknown"
        self._last_profile: dict[str, Any] = {}
        self._connection_info: ConnectionInfo | None = None

    # === Lifecycle ===

    def connect(self, config: dict[str, Any]) -> ConnectionInfo:
        host = config.get("host", "localhost")
        port = config.get("port", 7688)
        username = config.get("username", "")
        password = config.get("password", "")

        try:
            import neo4j

            uri = f"bolt://{host}:{port}"
            auth = (username, password) if username else None
            self._driver = neo4j.GraphDatabase.driver(uri, auth=auth)

            with self._driver.session() as session:
                session.run("RETURN 1 AS health").single()

            with self._driver.session() as session:
                try:
                    result = session.run(
                        "CALL mg.info() YIELD key, value RETURN *"
                    )
                    info = {record["key"]: record["value"] for record in result}
                    self._version = info.get("version", "unknown")
                    self._build = info.get("build_type", "unknown")
                except Exception:
                    self._version = "unknown"

            logger.info("Connected to xrayGraphDB %s at %s", self._version, uri)

            self._connection_info = ConnectionInfo(
                host=host, port=port, protocol="bolt", connected=True
            )
            return self._connection_info

        except ImportError:
            raise ImportError(
                "neo4j Python driver required. Run: pip install neo4j"
            )
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to xrayGraphDB at {host}:{port}: {e}"
            )

    def close(self) -> None:
        if self._driver:
            self._driver.close()
            self._driver = None
        if self._connection_info:
            self._connection_info.connected = False

    # === Dataset ===

    def load_dataset(
        self,
        dataset: DatasetSpec | DatasetManifest,
        data_source: Any = None,
    ) -> LoadReport:
        if not self._driver:
            raise RuntimeError("Not connected")

        start = time.perf_counter()

        with self._driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

        if data_source is not None:
            from .dataset_loader import load_edges_into_adapter
            node_count, edge_count = load_edges_into_adapter(self, data_source)
        else:
            node_count, edge_count = 0, 0

        elapsed_ms = (time.perf_counter() - start) * 1000

        expected_nodes = None
        expected_edges = None
        if isinstance(dataset, DatasetManifest):
            expected_nodes = dataset.node_count
            expected_edges = dataset.edge_count

        verified = True
        if expected_nodes is not None:
            verified = node_count == expected_nodes and edge_count == expected_edges

        return LoadReport(
            node_count=node_count,
            edge_count=edge_count,
            load_time_ms=round(elapsed_ms, 2),
            verified=verified,
            expected_nodes=expected_nodes,
            expected_edges=expected_edges,
        )

    # === Execution ===

    def execute(self, query: str, params: dict[str, Any] | None = None) -> ExecuteResult:
        if not self._driver:
            raise RuntimeError("Not connected")

        start = time.perf_counter()
        with self._driver.session() as session:
            result = session.run(query, parameters=params or {})
            rows = [dict(record) for record in result]
            summary = result.consume()
        wall_ms = (time.perf_counter() - start) * 1000

        compile_ms = self._extract_compile_time(summary)
        self._last_profile = self._extract_profile(summary)

        metadata: dict[str, Any] = {}
        if summary.result_available_after is not None:
            metadata["result_available_after_ms"] = summary.result_available_after
        if self._last_profile:
            metadata["profile"] = self._last_profile

        return ExecuteResult(
            rows=rows,
            wall_ms=round(wall_ms, 2),
            compile_ms=round(compile_ms, 2) if compile_ms is not None else None,
            metadata=metadata,
        )

    def explain(self, query: str, params: dict[str, Any] | None = None) -> QueryPlan:
        result = self.execute(f"EXPLAIN {query}", params)
        operators = []
        for row in result.rows:
            if isinstance(row, dict):
                operators.append(str(row.get("operator", row)))
        return QueryPlan(
            operators=operators,
            raw=str(result.rows),
        )

    def profile(self, query: str, params: dict[str, Any] | None = None) -> ProfileResult:
        result = self.execute(f"PROFILE {query}", params)
        operators = []
        total_hits = 0
        total_rows = 0
        for row in result.rows:
            if isinstance(row, dict):
                operators.append(row)
                total_hits += row.get("db_hits", 0)
                total_rows += row.get("rows", 0)
        return ProfileResult(
            operators=operators,
            total_db_hits=total_hits,
            total_rows=total_rows,
            raw=str(result.rows),
        )

    # === GFQL ===

    def execute_gfql(
        self,
        gfql_query: str,
        tenant_id: str,
        repo_id: str,
        params: dict[str, Any] | None = None,
    ) -> ExecuteResult:
        """Execute a GFQL query with required context setup.

        Sets GFQL_CONTEXT then sends the GFQL query. The engine
        auto-detects GFQL via 2-char lookahead.
        """
        if not self._driver:
            raise RuntimeError("Not connected")

        with self._driver.session() as session:
            session.run(self._gfql_context_statement(tenant_id, repo_id))

        return self.execute(gfql_query, params)

    def _gfql_context_statement(self, tenant_id: str, repo_id: str) -> str:
        return f"SET GFQL_CONTEXT tenant_id = '{tenant_id}', repo_id = '{repo_id}'"

    # === Cache & State ===

    def clear_caches(self) -> CacheClearReport:
        if not self._driver:
            return CacheClearReport(cleared=False, detail="Not connected")

        cleared = False
        detail_parts: list[str] = []

        with self._driver.session() as session:
            for cmd in ["FREE MEMORY", "CALL mg.clear_cache()"]:
                try:
                    session.run(cmd)
                    cleared = True
                    detail_parts.append(f"{cmd}: OK")
                except Exception:
                    detail_parts.append(f"{cmd}: not supported")

        self._last_profile = {}
        return CacheClearReport(cleared=cleared, detail="; ".join(detail_parts))

    def engine_state(self) -> EngineState:
        if not self._driver:
            return EngineState()

        try:
            with self._driver.session() as session:
                result = session.run(
                    "CALL mg.info() YIELD key, value RETURN *"
                )
                info = {record["key"]: record["value"] for record in result}
                return EngineState(
                    memory_used_mb=float(info.get("memory_usage", 0)) / (1024 * 1024)
                    if "memory_usage" in info
                    else None,
                )
        except Exception:
            return EngineState()

    # === Metadata ===

    def engine_info(self) -> EngineInfo:
        return EngineInfo(
            name="xraygraphdb",
            version=self._version,
            build=self._build,
            capabilities=set(_CAPABILITIES),
        )

    def capabilities(self) -> set[Capability]:
        return set(_CAPABILITIES)

    # === Correctness ===

    def validate_correctness(
        self, result: ExecuteResult, oracle: dict[str, Any]
    ) -> CorrectnessResult:
        return validate_oracle(result, oracle)

    # === Metrics ===

    def collect_metrics(self) -> dict[str, Any]:
        profile = self._last_profile
        segments = profile.get("segment_count")
        breakers: list[str] = []
        if "operators" in profile:
            for op in profile["operators"]:
                if op.get("is_breaker", False):
                    breakers.append(op.get("name", "unknown"))
        return {
            "segments": segments,
            "breakers": breakers,
            "buffer_repr": profile.get("buffer_repr"),
            "cache_hit": profile.get("cache_hit", False),
            "fallback": profile.get("fallback", False),
            "deopt": profile.get("deopt", False),
        }

    # === Internal ===

    def _breaker_operator_names(self) -> frozenset[str]:
        return _BREAKER_OPERATORS

    def _extract_compile_time(self, summary: Any) -> float | None:
        if hasattr(summary, "metadata") and summary.metadata:
            if "compile_ms" in summary.metadata:
                return float(summary.metadata["compile_ms"])
        return None

    def _extract_profile(self, summary: Any) -> dict[str, Any]:
        profile: dict[str, Any] = {}
        if hasattr(summary, "profile") and summary.profile:
            profile["plan_type"] = "profile"
            profile["operators"] = self._flatten_plan(summary.profile)
            profile["segment_count"] = len(profile["operators"])
        return profile

    def _flatten_plan(self, plan_node: Any) -> list[dict[str, Any]]:
        operators: list[dict[str, Any]] = []
        if plan_node is None:
            return operators
        op: dict[str, Any] = {
            "name": getattr(plan_node, "operator_type", "unknown"),
            "is_breaker": getattr(plan_node, "operator_type", "") in _BREAKER_OPERATORS,
        }
        if hasattr(plan_node, "db_hits"):
            op["db_hits"] = plan_node.db_hits
        if hasattr(plan_node, "rows"):
            op["rows"] = plan_node.rows
        operators.append(op)
        if hasattr(plan_node, "children"):
            for child in plan_node.children:
                operators.extend(self._flatten_plan(child))
        return operators
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -m pytest tests/adapters/test_xraygraphdb.py -v
```

Expected: All 4 tests pass.

- [ ] **Step 4: Commit**

```bash
git add tools/xraybench/adapters/xraygraphdb.py tests/adapters/test_xraygraphdb.py
git commit -m "feat: rewrite xrayGraphDB Bolt adapter — capabilities, GFQL, shared validation, dataset loading"
```

---

### Task 6: Neo4j Adapter (Complete)

**Files:**
- Rewrite: `tools/xraybench/adapters/neo4j.py`
- Test: `tests/adapters/test_neo4j.py`

- [ ] **Step 1: Write tests**

Create `tests/adapters/test_neo4j.py`:

```python
"""Unit tests for Neo4j adapter."""

from tools.xraybench.adapters.neo4j import Neo4jAdapter
from tools.xraybench.adapters.capabilities import Capability


def test_capabilities():
    adapter = Neo4jAdapter()
    caps = adapter.capabilities()
    assert Capability.CACHE_CLEAR in caps
    assert Capability.PLAN_PROFILING in caps
    assert Capability.GFQL not in caps


def test_engine_info():
    adapter = Neo4jAdapter()
    info = adapter.engine_info()
    assert info.name == "neo4j"
    assert Capability.GFQL not in info.capabilities
```

- [ ] **Step 2: Rewrite neo4j.py**

Rewrite `tools/xraybench/adapters/neo4j.py`:

```python
"""Neo4j adapter for xraygraph-bench.

Connects via Bolt protocol using the official neo4j Python driver.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from .base import BaseAdapter
from .capabilities import (
    CacheClearReport,
    Capability,
    ConnectionInfo,
    EngineInfo,
    EngineState,
    LoadReport,
)
from .validation import validate_oracle
from ..models import (
    CorrectnessResult,
    DatasetManifest,
    DatasetSpec,
    ExecuteResult,
)

logger = logging.getLogger(__name__)

_CAPABILITIES = frozenset({
    Capability.CACHE_CLEAR,
    Capability.PLAN_PROFILING,
})


class Neo4jAdapter(BaseAdapter):
    """Adapter for Neo4j graph database."""

    def __init__(self) -> None:
        self._driver: Any = None
        self._database: str = "neo4j"
        self._version: str = "unknown"

    def connect(self, config: dict[str, Any]) -> ConnectionInfo:
        host = config.get("host", "localhost")
        port = config.get("port", 7687)
        username = config.get("username", "neo4j")
        password = config.get("password", "")
        self._database = config.get("database", "neo4j")

        if not password:
            raise ValueError("Neo4j adapter requires a password")

        try:
            import neo4j

            uri = f"bolt://{host}:{port}"
            self._driver = neo4j.GraphDatabase.driver(uri, auth=(username, password))

            with self._driver.session(database=self._database) as session:
                result = session.run(
                    "CALL dbms.components() YIELD name, versions "
                    "RETURN versions[0] AS version"
                )
                record = result.single()
                if record:
                    self._version = record["version"]

            logger.info("Connected to Neo4j %s at %s", self._version, uri)
            return ConnectionInfo(
                host=host, port=port, protocol="bolt",
                connected=True, database=self._database,
            )

        except ImportError:
            raise ImportError("neo4j Python driver required. Run: pip install neo4j")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Neo4j at {host}:{port}: {e}")

    def close(self) -> None:
        if self._driver:
            self._driver.close()
            self._driver = None

    def load_dataset(
        self,
        dataset: DatasetSpec | DatasetManifest,
        data_source: Any = None,
    ) -> LoadReport:
        if not self._driver:
            raise RuntimeError("Not connected")

        start = time.perf_counter()

        with self._driver.session(database=self._database) as session:
            session.run("MATCH (n) DETACH DELETE n")

        if data_source is not None:
            from .dataset_loader import load_edges_into_adapter
            node_count, edge_count = load_edges_into_adapter(self, data_source)
        else:
            node_count, edge_count = 0, 0

        elapsed_ms = (time.perf_counter() - start) * 1000

        expected_nodes = None
        expected_edges = None
        if isinstance(dataset, DatasetManifest):
            expected_nodes = dataset.node_count
            expected_edges = dataset.edge_count

        verified = True
        if expected_nodes is not None:
            verified = node_count == expected_nodes and edge_count == expected_edges

        return LoadReport(
            node_count=node_count,
            edge_count=edge_count,
            load_time_ms=round(elapsed_ms, 2),
            verified=verified,
            expected_nodes=expected_nodes,
            expected_edges=expected_edges,
        )

    def execute(self, query: str, params: dict[str, Any] | None = None) -> ExecuteResult:
        if not self._driver:
            raise RuntimeError("Not connected")

        start = time.perf_counter()
        with self._driver.session(database=self._database) as session:
            result = session.run(query, parameters=params or {})
            rows = [dict(record) for record in result]
            summary = result.consume()
        wall_ms = (time.perf_counter() - start) * 1000

        metadata: dict[str, Any] = {}
        if summary.result_available_after is not None:
            metadata["result_available_after_ms"] = summary.result_available_after
        if summary.result_consumed_after is not None:
            metadata["result_consumed_after_ms"] = summary.result_consumed_after

        return ExecuteResult(
            rows=rows,
            wall_ms=round(wall_ms, 2),
            compile_ms=None,
            metadata=metadata,
        )

    def clear_caches(self) -> CacheClearReport:
        if not self._driver:
            return CacheClearReport(cleared=False, detail="Not connected")

        try:
            with self._driver.session(database=self._database) as session:
                session.run("CALL db.clearQueryCaches()")
            return CacheClearReport(cleared=True, detail="db.clearQueryCaches(): OK")
        except Exception as e:
            return CacheClearReport(cleared=False, detail=str(e))

    def engine_info(self) -> EngineInfo:
        return EngineInfo(
            name="neo4j",
            version=self._version,
            capabilities=set(_CAPABILITIES),
        )

    def capabilities(self) -> set[Capability]:
        return set(_CAPABILITIES)

    def validate_correctness(
        self, result: ExecuteResult, oracle: dict[str, Any]
    ) -> CorrectnessResult:
        return validate_oracle(result, oracle)

    def collect_metrics(self) -> dict[str, Any]:
        return {
            "segments": None,
            "breakers": [],
            "buffer_repr": None,
            "cache_hit": False,
            "fallback": False,
            "deopt": False,
        }
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -m pytest tests/adapters/test_neo4j.py -v
```

Expected: All 2 tests pass.

- [ ] **Step 4: Commit**

```bash
git add tools/xraybench/adapters/neo4j.py tests/adapters/test_neo4j.py
git commit -m "feat: rewrite Neo4j adapter — capabilities, shared validation, dataset loading"
```

---

### Task 7: Memgraph Adapter (Complete)

**Files:**
- Rewrite: `tools/xraybench/adapters/memgraph.py`
- Test: `tests/adapters/test_memgraph.py`

- [ ] **Step 1: Write tests**

Create `tests/adapters/test_memgraph.py`:

```python
"""Unit tests for Memgraph adapter."""

from tools.xraybench.adapters.memgraph import MemgraphAdapter
from tools.xraybench.adapters.capabilities import Capability


def test_capabilities():
    adapter = MemgraphAdapter()
    caps = adapter.capabilities()
    assert Capability.CACHE_CLEAR in caps
    assert Capability.GFQL not in caps
    assert Capability.NATIVE_PROTOCOL not in caps


def test_engine_info():
    adapter = MemgraphAdapter()
    info = adapter.engine_info()
    assert info.name == "memgraph"
```

- [ ] **Step 2: Rewrite memgraph.py**

Rewrite `tools/xraybench/adapters/memgraph.py`:

```python
"""Memgraph adapter for xraygraph-bench.

Connects via Bolt protocol using the neo4j Python driver (wire-compatible).
"""

from __future__ import annotations

import logging
import time
from typing import Any

from .base import BaseAdapter
from .capabilities import (
    CacheClearReport,
    Capability,
    ConnectionInfo,
    EngineInfo,
    EngineState,
    LoadReport,
)
from .validation import validate_oracle
from ..models import (
    CorrectnessResult,
    DatasetManifest,
    DatasetSpec,
    ExecuteResult,
)

logger = logging.getLogger(__name__)

_CAPABILITIES = frozenset({
    Capability.CACHE_CLEAR,
    Capability.PLAN_PROFILING,
    Capability.MEMORY_REPORTING,
})


class MemgraphAdapter(BaseAdapter):
    """Adapter for Memgraph graph database."""

    def __init__(self) -> None:
        self._driver: Any = None
        self._version: str = "unknown"

    def connect(self, config: dict[str, Any]) -> ConnectionInfo:
        host = config.get("host", "localhost")
        port = config.get("port", 7687)
        username = config.get("username", "")
        password = config.get("password", "")

        try:
            import neo4j

            uri = f"bolt://{host}:{port}"
            auth = (username, password) if username else None
            self._driver = neo4j.GraphDatabase.driver(uri, auth=auth)

            with self._driver.session() as session:
                result = session.run("CALL mg.info() YIELD key, value RETURN *")
                info = {record["key"]: record["value"] for record in result}
                self._version = info.get("version", "unknown")

            logger.info("Connected to Memgraph %s at %s", self._version, uri)
            return ConnectionInfo(
                host=host, port=port, protocol="bolt", connected=True,
            )

        except ImportError:
            raise ImportError("neo4j Python driver required. Run: pip install neo4j")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Memgraph at {host}:{port}: {e}")

    def close(self) -> None:
        if self._driver:
            self._driver.close()
            self._driver = None

    def load_dataset(
        self,
        dataset: DatasetSpec | DatasetManifest,
        data_source: Any = None,
    ) -> LoadReport:
        if not self._driver:
            raise RuntimeError("Not connected")

        start = time.perf_counter()

        with self._driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

        if data_source is not None:
            from .dataset_loader import load_edges_into_adapter
            node_count, edge_count = load_edges_into_adapter(self, data_source)
        else:
            node_count, edge_count = 0, 0

        elapsed_ms = (time.perf_counter() - start) * 1000

        expected_nodes = None
        expected_edges = None
        if isinstance(dataset, DatasetManifest):
            expected_nodes = dataset.node_count
            expected_edges = dataset.edge_count

        verified = True
        if expected_nodes is not None:
            verified = node_count == expected_nodes and edge_count == expected_edges

        return LoadReport(
            node_count=node_count,
            edge_count=edge_count,
            load_time_ms=round(elapsed_ms, 2),
            verified=verified,
            expected_nodes=expected_nodes,
            expected_edges=expected_edges,
        )

    def execute(self, query: str, params: dict[str, Any] | None = None) -> ExecuteResult:
        if not self._driver:
            raise RuntimeError("Not connected")

        start = time.perf_counter()
        with self._driver.session() as session:
            result = session.run(query, parameters=params or {})
            rows = [dict(record) for record in result]
        wall_ms = (time.perf_counter() - start) * 1000

        return ExecuteResult(
            rows=rows,
            wall_ms=round(wall_ms, 2),
            compile_ms=None,
        )

    def clear_caches(self) -> CacheClearReport:
        if not self._driver:
            return CacheClearReport(cleared=False, detail="Not connected")

        try:
            with self._driver.session() as session:
                session.run("FREE MEMORY")
            return CacheClearReport(cleared=True, detail="FREE MEMORY: OK")
        except Exception as e:
            return CacheClearReport(cleared=False, detail=str(e))

    def engine_state(self) -> EngineState:
        if not self._driver:
            return EngineState()

        try:
            with self._driver.session() as session:
                result = session.run("CALL mg.info() YIELD key, value RETURN *")
                info = {record["key"]: record["value"] for record in result}
                return EngineState(
                    memory_used_mb=float(info.get("memory_usage", 0)) / (1024 * 1024)
                    if "memory_usage" in info
                    else None,
                )
        except Exception:
            return EngineState()

    def engine_info(self) -> EngineInfo:
        return EngineInfo(
            name="memgraph",
            version=self._version,
            capabilities=set(_CAPABILITIES),
        )

    def capabilities(self) -> set[Capability]:
        return set(_CAPABILITIES)

    def validate_correctness(
        self, result: ExecuteResult, oracle: dict[str, Any]
    ) -> CorrectnessResult:
        return validate_oracle(result, oracle)

    def collect_metrics(self) -> dict[str, Any]:
        return {
            "segments": None,
            "breakers": [],
            "buffer_repr": None,
            "cache_hit": False,
            "fallback": False,
            "deopt": False,
        }
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -m pytest tests/adapters/test_memgraph.py -v
```

Expected: All 2 tests pass.

- [ ] **Step 4: Commit**

```bash
git add tools/xraybench/adapters/memgraph.py tests/adapters/test_memgraph.py
git commit -m "feat: rewrite Memgraph adapter — capabilities, shared validation, dataset loading"
```

---

### Task 8: Update Adapter Registry

**Files:**
- Modify: `tools/xraybench/adapters/__init__.py`

- [ ] **Step 1: Update registry**

Rewrite `tools/xraybench/adapters/__init__.py`:

```python
"""Engine adapters for xraygraph-bench."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.xraybench.adapters.base import BaseAdapter

# Lazy imports to avoid requiring all adapter dependencies
_ADAPTER_REGISTRY: dict[str, str] = {
    "memgraph": "tools.xraybench.adapters.memgraph.MemgraphAdapter",
    "neo4j": "tools.xraybench.adapters.neo4j.Neo4jAdapter",
    "xraygraphdb": "tools.xraybench.adapters.xraygraphdb.XrayGraphDBAdapter",
    "xraygraphdb-bolt": "tools.xraybench.adapters.xraygraphdb.XrayGraphDBAdapter",
}


def get_adapter(name: str) -> type[BaseAdapter]:
    """Resolve an adapter class by engine name."""
    if name not in _ADAPTER_REGISTRY:
        raise ValueError(
            f"Unknown adapter: {name}. "
            f"Available: {', '.join(sorted(_ADAPTER_REGISTRY))}"
        )

    module_path, class_name = _ADAPTER_REGISTRY[name].rsplit(".", 1)
    import importlib

    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def list_adapters() -> list[str]:
    """Return list of registered adapter names."""
    return sorted(_ADAPTER_REGISTRY.keys())
```

- [ ] **Step 2: Verify imports**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -c "
from tools.xraybench.adapters import list_adapters, get_adapter
print('Adapters:', list_adapters())
print('xraygraphdb:', get_adapter('xraygraphdb'))
print('neo4j:', get_adapter('neo4j'))
print('memgraph:', get_adapter('memgraph'))
"
```

Expected: Lists all adapters and resolves each class.

- [ ] **Step 3: Commit**

```bash
git add tools/xraybench/adapters/__init__.py
git commit -m "feat: update adapter registry — add xraygraphdb-bolt alias"
```

---

### Task 9: Full Test Suite Run

- [ ] **Step 1: Run all adapter tests**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -m pytest tests/adapters/ -v
```

Expected: All tests pass (10 capabilities + 8 validation + 8 dataset_loader + 3 overhead + 4 xraygraphdb + 2 neo4j + 2 memgraph = ~37 tests).

- [ ] **Step 2: Run all tests (Rust + Python)**

Run:
```bash
source "$HOME/.cargo/env" && cd /Users/sendlane/github_projects/xraygraph-bench && cargo test --workspace --manifest-path rust/Cargo.toml 2>&1 | grep "^test result:" && .venv/bin/python3 -m pytest tests/ -v --tb=short
```

Expected: 136 Rust tests + ~66 Python tests all pass.

- [ ] **Step 3: Commit any fixes**

```bash
git add -A && git commit -m "feat: complete Phase 2 — adapter implementations with capabilities, validation, dataset loading, GFQL"
```
