"""Shared correctness validation for xraygraph-bench adapters."""

from __future__ import annotations

from typing import Any

import xraybench_core

from tools.xraybench.models import CorrectnessResult, ExecuteResult

_checksum = xraybench_core.checksum


def _rows_to_lists(
    rows: list[dict[str, Any]],
    columns: list[str] | None = None,
) -> list[list[Any]]:
    """Convert result rows (list of dicts) to list of lists.

    If *columns* is provided, values are extracted in that order.
    Otherwise, the natural dict key order of the first row is used.
    """
    if not rows:
        return []
    if columns is None:
        columns = list(rows[0].keys())
    return [[row.get(col) for col in columns] for row in rows]


def validate_oracle(result: ExecuteResult, oracle: dict[str, Any]) -> CorrectnessResult:
    """Dispatch validation to the appropriate handler based on oracle type.

    Args:
        result: The execution result to validate.
        oracle: A dict with at minimum a ``type`` key, plus type-specific keys.

    Returns:
        CorrectnessResult with passed flag and human-readable detail.
    """
    oracle_type: str = oracle.get("type", "")

    if oracle_type == "row_count":
        return _validate_row_count(result, oracle)
    elif oracle_type == "row_count_range":
        return _validate_row_count_range(result, oracle)
    elif oracle_type in ("exact_match", "checksum"):
        return _validate_exact_match(result, oracle)
    elif oracle_type == "structural":
        return _validate_structural(result, oracle)
    elif oracle_type == "invariant":
        return _validate_invariant(result, oracle)
    else:
        return CorrectnessResult(
            passed=False,
            detail=f"Unsupported oracle type: '{oracle_type}'",
        )


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


def _validate_row_count(
    result: ExecuteResult, oracle: dict[str, Any]
) -> CorrectnessResult:
    expected: int = oracle["expected_row_count"]
    actual: int = result.row_count
    if actual == expected:
        return CorrectnessResult(passed=True, detail=f"row_count={actual} matches expected={expected}")
    return CorrectnessResult(
        passed=False,
        detail=f"row_count mismatch: got {actual}, expected {expected}",
    )


def _validate_row_count_range(
    result: ExecuteResult, oracle: dict[str, Any]
) -> CorrectnessResult:
    lo: int = oracle["expected_row_count_min"]
    hi: int = oracle["expected_row_count_max"]
    actual: int = result.row_count
    if lo <= actual <= hi:
        return CorrectnessResult(
            passed=True,
            detail=f"row_count={actual} within [{lo}, {hi}]",
        )
    return CorrectnessResult(
        passed=False,
        detail=f"row_count={actual} outside range [{lo}, {hi}]",
    )


def _validate_exact_match(
    result: ExecuteResult, oracle: dict[str, Any]
) -> CorrectnessResult:
    expected_checksum: str = oracle["expected_checksum"]
    columns: list[str] | None = oracle.get("expected_columns")
    row_lists = _rows_to_lists(result.rows, columns)
    actual_checksum: str = _checksum.hash_result_set(row_lists)
    if actual_checksum == expected_checksum:
        return CorrectnessResult(
            passed=True,
            detail=f"checksum matches: {actual_checksum}",
        )
    return CorrectnessResult(
        passed=False,
        detail=f"checksum mismatch: got {actual_checksum}, expected {expected_checksum}",
    )


def _validate_structural(
    result: ExecuteResult, oracle: dict[str, Any]
) -> CorrectnessResult:
    """Validate structural properties: max path depth and seed nodes."""
    failures: list[str] = []

    max_depth: int | None = oracle.get("max_depth")
    if max_depth is not None:
        for i, row in enumerate(result.rows):
            # Look for a 'path' or 'length' column; fall back to any numeric value
            length: int | None = None
            if "length" in row:
                length = int(row["length"])
            elif "path_length" in row:
                length = int(row["path_length"])
            elif "path" in row:
                path = row["path"]
                if isinstance(path, (list, tuple)):
                    length = len(path)
            if length is not None and length > max_depth:
                failures.append(
                    f"row {i}: path length {length} exceeds max_depth {max_depth}"
                )

    seed_id: Any = oracle.get("seed_id")
    if seed_id is not None:
        for i, row in enumerate(result.rows):
            row_seed = row.get("seed_id") or row.get("seed") or row.get("start")
            if row_seed is not None and row_seed != seed_id:
                failures.append(
                    f"row {i}: seed_id={row_seed!r} does not match expected {seed_id!r}"
                )

    if failures:
        return CorrectnessResult(
            passed=False,
            detail="; ".join(failures),
        )
    return CorrectnessResult(passed=True, detail="structural validation passed")


def _validate_invariant(
    result: ExecuteResult, oracle: dict[str, Any]
) -> CorrectnessResult:
    """Validate invariants: row_count_range + string invariant tags."""
    # First check row_count_range if present
    if "expected_row_count_min" in oracle and "expected_row_count_max" in oracle:
        rc_result = _validate_row_count_range(result, oracle)
        if not rc_result.passed:
            return rc_result

    failures: list[str] = []
    invariants: list[str] = oracle.get("invariants", [])

    for inv in invariants:
        if inv.startswith("all_paths_length_le:"):
            try:
                limit = int(inv.split(":", 1)[1])
            except (IndexError, ValueError):
                failures.append(f"malformed invariant: {inv!r}")
                continue
            for i, row in enumerate(result.rows):
                length: int | None = None
                if "length" in row:
                    length = int(row["length"])
                elif "path_length" in row:
                    length = int(row["path_length"])
                elif "path" in row:
                    path = row["path"]
                    if isinstance(path, (list, tuple)):
                        length = len(path)
                if length is not None and length > limit:
                    failures.append(
                        f"invariant '{inv}' violated at row {i}: length={length}"
                    )
        else:
            failures.append(f"unknown invariant: {inv!r}")

    if failures:
        return CorrectnessResult(passed=False, detail="; ".join(failures))
    return CorrectnessResult(passed=True, detail="all invariants passed")
