"""Tests for shared correctness validation (Task 2)."""

from __future__ import annotations

import xraybench_core

from tools.xraybench.adapters.validation import validate_oracle
from tools.xraybench.models import ExecuteResult

_checksum = xraybench_core.checksum


def _make_result(rows: list[dict]) -> ExecuteResult:
    return ExecuteResult(rows=rows, wall_ms=1.0)


# ---------------------------------------------------------------------------
# row_count
# ---------------------------------------------------------------------------


def test_row_count_pass() -> None:
    rows = [{"id": i} for i in range(100)]
    result = _make_result(rows)
    oracle = {"type": "row_count", "expected_row_count": 100}
    cr = validate_oracle(result, oracle)
    assert cr.passed is True


def test_row_count_fail() -> None:
    rows = [{"id": i} for i in range(50)]
    result = _make_result(rows)
    oracle = {"type": "row_count", "expected_row_count": 100}
    cr = validate_oracle(result, oracle)
    assert cr.passed is False
    assert "50" in cr.detail
    assert "100" in cr.detail


# ---------------------------------------------------------------------------
# row_count_range
# ---------------------------------------------------------------------------


def test_row_count_range_pass() -> None:
    rows = [{"id": i} for i in range(75)]
    result = _make_result(rows)
    oracle = {"type": "row_count_range", "expected_row_count_min": 50, "expected_row_count_max": 100}
    cr = validate_oracle(result, oracle)
    assert cr.passed is True


def test_row_count_range_fail() -> None:
    rows = [{"id": i} for i in range(200)]
    result = _make_result(rows)
    oracle = {"type": "row_count_range", "expected_row_count_min": 50, "expected_row_count_max": 100}
    cr = validate_oracle(result, oracle)
    assert cr.passed is False


# ---------------------------------------------------------------------------
# exact_match / checksum
# ---------------------------------------------------------------------------


def test_exact_match_pass() -> None:
    rows = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    result = _make_result(rows)
    # Compute reference checksum the same way validation.py does
    row_lists = [[row["id"], row["name"]] for row in rows]
    ref_checksum = _checksum.hash_result_set(row_lists)
    oracle = {
        "type": "exact_match",
        "expected_checksum": ref_checksum,
        "expected_columns": ["id", "name"],
    }
    cr = validate_oracle(result, oracle)
    assert cr.passed is True


def test_exact_match_fail() -> None:
    rows = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    result = _make_result(rows)
    oracle = {
        "type": "exact_match",
        "expected_checksum": "blake3:deadbeefdeadbeefdeadbeefdeadbeef",
        "expected_columns": ["id", "name"],
    }
    cr = validate_oracle(result, oracle)
    assert cr.passed is False


def test_exact_match_order_independent() -> None:
    """Same data in different row order should produce the same hash (Rust hasher sorts rows)."""
    rows_a = [{"id": 1, "val": "x"}, {"id": 2, "val": "y"}, {"id": 3, "val": "z"}]
    rows_b = [{"id": 3, "val": "z"}, {"id": 1, "val": "x"}, {"id": 2, "val": "y"}]

    # Compute reference hash from rows_a in natural order
    row_lists_a = [[r["id"], r["val"]] for r in rows_a]
    ref_checksum = _checksum.hash_result_set(row_lists_a)

    oracle = {
        "type": "exact_match",
        "expected_checksum": ref_checksum,
        "expected_columns": ["id", "val"],
    }

    cr_a = validate_oracle(_make_result(rows_a), oracle)
    cr_b = validate_oracle(_make_result(rows_b), oracle)

    assert cr_a.passed is True, f"rows_a failed: {cr_a.detail}"
    assert cr_b.passed is True, f"rows_b failed: {cr_b.detail}"


# ---------------------------------------------------------------------------
# unknown oracle type
# ---------------------------------------------------------------------------


def test_unknown_oracle_type() -> None:
    result = _make_result([{"id": 1}])
    oracle = {"type": "does_not_exist"}
    cr = validate_oracle(result, oracle)
    assert cr.passed is False
