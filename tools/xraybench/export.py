"""Export benchmark results to CSV or Parquet."""

import csv
import json
from pathlib import Path
from typing import Any


def load_results(results_dir: str | Path) -> list[dict[str, Any]]:
    """Load all JSON result files from a directory."""
    results_dir = Path(results_dir)
    results = []
    for path in sorted(results_dir.glob("*.json")):
        with open(path) as f:
            results.append(json.load(f))
    return results


def flatten_result(result: dict[str, Any]) -> dict[str, Any]:
    """Flatten a nested result dict for tabular export.

    Nested dicts become dot-separated keys:
    {"host": {"os": "Linux"}} -> {"host.os": "Linux"}
    """
    flat: dict[str, Any] = {}
    for key, value in result.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, (dict, list)):
                    flat[f"{key}.{sub_key}"] = json.dumps(sub_value)
                else:
                    flat[f"{key}.{sub_key}"] = sub_value
        elif isinstance(value, list):
            flat[key] = json.dumps(value)
        else:
            flat[key] = value
    return flat


def export_csv(results: list[dict[str, Any]], output_path: str | Path) -> int:
    """Export results to CSV. Returns row count."""
    if not results:
        return 0

    flat_results = [flatten_result(r) for r in results]

    # Collect all unique keys across all results
    all_keys: list[str] = []
    seen: set[str] = set()
    for r in flat_results:
        for k in r:
            if k not in seen:
                all_keys.append(k)
                seen.add(k)

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(flat_results)

    return len(flat_results)


def export_parquet(results: list[dict[str, Any]], output_path: str | Path) -> int:
    """Export results to Parquet. Requires pyarrow.

    Falls back to CSV if pyarrow is not installed.
    Returns row count.
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError(
            "pyarrow is required for Parquet export. "
            "Run: pip install pyarrow"
        )

    if not results:
        return 0

    flat_results = [flatten_result(r) for r in results]

    # Build table from flat dicts
    all_keys: list[str] = []
    seen: set[str] = set()
    for r in flat_results:
        for k in r:
            if k not in seen:
                all_keys.append(k)
                seen.add(k)

    columns: dict[str, list[Any]] = {}
    for key in all_keys:
        columns[key] = [r.get(key) for r in flat_results]

    table = pa.table(columns)
    pq.write_table(table, str(output_path))

    return len(flat_results)
