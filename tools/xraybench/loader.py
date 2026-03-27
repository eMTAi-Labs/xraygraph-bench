"""Dataset loading and benchmark spec parsing."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .models import BenchmarkSpec, DatasetManifest


def load_benchmark_spec(path: str | Path) -> BenchmarkSpec:
    """Load a benchmark spec from a YAML file.

    Args:
        path: Path to a benchmark.yaml file.

    Returns:
        Parsed BenchmarkSpec object.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the YAML is invalid or missing required fields.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Benchmark spec not found: {path}")

    try:
        import yaml
    except ImportError:
        raise ImportError("PyYAML is required. Run: pip install pyyaml")

    with open(path) as f:
        data = yaml.safe_load(f)

    if data is None:
        raise ValueError(f"Empty benchmark spec: {path}")

    try:
        return BenchmarkSpec.from_dict(data)
    except (KeyError, TypeError) as e:
        raise ValueError(f"Invalid benchmark spec {path}: {e}")


def load_dataset_manifest(path: str | Path) -> DatasetManifest:
    """Load a dataset manifest from a YAML file.

    Args:
        path: Path to a manifest.yaml file.

    Returns:
        Parsed DatasetManifest object.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Dataset manifest not found: {path}")

    try:
        import yaml
    except ImportError:
        raise ImportError("PyYAML is required. Run: pip install pyyaml")

    with open(path) as f:
        data = yaml.safe_load(f)

    if data is None:
        raise ValueError(f"Empty dataset manifest: {path}")

    return DatasetManifest.from_dict(data)


def discover_benchmarks(base_dir: str | Path | None = None) -> list[dict[str, Any]]:
    """Discover all benchmark specs under the benchmarks directory.

    Args:
        base_dir: Root directory to search. Defaults to the repo's benchmarks/
                  directory.

    Returns:
        List of dicts with 'name', 'family', 'path' for each discovered
        benchmark.
    """
    if base_dir is None:
        base_dir = (
            Path(__file__).resolve().parent.parent.parent / "benchmarks" / "suites"
        )
    else:
        base_dir = Path(base_dir)

    if not base_dir.exists():
        return []

    benchmarks = []
    for yaml_path in sorted(base_dir.rglob("benchmark.yaml")):
        try:
            spec = load_benchmark_spec(yaml_path)
            benchmarks.append(
                {
                    "name": spec.name,
                    "family": spec.family,
                    "version": spec.version,
                    "path": str(yaml_path),
                    "tags": spec.tags,
                }
            )
        except Exception as e:
            benchmarks.append(
                {
                    "name": yaml_path.parent.name,
                    "family": yaml_path.parent.parent.name,
                    "path": str(yaml_path),
                    "error": str(e),
                }
            )

    return benchmarks
