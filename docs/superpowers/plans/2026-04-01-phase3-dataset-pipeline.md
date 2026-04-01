# Phase 3: Dataset Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the complete dataset pipeline — synthetic generation via Rust core to `/data/xraybench/`, SNAP download/ingestion, manifest generation with SHA-256 verification, and a CLI `xraybench generate` command that produces ready-to-load datasets.

**Architecture:** The `DatasetManager` orchestrates generation/download → file storage → manifest creation → verification. Synthetic datasets use `xraybench_core.generators` (Rust) for speed, writing binary and CSV edge lists. External datasets (SNAP) download, decompress, and convert to canonical edge-list format. Every dataset gets a YAML manifest with SHA-256 hashes of all data files. Before any benchmark run, the manager verifies file hashes against the manifest — refuse to run on stale data.

**Tech Stack:** Python 3.12, xraybench_core (Rust generators + checksum), pyyaml, hashlib, urllib

**Spec:** `docs/superpowers/specs/2026-03-31-full-implementation-design.md` — Section 4

---

## File Structure

```
tools/xraybench/
  dataset_manager.py                  # CREATE: orchestrates generation, download, manifest, verification
  datasets.py                        # REWRITE: refactor existing SNAP/OGB code, integrate with manager
  cli.py                             # MODIFY: add 'generate' and 'verify-dataset' commands
tests/
  datasets/
    __init__.py                      # CREATE
    test_dataset_manager.py          # CREATE
    test_datasets.py                 # CREATE
```

---

### Task 0: Dataset Manager — Generation and Manifest Creation

**Files:**
- Create: `tools/xraybench/dataset_manager.py`
- Create: `tests/datasets/__init__.py`
- Create: `tests/datasets/test_dataset_manager.py`

- [ ] **Step 1: Write tests for dataset manager**

Create `tests/datasets/__init__.py` (empty).

Create `tests/datasets/test_dataset_manager.py`:

```python
"""Tests for the dataset manager."""

import os
import tempfile
import shutil
import yaml

from tools.xraybench.dataset_manager import DatasetManager


class TestSyntheticGeneration:
    """Tests for synthetic dataset generation."""

    def setup_method(self):
        self.data_dir = tempfile.mkdtemp(prefix="xraybench_test_")
        self.manager = DatasetManager(data_dir=self.data_dir)

    def teardown_method(self):
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def test_generate_chain(self):
        manifest = self.manager.generate_synthetic(
            name="test-chain",
            generator="chain",
            params={"length": 100, "seed": 42},
        )
        assert manifest["node_count"] == 100
        assert manifest["edge_count"] == 99
        assert manifest["type"] == "synthetic"
        assert manifest["generator"]["function"] == "chain"

    def test_generate_hub(self):
        manifest = self.manager.generate_synthetic(
            name="test-hub",
            generator="hub",
            params={"hub_count": 3, "spokes_per_hub": 10, "seed": 42},
        )
        assert manifest["node_count"] == 33  # 3 hubs + 30 spokes
        assert manifest["edge_count"] == 30

    def test_generate_deep_traversal(self):
        manifest = self.manager.generate_synthetic(
            name="test-deep",
            generator="deep_traversal",
            params={"num_roots": 1, "fanout_per_level": [5, 3], "seed": 42},
        )
        assert manifest["node_count"] > 15
        assert manifest["edge_count"] >= 20

    def test_generates_files(self):
        self.manager.generate_synthetic(
            name="test-files",
            generator="chain",
            params={"length": 50, "seed": 42},
        )
        dataset_dir = os.path.join(self.data_dir, "synthetic", "test-files")
        assert os.path.isfile(os.path.join(dataset_dir, "edges.bin"))
        assert os.path.isfile(os.path.join(dataset_dir, "edges.csv"))
        assert os.path.isfile(os.path.join(dataset_dir, "manifest.yaml"))

    def test_manifest_has_checksums(self):
        self.manager.generate_synthetic(
            name="test-checksums",
            generator="chain",
            params={"length": 50, "seed": 42},
        )
        dataset_dir = os.path.join(self.data_dir, "synthetic", "test-checksums")
        with open(os.path.join(dataset_dir, "manifest.yaml")) as f:
            manifest = yaml.safe_load(f)
        assert "files" in manifest
        assert "edges.bin" in manifest["files"]
        assert "sha256" in manifest["files"]["edges.bin"]
        assert len(manifest["files"]["edges.bin"]["sha256"]) == 64

    def test_deterministic_generation(self):
        m1 = self.manager.generate_synthetic(
            name="test-det-1",
            generator="chain",
            params={"length": 100, "seed": 42},
        )
        m2 = self.manager.generate_synthetic(
            name="test-det-2",
            generator="chain",
            params={"length": 100, "seed": 42},
        )
        dir1 = os.path.join(self.data_dir, "synthetic", "test-det-1")
        dir2 = os.path.join(self.data_dir, "synthetic", "test-det-2")
        with open(os.path.join(dir1, "manifest.yaml")) as f:
            man1 = yaml.safe_load(f)
        with open(os.path.join(dir2, "manifest.yaml")) as f:
            man2 = yaml.safe_load(f)
        assert man1["files"]["edges.bin"]["sha256"] == man2["files"]["edges.bin"]["sha256"]


class TestVerification:
    """Tests for dataset verification."""

    def setup_method(self):
        self.data_dir = tempfile.mkdtemp(prefix="xraybench_test_")
        self.manager = DatasetManager(data_dir=self.data_dir)

    def teardown_method(self):
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def test_verify_valid_dataset(self):
        self.manager.generate_synthetic(
            name="test-verify",
            generator="chain",
            params={"length": 50, "seed": 42},
        )
        result = self.manager.verify("synthetic", "test-verify")
        assert result["valid"] is True
        assert len(result["errors"]) == 0

    def test_verify_corrupted_data(self):
        self.manager.generate_synthetic(
            name="test-corrupt",
            generator="chain",
            params={"length": 50, "seed": 42},
        )
        # Corrupt the binary file
        edges_path = os.path.join(
            self.data_dir, "synthetic", "test-corrupt", "edges.bin"
        )
        with open(edges_path, "wb") as f:
            f.write(b"corrupted data padding!")
        result = self.manager.verify("synthetic", "test-corrupt")
        assert result["valid"] is False
        assert len(result["errors"]) > 0

    def test_verify_missing_manifest(self):
        os.makedirs(os.path.join(self.data_dir, "synthetic", "no-manifest"))
        result = self.manager.verify("synthetic", "no-manifest")
        assert result["valid"] is False

    def test_verify_missing_file(self):
        self.manager.generate_synthetic(
            name="test-missing",
            generator="chain",
            params={"length": 50, "seed": 42},
        )
        # Remove the binary file
        edges_path = os.path.join(
            self.data_dir, "synthetic", "test-missing", "edges.bin"
        )
        os.remove(edges_path)
        result = self.manager.verify("synthetic", "test-missing")
        assert result["valid"] is False


class TestListing:
    """Tests for listing available datasets."""

    def setup_method(self):
        self.data_dir = tempfile.mkdtemp(prefix="xraybench_test_")
        self.manager = DatasetManager(data_dir=self.data_dir)

    def teardown_method(self):
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def test_list_empty(self):
        datasets = self.manager.list_datasets()
        assert datasets == []

    def test_list_after_generate(self):
        self.manager.generate_synthetic(
            name="ds-a", generator="chain", params={"length": 10, "seed": 1}
        )
        self.manager.generate_synthetic(
            name="ds-b", generator="chain", params={"length": 10, "seed": 2}
        )
        datasets = self.manager.list_datasets()
        names = [d["name"] for d in datasets]
        assert "ds-a" in names
        assert "ds-b" in names
```

- [ ] **Step 2: Implement dataset_manager.py**

Create `tools/xraybench/dataset_manager.py`:

```python
"""Dataset manager — generation, verification, and listing.

Orchestrates synthetic dataset generation via Rust core, stores data
files and manifests under the configured data directory, and provides
SHA-256 verification to ensure data integrity before benchmark runs.
"""

from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = "/data/xraybench"


class DatasetManager:
    """Manages benchmark datasets on disk.

    Responsible for:
    - Generating synthetic datasets via xraybench_core.generators
    - Writing edge-list files (binary + CSV)
    - Creating YAML manifests with SHA-256 checksums
    - Verifying dataset integrity before benchmark runs
    - Listing available datasets
    """

    def __init__(self, data_dir: str | Path = DEFAULT_DATA_DIR) -> None:
        self.data_dir = Path(data_dir)

    def generate_synthetic(
        self,
        name: str,
        generator: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Generate a synthetic dataset and write to disk.

        Args:
            name: Dataset name (used as directory name).
            generator: Generator name (chain, hub, power_law, deep_traversal, community).
            params: Generator-specific parameters (must include seed).

        Returns:
            The manifest dict.
        """
        import xraybench_core

        dataset_dir = self.data_dir / "synthetic" / name
        dataset_dir.mkdir(parents=True, exist_ok=True)

        seed = params.get("seed", 42)

        # Generate edges using Rust core
        if generator == "chain":
            edges = xraybench_core.generators.generate_chain(
                length=params["length"], seed=seed
            )
            node_count = params["length"]
        elif generator == "hub":
            node_count, edges = xraybench_core.generators.generate_hub_graph(
                hub_count=params["hub_count"],
                spokes_per_hub=params["spokes_per_hub"],
                seed=seed,
            )
        elif generator == "deep_traversal":
            node_count, edges = xraybench_core.generators.generate_deep_traversal(
                num_roots=params["num_roots"],
                fanout_per_level=params["fanout_per_level"],
                seed=seed,
            )
        elif generator == "power_law":
            edges = xraybench_core.generators.generate_power_law_edges(
                node_count=params["node_count"],
                m=params.get("m", 3),
                seed=seed,
            )
            node_count = params["node_count"]
        elif generator == "community":
            # Community generator not yet exposed via PyO3 — use power_law as proxy
            edges = xraybench_core.generators.generate_power_law_edges(
                node_count=params.get("node_count", 1000),
                m=params.get("m", 3),
                seed=seed,
            )
            node_count = params.get("node_count", 1000)
        else:
            raise ValueError(f"Unknown generator: {generator}")

        edge_count = len(edges)

        # Write files
        bin_path = str(dataset_dir / "edges.bin")
        csv_path = str(dataset_dir / "edges.csv")

        xraybench_core.generators.write_edges_binary(edges, bin_path)
        xraybench_core.generators.write_edges_csv(edges, csv_path)

        # Compute checksums
        files_info = {
            "edges.bin": {
                "sha256": _file_sha256(bin_path),
                "size_bytes": os.path.getsize(bin_path),
            },
            "edges.csv": {
                "sha256": _file_sha256(csv_path),
                "size_bytes": os.path.getsize(csv_path),
            },
        }

        # Build manifest
        manifest: dict[str, Any] = {
            "name": name,
            "version": f"seed-{seed}",
            "type": "synthetic",
            "description": f"Synthetic {generator} graph (seed={seed})",
            "generator": {
                "function": generator,
                "seed": seed,
                "params": params,
            },
            "format": "edge-list",
            "node_count": node_count,
            "edge_count": edge_count,
            "labels": ["Node"],
            "edge_types": ["EDGE"],
            "files": files_info,
        }

        # Write manifest
        manifest_path = dataset_dir / "manifest.yaml"
        with open(manifest_path, "w") as f:
            yaml.dump(manifest, f, default_flow_style=False, sort_keys=False)

        logger.info(
            "Generated %s: %d nodes, %d edges → %s",
            name, node_count, edge_count, dataset_dir,
        )

        return manifest

    def verify(self, dataset_type: str, name: str) -> dict[str, Any]:
        """Verify dataset integrity by checking file checksums.

        Args:
            dataset_type: Dataset type directory (e.g., "synthetic", "snap").
            name: Dataset name.

        Returns:
            Dict with "valid" (bool) and "errors" (list of strings).
        """
        dataset_dir = self.data_dir / dataset_type / name
        manifest_path = dataset_dir / "manifest.yaml"

        if not manifest_path.exists():
            return {
                "valid": False,
                "errors": [f"Manifest not found: {manifest_path}"],
            }

        with open(manifest_path) as f:
            manifest = yaml.safe_load(f)

        errors: list[str] = []
        files_info = manifest.get("files", {})

        for filename, info in files_info.items():
            file_path = dataset_dir / filename
            if not file_path.exists():
                errors.append(f"Missing file: {filename}")
                continue

            expected_sha = info.get("sha256", "")
            actual_sha = _file_sha256(str(file_path))
            if actual_sha != expected_sha:
                errors.append(
                    f"Checksum mismatch for {filename}: "
                    f"expected {expected_sha[:16]}..., got {actual_sha[:16]}..."
                )

        return {
            "valid": len(errors) == 0,
            "errors": errors,
        }

    def list_datasets(self) -> list[dict[str, Any]]:
        """List all datasets with manifests.

        Returns:
            List of dicts with name, type, node_count, edge_count.
        """
        datasets: list[dict[str, Any]] = []

        if not self.data_dir.exists():
            return datasets

        for type_dir in sorted(self.data_dir.iterdir()):
            if not type_dir.is_dir():
                continue
            for ds_dir in sorted(type_dir.iterdir()):
                if not ds_dir.is_dir():
                    continue
                manifest_path = ds_dir / "manifest.yaml"
                if manifest_path.exists():
                    with open(manifest_path) as f:
                        manifest = yaml.safe_load(f)
                    datasets.append({
                        "name": manifest.get("name", ds_dir.name),
                        "type": manifest.get("type", type_dir.name),
                        "node_count": manifest.get("node_count", 0),
                        "edge_count": manifest.get("edge_count", 0),
                        "generator": manifest.get("generator", {}).get("function"),
                    })

        return datasets

    def get_manifest(self, dataset_type: str, name: str) -> dict[str, Any] | None:
        """Load a dataset manifest."""
        manifest_path = self.data_dir / dataset_type / name / "manifest.yaml"
        if not manifest_path.exists():
            return None
        with open(manifest_path) as f:
            return yaml.safe_load(f)

    def get_edges_path(self, dataset_type: str, name: str, fmt: str = "bin") -> Path | None:
        """Get path to edge-list file."""
        ext = "bin" if fmt == "bin" else "csv"
        path = self.data_dir / dataset_type / name / f"edges.{ext}"
        return path if path.exists() else None


def _file_sha256(path: str) -> str:
    """Compute SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -m pytest tests/datasets/test_dataset_manager.py -v
```

Expected: All 12 tests pass.

- [ ] **Step 4: Commit**

```bash
git add tools/xraybench/dataset_manager.py tests/datasets/
git commit -m "feat: add DatasetManager — synthetic generation via Rust core, SHA-256 manifests, verification"
```

---

### Task 1: Refactor datasets.py — SNAP Download Integration

**Files:**
- Rewrite: `tools/xraybench/datasets.py`
- Test: `tests/datasets/test_datasets.py`

- [ ] **Step 1: Write tests for SNAP integration**

Create `tests/datasets/test_datasets.py`:

```python
"""Tests for SNAP/OGB dataset utilities."""

import os
import tempfile
import shutil

from tools.xraybench.datasets import (
    SNAP_DATASETS,
    OGB_DATASETS,
    parse_snap_edge_list,
    file_checksum,
)


def test_snap_registry():
    assert "soc-LiveJournal1" in SNAP_DATASETS
    assert "web-Google" in SNAP_DATASETS
    assert SNAP_DATASETS["web-Google"]["nodes"] == 875713


def test_ogb_registry():
    assert "ogbn-products" in OGB_DATASETS
    assert OGB_DATASETS["ogbn-products"]["nodes"] == 2449029


def test_parse_snap_edge_list():
    """Test parsing a mock SNAP edge list file."""
    tmpdir = tempfile.mkdtemp()
    try:
        edge_file = os.path.join(tmpdir, "test.txt")
        with open(edge_file, "w") as f:
            f.write("# Comment line\n")
            f.write("# Another comment\n")
            f.write("0\t1\n")
            f.write("1\t2\n")
            f.write("2\t3\n")
            f.write("3\t0\n")
        edges = parse_snap_edge_list(edge_file)
        assert len(edges) == 4
        assert edges[0] == (0, 1)
        assert edges[-1] == (3, 0)
    finally:
        shutil.rmtree(tmpdir)


def test_parse_snap_skips_comments_and_blanks():
    tmpdir = tempfile.mkdtemp()
    try:
        edge_file = os.path.join(tmpdir, "test.txt")
        with open(edge_file, "w") as f:
            f.write("# Header\n")
            f.write("\n")
            f.write("0 1\n")
            f.write("# Mid comment\n")
            f.write("\n")
            f.write("2 3\n")
        edges = parse_snap_edge_list(edge_file)
        assert len(edges) == 2
    finally:
        shutil.rmtree(tmpdir)


def test_file_checksum():
    tmpdir = tempfile.mkdtemp()
    try:
        path = os.path.join(tmpdir, "test.bin")
        with open(path, "wb") as f:
            f.write(b"hello world")
        cs = file_checksum(path)
        assert len(cs) == 64  # SHA-256 hex
        # Known SHA-256 of "hello world"
        assert cs == "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    finally:
        shutil.rmtree(tmpdir)


def test_file_checksum_deterministic():
    tmpdir = tempfile.mkdtemp()
    try:
        path = os.path.join(tmpdir, "test.bin")
        with open(path, "wb") as f:
            f.write(b"deterministic content")
        cs1 = file_checksum(path)
        cs2 = file_checksum(path)
        assert cs1 == cs2
    finally:
        shutil.rmtree(tmpdir)
```

- [ ] **Step 2: Run tests to verify parsing and checksum**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -m pytest tests/datasets/test_datasets.py -v
```

Expected: All 6 tests pass (the existing datasets.py already has these functions).

- [ ] **Step 3: Commit**

```bash
git add tests/datasets/test_datasets.py
git commit -m "test: add tests for SNAP parsing and file checksums"
```

---

### Task 2: CLI Commands — generate and verify-dataset

**Files:**
- Modify: `tools/xraybench/cli.py`
- Test: `tests/datasets/test_cli_generate.py`

- [ ] **Step 1: Write tests for CLI generate**

Create `tests/datasets/test_cli_generate.py`:

```python
"""Tests for CLI generate and verify-dataset commands."""

import os
import tempfile
import shutil
import subprocess
import sys
import yaml


class TestCLIGenerate:
    """Test the xraybench generate CLI command."""

    def setup_method(self):
        self.data_dir = tempfile.mkdtemp(prefix="xraybench_cli_test_")

    def teardown_method(self):
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def test_generate_chain(self):
        result = subprocess.run(
            [
                sys.executable, "-m", "tools.xraybench.cli",
                "generate",
                "--generator", "chain",
                "--param", "length=50",
                "--param", "seed=42",
                "--name", "cli-test-chain",
                "--data-dir", self.data_dir,
            ],
            capture_output=True, text=True,
            cwd="/Users/sendlane/github_projects/xraygraph-bench",
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        dataset_dir = os.path.join(self.data_dir, "synthetic", "cli-test-chain")
        assert os.path.isfile(os.path.join(dataset_dir, "manifest.yaml"))
        assert os.path.isfile(os.path.join(dataset_dir, "edges.bin"))

    def test_generate_deep_traversal(self):
        result = subprocess.run(
            [
                sys.executable, "-m", "tools.xraybench.cli",
                "generate",
                "--generator", "deep_traversal",
                "--param", "num_roots=1",
                "--param", "fanout_per_level=[5,3]",
                "--param", "seed=42",
                "--name", "cli-test-deep",
                "--data-dir", self.data_dir,
            ],
            capture_output=True, text=True,
            cwd="/Users/sendlane/github_projects/xraygraph-bench",
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"

    def test_verify_valid(self):
        # First generate
        subprocess.run(
            [
                sys.executable, "-m", "tools.xraybench.cli",
                "generate",
                "--generator", "chain",
                "--param", "length=20",
                "--param", "seed=42",
                "--name", "cli-test-verify",
                "--data-dir", self.data_dir,
            ],
            capture_output=True, text=True,
            cwd="/Users/sendlane/github_projects/xraygraph-bench",
        )
        # Then verify
        result = subprocess.run(
            [
                sys.executable, "-m", "tools.xraybench.cli",
                "verify-dataset",
                "--type", "synthetic",
                "--name", "cli-test-verify",
                "--data-dir", self.data_dir,
            ],
            capture_output=True, text=True,
            cwd="/Users/sendlane/github_projects/xraygraph-bench",
        )
        assert result.returncode == 0
        assert "valid" in result.stdout.lower() or "pass" in result.stdout.lower()
```

- [ ] **Step 2: Add generate and verify-dataset commands to cli.py**

Read the existing `tools/xraybench/cli.py` to understand the argparse structure, then add two new subcommands:

**`generate` command:**
- `--generator` (required): chain, hub, power_law, deep_traversal
- `--name` (required): dataset name
- `--param` (repeatable): key=value pairs (auto-parse ints, floats, lists)
- `--data-dir` (optional, default `/data/xraybench`): override data directory

Parses params, creates DatasetManager, calls generate_synthetic, prints summary.

**`verify-dataset` command:**
- `--type` (required): synthetic, snap, ogb
- `--name` (required): dataset name
- `--data-dir` (optional): override data directory

Creates DatasetManager, calls verify, prints result, exits non-zero on failure.

**Parameter parsing helper:** Convert "length=50" to {"length": 50}. Auto-detect types: `[5,3]` → list, `50` → int, `3.14` → float, `"text"` → str.

- [ ] **Step 3: Run CLI tests**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -m pytest tests/datasets/test_cli_generate.py -v
```

Expected: All 3 tests pass.

- [ ] **Step 4: Commit**

```bash
git add tools/xraybench/cli.py tests/datasets/test_cli_generate.py
git commit -m "feat: add CLI generate and verify-dataset commands"
```

---

### Task 3: SNAP Dataset Download and Conversion

**Files:**
- Modify: `tools/xraybench/dataset_manager.py`
- Test: `tests/datasets/test_snap_download.py`

- [ ] **Step 1: Write tests for SNAP integration**

Create `tests/datasets/test_snap_download.py`:

```python
"""Tests for SNAP download integration in DatasetManager.

Note: These tests do NOT download real SNAP data. They test the
conversion pipeline with mock data.
"""

import os
import tempfile
import shutil
import yaml

from tools.xraybench.dataset_manager import DatasetManager, _file_sha256


class TestSnapConversion:
    """Test SNAP edge-list conversion and manifest creation."""

    def setup_method(self):
        self.data_dir = tempfile.mkdtemp(prefix="xraybench_snap_test_")
        self.manager = DatasetManager(data_dir=self.data_dir)

    def teardown_method(self):
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def test_ingest_snap_edge_list(self):
        """Test ingesting a mock SNAP-format edge list."""
        # Create a mock SNAP edge list
        mock_dir = tempfile.mkdtemp()
        edge_file = os.path.join(mock_dir, "mock-graph.txt")
        with open(edge_file, "w") as f:
            f.write("# Mock SNAP graph\n")
            f.write("# Nodes: 5 Edges: 6\n")
            for s, t in [(0, 1), (1, 2), (2, 3), (3, 4), (4, 0), (0, 2)]:
                f.write(f"{s}\t{t}\n")

        try:
            manifest = self.manager.ingest_edge_list(
                name="mock-snap",
                dataset_type="snap",
                edge_list_path=edge_file,
                metadata={
                    "description": "Mock SNAP graph for testing",
                    "source": {"url": "https://example.com/mock.txt.gz"},
                },
            )
            assert manifest["node_count"] == 5
            assert manifest["edge_count"] == 6
            assert manifest["type"] == "snap"

            # Verify files exist
            ds_dir = os.path.join(self.data_dir, "snap", "mock-snap")
            assert os.path.isfile(os.path.join(ds_dir, "edges.bin"))
            assert os.path.isfile(os.path.join(ds_dir, "edges.csv"))
            assert os.path.isfile(os.path.join(ds_dir, "manifest.yaml"))
        finally:
            shutil.rmtree(mock_dir)

    def test_ingest_preserves_edge_data(self):
        """Verify ingested binary data matches the source."""
        mock_dir = tempfile.mkdtemp()
        edge_file = os.path.join(mock_dir, "test.txt")
        expected_edges = [(0, 1), (1, 2), (2, 0)]
        with open(edge_file, "w") as f:
            for s, t in expected_edges:
                f.write(f"{s}\t{t}\n")

        try:
            self.manager.ingest_edge_list(
                name="test-preserve",
                dataset_type="snap",
                edge_list_path=edge_file,
            )

            # Read back the CSV and verify
            csv_path = os.path.join(
                self.data_dir, "snap", "test-preserve", "edges.csv"
            )
            with open(csv_path) as f:
                lines = f.readlines()
            # Skip header
            data_lines = [l.strip() for l in lines[1:] if l.strip()]
            parsed = [tuple(int(x) for x in l.split(",")) for l in data_lines]
            assert parsed == expected_edges
        finally:
            shutil.rmtree(mock_dir)

    def test_ingest_creates_verifiable_manifest(self):
        """Verify that ingested dataset passes verification."""
        mock_dir = tempfile.mkdtemp()
        edge_file = os.path.join(mock_dir, "test.txt")
        with open(edge_file, "w") as f:
            for s, t in [(0, 1), (1, 2)]:
                f.write(f"{s}\t{t}\n")

        try:
            self.manager.ingest_edge_list(
                name="test-verifiable",
                dataset_type="snap",
                edge_list_path=edge_file,
            )
            result = self.manager.verify("snap", "test-verifiable")
            assert result["valid"] is True
        finally:
            shutil.rmtree(mock_dir)
```

- [ ] **Step 2: Add ingest_edge_list to DatasetManager**

Add to `tools/xraybench/dataset_manager.py`:

```python
    def ingest_edge_list(
        self,
        name: str,
        dataset_type: str,
        edge_list_path: str | Path,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Ingest a text edge-list file (SNAP format) into the data directory.

        Parses the edge list, writes binary and CSV files, creates manifest.

        Args:
            name: Dataset name.
            dataset_type: Type directory (e.g., "snap", "ogb").
            edge_list_path: Path to the text edge list (tab or space separated).
            metadata: Additional manifest fields (description, source, etc.).

        Returns:
            The manifest dict.
        """
        import xraybench_core

        # Parse edge list
        edges: list[tuple[int, int]] = []
        node_ids: set[int] = set()
        with open(edge_list_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        src, dst = int(parts[0]), int(parts[1])
                        edges.append((src, dst))
                        node_ids.add(src)
                        node_ids.add(dst)
                    except ValueError:
                        continue

        node_count = len(node_ids)
        edge_count = len(edges)

        # Write files
        dataset_dir = self.data_dir / dataset_type / name
        dataset_dir.mkdir(parents=True, exist_ok=True)

        bin_path = str(dataset_dir / "edges.bin")
        csv_path = str(dataset_dir / "edges.csv")

        xraybench_core.generators.write_edges_binary(edges, bin_path)
        xraybench_core.generators.write_edges_csv(edges, csv_path)

        # Checksums
        files_info = {
            "edges.bin": {
                "sha256": _file_sha256(bin_path),
                "size_bytes": os.path.getsize(bin_path),
            },
            "edges.csv": {
                "sha256": _file_sha256(csv_path),
                "size_bytes": os.path.getsize(csv_path),
            },
        }

        # Build manifest
        manifest: dict[str, Any] = {
            "name": name,
            "version": "1.0",
            "type": dataset_type,
            "description": (metadata or {}).get("description", f"{dataset_type} dataset: {name}"),
            "format": "edge-list",
            "node_count": node_count,
            "edge_count": edge_count,
            "labels": ["Node"],
            "edge_types": ["EDGE"],
            "files": files_info,
        }

        if metadata:
            if "source" in metadata:
                manifest["source"] = metadata["source"]
            if "citation" in metadata:
                manifest["source"] = manifest.get("source", {})
                manifest["source"]["citation"] = metadata["citation"]

        # Write manifest
        with open(dataset_dir / "manifest.yaml", "w") as f:
            yaml.dump(manifest, f, default_flow_style=False, sort_keys=False)

        logger.info(
            "Ingested %s/%s: %d nodes, %d edges → %s",
            dataset_type, name, node_count, edge_count, dataset_dir,
        )

        return manifest
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -m pytest tests/datasets/test_snap_download.py -v
```

Expected: All 3 tests pass.

- [ ] **Step 4: Commit**

```bash
git add tools/xraybench/dataset_manager.py tests/datasets/test_snap_download.py
git commit -m "feat: add SNAP edge-list ingestion — parse, convert to binary/CSV, create verified manifest"
```

---

### Task 4: Full Test Suite and Integration Verification

- [ ] **Step 1: Run all dataset tests**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && .venv/bin/python3 -m pytest tests/datasets/ -v
```

Expected: All ~21 tests pass (12 manager + 6 datasets + 3 CLI).

- [ ] **Step 2: Run full project test suite**

Run:
```bash
source "$HOME/.cargo/env" && cd /Users/sendlane/github_projects/xraygraph-bench && cargo test --workspace --manifest-path rust/Cargo.toml 2>&1 | grep "^test result:" && .venv/bin/python3 -m pytest tests/ -q
```

Expected: 136 Rust + ~91 Python tests all pass.

- [ ] **Step 3: Commit any fixes**

```bash
git add -A && git commit -m "feat: complete Phase 3 — dataset pipeline with synthetic generation, SNAP ingestion, SHA-256 verification"
```
