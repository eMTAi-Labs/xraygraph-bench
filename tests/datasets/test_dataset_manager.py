"""Tests for DatasetManager — synthetic generation, verification, and listing."""

from __future__ import annotations

import os
import shutil
import tempfile

import pytest

from tools.xraybench.dataset_manager import DatasetManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_manager() -> tuple[DatasetManager, str]:
    """Create a DatasetManager backed by a fresh temp directory."""
    data_dir = tempfile.mkdtemp()
    return DatasetManager(data_dir=data_dir), data_dir


# ---------------------------------------------------------------------------
# TestSyntheticGeneration (6 tests)
# ---------------------------------------------------------------------------

class TestSyntheticGeneration:

    def setup_method(self) -> None:
        self.manager, self.data_dir = _make_manager()

    def teardown_method(self) -> None:
        shutil.rmtree(self.data_dir, ignore_errors=True)

    # --- test_generate_chain -------------------------------------------

    def test_generate_chain(self) -> None:
        manifest = self.manager.generate_synthetic(
            name="test-chain",
            generator="chain",
            params={"length": 100, "seed": 42},
        )
        assert manifest["node_count"] == 100
        assert manifest["edge_count"] == 99

    # --- test_generate_hub ---------------------------------------------

    def test_generate_hub(self) -> None:
        manifest = self.manager.generate_synthetic(
            name="test-hub",
            generator="hub",
            params={"hub_count": 3, "spokes_per_hub": 10, "seed": 42},
        )
        # 3 hub nodes + 3*10 spoke nodes = 33; 3*10 = 30 edges
        assert manifest["node_count"] == 33
        assert manifest["edge_count"] == 30

    # --- test_generate_deep_traversal ----------------------------------

    def test_generate_deep_traversal(self) -> None:
        manifest = self.manager.generate_synthetic(
            name="test-deep",
            generator="deep_traversal",
            params={"num_roots": 1, "fanout_per_level": [5, 3], "seed": 42},
        )
        assert manifest["node_count"] > 15
        assert manifest["edge_count"] >= 20

    # --- test_generates_files ------------------------------------------

    def test_generates_files(self) -> None:
        self.manager.generate_synthetic(
            name="test-files",
            generator="chain",
            params={"length": 50, "seed": 1},
        )
        dataset_dir = os.path.join(self.data_dir, "synthetic", "test-files")
        assert os.path.isfile(os.path.join(dataset_dir, "edges.bin"))
        assert os.path.isfile(os.path.join(dataset_dir, "edges.csv"))
        assert os.path.isfile(os.path.join(dataset_dir, "manifest.yaml"))

    # --- test_manifest_has_checksums -----------------------------------

    def test_manifest_has_checksums(self) -> None:
        manifest = self.manager.generate_synthetic(
            name="test-checksums",
            generator="chain",
            params={"length": 20, "seed": 7},
        )
        files = manifest["files"]
        assert "edges.bin" in files
        assert "edges.csv" in files
        for filename, info in files.items():
            sha = info.get("sha256", "")
            assert len(sha) == 64, f"{filename} sha256 should be 64 hex chars, got {len(sha)}"

    # --- test_deterministic_generation ---------------------------------

    def test_deterministic_generation(self) -> None:
        m1 = self.manager.generate_synthetic(
            name="det-1",
            generator="chain",
            params={"length": 100, "seed": 99},
        )
        m2 = self.manager.generate_synthetic(
            name="det-2",
            generator="chain",
            params={"length": 100, "seed": 99},
        )
        assert m1["files"]["edges.bin"]["sha256"] == m2["files"]["edges.bin"]["sha256"]
        assert m1["files"]["edges.csv"]["sha256"] == m2["files"]["edges.csv"]["sha256"]


# ---------------------------------------------------------------------------
# TestVerification (4 tests)
# ---------------------------------------------------------------------------

class TestVerification:

    def setup_method(self) -> None:
        self.manager, self.data_dir = _make_manager()

    def teardown_method(self) -> None:
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def _generate(self, name: str = "verify-chain") -> str:
        """Generate a small chain dataset and return its directory."""
        self.manager.generate_synthetic(
            name=name,
            generator="chain",
            params={"length": 50, "seed": 42},
        )
        return os.path.join(self.data_dir, "synthetic", name)

    # --- test_verify_valid_dataset -------------------------------------

    def test_verify_valid_dataset(self) -> None:
        self._generate("valid-chain")
        result = self.manager.verify("synthetic", "valid-chain")
        assert result["valid"] is True
        assert result["errors"] == []

    # --- test_verify_corrupted_data ------------------------------------

    def test_verify_corrupted_data(self) -> None:
        dataset_dir = self._generate("corrupt-chain")
        bin_path = os.path.join(dataset_dir, "edges.bin")
        # Overwrite a few bytes to corrupt the file
        with open(bin_path, "r+b") as f:
            f.seek(0)
            f.write(b"\xff\xff\xff\xff")

        result = self.manager.verify("synthetic", "corrupt-chain")
        assert result["valid"] is False
        assert any("edges.bin" in err for err in result["errors"])

    # --- test_verify_missing_manifest ----------------------------------

    def test_verify_missing_manifest(self) -> None:
        result = self.manager.verify("synthetic", "nonexistent-dataset")
        assert result["valid"] is False
        assert len(result["errors"]) > 0

    # --- test_verify_missing_file --------------------------------------

    def test_verify_missing_file(self) -> None:
        dataset_dir = self._generate("missing-file-chain")
        bin_path = os.path.join(dataset_dir, "edges.bin")
        os.remove(bin_path)

        result = self.manager.verify("synthetic", "missing-file-chain")
        assert result["valid"] is False
        assert any("edges.bin" in err for err in result["errors"])


# ---------------------------------------------------------------------------
# TestListing (2 tests)
# ---------------------------------------------------------------------------

class TestListing:

    def setup_method(self) -> None:
        self.manager, self.data_dir = _make_manager()

    def teardown_method(self) -> None:
        shutil.rmtree(self.data_dir, ignore_errors=True)

    # --- test_list_empty -----------------------------------------------

    def test_list_empty(self) -> None:
        result = self.manager.list_datasets()
        assert result == []

    # --- test_list_after_generate --------------------------------------

    def test_list_after_generate(self) -> None:
        self.manager.generate_synthetic(
            name="alpha",
            generator="chain",
            params={"length": 20, "seed": 1},
        )
        self.manager.generate_synthetic(
            name="beta",
            generator="hub",
            params={"hub_count": 2, "spokes_per_hub": 5, "seed": 2},
        )
        datasets = self.manager.list_datasets()
        names = {d["name"] for d in datasets}
        assert "alpha" in names
        assert "beta" in names
        assert len(datasets) >= 2
