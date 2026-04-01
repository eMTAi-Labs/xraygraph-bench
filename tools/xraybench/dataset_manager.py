"""DatasetManager — synthetic graph generation, manifest tracking, and verification.

Generates benchmark datasets using xraybench_core Rust generators, writes
binary and CSV edge files, and produces SHA-256 manifest files for integrity
verification.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

import yaml
from xraybench_core import generators


# ---------------------------------------------------------------------------
# Module-level helper
# ---------------------------------------------------------------------------

def _file_sha256(path: str | Path) -> str:
    """Return the SHA-256 hex digest of a file, reading in 64 KB chunks."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# DatasetManager
# ---------------------------------------------------------------------------

class DatasetManager:
    """Manage synthetic benchmark datasets on disk."""

    def __init__(self, data_dir: str = "/data/xraybench") -> None:
        self.data_dir = Path(data_dir)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_synthetic(
        self,
        name: str,
        generator: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Generate a synthetic dataset and write it to disk.

        Args:
            name: Dataset name (used as directory name).
            generator: One of 'chain', 'hub', 'deep_traversal', 'power_law'.
            params: Generator-specific parameters (must include 'seed').

        Returns:
            The manifest dict that was written to manifest.yaml.

        Raises:
            ValueError: If the generator name is not recognised.
        """
        out_dir = self.data_dir / "synthetic" / name
        out_dir.mkdir(parents=True, exist_ok=True)

        node_count, edges = self._run_generator(generator, params)

        bin_path = out_dir / "edges.bin"
        csv_path = out_dir / "edges.csv"
        generators.write_edges_binary(edges, str(bin_path))
        generators.write_edges_csv(edges, str(csv_path))

        seed = params.get("seed", 0)
        manifest: dict[str, Any] = {
            "name": name,
            "version": f"seed-{seed}",
            "type": "synthetic",
            "description": f"Synthetic {generator} graph (seed={seed})",
            "generator": {
                "function": generator,
                "seed": seed,
                "params": dict(params),
            },
            "format": "edge-list",
            "node_count": node_count,
            "edge_count": len(edges),
            "labels": ["Node"],
            "edge_types": ["EDGE"],
            "files": {
                "edges.bin": {
                    "sha256": _file_sha256(bin_path),
                    "size_bytes": bin_path.stat().st_size,
                },
                "edges.csv": {
                    "sha256": _file_sha256(csv_path),
                    "size_bytes": csv_path.stat().st_size,
                },
            },
        }

        manifest_path = out_dir / "manifest.yaml"
        with open(manifest_path, "w") as f:
            yaml.dump(manifest, f, default_flow_style=False, sort_keys=False)

        return manifest

    def verify(self, dataset_type: str, name: str) -> dict[str, Any]:
        """Verify the integrity of a dataset by checking SHA-256 checksums.

        Args:
            dataset_type: Dataset category (e.g. 'synthetic').
            name: Dataset name.

        Returns:
            ``{"valid": bool, "errors": list[str]}``
        """
        dataset_dir = self.data_dir / dataset_type / name
        manifest_path = dataset_dir / "manifest.yaml"

        errors: list[str] = []

        if not manifest_path.exists():
            errors.append(f"manifest.yaml not found in {dataset_dir}")
            return {"valid": False, "errors": errors}

        with open(manifest_path) as f:
            manifest: dict[str, Any] = yaml.safe_load(f)

        for filename, file_info in manifest.get("files", {}).items():
            file_path = dataset_dir / filename
            if not file_path.exists():
                errors.append(f"Missing file: {filename}")
                continue

            expected = file_info.get("sha256", "")
            actual = _file_sha256(file_path)
            if actual != expected:
                errors.append(
                    f"Checksum mismatch for {filename}: "
                    f"expected {expected}, got {actual}"
                )

        return {"valid": len(errors) == 0, "errors": errors}

    def list_datasets(self) -> list[dict[str, Any]]:
        """Scan data_dir for manifest.yaml files and return dataset summaries.

        Returns:
            List of dicts with keys: name, type, node_count, edge_count, generator.
        """
        results: list[dict[str, Any]] = []

        if not self.data_dir.exists():
            return results

        for manifest_path in sorted(self.data_dir.rglob("manifest.yaml")):
            try:
                with open(manifest_path) as f:
                    m: dict[str, Any] = yaml.safe_load(f)
                results.append(
                    {
                        "name": m.get("name", ""),
                        "type": m.get("type", ""),
                        "node_count": m.get("node_count", 0),
                        "edge_count": m.get("edge_count", 0),
                        "generator": m.get("generator", {}).get("function", ""),
                    }
                )
            except Exception:  # noqa: BLE001
                pass

        return results

    def get_manifest(self, dataset_type: str, name: str) -> dict[str, Any] | None:
        """Load and return the manifest for a dataset.

        Args:
            dataset_type: Dataset category (e.g. 'synthetic').
            name: Dataset name.

        Returns:
            Manifest dict, or None if not found.
        """
        manifest_path = self.data_dir / dataset_type / name / "manifest.yaml"
        if not manifest_path.exists():
            return None
        with open(manifest_path) as f:
            return yaml.safe_load(f)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_generator(
        self,
        generator: str,
        params: dict[str, Any],
    ) -> tuple[int, list[tuple[int, int]]]:
        """Dispatch to the appropriate xraybench_core generator.

        Returns:
            (node_count, edges)
        """
        if generator == "chain":
            length: int = params["length"]
            seed: int = params.get("seed", 0)
            edges = generators.generate_chain(length, seed)
            node_count = length
            return node_count, edges

        if generator == "hub":
            hub_count: int = params["hub_count"]
            spokes_per_hub: int = params["spokes_per_hub"]
            seed = params.get("seed", 0)
            node_count, edges = generators.generate_hub_graph(hub_count, spokes_per_hub, seed)
            return node_count, edges

        if generator == "deep_traversal":
            num_roots: int = params["num_roots"]
            fanout_per_level: list[int] = params["fanout_per_level"]
            seed = params.get("seed", 0)
            node_count, edges = generators.generate_deep_traversal(num_roots, fanout_per_level, seed)
            return node_count, edges

        if generator == "power_law":
            node_count = params["node_count"]
            m: int = params.get("m", 2)
            seed = params.get("seed", 0)
            edges = generators.generate_power_law_edges(node_count, m, seed)
            return node_count, edges

        raise ValueError(
            f"Unknown generator: {generator!r}. "
            "Supported: chain, hub, deep_traversal, power_law"
        )
