"""Dataset download and ingestion for SNAP and OGB datasets."""

from __future__ import annotations

import gzip
import hashlib
import logging
import shutil
import urllib.request
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SNAP_DATASETS = {
    "soc-LiveJournal1": {
        "url": "https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz",
        "nodes": 4847571,
        "edges": 68993773,
        "description": "LiveJournal social network (directed)",
    },
    "web-Google": {
        "url": "https://snap.stanford.edu/data/web-Google.txt.gz",
        "nodes": 875713,
        "edges": 5105039,
        "description": "Google web graph (directed)",
    },
}

OGB_DATASETS = {
    "ogbn-products": {
        "nodes": 2449029,
        "edges": 61859140,
        "description": "Amazon product co-purchasing network",
    },
    "ogbn-papers100M": {
        "nodes": 111059956,
        "edges": 1615685872,
        "description": "Citation network from Microsoft Academic Graph",
    },
}

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "datasets"


def download_snap_dataset(
    name: str,
    output_dir: str | Path | None = None,
) -> Path:
    """Download a SNAP dataset.

    Args:
        name: SNAP dataset name (e.g., 'soc-LiveJournal1').
        output_dir: Directory to save the file. Defaults to datasets/snap/data/.

    Returns:
        Path to the decompressed edge list file.

    Raises:
        ValueError: If the dataset name is not recognized.
    """
    if name not in SNAP_DATASETS:
        raise ValueError(
            f"Unknown SNAP dataset: {name}. "
            f"Available: {', '.join(sorted(SNAP_DATASETS))}"
        )

    info = SNAP_DATASETS[name]
    if output_dir is None:
        output_dir = DATA_DIR / "snap" / "data"
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    gz_path = output_dir / f"{name}.txt.gz"
    txt_path = output_dir / f"{name}.txt"

    if txt_path.exists():
        logger.info("Dataset already downloaded: %s", txt_path)
        return txt_path

    logger.info("Downloading %s from %s", name, info["url"])
    urllib.request.urlretrieve(info["url"], gz_path)

    logger.info("Decompressing %s", gz_path)
    with gzip.open(gz_path, "rb") as f_in:
        with open(txt_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    gz_path.unlink()
    logger.info("Dataset ready: %s", txt_path)
    return txt_path


def parse_snap_edge_list(path: str | Path) -> list[tuple[int, int]]:
    """Parse a SNAP edge list file.

    Args:
        path: Path to the decompressed edge list file.

    Returns:
        List of (source, target) tuples.
    """
    edges: list[tuple[int, int]] = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                try:
                    src, dst = int(parts[0]), int(parts[1])
                    edges.append((src, dst))
                except ValueError:
                    continue
    return edges


def snap_to_cypher(
    edges: list[tuple[int, int]],
    edge_type: str = "EDGE",
    batch_size: int = 1000,
) -> list[str]:
    """Convert SNAP edge list to Cypher CREATE statements.

    Args:
        edges: List of (source, target) tuples.
        edge_type: Relationship type name.
        batch_size: Number of edges per CREATE statement.

    Returns:
        List of Cypher CREATE statement strings.
    """
    # Collect unique nodes
    nodes: set[int] = set()
    for src, dst in edges:
        nodes.add(src)
        nodes.add(dst)

    statements: list[str] = []

    # Create nodes in batches
    node_list = sorted(nodes)
    for i in range(0, len(node_list), batch_size):
        batch = node_list[i : i + batch_size]
        creates = ", ".join(f"(:Node {{id: {nid}}})" for nid in batch)
        statements.append(f"CREATE {creates}")

    # Create edges in batches
    for i in range(0, len(edges), batch_size):
        batch = edges[i : i + batch_size]
        merges = []
        for src, dst in batch:
            merges.append(
                f"MATCH (a:Node {{id: {src}}}), (b:Node {{id: {dst}}}) "
                f"CREATE (a)-[:{edge_type}]->(b)"
            )
        statements.extend(merges)

    return statements


def download_ogb_dataset(name: str) -> dict[str, Any]:
    """Download an OGB dataset using the ogb Python package.

    Args:
        name: OGB dataset name (e.g., 'ogbn-products').

    Returns:
        Dictionary with 'edge_index', 'node_features', and metadata.

    Raises:
        ImportError: If the ogb package is not installed.
        ValueError: If the dataset name is not recognized.
    """
    if name not in OGB_DATASETS:
        raise ValueError(
            f"Unknown OGB dataset: {name}. "
            f"Available: {', '.join(sorted(OGB_DATASETS))}"
        )

    try:
        from ogb.nodeproppred import NodePropPredDataset
    except ImportError:
        raise ImportError(
            "The ogb package is required for OGB datasets. "
            "Run: pip install ogb"
        )

    logger.info("Downloading OGB dataset: %s", name)
    dataset = NodePropPredDataset(name=name, root=str(DATA_DIR / "ogb" / "data"))
    graph = dataset[0]

    return {
        "edge_index": graph["edge_index"],
        "node_feat": graph.get("node_feat"),
        "num_nodes": graph["num_nodes"],
        "metadata": OGB_DATASETS[name],
    }


def file_checksum(path: str | Path, algorithm: str = "sha256") -> str:
    """Compute a file checksum.

    Args:
        path: Path to the file.
        algorithm: Hash algorithm name (default: sha256).

    Returns:
        Hex digest string.
    """
    h = hashlib.new(algorithm)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
