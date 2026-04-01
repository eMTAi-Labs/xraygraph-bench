"""Tests for SNAP edge-list ingestion via DatasetManager.ingest_edge_list."""

import os
import shutil
import tempfile

from tools.xraybench.dataset_manager import DatasetManager


class TestSnapConversion:
    def setup_method(self):
        self.data_dir = tempfile.mkdtemp(prefix="xraybench_snap_test_")
        self.manager = DatasetManager(data_dir=self.data_dir)

    def teardown_method(self):
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def test_ingest_snap_edge_list(self):
        mock_dir = tempfile.mkdtemp()
        edge_file = os.path.join(mock_dir, "mock-graph.txt")
        with open(edge_file, "w") as f:
            f.write("# Mock SNAP graph\n")
            for s, t in [(0, 1), (1, 2), (2, 3), (3, 4), (4, 0), (0, 2)]:
                f.write(f"{s}\t{t}\n")
        try:
            manifest = self.manager.ingest_edge_list(
                name="mock-snap", dataset_type="snap",
                edge_list_path=edge_file,
                metadata={"description": "Mock SNAP graph"},
            )
            assert manifest["node_count"] == 5
            assert manifest["edge_count"] == 6
            assert manifest["type"] == "snap"
            ds_dir = os.path.join(self.data_dir, "snap", "mock-snap")
            assert os.path.isfile(os.path.join(ds_dir, "edges.bin"))
            assert os.path.isfile(os.path.join(ds_dir, "edges.csv"))
            assert os.path.isfile(os.path.join(ds_dir, "manifest.yaml"))
        finally:
            shutil.rmtree(mock_dir)

    def test_ingest_preserves_edge_data(self):
        mock_dir = tempfile.mkdtemp()
        edge_file = os.path.join(mock_dir, "test.txt")
        expected_edges = [(0, 1), (1, 2), (2, 0)]
        with open(edge_file, "w") as f:
            for s, t in expected_edges:
                f.write(f"{s}\t{t}\n")
        try:
            self.manager.ingest_edge_list(
                name="test-preserve", dataset_type="snap",
                edge_list_path=edge_file,
            )
            csv_path = os.path.join(self.data_dir, "snap", "test-preserve", "edges.csv")
            with open(csv_path) as f:
                lines = f.readlines()
            data_lines = [l.strip() for l in lines[1:] if l.strip()]
            parsed = [tuple(int(x) for x in l.split(",")) for l in data_lines]
            assert parsed == expected_edges
        finally:
            shutil.rmtree(mock_dir)

    def test_ingest_creates_verifiable_manifest(self):
        mock_dir = tempfile.mkdtemp()
        edge_file = os.path.join(mock_dir, "test.txt")
        with open(edge_file, "w") as f:
            for s, t in [(0, 1), (1, 2)]:
                f.write(f"{s}\t{t}\n")
        try:
            self.manager.ingest_edge_list(
                name="test-verifiable", dataset_type="snap",
                edge_list_path=edge_file,
            )
            result = self.manager.verify("snap", "test-verifiable")
            assert result["valid"] is True
        finally:
            shutil.rmtree(mock_dir)
