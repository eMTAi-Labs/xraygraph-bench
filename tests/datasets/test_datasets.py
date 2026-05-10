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
        assert len(cs) == 64
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
