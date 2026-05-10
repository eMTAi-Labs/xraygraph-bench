import os
import tempfile
import xraybench_core


def test_deep_traversal_basic():
    node_count, edges = xraybench_core.generators.generate_deep_traversal(
        num_roots=1, fanout_per_level=[3, 2], seed=42
    )
    assert node_count == 10  # 1 + 3 + 6
    assert len(edges) >= 9


def test_deep_traversal_deterministic():
    _, e1 = xraybench_core.generators.generate_deep_traversal(1, [5, 3], 42)
    _, e2 = xraybench_core.generators.generate_deep_traversal(1, [5, 3], 42)
    assert e1 == e2


def test_power_law():
    edges = xraybench_core.generators.generate_power_law_edges(1000, 3, 42)
    assert len(edges) > 100


def test_hub_graph():
    node_count, edges = xraybench_core.generators.generate_hub_graph(5, 100, 42)
    assert node_count == 505
    assert len(edges) == 500


def test_chain():
    edges = xraybench_core.generators.generate_chain(100, 42)
    assert len(edges) == 99
    assert edges[0] == (0, 1)


def test_estimate_node_count():
    est = xraybench_core.generators.estimate_node_count(1, [10, 5, 3])
    assert est == 1 + 10 + 50 + 150


def test_write_edges_binary():
    edges = [(0, 1), (1, 2), (2, 3)]
    with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as f:
        path = f.name
    try:
        xraybench_core.generators.write_edges_binary(edges, path)
        assert os.path.getsize(path) == 3 * 16
    finally:
        os.unlink(path)


def test_write_edges_csv():
    edges = [(0, 1), (1, 2)]
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        path = f.name
    try:
        xraybench_core.generators.write_edges_csv(edges, path)
        with open(path) as f:
            lines = f.readlines()
        assert lines[0].strip() == "source,target"
        assert lines[1].strip() == "0,1"
    finally:
        os.unlink(path)
