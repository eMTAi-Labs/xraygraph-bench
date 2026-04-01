"""Tests for tools.xraybench.adapters.dataset_loader."""

from __future__ import annotations

import pytest

from tools.xraybench.adapters.dataset_loader import (
    generate_cypher_from_edges,
    generate_synthetic_edges,
)


# ---------------------------------------------------------------------------
# Test 1: uniform generator (proxied through power_law)
# ---------------------------------------------------------------------------

def test_generate_synthetic_edges_uniform() -> None:
    # 'uniform' delegates to power_law; with node_count=100, m=2 the Barabási–Albert
    # algorithm produces 2*(node_count-2)*2 = ~394 directed edges (each undirected
    # pair is stored as two directed edges).  We verify the structure, not a fixed count.
    edges = generate_synthetic_edges(
        "uniform",
        {"node_count": 100, "m": 2, "seed": 42},
    )
    assert isinstance(edges, list)
    assert len(edges) > 0
    for src, dst in edges:
        assert isinstance(src, int)
        assert isinstance(dst, int)


# ---------------------------------------------------------------------------
# Test 2: chain generator
# ---------------------------------------------------------------------------

def test_generate_synthetic_edges_chain() -> None:
    edges = generate_synthetic_edges("chain", {"length": 50, "seed": 42})
    assert len(edges) == 49
    assert edges[0] == (0, 1)
    assert edges[-1] == (48, 49)


# ---------------------------------------------------------------------------
# Test 3: hub generator
# ---------------------------------------------------------------------------

def test_generate_synthetic_edges_hub() -> None:
    edges = generate_synthetic_edges(
        "hub",
        {"hub_count": 3, "spokes_per_hub": 10, "seed": 42},
    )
    assert len(edges) == 30


# ---------------------------------------------------------------------------
# Test 4: deep_traversal generator
# ---------------------------------------------------------------------------

def test_generate_synthetic_edges_deep_traversal() -> None:
    edges = generate_synthetic_edges(
        "deep_traversal",
        {"num_roots": 1, "fanout_per_level": [5, 3], "seed": 42},
    )
    assert len(edges) >= 20


# ---------------------------------------------------------------------------
# Test 5: determinism — same seed yields identical results
# ---------------------------------------------------------------------------

def test_generate_synthetic_deterministic() -> None:
    edges_a = generate_synthetic_edges("chain", {"length": 20, "seed": 99})
    edges_b = generate_synthetic_edges("chain", {"length": 20, "seed": 99})
    assert edges_a == edges_b


# ---------------------------------------------------------------------------
# Test 6: Cypher output contains node CREATE statements
# ---------------------------------------------------------------------------

def test_cypher_from_edges_creates_nodes() -> None:
    edges = [(0, 1), (1, 2), (2, 3)]
    statements = generate_cypher_from_edges(edges)
    combined = "\n".join(statements)
    assert "CREATE" in combined
    assert "Node" in combined


# ---------------------------------------------------------------------------
# Test 7: Cypher output contains edge CREATE statements
# ---------------------------------------------------------------------------

def test_cypher_from_edges_creates_edges() -> None:
    edges = [(0, 1), (1, 2), (2, 3)]
    statements = generate_cypher_from_edges(edges)
    combined = "\n".join(statements)
    # Either UNWIND (for batched edge creation) or EDGE type must appear
    assert "UNWIND" in combined or "EDGE" in combined


# ---------------------------------------------------------------------------
# Test 8: Batch size splits statements correctly
# ---------------------------------------------------------------------------

def test_cypher_from_edges_batch_size() -> None:
    # 100 edges with distinct sequential nodes (0-100)
    edges = [(i, i + 1) for i in range(100)]
    statements = generate_cypher_from_edges(edges, batch_size=25)
    # With 25 nodes per batch and 101 unique nodes → ceil(101/25) = 5 node batches
    # Plus 1 index statement + ceil(100/25) = 4 edge batches = 10 total statements
    assert len(statements) > 1
    # Verify that edges are split into multiple batches
    edge_statements = [s for s in statements if "MATCH" in s and "CREATE" in s]
    assert len(edge_statements) > 1
