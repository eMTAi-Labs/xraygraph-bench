"""Shared dataset loader for xraygraph-bench.

Provides utilities to generate synthetic graph edges via xraybench_core
generators, convert edges to batched Cypher statements, and ingest data
into any adapter that implements BaseAdapter.
"""

from __future__ import annotations

from typing import Any

from xraybench_core import generators


def generate_synthetic_edges(generator: str, params: dict[str, Any]) -> list[tuple[int, int]]:
    """Generate a list of directed edges using the specified generator.

    Args:
        generator: One of 'uniform', 'chain', 'hub', 'deep_traversal', 'power_law'.
        params: Generator-specific parameters (see below).

    Generator parameters:
        uniform:
            node_count (int): Number of nodes.
            m (int): Edges per new node (Barabási–Albert style).
            seed (int): RNG seed.
        chain:
            length (int): Number of nodes in the chain (edges = length - 1).
            seed (int): RNG seed.
        hub:
            hub_count (int): Number of hub nodes.
            spokes_per_hub (int): Number of spoke nodes per hub.
            seed (int): RNG seed.
        deep_traversal:
            num_roots (int): Number of root nodes.
            fanout_per_level (list[int]): Fanout at each depth level.
            seed (int): RNG seed.
        power_law:
            node_count (int): Number of nodes.
            m (int): Edges per new node.
            seed (int): RNG seed.

    Returns:
        List of (source_id, target_id) integer tuples.

    Raises:
        ValueError: If *generator* is not a supported name.
    """
    if generator == "uniform":
        # 'uniform' has no separate exposed API; delegate to power_law as proxy
        node_count = params.get("node_count", 100)
        m = params.get("m", 2)
        seed = params.get("seed", 42)
        return list(generators.generate_power_law_edges(node_count, m, seed))

    elif generator == "chain":
        length = params.get("length", 10)
        seed = params.get("seed", 42)
        return list(generators.generate_chain(length, seed))

    elif generator == "hub":
        hub_count = params.get("hub_count", 1)
        spokes_per_hub = params.get("spokes_per_hub", 5)
        seed = params.get("seed", 42)
        _node_count, edges = generators.generate_hub_graph(hub_count, spokes_per_hub, seed)
        return list(edges)

    elif generator == "deep_traversal":
        num_roots = params.get("num_roots", 1)
        fanout_per_level = params.get("fanout_per_level", [3, 2])
        seed = params.get("seed", 42)
        _node_count, edges = generators.generate_deep_traversal(num_roots, fanout_per_level, seed)
        return list(edges)

    elif generator == "power_law":
        node_count = params.get("node_count", 100)
        m = params.get("m", 2)
        seed = params.get("seed", 42)
        return list(generators.generate_power_law_edges(node_count, m, seed))

    else:
        raise ValueError(
            f"Unknown generator: {generator!r}. "
            "Supported generators: 'uniform', 'chain', 'hub', 'deep_traversal', 'power_law'."
        )


def generate_cypher_from_edges(
    edges: list[tuple[int, int]],
    batch_size: int = 1000,
    node_label: str = "Node",
    edge_type: str = "EDGE",
) -> list[str]:
    """Convert a list of edges into batched Cypher CREATE statements.

    The generated statements follow this order:
    1. UNWIND-based node CREATE statements (one per batch of node IDs).
    2. A single CREATE INDEX ON :<node_label>(<id_prop>) statement.
    3. UNWIND-based relationship CREATE statements (one per batch of edges).

    Args:
        edges: Directed edge list as (source_id, target_id) integer tuples.
        batch_size: Maximum number of nodes or edges per UNWIND statement.
        node_label: Label to apply to every created node.
        edge_type: Relationship type for every created edge.

    Returns:
        Ordered list of Cypher statement strings.
    """
    statements: list[str] = []

    # 1. Collect unique node IDs, preserving deterministic ordering
    seen: set[int] = set()
    node_ids: list[int] = []
    for src, dst in edges:
        if src not in seen:
            seen.add(src)
            node_ids.append(src)
        if dst not in seen:
            seen.add(dst)
            node_ids.append(dst)

    # 2. Create nodes in batches
    for i in range(0, max(len(node_ids), 1), batch_size):
        batch = node_ids[i : i + batch_size]
        if not batch:
            break
        ids_literal = str(batch)
        statements.append(
            f"UNWIND {ids_literal} AS nid CREATE (:{node_label} {{id: nid}})"
        )

    # 3. Create index
    statements.append(f"CREATE INDEX ON :{node_label}(id)")

    # 4. Create edges in batches
    for i in range(0, len(edges), batch_size):
        batch = edges[i : i + batch_size]
        pairs_literal = str([{"s": s, "t": t} for s, t in batch])
        statements.append(
            f"UNWIND {pairs_literal} AS e "
            f"MATCH (a:{node_label} {{id: e.s}}), (b:{node_label} {{id: e.t}}) "
            f"CREATE (a)-[:{edge_type}]->(b)"
        )

    return statements


def load_edges_into_adapter(
    adapter: Any,
    edges: list[tuple[int, int]],
    batch_size: int = 1000,
    node_label: str = "Node",
    edge_type: str = "EDGE",
) -> tuple[int, int]:
    """Ingest edges into a graph database via an adapter.

    Generates Cypher statements from *edges*, executes each statement via
    *adapter.execute()*, then queries the engine to verify and return the
    actual node and edge counts.

    Args:
        adapter: An object with an ``execute(query, params=None)`` method
            (typically a :class:`~tools.xraybench.adapters.base.BaseAdapter`).
        edges: Directed edge list as (source_id, target_id) integer tuples.
        batch_size: Batch size passed to :func:`generate_cypher_from_edges`.
        node_label: Node label passed to :func:`generate_cypher_from_edges`.
        edge_type: Relationship type passed to :func:`generate_cypher_from_edges`.

    Returns:
        ``(node_count, edge_count)`` as reported by the engine after ingestion.
    """
    statements = generate_cypher_from_edges(
        edges, batch_size=batch_size, node_label=node_label, edge_type=edge_type
    )

    for stmt in statements:
        adapter.execute(stmt)

    # Verify counts
    node_result = adapter.execute(f"MATCH (n:{node_label}) RETURN count(n) AS cnt")
    edge_result = adapter.execute(
        f"MATCH (a:{node_label})-[r:{edge_type}]->(b:{node_label}) RETURN count(r) AS cnt"
    )

    node_count = int(node_result.rows[0]["cnt"]) if node_result.rows else 0
    edge_count = int(edge_result.rows[0]["cnt"]) if edge_result.rows else 0

    return node_count, edge_count
