"""Synthetic graph generators for benchmark datasets.

All generators accept a seed parameter for deterministic output. Given the
same seed and parameters, the same graph is produced.

Generators output Cypher CREATE statements by default. Use the output
parameter to select alternative formats.
"""

from __future__ import annotations

import random
from typing import Any, Iterator


def uniform_nodes(
    node_count: int = 1_000_000,
    properties: dict[str, str] | None = None,
    nullable_ratio: float = 0.0,
    seed: int = 42,
    batch_size: int = 1000,
) -> Iterator[str]:
    """Generate nodes with uniformly distributed property values.

    Args:
        node_count: Number of nodes to generate.
        properties: Property name to type mapping. Supported types:
            integer, float, string.
        nullable_ratio: Fraction of nullable_field values set to null.
        seed: Random seed for deterministic generation.
        batch_size: Number of nodes per CREATE statement.

    Yields:
        Cypher CREATE statements.
    """
    if properties is None:
        properties = {"id": "integer", "value": "float", "category": "string"}

    rng = random.Random(seed)
    categories = [f"cat_{i}" for i in range(100)]

    for batch_start in range(0, node_count, batch_size):
        batch_end = min(batch_start + batch_size, node_count)
        creates: list[str] = []

        for i in range(batch_start, batch_end):
            props: dict[str, Any] = {}
            for prop_name, prop_type in properties.items():
                if prop_name == "id":
                    props["id"] = i
                elif prop_type == "integer":
                    props[prop_name] = rng.randint(0, 1_000_000)
                elif prop_type == "float":
                    if prop_name == "nullable_field" and rng.random() < nullable_ratio:
                        continue  # Skip to leave as null
                    props[prop_name] = round(rng.random(), 6)
                elif prop_type == "string":
                    props[prop_name] = rng.choice(categories)

            prop_str = ", ".join(
                f"{k}: {_cypher_value(v)}" for k, v in props.items()
            )
            creates.append(f"(:Node {{{prop_str}}})")

        yield f"CREATE {', '.join(creates)}"


def power_law_graph(
    node_count: int = 1_000_000,
    edge_count: int = 10_000_000,
    power_law_exponent: float = 2.1,  # noqa: ARG001
    seed: int = 42,
    batch_size: int = 1000,
) -> Iterator[str]:
    """Generate a graph with power-law degree distribution.

    Uses a simplified preferential attachment model. Nodes with higher
    degree are more likely to receive new edges.

    Args:
        node_count: Number of nodes.
        edge_count: Number of edges.
        power_law_exponent: Controls the skewness of degree distribution.
        seed: Random seed.
        batch_size: Edges per CREATE batch.

    Yields:
        Cypher CREATE statements (first nodes, then edges).
    """
    rng = random.Random(seed)

    # Generate nodes
    for batch_start in range(0, node_count, batch_size):
        batch_end = min(batch_start + batch_size, node_count)
        creates = [f"(:Node {{id: {i}}})" for i in range(batch_start, batch_end)]
        yield f"CREATE {', '.join(creates)}"

    # Generate edges using preferential attachment
    degree = [1] * node_count  # Initialize all with degree 1
    total_degree = node_count

    batch: list[tuple[int, int]] = []

    for _ in range(edge_count):
        # Source: uniform random
        src = rng.randint(0, node_count - 1)
        # Target: preferential attachment (probability proportional to degree)
        target_threshold = rng.random() * total_degree
        cumulative = 0
        dst = 0
        for j in range(node_count):
            cumulative += degree[j]
            if cumulative >= target_threshold:
                dst = j
                break

        if src != dst:
            batch.append((src, dst))
            degree[dst] += 1
            total_degree += 1

        if len(batch) >= batch_size:
            for s, d in batch:
                yield (
                    f"MATCH (a:Node {{id: {s}}}), (b:Node {{id: {d}}}) "
                    f"CREATE (a)-[:EDGE]->(b)"
                )
            batch = []

    # Flush remaining
    for s, d in batch:
        yield (
            f"MATCH (a:Node {{id: {s}}}), (b:Node {{id: {d}}}) "
            f"CREATE (a)-[:EDGE]->(b)"
        )


def hub_graph(
    hub_count: int = 100,
    spoke_count_per_hub: int = 100_000,
    edge_type: str = "CONNECTED_TO",
    seed: int = 42,
    batch_size: int = 1000,
) -> Iterator[str]:
    """Generate a hub-and-spoke graph.

    Args:
        hub_count: Number of hub nodes.
        spoke_count_per_hub: Number of spoke nodes per hub.
        edge_type: Relationship type for hub-spoke edges.
        seed: Random seed.
        batch_size: Nodes/edges per CREATE batch.

    Yields:
        Cypher CREATE statements.
    """
    rng = random.Random(seed)
    total_spokes = hub_count * spoke_count_per_hub

    # Create hub nodes
    hub_creates = [f"(:Hub {{id: {i}}})" for i in range(hub_count)]
    yield f"CREATE {', '.join(hub_creates)}"

    # Create spoke nodes in batches
    for batch_start in range(0, total_spokes, batch_size):
        batch_end = min(batch_start + batch_size, total_spokes)
        creates: list[str] = []
        for i in range(batch_start, batch_end):
            weight = round(rng.random(), 4)
            creates.append(
                f"(:Spoke {{id: {i}, weight: {weight}, label: 'spoke_{i % 100}'}})"
            )
        yield f"CREATE {', '.join(creates)}"

    # Create edges: each hub connects to its spokes
    for hub_id in range(hub_count):
        spoke_start = hub_id * spoke_count_per_hub
        spoke_end = spoke_start + spoke_count_per_hub

        for batch_start in range(spoke_start, spoke_end, batch_size):
            batch_end = min(batch_start + batch_size, spoke_end)
            for spoke_id in range(batch_start, batch_end):
                edge_weight = round(rng.random(), 4)
                yield (
                    f"MATCH (h:Hub {{id: {hub_id}}}), (s:Spoke {{id: {spoke_id}}}) "
                    f"CREATE (h)-[:{edge_type} {{weight: {edge_weight}}}]->(s)"
                )


def community_graph(
    community_count: int = 100,
    nodes_per_community: int = 5000,
    intra_edge_density: float = 0.01,
    inter_edge_density: float = 0.0001,
    seed: int = 42,
    batch_size: int = 1000,
) -> Iterator[str]:
    """Generate a graph with community structure.

    Args:
        community_count: Number of communities.
        nodes_per_community: Nodes per community.
        intra_edge_density: Edge density within communities.
        inter_edge_density: Edge density between communities.
        seed: Random seed.
        batch_size: Items per CREATE batch.

    Yields:
        Cypher CREATE statements.
    """
    rng = random.Random(seed)
    total_nodes = community_count * nodes_per_community

    # Create nodes with community assignment
    for batch_start in range(0, total_nodes, batch_size):
        batch_end = min(batch_start + batch_size, total_nodes)
        creates: list[str] = []
        for i in range(batch_start, batch_end):
            community = i // nodes_per_community
            creates.append(f"(:Node {{id: {i}, community: {community}}})")
        yield f"CREATE {', '.join(creates)}"

    # Create intra-community edges
    intra_edges_per_node = max(1, int(nodes_per_community * intra_edge_density))
    for c in range(community_count):
        base = c * nodes_per_community
        for i in range(nodes_per_community):
            src = base + i
            for _ in range(intra_edges_per_node):
                dst = base + rng.randint(0, nodes_per_community - 1)
                if src != dst:
                    yield (
                        f"MATCH (a:Node {{id: {src}}}), (b:Node {{id: {dst}}}) "
                        f"CREATE (a)-[:EDGE]->(b)"
                    )

    # Create inter-community edges
    inter_edges_total = int(
        community_count * (community_count - 1) * nodes_per_community * inter_edge_density
    )
    for _ in range(inter_edges_total):
        c1 = rng.randint(0, community_count - 1)
        c2 = rng.randint(0, community_count - 2)
        if c2 >= c1:
            c2 += 1
        src = c1 * nodes_per_community + rng.randint(0, nodes_per_community - 1)
        dst = c2 * nodes_per_community + rng.randint(0, nodes_per_community - 1)
        yield (
            f"MATCH (a:Node {{id: {src}}}), (b:Node {{id: {dst}}}) "
            f"CREATE (a)-[:EDGE]->(b)"
        )


def chain_graph(
    length: int = 1_000_000,
    seed: int = 42,
    batch_size: int = 1000,
) -> Iterator[str]:
    """Generate a simple chain (linked list) graph.

    Useful for measuring sequential traversal cost without fan-out.

    Args:
        length: Number of nodes in the chain.
        seed: Random seed (for property values).
        batch_size: Nodes per CREATE batch.

    Yields:
        Cypher CREATE statements.
    """
    rng = random.Random(seed)

    # Create nodes
    for batch_start in range(0, length, batch_size):
        batch_end = min(batch_start + batch_size, length)
        creates = [
            f"(:Node {{id: {i}, value: {round(rng.random(), 4)}}})"
            for i in range(batch_start, batch_end)
        ]
        yield f"CREATE {', '.join(creates)}"

    # Create chain edges
    for i in range(length - 1):
        yield (
            f"MATCH (a:Node {{id: {i}}}), (b:Node {{id: {i + 1}}}) "
            f"CREATE (a)-[:NEXT]->(b)"
        )


def _cypher_value(value: Any) -> str:
    """Format a Python value as a Cypher literal."""
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    elif isinstance(value, bool):
        return "true" if value else "false"
    elif isinstance(value, (int, float)):
        return str(value)
    elif value is None:
        return "null"
    else:
        return str(value)
