use xraybench_types::Edge;

/// Generate a hub-and-spoke graph.
///
/// Hubs are nodes 0..hub_count.
/// Spokes are nodes hub_count..total_node_count.
/// Each hub connects to exactly `spokes_per_hub` spokes.
/// Spokes are assigned round-robin across hubs for determinism (seed unused beyond API consistency).
///
/// Returns (total_node_count, edges).
pub fn generate_hub_graph(hub_count: u64, spokes_per_hub: u64, seed: u64) -> (u64, Vec<Edge>) {
    let _ = seed; // preserved for API consistency; graph is deterministic by construction
    let total_nodes = hub_count + hub_count * spokes_per_hub;
    let mut edges = Vec::with_capacity((hub_count * spokes_per_hub) as usize);

    for hub in 0..hub_count {
        for s in 0..spokes_per_hub {
            let spoke = hub_count + hub * spokes_per_hub + s;
            edges.push(Edge {
                source: hub,
                target: spoke,
            });
        }
    }

    (total_nodes, edges)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correct_counts() {
        let (node_count, edges) = generate_hub_graph(5, 10, 42);
        assert_eq!(node_count, 5 + 5 * 10); // 55 nodes
        assert_eq!(edges.len(), 5 * 10); // 50 edges
    }

    #[test]
    fn hub_degree() {
        let hub_count = 4u64;
        let spokes_per_hub = 7u64;
        let (_, edges) = generate_hub_graph(hub_count, spokes_per_hub, 0);
        for hub in 0..hub_count {
            let out_degree = edges.iter().filter(|e| e.source == hub).count();
            assert_eq!(
                out_degree, spokes_per_hub as usize,
                "hub {hub} should have exactly {spokes_per_hub} outgoing edges"
            );
        }
    }

    #[test]
    fn deterministic() {
        let (nc_a, ea) = generate_hub_graph(3, 5, 42);
        let (nc_b, eb) = generate_hub_graph(3, 5, 42);
        assert_eq!(nc_a, nc_b);
        assert_eq!(ea.len(), eb.len());
        for (a, b) in ea.iter().zip(eb.iter()) {
            assert_eq!(a.source, b.source);
            assert_eq!(a.target, b.target);
        }
    }
}
