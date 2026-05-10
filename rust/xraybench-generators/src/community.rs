use rand::Rng;
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha20Rng;
use xraybench_types::Edge;

/// Stochastic Block Model community graph.
///
/// - Nodes are partitioned into `community_count` communities of `nodes_per_community` each.
/// - Intra-community edges per node: `max(1, floor(npc * intra_density))`
/// - Inter-community edges total: `floor(cc*(cc-1)*npc * inter_density)`
///
/// Returns (total_node_count, edges).
pub fn generate_community_graph(
    community_count: u64,
    nodes_per_community: u64,
    intra_density: f64,
    inter_density: f64,
    seed: u64,
) -> (u64, Vec<Edge>) {
    let mut rng = ChaCha20Rng::seed_from_u64(seed);

    let total_nodes = community_count * nodes_per_community;
    let mut edges = Vec::new();

    let intra_per_node = ((nodes_per_community as f64 * intra_density).floor() as u64).max(1);

    // Intra-community edges
    for c in 0..community_count {
        let base = c * nodes_per_community;
        for node_offset in 0..nodes_per_community {
            let node = base + node_offset;
            let mut count = 0u64;
            while count < intra_per_node {
                let candidate = base + rng.gen_range(0..nodes_per_community);
                if candidate != node {
                    edges.push(Edge {
                        source: node,
                        target: candidate,
                    });
                    count += 1;
                }
            }
        }
    }

    // Inter-community edges
    let inter_total = ((community_count * (community_count - 1)) as f64
        * nodes_per_community as f64
        * inter_density)
        .floor() as u64;

    for _ in 0..inter_total {
        let c1 = rng.gen_range(0..community_count);
        let mut c2 = rng.gen_range(0..community_count);
        while c2 == c1 {
            c2 = rng.gen_range(0..community_count);
        }
        let source = c1 * nodes_per_community + rng.gen_range(0..nodes_per_community);
        let target = c2 * nodes_per_community + rng.gen_range(0..nodes_per_community);
        edges.push(Edge { source, target });
    }

    (total_nodes, edges)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_community_structure() {
        // High intra_density, low inter_density
        let (_n, edges) = generate_community_graph(4, 50, 0.5, 0.01, 42);

        // Count intra vs inter edges
        let community_size = 50u64;
        let community_count = 4u64;

        let intra = edges
            .iter()
            .filter(|e| e.source / community_size == e.target / community_size)
            .count();
        let inter = edges
            .iter()
            .filter(|e| {
                e.source / community_size != e.target / community_size
                    && e.source < community_count * community_size
                    && e.target < community_count * community_size
            })
            .count();

        assert!(
            intra > 5 * inter,
            "intra ({intra}) should be > 5x inter ({inter})"
        );
    }

    #[test]
    fn deterministic() {
        let (nc_a, ea) = generate_community_graph(3, 20, 0.3, 0.05, 99);
        let (nc_b, eb) = generate_community_graph(3, 20, 0.3, 0.05, 99);
        assert_eq!(nc_a, nc_b);
        assert_eq!(ea.len(), eb.len());
        for (a, b) in ea.iter().zip(eb.iter()) {
            assert_eq!(a.source, b.source);
            assert_eq!(a.target, b.target);
        }
    }

    #[test]
    fn correct_node_count() {
        let (nc, _) = generate_community_graph(5, 100, 0.2, 0.02, 1);
        assert_eq!(nc, 500);
    }
}
