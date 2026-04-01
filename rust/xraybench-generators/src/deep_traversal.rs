use rand::Rng;
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha20Rng;
use xraybench_types::Edge;

const FRONTIER_CAP: usize = 10_000_000;

/// Estimate the theoretical node count for a deep traversal graph.
pub fn estimate_node_count(num_roots: u64, fanout_per_level: &[u32]) -> u64 {
    let mut total = num_roots;
    let mut level_size = num_roots;
    for &fanout in fanout_per_level {
        level_size *= fanout as u64;
        total += level_size;
    }
    total
}

/// Estimate the theoretical edge count (excluding cross-edges).
pub fn estimate_edge_count(num_roots: u64, fanout_per_level: &[u32]) -> u64 {
    let mut total_edges = 0u64;
    let mut level_size = num_roots;
    for &fanout in fanout_per_level {
        total_edges += level_size * fanout as u64;
        level_size *= fanout as u64;
    }
    total_edges
}

/// Generate a deep traversal graph with cross-edges.
///
/// Roots are 0..num_roots. Each level expands frontier by `fanout_per_level[level]`.
/// Adds ~5% cross-edges within the same level (sampled from frontier pairs).
/// Safety cap: if frontier > 10M nodes, sample down.
///
/// Returns (total_node_count, edges).
pub fn generate_deep_traversal(
    num_roots: u64,
    fanout_per_level: &[u32],
    seed: u64,
) -> (u64, Vec<Edge>) {
    let mut rng = ChaCha20Rng::seed_from_u64(seed);

    if fanout_per_level.is_empty() {
        return (num_roots, Vec::new());
    }

    let estimated = estimate_node_count(num_roots, fanout_per_level);
    let mut edges = Vec::with_capacity(estimate_edge_count(num_roots, fanout_per_level) as usize);

    let mut next_id: u64 = num_roots;
    let mut frontier: Vec<u64> = (0..num_roots).collect();

    for &fanout in fanout_per_level {
        if fanout == 0 {
            frontier.clear();
            break;
        }

        // Safety cap: sample frontier if too large
        if frontier.len() > FRONTIER_CAP {
            let mut sampled = Vec::with_capacity(FRONTIER_CAP);
            for _ in 0..FRONTIER_CAP {
                let idx = rng.gen_range(0..frontier.len());
                sampled.push(frontier[idx]);
            }
            frontier = sampled;
        }

        let mut new_frontier = Vec::with_capacity(frontier.len() * fanout as usize);

        for &parent in &frontier {
            for _ in 0..fanout {
                let child = next_id;
                next_id += 1;
                edges.push(Edge {
                    source: parent,
                    target: child,
                });
                new_frontier.push(child);
            }
        }

        // Add ~5% cross-edges within this level
        let cross_edge_count = (new_frontier.len() as f64 * 0.05).ceil() as usize;
        let level_len = new_frontier.len();
        if level_len >= 2 {
            for _ in 0..cross_edge_count {
                let a_idx = rng.gen_range(0..level_len);
                let b_idx = rng.gen_range(0..level_len);
                if a_idx != b_idx {
                    edges.push(Edge {
                        source: new_frontier[a_idx],
                        target: new_frontier[b_idx],
                    });
                }
            }
        }

        frontier = new_frontier;
    }

    let actual_node_count = next_id;
    // Use estimated if it matches closely (it should), otherwise actual
    let _ = estimated;

    (actual_node_count, edges)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_root_single_level() {
        let (node_count, edges) = generate_deep_traversal(1, &[3], 42);
        // 1 root + 3 children = 4 nodes
        assert_eq!(node_count, 4);
        // 3 tree edges + possibly 0 cross edges (only 3 nodes at level, cross = ceil(3*0.05)=1 but
        // they might self-collide)
        assert!(edges.len() >= 3, "should have at least 3 tree edges");
        // All tree edges: root 0 -> children 1,2,3
        let tree_edges: Vec<_> = edges.iter().filter(|e| e.source == 0).collect();
        assert_eq!(tree_edges.len(), 3);
    }

    #[test]
    fn deterministic() {
        let (nc_a, ea) = generate_deep_traversal(5, &[4, 3], 42);
        let (nc_b, eb) = generate_deep_traversal(5, &[4, 3], 42);
        assert_eq!(nc_a, nc_b);
        assert_eq!(ea.len(), eb.len());
        for (a, b) in ea.iter().zip(eb.iter()) {
            assert_eq!(a.source, b.source);
            assert_eq!(a.target, b.target);
        }
    }

    #[test]
    fn multiple_roots() {
        let (node_count, edges) = generate_deep_traversal(3, &[2], 1);
        // 3 roots + 3*2=6 children = 9 nodes
        assert_eq!(node_count, 9);
        // 6 tree edges minimum
        let tree_edges: Vec<_> = edges.iter().filter(|e| e.source < 3).collect();
        assert_eq!(tree_edges.len(), 6);
    }

    #[test]
    fn realistic_schedule() {
        // [10, 5, 3, 2] with 1 root: 1+10+50+150+300 = 511 nodes
        let (node_count, _edges) = generate_deep_traversal(1, &[10, 5, 3, 2], 0);
        assert_eq!(node_count, 511);
    }

    #[test]
    fn estimate_matches_actual() {
        let fanout = vec![5, 4, 3];
        let num_roots = 2u64;
        let estimated = estimate_node_count(num_roots, &fanout);
        let (actual, _) = generate_deep_traversal(num_roots, &fanout, 0);
        assert_eq!(
            estimated, actual,
            "estimate_node_count should match actual: {estimated} vs {actual}"
        );
    }

    #[test]
    fn edges_reference_valid_nodes() {
        let (node_count, edges) = generate_deep_traversal(4, &[3, 2], 7);
        for e in &edges {
            assert!(
                e.source < node_count,
                "source {} >= node_count {}",
                e.source,
                node_count
            );
            assert!(
                e.target < node_count,
                "target {} >= node_count {}",
                e.target,
                node_count
            );
        }
    }

    #[test]
    fn empty_fanout() {
        let (node_count, edges) = generate_deep_traversal(5, &[], 42);
        assert_eq!(node_count, 5);
        assert!(edges.is_empty());
    }

    #[test]
    fn cross_edges_exist() {
        // 10 roots, [10,10] => level 1 has 100 nodes, level 2 has 1000 nodes
        // Cross edges at each level should exist
        let (_, edges) = generate_deep_traversal(10, &[10, 10], 42);
        let tree_edge_estimate = estimate_edge_count(10, &[10, 10]);
        // Total should exceed tree edges due to cross-edges
        assert!(
            edges.len() as u64 > tree_edge_estimate,
            "total edges ({}) should exceed tree edges ({}) due to cross-edges",
            edges.len(),
            tree_edge_estimate
        );
    }
}
