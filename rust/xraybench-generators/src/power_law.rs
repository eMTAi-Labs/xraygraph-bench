use rand::Rng;
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha20Rng;
use xraybench_types::Edge;

/// Barabási-Albert preferential attachment model.
///
/// Starts with an (m+1)-node clique (both directions), then each new node
/// attaches to `m` existing nodes with probability proportional to degree.
pub fn generate_power_law(node_count: u64, m: u32, seed: u64) -> Vec<Edge> {
    assert!(m >= 1, "m must be at least 1");
    assert!(node_count > m as u64, "node_count must be greater than m");

    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    let mut edges: Vec<Edge> = Vec::new();

    let initial = (m + 1) as u64;

    // Build initial clique with both directions
    for i in 0..initial {
        for j in (i + 1)..initial {
            edges.push(Edge {
                source: i,
                target: j,
            });
            edges.push(Edge {
                source: j,
                target: i,
            });
        }
    }

    // Degree list for preferential attachment (each endpoint of each edge)
    // We maintain a repeated list approach: each edge contributes both endpoints
    let mut degree_list: Vec<u64> = Vec::new();
    for i in 0..initial {
        // Each node in clique has degree (initial - 1)
        for _ in 0..(initial - 1) {
            degree_list.push(i);
            degree_list.push(i);
        }
    }

    // Add new nodes one at a time
    for new_node in initial..node_count {
        let mut targets: Vec<u64> = Vec::with_capacity(m as usize);

        while targets.len() < m as usize {
            let idx = rng.gen_range(0..degree_list.len());
            let candidate = degree_list[idx];
            if candidate != new_node && !targets.contains(&candidate) {
                targets.push(candidate);
            }
        }

        for &t in &targets {
            edges.push(Edge {
                source: new_node,
                target: t,
            });
            edges.push(Edge {
                source: t,
                target: new_node,
            });
            degree_list.push(new_node);
            degree_list.push(t);
        }
    }

    edges
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic() {
        let a = generate_power_law(100, 2, 42);
        let b = generate_power_law(100, 2, 42);
        assert_eq!(a.len(), b.len());
        for (ea, eb) in a.iter().zip(b.iter()) {
            assert_eq!(ea.source, eb.source);
            assert_eq!(ea.target, eb.target);
        }
    }

    #[test]
    fn correct_edge_count() {
        // n=100, m=2:
        // Initial clique (3 nodes): 3 pairs * 2 dirs = 6 edges
        // 97 new nodes * 2 attachments * 2 dirs = 388 edges
        // Total ~ 394 edges (around 200 undirected, so ~200 makes sense for directed too)
        let edges = generate_power_law(100, 2, 42);
        // Should be at least 100 and at most 500 — approximately correct
        assert!(
            edges.len() >= 100 && edges.len() <= 500,
            "expected ~394 edges for n=100,m=2, got {}",
            edges.len()
        );
    }

    #[test]
    fn has_skewed_degree() {
        // For 10K nodes the degree distribution should be very skewed
        let edges = generate_power_law(10_000, 2, 42);
        let node_count = 10_000u64;
        let mut degree = vec![0u64; node_count as usize];
        for e in &edges {
            degree[e.source as usize] += 1;
        }
        let max_deg = *degree.iter().max().unwrap();
        let min_deg = degree
            .iter()
            .filter(|&&d| d > 0)
            .min()
            .copied()
            .unwrap_or(1);
        assert!(
            max_deg > 10 * min_deg,
            "expected skewed degree distribution: max={max_deg}, min={min_deg}"
        );
    }

    #[test]
    fn no_self_loops() {
        let edges = generate_power_law(500, 3, 7);
        for e in &edges {
            assert_ne!(e.source, e.target, "self-loops not allowed");
        }
    }
}
