use xraybench_types::Edge;

/// Generate a simple directed chain: 0→1→2→...→(length-1).
///
/// `seed` is preserved for API consistency but not used (the chain is fully
/// deterministic by construction).
pub fn generate_chain(length: u64, seed: u64) -> Vec<Edge> {
    let _ = seed;
    if length <= 1 {
        return Vec::new();
    }
    let mut edges = Vec::with_capacity((length - 1) as usize);
    for i in 0..(length - 1) {
        edges.push(Edge {
            source: i,
            target: i + 1,
        });
    }
    edges
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correct_length() {
        let edges = generate_chain(10, 0);
        assert_eq!(edges.len(), 9);
    }

    #[test]
    fn chain_structure() {
        let edges = generate_chain(5, 0);
        assert_eq!(
            edges[0],
            Edge {
                source: 0,
                target: 1
            }
        );
        assert_eq!(
            edges[1],
            Edge {
                source: 1,
                target: 2
            }
        );
        assert_eq!(
            edges[2],
            Edge {
                source: 2,
                target: 3
            }
        );
        assert_eq!(
            edges[3],
            Edge {
                source: 3,
                target: 4
            }
        );
    }

    #[test]
    fn single_node() {
        let edges = generate_chain(1, 42);
        assert!(edges.is_empty(), "single node chain should have no edges");
    }

    #[test]
    fn deterministic() {
        let a = generate_chain(100, 1);
        let b = generate_chain(100, 1);
        assert_eq!(a.len(), b.len());
        for (ea, eb) in a.iter().zip(b.iter()) {
            assert_eq!(ea.source, eb.source);
            assert_eq!(ea.target, eb.target);
        }
    }
}
