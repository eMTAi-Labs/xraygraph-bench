use rand::Rng;
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha20Rng;
use xraybench_types::{Edge, Node, PropertyValue};

/// Generate uniformly distributed nodes with deterministic properties.
///
/// - First property is always `Integer(id)`.
/// - Remaining properties cycle through Integer / Float / Text.
/// - `nullable_ratio` causes Float properties to become Null at that rate.
pub fn generate_uniform_nodes(
    node_count: u64,
    property_count: usize,
    nullable_ratio: f64,
    seed: u64,
) -> Vec<Node> {
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    let mut nodes = Vec::with_capacity(node_count as usize);

    for id in 0..node_count {
        let mut properties = Vec::with_capacity(property_count);
        for p in 0..property_count {
            if p == 0 {
                properties.push(PropertyValue::Integer(id as i64));
            } else {
                // cycle: 0 -> Integer, 1 -> Float, 2 -> Text, then repeat
                match (p - 1) % 3 {
                    0 => {
                        let val: i64 = rng.gen_range(0..1_000_000);
                        properties.push(PropertyValue::Integer(val));
                    }
                    1 => {
                        // Float — may be nullable
                        let roll: f64 = rng.gen();
                        if roll < nullable_ratio {
                            properties.push(PropertyValue::Null);
                        } else {
                            let val: f64 = rng.gen_range(0.0..1000.0);
                            properties.push(PropertyValue::Float(val));
                        }
                    }
                    _ => {
                        let len: usize = rng.gen_range(4..12);
                        let text: String = (0..len)
                            .map(|_| rng.gen_range(b'a'..=b'z') as char)
                            .collect();
                        properties.push(PropertyValue::Text(text));
                    }
                }
            }
        }
        nodes.push(Node { id, properties });
    }
    nodes
}

/// Generate uniformly distributed random edges with no self-loops.
pub fn generate_uniform_edges(node_count: u64, edge_count: u64, seed: u64) -> Vec<Edge> {
    assert!(node_count >= 2, "need at least 2 nodes to generate edges");
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    let mut edges = Vec::with_capacity(edge_count as usize);

    for _ in 0..edge_count {
        loop {
            let source = rng.gen_range(0..node_count);
            let target = rng.gen_range(0..node_count);
            if source != target {
                edges.push(Edge { source, target });
                break;
            }
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
        let a = generate_uniform_nodes(100, 4, 0.1, 42);
        let b = generate_uniform_nodes(100, 4, 0.1, 42);
        assert_eq!(a.len(), b.len());
        for (na, nb) in a.iter().zip(b.iter()) {
            assert_eq!(na.id, nb.id);
            assert_eq!(na.properties, nb.properties);
        }
    }

    #[test]
    fn different_seeds_different() {
        let a = generate_uniform_nodes(100, 4, 0.0, 42);
        let b = generate_uniform_nodes(100, 4, 0.0, 99);
        // At least some property should differ (beyond id)
        let differ = a
            .iter()
            .zip(b.iter())
            .any(|(na, nb)| na.properties[1..] != nb.properties[1..]);
        assert!(
            differ,
            "different seeds should produce different properties"
        );
    }

    #[test]
    fn correct_count() {
        let nodes = generate_uniform_nodes(250, 3, 0.0, 1);
        assert_eq!(nodes.len(), 250);
        for (i, n) in nodes.iter().enumerate() {
            assert_eq!(n.id, i as u64);
            assert_eq!(n.properties.len(), 3);
            assert_eq!(n.properties[0], PropertyValue::Integer(i as i64));
        }
    }

    #[test]
    fn nullable_produces_nulls() {
        let nodes = generate_uniform_nodes(1000, 3, 1.0, 7);
        // With nullable_ratio=1.0, every Float (property index 2) should be Null
        let nulls = nodes
            .iter()
            .filter(|n| n.properties[2] == PropertyValue::Null)
            .count();
        assert_eq!(
            nulls, 1000,
            "all Float properties should be Null at ratio=1.0"
        );
    }

    #[test]
    fn edges_no_self_loops() {
        let edges = generate_uniform_edges(500, 1000, 42);
        for e in &edges {
            assert_ne!(e.source, e.target, "self-loops not allowed");
        }
    }

    #[test]
    fn edge_count_correct() {
        let edges = generate_uniform_edges(100, 200, 0);
        assert_eq!(edges.len(), 200);
    }
}
