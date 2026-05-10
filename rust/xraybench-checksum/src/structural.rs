use std::collections::HashSet;

use xraybench_types::{BenchError, Edge, Result};

// ── Path type ─────────────────────────────────────────────────────────────────

/// A path is an ordered list of node IDs.
pub type Path = Vec<u64>;

// ── Path canonicalization ─────────────────────────────────────────────────────

/// Canonicalize a path so that the direction with the smaller first element
/// comes first.  Single-node paths are returned as-is.  If the path is
/// reversed (last < first), reverse it.
pub fn canonicalize_path(path: &Path) -> Path {
    if path.len() <= 1 {
        return path.clone();
    }
    let first = *path.first().unwrap();
    let last = *path.last().unwrap();
    if last < first {
        path.iter().copied().rev().collect()
    } else {
        path.clone()
    }
}

// ── Length validation ─────────────────────────────────────────────────────────

/// Validate that every path has between `min_len` and `max_len` nodes
/// (where the number of hops = len – 1).
pub fn validate_path_lengths(paths: &[Path], min_len: usize, max_len: usize) -> Result<bool> {
    for (i, path) in paths.iter().enumerate() {
        if path.len() < min_len {
            return Err(BenchError::InvalidData(format!(
                "path {i} has length {} < min_len {min_len}",
                path.len()
            )));
        }
        if path.len() > max_len {
            return Err(BenchError::InvalidData(format!(
                "path {i} has length {} > max_len {max_len}",
                path.len()
            )));
        }
    }
    Ok(true)
}

// ── Seed validation ───────────────────────────────────────────────────────────

/// Validate that every path starts from `seed_id`.
pub fn validate_paths_from_seed(paths: &[Path], seed_id: u64) -> Result<bool> {
    for (i, path) in paths.iter().enumerate() {
        match path.first() {
            None => {
                return Err(BenchError::InvalidData(format!("path {i} is empty")));
            }
            Some(&first) if first != seed_id => {
                return Err(BenchError::InvalidData(format!(
                    "path {i} starts at {first}, expected seed {seed_id}"
                )));
            }
            _ => {}
        }
    }
    Ok(true)
}

// ── Duplicate detection ───────────────────────────────────────────────────────

/// Validate that no two paths are duplicates after canonicalization.
pub fn validate_no_duplicate_paths(paths: &[Path]) -> Result<bool> {
    let mut seen: HashSet<Path> = HashSet::new();
    for (i, path) in paths.iter().enumerate() {
        let canonical = canonicalize_path(path);
        if !seen.insert(canonical.clone()) {
            return Err(BenchError::InvalidData(format!(
                "duplicate path detected at index {i}: {canonical:?}"
            )));
        }
    }
    Ok(true)
}

// ── Graph topology validation ─────────────────────────────────────────────────

/// Validate that every consecutive node pair in every path corresponds to an
/// edge in `edges`.  The edge set is treated as directed (source → target).
pub fn validate_paths_on_graph(paths: &[Path], edges: &[Edge]) -> Result<bool> {
    // Build a hash set of (source, target) pairs for O(1) lookup.
    let edge_set: HashSet<(u64, u64)> = edges.iter().map(|e| (e.source, e.target)).collect();

    for (pi, path) in paths.iter().enumerate() {
        for wi in 0..path.len().saturating_sub(1) {
            let u = path[wi];
            let v = path[wi + 1];
            if !edge_set.contains(&(u, v)) {
                return Err(BenchError::InvalidData(format!(
                    "path {pi}: edge ({u}, {v}) at position {wi} not in graph"
                )));
            }
        }
    }
    Ok(true)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // helpers
    fn e(s: u64, t: u64) -> Edge {
        Edge {
            source: s,
            target: t,
        }
    }

    // ── canonicalize_path ─────────────────────────────────────────────────────

    #[test]
    fn canonicalize_already_ordered() {
        let p = vec![1, 2, 3];
        assert_eq!(canonicalize_path(&p), vec![1, 2, 3]);
    }

    #[test]
    fn canonicalize_reversed() {
        let p = vec![3, 2, 1];
        assert_eq!(canonicalize_path(&p), vec![1, 2, 3]);
    }

    #[test]
    fn canonicalize_single_node() {
        let p = vec![42];
        assert_eq!(canonicalize_path(&p), vec![42]);
    }

    // ── validate_path_lengths ─────────────────────────────────────────────────

    #[test]
    fn path_lengths_valid() {
        let paths = vec![vec![1, 2, 3], vec![4, 5, 6]];
        assert!(validate_path_lengths(&paths, 2, 4).unwrap());
    }

    #[test]
    fn path_lengths_too_short() {
        let paths = vec![vec![1]]; // len=1, min=2
        assert!(validate_path_lengths(&paths, 2, 5).is_err());
    }

    #[test]
    fn path_lengths_too_long() {
        let paths = vec![vec![1, 2, 3, 4, 5]]; // len=5, max=3
        assert!(validate_path_lengths(&paths, 1, 3).is_err());
    }

    // ── validate_paths_from_seed ──────────────────────────────────────────────

    #[test]
    fn paths_from_seed_valid() {
        let paths = vec![vec![1, 2, 3], vec![1, 4]];
        assert!(validate_paths_from_seed(&paths, 1).unwrap());
    }

    #[test]
    fn paths_from_wrong_seed() {
        let paths = vec![vec![1, 2], vec![3, 4]]; // second starts at 3, not 1
        assert!(validate_paths_from_seed(&paths, 1).is_err());
    }

    // ── validate_no_duplicate_paths ───────────────────────────────────────────

    #[test]
    fn no_duplicates_valid() {
        let paths = vec![vec![1, 2, 3], vec![4, 5, 6]];
        assert!(validate_no_duplicate_paths(&paths).unwrap());
    }

    #[test]
    fn duplicate_detected() {
        let paths = vec![vec![1, 2, 3], vec![1, 2, 3]];
        assert!(validate_no_duplicate_paths(&paths).is_err());
    }

    #[test]
    fn reversed_duplicate_detected() {
        // [1,2,3] and [3,2,1] canonicalize to [1,2,3] → duplicate
        let paths = vec![vec![1, 2, 3], vec![3, 2, 1]];
        assert!(validate_no_duplicate_paths(&paths).is_err());
    }

    // ── validate_paths_on_graph ───────────────────────────────────────────────

    #[test]
    fn paths_on_graph_valid() {
        let edges = vec![e(1, 2), e(2, 3)];
        let paths = vec![vec![1, 2, 3]];
        assert!(validate_paths_on_graph(&paths, &edges).unwrap());
    }

    #[test]
    fn paths_on_graph_invalid_edge() {
        let edges = vec![e(1, 2)];
        let paths = vec![vec![1, 2, 3]]; // edge (2,3) missing
        assert!(validate_paths_on_graph(&paths, &edges).is_err());
    }
}
