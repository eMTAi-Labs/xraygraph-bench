use xraybench_types::{BenchError, PropertyValue, Result};

use crate::canonical::{canonical_row, canonical_rows_sorted};

// ── ResultHasher ──────────────────────────────────────────────────────────────

/// Streaming BLAKE3 hasher for result-set rows.
pub struct ResultHasher {
    inner: blake3::Hasher,
    row_count: u64,
}

impl ResultHasher {
    pub fn new() -> Self {
        Self {
            inner: blake3::Hasher::new(),
            row_count: 0,
        }
    }

    /// Feed one row into the hash in canonical form.
    pub fn update_row(&mut self, row: &[PropertyValue]) {
        let encoded = canonical_row(row);
        self.inner.update(&encoded);
        self.row_count += 1;
    }

    /// Finalize and return the hash as `"blake3:<hex>"`.
    pub fn finalize_hex(&self) -> String {
        format!("blake3:{}", self.inner.finalize().to_hex())
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }
}

impl Default for ResultHasher {
    fn default() -> Self {
        Self::new()
    }
}

// ── Order-independent hashing ─────────────────────────────────────────────────

/// Hash a result set in order-independent fashion:
/// canonicalize every row, sort, then hash the sequence.
pub fn hash_result_set(rows: &[Vec<PropertyValue>]) -> String {
    let sorted = canonical_rows_sorted(rows);
    let mut hasher = blake3::Hasher::new();
    for encoded in &sorted {
        hasher.update(encoded);
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

/// Same as `hash_result_set` but also returns the row count.
pub fn hash_result_set_with_count(rows: &[Vec<PropertyValue>]) -> (String, u64) {
    let hash = hash_result_set(rows);
    (hash, rows.len() as u64)
}

/// Return `Ok(true)` when `hash_result_set(rows) == reference`.
pub fn verify_hash(rows: &[Vec<PropertyValue>], reference: &str) -> Result<bool> {
    if !reference.starts_with("blake3:") {
        return Err(BenchError::InvalidData(format!(
            "unsupported hash prefix in reference: {reference}"
        )));
    }
    Ok(hash_result_set(rows) == reference)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_consistent_hash() {
        let h1 = hash_result_set(&[]);
        let h2 = hash_result_set(&[]);
        assert_eq!(h1, h2);
        assert!(h1.starts_with("blake3:"));
    }

    #[test]
    fn single_row_hash() {
        let rows = vec![vec![PropertyValue::Integer(42)]];
        let h = hash_result_set(&rows);
        assert!(h.starts_with("blake3:"));
        // BLAKE3 hex is 64 chars; with prefix "blake3:" → 71 chars
        assert_eq!(h.len(), "blake3:".len() + 64);
    }

    #[test]
    fn order_independent() {
        let rows_a = vec![
            vec![PropertyValue::Integer(1)],
            vec![PropertyValue::Integer(2)],
            vec![PropertyValue::Integer(3)],
        ];
        let rows_b = vec![
            vec![PropertyValue::Integer(3)],
            vec![PropertyValue::Integer(1)],
            vec![PropertyValue::Integer(2)],
        ];
        assert_eq!(hash_result_set(&rows_a), hash_result_set(&rows_b));
    }

    #[test]
    fn different_data_different_hash() {
        let a = vec![vec![PropertyValue::Integer(1)]];
        let b = vec![vec![PropertyValue::Integer(2)]];
        assert_ne!(hash_result_set(&a), hash_result_set(&b));
    }

    #[test]
    fn hash_with_count() {
        let rows = vec![
            vec![PropertyValue::Text("a".into())],
            vec![PropertyValue::Text("b".into())],
        ];
        let (hash, count) = hash_result_set_with_count(&rows);
        assert_eq!(count, 2);
        assert!(hash.starts_with("blake3:"));
    }

    #[test]
    fn verify_correct() {
        let rows = vec![vec![PropertyValue::Boolean(true)]];
        let reference = hash_result_set(&rows);
        assert!(verify_hash(&rows, &reference).unwrap());
    }

    #[test]
    fn verify_incorrect() {
        let rows = vec![vec![PropertyValue::Boolean(true)]];
        let wrong = hash_result_set(&[vec![PropertyValue::Boolean(false)]]);
        assert!(!verify_hash(&rows, &wrong).unwrap());
    }

    #[test]
    fn streaming_hasher_basic() {
        let rows = vec![
            vec![PropertyValue::Integer(10)],
            vec![PropertyValue::Integer(20)],
        ];
        // The streaming hasher hashes in insertion order, so it will differ from
        // hash_result_set (which sorts). We just verify it produces a valid prefix
        // and that identical inputs produce identical outputs.
        let mut h1 = ResultHasher::new();
        for r in &rows {
            h1.update_row(r);
        }
        assert_eq!(h1.row_count(), 2);
        let hex1 = h1.finalize_hex();
        assert!(hex1.starts_with("blake3:"));

        let mut h2 = ResultHasher::new();
        for r in &rows {
            h2.update_row(r);
        }
        assert_eq!(h1.finalize_hex(), h2.finalize_hex());
    }
}
