use xraybench_types::{BenchError, PropertyValue, Result};

// ── Canonical serialization ───────────────────────────────────────────────────

/// Type-tagged deterministic serialization of a single PropertyValue.
///
/// Format:
///   Null:    [0x00]
///   Boolean: [0x01, 0x00|0x01]
///   Integer: [0x02, i64 big-endian (8 bytes)]
///   Float:   [0x03, f64 big-endian (8 bytes), with -0 → +0 and NaN → canonical NaN]
///   Text:    [0x04, u32 len big-endian (4 bytes), UTF-8 bytes]
pub fn canonical_value(value: &PropertyValue) -> Vec<u8> {
    match value {
        PropertyValue::Null => vec![0x00],
        PropertyValue::Boolean(b) => vec![0x01, if *b { 0x01 } else { 0x00 }],
        PropertyValue::Integer(i) => {
            let mut buf = vec![0x02];
            buf.extend_from_slice(&i.to_be_bytes());
            buf
        }
        PropertyValue::Float(f) => {
            let mut buf = vec![0x03];
            // Normalize: -0.0 → +0.0, NaN → canonical NaN bit pattern
            let normalized: f64 = if f.is_nan() {
                f64::from_bits(0x7FF8_0000_0000_0000) // canonical quiet NaN
            } else if *f == 0.0 {
                0.0_f64 // eliminates -0.0
            } else {
                *f
            };
            buf.extend_from_slice(&normalized.to_be_bytes());
            buf
        }
        PropertyValue::Text(s) => {
            let bytes = s.as_bytes();
            let len = bytes.len() as u32;
            let mut buf = vec![0x04];
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(bytes);
            buf
        }
    }
}

/// Serialize a row as: u32 column-count (big-endian) + concatenated canonical values.
pub fn canonical_row(values: &[PropertyValue]) -> Vec<u8> {
    let count = values.len() as u32;
    let mut buf = count.to_be_bytes().to_vec();
    for v in values {
        buf.extend(canonical_value(v));
    }
    buf
}

/// Canonicalize each row and return the sorted list of byte vectors.
pub fn canonical_rows_sorted(rows: &[Vec<PropertyValue>]) -> Vec<Vec<u8>> {
    let mut encoded: Vec<Vec<u8>> = rows.iter().map(|r| canonical_row(r)).collect();
    encoded.sort();
    encoded
}

// ── Float ULP comparison ──────────────────────────────────────────────────────

/// Compare two f64 values within `max_ulp` Units in the Last Place.
///
/// Special cases:
///   - NaN == NaN → true
///   - +0.0 == -0.0 → true
///   - Opposite signs (other than ±0) → false
pub fn float_eq_ulp(a: f64, b: f64, max_ulp: u32) -> bool {
    // NaN == NaN
    if a.is_nan() && b.is_nan() {
        return true;
    }
    // One NaN, one not
    if a.is_nan() || b.is_nan() {
        return false;
    }
    // Handle ±0
    if a == 0.0 && b == 0.0 {
        return true;
    }
    // Different signs (not zero) → can't be within ULP
    if a.is_sign_negative() != b.is_sign_negative() {
        return false;
    }
    let a_bits = a.to_bits() as i64;
    let b_bits = b.to_bits() as i64;
    let ulp_diff = (a_bits - b_bits).unsigned_abs();
    ulp_diff <= max_ulp as u64
}

// ── Row equivalence ───────────────────────────────────────────────────────────

/// Compare two rows element-by-element, using ULP tolerance for floats.
pub fn rows_equivalent(
    row_a: &[PropertyValue],
    row_b: &[PropertyValue],
    float_ulp: u32,
) -> Result<bool> {
    if row_a.len() != row_b.len() {
        return Ok(false);
    }
    for (a, b) in row_a.iter().zip(row_b.iter()) {
        let eq = match (a, b) {
            (PropertyValue::Null, PropertyValue::Null) => true,
            (PropertyValue::Boolean(x), PropertyValue::Boolean(y)) => x == y,
            (PropertyValue::Integer(x), PropertyValue::Integer(y)) => x == y,
            (PropertyValue::Float(x), PropertyValue::Float(y)) => float_eq_ulp(*x, *y, float_ulp),
            (PropertyValue::Text(x), PropertyValue::Text(y)) => x == y,
            _ => {
                return Err(BenchError::InvalidData(format!(
                    "type mismatch: {:?} vs {:?}",
                    a, b
                )))
            }
        };
        if !eq {
            return Ok(false);
        }
    }
    Ok(true)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_null() {
        let v = canonical_value(&PropertyValue::Null);
        assert_eq!(v, vec![0x00]);
    }

    #[test]
    fn canonical_bool() {
        let t = canonical_value(&PropertyValue::Boolean(true));
        let f = canonical_value(&PropertyValue::Boolean(false));
        assert_eq!(t, vec![0x01, 0x01]);
        assert_eq!(f, vec![0x01, 0x00]);
    }

    #[test]
    fn canonical_integer_deterministic() {
        let v = canonical_value(&PropertyValue::Integer(256));
        // tag + 8 bytes big-endian
        assert_eq!(v.len(), 9);
        assert_eq!(v[0], 0x02);
        // 256 in big-endian i64 = 0x00_00_00_00_00_00_01_00
        assert_eq!(&v[1..], &256_i64.to_be_bytes());

        // Calling twice gives same result
        let v2 = canonical_value(&PropertyValue::Integer(256));
        assert_eq!(v, v2);
    }

    #[test]
    fn canonical_float_neg_zero() {
        let pos = canonical_value(&PropertyValue::Float(0.0_f64));
        let neg = canonical_value(&PropertyValue::Float(-0.0_f64));
        assert_eq!(pos, neg, "-0 and +0 must canonicalize identically");
    }

    #[test]
    fn canonical_text_length_prefixed() {
        let v = canonical_value(&PropertyValue::Text("hi".to_string()));
        // [0x04, 0x00 0x00 0x00 0x02, 'h', 'i']
        assert_eq!(v[0], 0x04);
        assert_eq!(&v[1..5], &2_u32.to_be_bytes());
        assert_eq!(&v[5..], b"hi");
    }

    #[test]
    fn row_sorting_deterministic() {
        let rows: Vec<Vec<PropertyValue>> = vec![
            vec![PropertyValue::Integer(3)],
            vec![PropertyValue::Integer(1)],
            vec![PropertyValue::Integer(2)],
        ];
        let sorted1 = canonical_rows_sorted(&rows);
        let sorted2 = canonical_rows_sorted(&rows);
        assert_eq!(sorted1, sorted2);
        // Should be in ascending order
        assert!(sorted1[0] <= sorted1[1]);
        assert!(sorted1[1] <= sorted1[2]);
    }

    #[test]
    fn float_ulp_exact() {
        assert!(float_eq_ulp(1.0_f64, 1.0_f64, 0));
        assert!(float_eq_ulp(1.0_f64, 1.0_f64, 4));
    }

    #[test]
    fn float_ulp_close() {
        // Two floats exactly 2 ULP apart
        let a = 1.0_f64;
        let b = f64::from_bits(a.to_bits() + 2);
        assert!(float_eq_ulp(a, b, 2));
        assert!(!float_eq_ulp(a, b, 1));
    }

    #[test]
    fn float_ulp_nan() {
        assert!(float_eq_ulp(f64::NAN, f64::NAN, 0));
        assert!(!float_eq_ulp(f64::NAN, 1.0, 0));
        assert!(!float_eq_ulp(1.0, f64::NAN, 0));
    }

    #[test]
    fn rows_equivalent_basic() {
        let a = vec![PropertyValue::Integer(1), PropertyValue::Text("x".into())];
        let b = vec![PropertyValue::Integer(1), PropertyValue::Text("x".into())];
        assert!(rows_equivalent(&a, &b, 4).unwrap());
    }

    #[test]
    fn rows_different_types_not_equivalent() {
        let a = vec![PropertyValue::Integer(1)];
        let b = vec![PropertyValue::Float(1.0)];
        // Type mismatch → Err
        assert!(rows_equivalent(&a, &b, 4).is_err());
    }
}
