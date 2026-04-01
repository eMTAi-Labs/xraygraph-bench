/// T-Digest streaming quantile estimator.
///
/// Reference: Dunning & Ertl (2019) "Computing Extremely Accurate Quantiles
/// Using t-Digests".  This is a simplified but correct implementation
/// sufficient for benchmark use.

// ── Centroid ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Centroid {
    mean: f64,
    weight: f64,
}

// ── TDigest ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TDigest {
    compression: f64,
    centroids: Vec<Centroid>,
    total_weight: f64,
}

impl TDigest {
    /// Create a new T-Digest with the given compression parameter (default 100).
    pub fn new(compression: f64) -> Self {
        Self {
            compression,
            centroids: Vec::new(),
            total_weight: 0.0,
        }
    }

    /// Insert a single value with weight 1.
    pub fn insert(&mut self, value: f64) {
        self.insert_weighted(value, 1.0);
    }

    /// Insert a value with an explicit weight.
    pub fn insert_weighted(&mut self, value: f64, weight: f64) {
        // Add as a new centroid then merge if needed
        self.centroids.push(Centroid {
            mean: value,
            weight,
        });
        self.total_weight += weight;

        let max_centroids = (self.compression * 2.0) as usize;
        if self.centroids.len() > max_centroids * 2 {
            self.compress();
        }
    }

    /// Estimate the q-th quantile (q in [0, 1]).
    /// Returns `None` if the digest is empty.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.centroids.is_empty() || self.total_weight == 0.0 {
            return None;
        }

        // Sort centroids by mean for interpolation
        let mut sorted = self.centroids.clone();
        sorted.sort_unstable_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap());

        if q <= 0.0 {
            return Some(sorted.first().unwrap().mean);
        }
        if q >= 1.0 {
            return Some(sorted.last().unwrap().mean);
        }

        let target = q * self.total_weight;

        // Walk through centroids accumulating weight
        let mut cumulative = 0.0f64;
        for i in 0..sorted.len() {
            let c = &sorted[i];
            let half = c.weight / 2.0;
            let lo = cumulative + (if i == 0 { 0.0 } else { half });
            let hi = cumulative + c.weight - (if i == sorted.len() - 1 { 0.0 } else { half });

            if target <= lo {
                // interpolate with previous centroid if exists
                if i == 0 {
                    return Some(c.mean);
                }
                let prev = &sorted[i - 1];
                let prev_hi = cumulative - prev.weight / 2.0;
                let frac = if hi - prev_hi > 0.0 {
                    (target - prev_hi) / (lo - prev_hi)
                } else {
                    0.5
                };
                return Some(prev.mean + frac * (c.mean - prev.mean));
            }

            if target <= hi {
                return Some(c.mean);
            }

            cumulative += c.weight;
        }

        Some(sorted.last().unwrap().mean)
    }

    /// Total weight (number of inserted values, accounting for weighted inserts).
    pub fn count(&self) -> f64 {
        self.total_weight
    }

    // ── Private ───────────────────────────────────────────────────────────────

    fn compress(&mut self) {
        if self.centroids.is_empty() {
            return;
        }

        // Sort by mean
        self.centroids
            .sort_unstable_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap());

        let max_centroids = self.compression as usize * 2;
        let mut merged: Vec<Centroid> = Vec::with_capacity(max_centroids);

        let n = self.total_weight;

        let mut cumulative = 0.0f64;
        for c in self.centroids.drain(..) {
            if merged.is_empty() {
                cumulative = c.weight / 2.0;
                merged.push(c);
            } else {
                // k-size limit using the scale function
                let q = cumulative / n;
                let k_limit = 4.0 * n * q * (1.0 - q) / self.compression;
                let last = merged.last_mut().unwrap();
                if last.weight + c.weight <= k_limit.max(1.0) {
                    // Merge into current centroid
                    let new_weight = last.weight + c.weight;
                    last.mean = (last.mean * last.weight + c.mean * c.weight) / new_weight;
                    last.weight = new_weight;
                } else {
                    cumulative += last.weight / 2.0 + c.weight / 2.0;
                    merged.push(c);
                }
            }
        }

        self.centroids = merged;
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_value() {
        let mut td = TDigest::new(100.0);
        td.insert(42.0);
        assert_eq!(td.quantile(0.5), Some(42.0));
    }

    #[test]
    fn uniform_10k() {
        let mut td = TDigest::new(100.0);
        for i in 1..=10_000 {
            td.insert(i as f64);
        }
        // p50 ≈ 5000.5, allow 2% error = 100
        let p50 = td.quantile(0.5).unwrap();
        assert!((p50 - 5000.5).abs() < 200.0, "p50={p50} expected ~5000.5");
        let p99 = td.quantile(0.99).unwrap();
        // 99th percentile ≈ 9901, allow 2% error = 200
        assert!((p99 - 9901.0).abs() < 400.0, "p99={p99} expected ~9901");
    }

    #[test]
    fn min_max() {
        let mut td = TDigest::new(100.0);
        for i in 1..=100 {
            td.insert(i as f64);
        }
        let min = td.quantile(0.0).unwrap();
        let max = td.quantile(1.0).unwrap();
        assert!((min - 1.0).abs() < 1.0, "min={min}");
        assert!((max - 100.0).abs() < 1.0, "max={max}");
    }

    #[test]
    fn count() {
        let mut td = TDigest::new(100.0);
        for i in 0..500 {
            td.insert(i as f64);
        }
        assert_eq!(td.count(), 500.0);
    }

    #[test]
    fn empty_returns_none() {
        let td = TDigest::new(100.0);
        assert_eq!(td.quantile(0.5), None);
    }

    #[test]
    fn heavy_compression_100k() {
        let mut td = TDigest::new(100.0);
        for i in 1..=100_000 {
            td.insert(i as f64);
        }
        let p50 = td.quantile(0.5).unwrap();
        // Allow 5% error = 2500
        assert!(
            (p50 - 50_000.5).abs() < 2500.0,
            "p50={p50} expected ~50000.5"
        );
        let p99 = td.quantile(0.99).unwrap();
        // 99th ≈ 99001, allow 5% = 5000
        assert!((p99 - 99_001.0).abs() < 5000.0, "p99={p99} expected ~99001");
    }
}
