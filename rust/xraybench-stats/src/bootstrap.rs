use rand::Rng;
use rand_chacha::ChaCha20Rng;
use rand::SeedableRng;
use xraybench_types::{BenchError, Result};

// ── Config & Result types ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    pub n_resamples: usize,
    pub confidence: f64,
    pub seed: u64,
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            n_resamples: 10_000,
            confidence: 0.95,
            seed: 42,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BootstrapCI {
    pub lower: f64,
    pub upper: f64,
    pub confidence: f64,
    pub point_estimate: f64,
    pub bias: f64,
    pub acceleration: f64,
}

// ── BCa bootstrap CI for the mean ────────────────────────────────────────────

/// Bias-Corrected and Accelerated (BCa) bootstrap confidence interval
/// for the mean of `data`.
pub fn bca_mean_ci(data: &[f64], config: BootstrapConfig) -> Result<BootstrapCI> {
    if data.len() < 3 {
        return Err(BenchError::InsufficientSamples {
            needed: 3,
            got: data.len(),
        });
    }

    let n = data.len();
    let point_estimate = data.iter().sum::<f64>() / n as f64;

    // ── Step 1: Generate bootstrap distribution ───────────────────────────────
    let mut rng = ChaCha20Rng::seed_from_u64(config.seed);
    let mut boot_means: Vec<f64> = Vec::with_capacity(config.n_resamples);

    for _ in 0..config.n_resamples {
        let mut sum = 0.0f64;
        for _ in 0..n {
            let idx = rng.gen_range(0..n);
            sum += data[idx];
        }
        boot_means.push(sum / n as f64);
    }

    // ── Step 2: Bias correction z0 ────────────────────────────────────────────
    let below = boot_means.iter().filter(|&&b| b < point_estimate).count();
    let p_below = below as f64 / config.n_resamples as f64;
    // Clamp to avoid ±inf
    let p_below = p_below.clamp(1e-6, 1.0 - 1e-6);
    let z0 = normal_ppf(p_below);

    // ── Step 3: Acceleration via jackknife ────────────────────────────────────
    let jk_means: Vec<f64> = (0..n)
        .map(|i| {
            let s: f64 = data.iter().enumerate().filter(|&(j, _)| j != i).map(|(_,&v)| v).sum();
            s / (n - 1) as f64
        })
        .collect();
    let jk_grand = jk_means.iter().sum::<f64>() / n as f64;
    let num: f64 = jk_means.iter().map(|&m| (jk_grand - m).powi(3)).sum();
    let denom: f64 = jk_means.iter().map(|&m| (jk_grand - m).powi(2)).sum();
    let acceleration = if denom.abs() < 1e-15 {
        0.0
    } else {
        num / (6.0 * denom.powf(1.5))
    };

    // ── Step 4: BCa adjusted percentiles ─────────────────────────────────────
    let alpha = 1.0 - config.confidence;
    let z_lo = normal_ppf(alpha / 2.0);
    let z_hi = normal_ppf(1.0 - alpha / 2.0);

    let p_lo = bca_percentile(z0, z_lo, acceleration);
    let p_hi = bca_percentile(z0, z_hi, acceleration);

    // ── Step 5: Interpolate into sorted bootstrap distribution ────────────────
    boot_means.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
    let lower = quantile_from_sorted(&boot_means, p_lo);
    let upper = quantile_from_sorted(&boot_means, p_hi);

    let bias = boot_means.iter().sum::<f64>() / config.n_resamples as f64 - point_estimate;

    Ok(BootstrapCI {
        lower,
        upper,
        confidence: config.confidence,
        point_estimate,
        bias,
        acceleration,
    })
}

// ── Math helpers ──────────────────────────────────────────────────────────────

/// BCa adjusted percentile for a given z-score.
fn bca_percentile(z0: f64, z: f64, accel: f64) -> f64 {
    let num = z0 + z;
    let denom = 1.0 - accel * (z0 + z);
    let arg = (z0 + num / denom).clamp(-8.0, 8.0);
    normal_cdf(arg)
}

/// Linear-interpolation quantile from a pre-sorted slice.
fn quantile_from_sorted(sorted: &[f64], p: f64) -> f64 {
    let n = sorted.len();
    if n == 0 {
        return 0.0;
    }
    let p = p.clamp(0.0, 1.0);
    let idx = p * (n - 1) as f64;
    let lo = idx.floor() as usize;
    let hi = (lo + 1).min(n - 1);
    let frac = idx - lo as f64;
    sorted[lo] * (1.0 - frac) + sorted[hi] * frac
}

/// Standard-normal CDF using the error function.
/// Φ(x) = 0.5 * [1 + erf(x / sqrt(2))]
pub fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

/// Inverse standard-normal CDF (probit) using the rational approximation
/// by Beasley-Springer-Moro.
pub fn normal_ppf(p: f64) -> f64 {
    // Coefficients for the central region
    const A: [f64; 4] = [
        2.515_517,
        0.802_853,
        0.010_328,
        0.0,
    ];
    const B: [f64; 3] = [
        1.432_788,
        0.189_269,
        0.001_308,
    ];

    if p <= 0.0 {
        return f64::NEG_INFINITY;
    }
    if p >= 1.0 {
        return f64::INFINITY;
    }

    let sign = if p < 0.5 { -1.0 } else { 1.0 };
    let p2 = if p < 0.5 { p } else { 1.0 - p };

    let t = (-2.0 * p2.ln()).sqrt();
    let num = A[0] + A[1] * t + A[2] * t * t + A[3] * t * t * t;
    let den = 1.0 + B[0] * t + B[1] * t * t + B[2] * t * t * t;
    sign * (t - num / den)
}

/// Error function using Abramowitz & Stegun 7.1.26 (max |ε| ≤ 1.5×10⁻⁷).
pub fn erf(x: f64) -> f64 {
    const P: f64 = 0.327_591_1;
    const A1: f64 = 0.254_829_592;
    const A2: f64 = -0.284_496_736;
    const A3: f64 = 1.421_413_741;
    const A4: f64 = -1.453_152_027;
    const A5: f64 = 1.061_405_429;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + P * x);
    let poly = t * (A1 + t * (A2 + t * (A3 + t * (A4 + t * A5))));
    sign * (1.0 - poly * (-x * x).exp())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symmetric_data() {
        let data: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let ci = bca_mean_ci(&data, BootstrapConfig::default()).unwrap();
        // mean = 50.5, CI should straddle it
        assert!(ci.lower < ci.point_estimate);
        assert!(ci.upper > ci.point_estimate);
        assert!((ci.point_estimate - 50.5).abs() < 1e-9);
    }

    #[test]
    fn tight_data() {
        // very low-variance data → CI width < 0.2
        let data: Vec<f64> = vec![10.0; 100];
        let ci = bca_mean_ci(&data, BootstrapConfig::default()).unwrap();
        assert!((ci.upper - ci.lower) < 0.2, "CI width = {}", ci.upper - ci.lower);
    }

    #[test]
    fn skewed_data() {
        // log-normal-ish skewed data
        let data: Vec<f64> = (1..=200).map(|i| (i as f64).exp().ln()).collect();
        let ci = bca_mean_ci(&data, BootstrapConfig::default()).unwrap();
        assert!(ci.lower < ci.upper);
    }

    #[test]
    fn reproducible() {
        let data: Vec<f64> = (1..=50).map(|i| i as f64).collect();
        let cfg1 = BootstrapConfig { seed: 123, ..Default::default() };
        let cfg2 = BootstrapConfig { seed: 123, ..Default::default() };
        let ci1 = bca_mean_ci(&data, cfg1).unwrap();
        let ci2 = bca_mean_ci(&data, cfg2).unwrap();
        assert_eq!(ci1.lower, ci2.lower);
        assert_eq!(ci1.upper, ci2.upper);
    }

    #[test]
    fn too_few_samples() {
        let data = vec![1.0, 2.0];
        assert!(bca_mean_ci(&data, BootstrapConfig::default()).is_err());
    }

    #[test]
    fn normal_ppf_standard_values() {
        // Φ⁻¹(0.5) = 0
        assert!((normal_ppf(0.5)).abs() < 0.01, "ppf(0.5)={}", normal_ppf(0.5));
        // Φ⁻¹(0.975) ≈ 1.96
        assert!((normal_ppf(0.975) - 1.96).abs() < 0.01,
            "ppf(0.975)={}", normal_ppf(0.975));
    }

    #[test]
    fn normal_cdf_standard_values() {
        // Φ(0) = 0.5
        assert!((normal_cdf(0.0) - 0.5).abs() < 1e-6);
        // Φ(1.96) ≈ 0.975
        assert!((normal_cdf(1.96) - 0.975).abs() < 0.005);
        // Φ(-1.96) ≈ 0.025
        assert!((normal_cdf(-1.96) - 0.025).abs() < 0.005);
    }
}
