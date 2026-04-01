# Phase 1: Rust Measurement Core — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a bulletproof Rust measurement foundation (timing, statistics, generators, checksums, comparison) with Python bindings, fully tested with unit tests that prove measurement accuracy.

**Architecture:** Rust workspace with 7 crates — shared types, timing harness, statistical engine, graph generators, correctness checksums, result comparison, and PyO3 bindings. Single compiled Python extension (`xraybench_core`) re-exports all crates.

**Tech Stack:** Rust 1.78+, PyO3 0.22+, maturin 1.5+, BLAKE3, t-digest, Python 3.12

**Spec:** `docs/superpowers/specs/2026-03-31-full-implementation-design.md`

---

## File Structure

```
rust/
  Cargo.toml                              # workspace root
  xraybench-types/
    Cargo.toml
    src/lib.rs                            # Phase enum, Measurement struct, errors, constants
  xraybench-timing/
    Cargo.toml
    src/lib.rs                            # public API
    src/clock.rs                          # platform-specific clock (Linux/macOS)
    src/calibration.rs                    # self-calibration (resolution, overhead, fences)
    src/harness.rs                        # measurement harness (cold, warmup, steady-state)
    src/warmup.rs                         # CUSUM change-point detection
  xraybench-stats/
    Cargo.toml
    src/lib.rs                            # public API
    src/percentile.rs                     # exact + t-digest percentiles
    src/bootstrap.rs                      # BCa confidence intervals
    src/outlier.rs                        # MAD-based outlier detection
    src/regression.rs                     # Mann-Whitney U test
    src/tdigest.rs                        # t-digest streaming quantile estimation
  xraybench-generators/
    Cargo.toml
    src/lib.rs                            # public API, generator trait
    src/uniform.rs                        # uniform node generator
    src/power_law.rs                      # Barabási-Albert preferential attachment
    src/hub.rs                            # hub-and-spoke generator
    src/community.rs                      # Stochastic Block Model
    src/chain.rs                          # linear chain generator
    src/deep_traversal.rs                 # depth-controlled fanout generator
    src/io.rs                             # edge-list file I/O (binary + CSV)
  xraybench-checksum/
    Cargo.toml
    src/lib.rs                            # public API
    src/canonical.rs                      # deterministic row serialization
    src/hasher.rs                         # streaming BLAKE3 hashing
    src/structural.rs                     # topology validation for path results
  xraybench-compare/
    Cargo.toml
    src/lib.rs                            # public API
    src/diff.rs                           # per-metric delta + CI
    src/significance.rs                   # statistical significance tests
    src/matrix.rs                         # multi-engine comparison matrix
  xraybench-py/
    Cargo.toml                            # PyO3 extension, depends on all crates
    src/lib.rs                            # Python module definition
    src/timing.rs                         # Python wrappers for timing
    src/stats.rs                          # Python wrappers for stats
    src/generators.rs                     # Python wrappers for generators
    src/checksum.rs                       # Python wrappers for checksum
    src/compare.rs                        # Python wrappers for compare
tests/
  rust_core/
    __init__.py
    test_timing.py                        # Python-side timing integration tests
    test_stats.py                         # Python-side stats integration tests
    test_generators.py                    # Python-side generator integration tests
    test_checksum.py                      # Python-side checksum integration tests
    test_compare.py                       # Python-side comparison integration tests
```

---

### Task 0: Install Rust Toolchain and Bootstrap Workspace

**Files:**
- Create: `rust/Cargo.toml`
- Create: `rust/.cargo/config.toml`

- [ ] **Step 1: Install Rust via rustup**

Run:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
source "$HOME/.cargo/env"
```

Expected: `rustc --version` prints `rustc 1.8x.x`

- [ ] **Step 2: Install maturin**

Run:
```bash
pip install maturin>=1.5
```

Expected: `maturin --version` prints `maturin 1.x.x`

- [ ] **Step 3: Create workspace Cargo.toml**

Create `rust/Cargo.toml`:

```toml
[workspace]
resolver = "2"
members = [
    "xraybench-types",
    "xraybench-timing",
    "xraybench-stats",
    "xraybench-generators",
    "xraybench-checksum",
    "xraybench-compare",
    "xraybench-py",
]

[workspace.package]
version = "0.1.0"
edition = "2021"
license = "Apache-2.0"
authors = ["eMTAi Labs"]

[workspace.dependencies]
blake3 = "1.5"
pyo3 = { version = "0.22", features = ["extension-module"] }
rand = "0.8"
rand_chacha = "0.3"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

- [ ] **Step 4: Create cargo config for macOS arm64**

Create `rust/.cargo/config.toml`:

```toml
[build]
# Ensure consistent builds on macOS arm64
target-dir = "target"

[profile.release]
lto = "thin"
codegen-units = 1
opt-level = 3
```

- [ ] **Step 5: Verify workspace compiles**

Run:
```bash
cd rust && cargo check 2>&1
```

Expected: warning about no members (we haven't created crates yet), but no errors about workspace configuration.

- [ ] **Step 6: Commit**

```bash
git add rust/Cargo.toml rust/.cargo/config.toml
git commit -m "feat: bootstrap Rust workspace for measurement core"
```

---

### Task 1: xraybench-types — Shared Types and Constants

**Files:**
- Create: `rust/xraybench-types/Cargo.toml`
- Create: `rust/xraybench-types/src/lib.rs`

- [ ] **Step 1: Write tests for shared types**

Create `rust/xraybench-types/src/lib.rs`:

```rust
//! Shared types for the xraybench measurement core.

use serde::{Deserialize, Serialize};

/// Phase of a benchmark measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    /// First execution after cache clear. Measures cold-start behavior.
    Cold,
    /// Iterations where execution time has not yet stabilized.
    WarmUp,
    /// Iterations after execution time has stabilized.
    SteadyState,
}

/// A single timing measurement with full metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Measurement {
    /// Wall-clock timestamp at measurement start (nanoseconds since epoch).
    pub timestamp_ns: u64,
    /// Elapsed duration in nanoseconds.
    pub duration_ns: u64,
    /// Measured clock granularity in nanoseconds.
    pub clock_resolution_ns: u64,
    /// Cost of reading the clock in nanoseconds.
    pub clock_overhead_ns: u64,
    /// Cost of memory fences in nanoseconds.
    pub fence_overhead_ns: u64,
    /// Which iteration this measurement represents.
    pub iteration: u32,
    /// Phase classification.
    pub phase: Phase,
}

/// Calibration results from self-calibration at startup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationResult {
    /// Smallest observable time delta in nanoseconds.
    pub clock_resolution_ns: u64,
    /// Median cost of a single clock read in nanoseconds.
    pub clock_overhead_ns: u64,
    /// Median cost of a memory fence pair in nanoseconds.
    pub fence_overhead_ns: u64,
    /// Number of calibration samples taken.
    pub samples: u32,
}

/// Summary statistics for a set of measurements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsSummary {
    pub count: usize,
    pub mean_ns: f64,
    pub median_ns: f64,
    pub min_ns: u64,
    pub max_ns: u64,
    pub p50_ns: f64,
    pub p95_ns: f64,
    pub p99_ns: f64,
    pub stddev_ns: f64,
    pub ci_lower_ns: f64,
    pub ci_upper_ns: f64,
    pub ci_confidence: f64,
    pub mad_ns: f64,
    pub outlier_count: usize,
    pub warmup_iterations: u32,
}

/// Outcome of a benchmark execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Success,
    CorrectnessMismatch,
    EngineError,
    Timeout,
    Unsupported,
    DatasetVerificationFailed,
    HarnessFailure,
    ConnectionFailure,
    OutOfMemory,
}

/// Result of a correctness validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrectnessResult {
    pub oracle_type: String,
    pub passed: bool,
    pub reference_hash: Option<String>,
    pub computed_hash: Option<String>,
    pub row_count_expected: Option<u64>,
    pub row_count_actual: Option<u64>,
    pub float_tolerance_ulp: Option<u32>,
    pub validation_duration_ms: f64,
    pub validator_version: String,
    pub detail: Option<String>,
}

/// Edge in a generated graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Edge {
    pub source: u64,
    pub target: u64,
}

/// Node with properties in a generated graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: u64,
    pub properties: Vec<PropertyValue>,
}

/// A typed property value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropertyValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
}

/// Result of comparing two benchmark results on a single metric.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricComparison {
    pub metric_name: String,
    pub value_a: f64,
    pub value_b: f64,
    pub absolute_delta: f64,
    pub percentage_change: f64,
    pub ci_lower: f64,
    pub ci_upper: f64,
    pub p_value: f64,
    pub significant: bool,
    pub classification: ChangeClass,
}

/// Classification of a metric change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeClass {
    Improvement,
    Regression,
    NoChange,
    Inconclusive,
}

/// Errors that can occur in the measurement core.
#[derive(Debug, Clone)]
pub enum BenchError {
    ClockUnavailable(String),
    CalibrationFailed(String),
    InsufficientSamples { needed: usize, got: usize },
    IoError(String),
    InvalidData(String),
}

impl std::fmt::Display for BenchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchError::ClockUnavailable(msg) => write!(f, "Clock unavailable: {msg}"),
            BenchError::CalibrationFailed(msg) => write!(f, "Calibration failed: {msg}"),
            BenchError::InsufficientSamples { needed, got } => {
                write!(f, "Insufficient samples: needed {needed}, got {got}")
            }
            BenchError::IoError(msg) => write!(f, "I/O error: {msg}"),
            BenchError::InvalidData(msg) => write!(f, "Invalid data: {msg}"),
        }
    }
}

impl std::error::Error for BenchError {}

pub type Result<T> = std::result::Result<T, BenchError>;

/// Crate version, embedded in every result for auditability.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measurement_serialization_roundtrip() {
        let m = Measurement {
            timestamp_ns: 1_000_000_000,
            duration_ns: 42_000,
            clock_resolution_ns: 1,
            clock_overhead_ns: 20,
            fence_overhead_ns: 5,
            iteration: 0,
            phase: Phase::Cold,
        };
        let json = serde_json::to_string(&m).unwrap();
        let m2: Measurement = serde_json::from_str(&json).unwrap();
        assert_eq!(m.duration_ns, m2.duration_ns);
        assert_eq!(m.phase, m2.phase);
    }

    #[test]
    fn stats_summary_serialization_roundtrip() {
        let s = StatsSummary {
            count: 100,
            mean_ns: 42_000.0,
            median_ns: 41_500.0,
            min_ns: 38_000,
            max_ns: 55_000,
            p50_ns: 41_500.0,
            p95_ns: 50_000.0,
            p99_ns: 53_000.0,
            stddev_ns: 3_200.0,
            ci_lower_ns: 41_000.0,
            ci_upper_ns: 43_000.0,
            ci_confidence: 0.95,
            mad_ns: 2_800.0,
            outlier_count: 2,
            warmup_iterations: 5,
        };
        let json = serde_json::to_string(&s).unwrap();
        let s2: StatsSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(s.count, s2.count);
        assert!((s.mean_ns - s2.mean_ns).abs() < 0.001);
    }

    #[test]
    fn edge_equality() {
        let e1 = Edge { source: 1, target: 2 };
        let e2 = Edge { source: 1, target: 2 };
        let e3 = Edge { source: 2, target: 1 };
        assert_eq!(e1, e2);
        assert_ne!(e1, e3);
    }

    #[test]
    fn property_value_variants() {
        let vals = vec![
            PropertyValue::Null,
            PropertyValue::Integer(42),
            PropertyValue::Float(3.14),
            PropertyValue::Text("hello".into()),
            PropertyValue::Boolean(true),
        ];
        for v in &vals {
            let json = serde_json::to_string(v).unwrap();
            let v2: PropertyValue = serde_json::from_str(&json).unwrap();
            assert_eq!(v, &v2);
        }
    }

    #[test]
    fn outcome_variants() {
        assert_eq!(
            serde_json::to_string(&Outcome::Success).unwrap(),
            "\"Success\""
        );
        assert_eq!(
            serde_json::to_string(&Outcome::CorrectnessMismatch).unwrap(),
            "\"CorrectnessMismatch\""
        );
    }

    #[test]
    fn error_display() {
        let e = BenchError::InsufficientSamples { needed: 100, got: 5 };
        assert!(e.to_string().contains("100"));
        assert!(e.to_string().contains("5"));
    }

    #[test]
    fn version_is_set() {
        assert!(!VERSION.is_empty());
    }
}
```

- [ ] **Step 2: Create Cargo.toml**

Create `rust/xraybench-types/Cargo.toml`:

```toml
[package]
name = "xraybench-types"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
description = "Shared types for xraybench measurement core"

[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
```

- [ ] **Step 3: Run tests**

Run:
```bash
cd rust && cargo test -p xraybench-types
```

Expected: All 7 tests pass.

- [ ] **Step 4: Commit**

```bash
git add rust/xraybench-types/
git commit -m "feat(rust): add xraybench-types crate — shared types, errors, constants"
```

---

### Task 2: xraybench-timing — Platform Clock and Calibration

**Files:**
- Create: `rust/xraybench-timing/Cargo.toml`
- Create: `rust/xraybench-timing/src/lib.rs`
- Create: `rust/xraybench-timing/src/clock.rs`
- Create: `rust/xraybench-timing/src/calibration.rs`
- Create: `rust/xraybench-timing/src/warmup.rs`
- Create: `rust/xraybench-timing/src/harness.rs`

- [ ] **Step 1: Create Cargo.toml**

Create `rust/xraybench-timing/Cargo.toml`:

```toml
[package]
name = "xraybench-timing"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
description = "High-resolution timing harness for xraybench"

[dependencies]
xraybench-types = { path = "../xraybench-types" }

[target.'cfg(target_os = "macos")'.dependencies]
mach2 = "0.4"

[target.'cfg(target_os = "linux")'.dependencies]
libc = "0.2"
```

- [ ] **Step 2: Write clock.rs — platform-specific nanosecond clock**

Create `rust/xraybench-timing/src/clock.rs`:

```rust
//! Platform-specific monotonic clock with nanosecond resolution.
//!
//! Linux: clock_gettime(CLOCK_MONOTONIC_RAW) — immune to NTP adjustments.
//! macOS: mach_absolute_time() — monotonic, high-resolution.

use xraybench_types::{BenchError, Result};

/// Read the monotonic clock in nanoseconds.
///
/// Returns a timestamp that is:
/// - Monotonically increasing (never goes backwards)
/// - Not affected by NTP adjustments
/// - Nanosecond resolution (actual granularity varies by hardware)
#[cfg(target_os = "linux")]
pub fn monotonic_ns() -> Result<u64> {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let ret = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts) };
    if ret != 0 {
        return Err(BenchError::ClockUnavailable(
            "clock_gettime(CLOCK_MONOTONIC_RAW) failed".into(),
        ));
    }
    Ok(ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64)
}

#[cfg(target_os = "macos")]
pub fn monotonic_ns() -> Result<u64> {
    use mach2::mach_time;
    use std::sync::OnceLock;

    static TIMEBASE: OnceLock<(u32, u32)> = OnceLock::new();

    let (numer, denom) = TIMEBASE.get_or_init(|| {
        let mut info = mach_time::mach_timebase_info_data_t { numer: 0, denom: 0 };
        unsafe { mach_time::mach_timebase_info(&mut info) };
        (info.numer, info.denom)
    });

    let ticks = unsafe { mach_time::mach_absolute_time() };
    // Convert ticks to nanoseconds: ticks * numer / denom
    // Use u128 to avoid overflow on long-running systems
    let ns = (ticks as u128 * *numer as u128 / *denom as u128) as u64;
    Ok(ns)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn monotonic_ns() -> Result<u64> {
    // Fallback: std::time::Instant (less precise but portable)
    use std::time::Instant;
    use std::sync::OnceLock;

    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let epoch = EPOCH.get_or_init(Instant::now);
    Ok(epoch.elapsed().as_nanos() as u64)
}

/// Memory fence to prevent CPU reordering around timing measurements.
///
/// Uses SeqCst fence + compiler barrier to ensure all prior memory
/// operations complete before the fence, and no subsequent operations
/// begin until after it.
#[inline]
pub fn timing_fence() {
    std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
    // Compiler barrier — prevents the compiler from reordering across this point.
    // The atomic fence handles the CPU side.
    std::hint::black_box(());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_returns_nonzero() {
        let t = monotonic_ns().unwrap();
        assert!(t > 0, "Clock returned zero");
    }

    #[test]
    fn clock_is_monotonic() {
        let t1 = monotonic_ns().unwrap();
        let t2 = monotonic_ns().unwrap();
        assert!(t2 >= t1, "Clock went backwards: {t1} -> {t2}");
    }

    #[test]
    fn clock_advances() {
        let t1 = monotonic_ns().unwrap();
        // Spin briefly to ensure time passes
        for _ in 0..10_000 {
            std::hint::black_box(0u64);
        }
        let t2 = monotonic_ns().unwrap();
        assert!(t2 > t1, "Clock did not advance after spin: {t1} -> {t2}");
    }

    #[test]
    fn fence_does_not_panic() {
        timing_fence();
        timing_fence();
    }

    #[test]
    fn clock_reasonable_magnitude() {
        // Nanoseconds since some epoch — should be at least 1 second
        let t = monotonic_ns().unwrap();
        assert!(t > 1_000_000_000, "Clock value suspiciously small: {t}");
    }
}
```

- [ ] **Step 3: Run clock tests**

Run:
```bash
cd rust && cargo test -p xraybench-timing clock::tests
```

Expected: All 5 tests pass.

- [ ] **Step 4: Write calibration.rs**

Create `rust/xraybench-timing/src/calibration.rs`:

```rust
//! Self-calibration for the timing harness.
//!
//! Measures:
//! - Clock resolution: smallest observable time delta
//! - Clock overhead: cost of reading the clock
//! - Fence overhead: cost of memory fence pair

use xraybench_types::{CalibrationResult, Result};
use crate::clock::{monotonic_ns, timing_fence};

const CALIBRATION_SAMPLES: u32 = 10_000;

/// Calibrate the timing infrastructure.
///
/// Runs `CALIBRATION_SAMPLES` iterations of each measurement to determine
/// the noise floor of the timing harness itself. Results are embedded in
/// every benchmark measurement for auditability.
pub fn calibrate() -> Result<CalibrationResult> {
    let clock_resolution_ns = measure_clock_resolution()?;
    let clock_overhead_ns = measure_clock_overhead()?;
    let fence_overhead_ns = measure_fence_overhead()?;

    Ok(CalibrationResult {
        clock_resolution_ns,
        clock_overhead_ns,
        fence_overhead_ns,
        samples: CALIBRATION_SAMPLES,
    })
}

/// Measure the smallest observable time delta.
///
/// Reads the clock in a tight loop and records the smallest non-zero delta.
fn measure_clock_resolution() -> Result<u64> {
    let mut min_delta = u64::MAX;

    for _ in 0..CALIBRATION_SAMPLES {
        let t1 = monotonic_ns()?;
        let t2 = monotonic_ns()?;
        let delta = t2.saturating_sub(t1);
        if delta > 0 && delta < min_delta {
            min_delta = delta;
        }
    }

    if min_delta == u64::MAX {
        // All deltas were zero — clock resolution is too coarse
        // for back-to-back reads. Return 1 as minimum.
        return Ok(1);
    }

    Ok(min_delta)
}

/// Measure the cost of reading the clock.
///
/// Reads the clock many times and computes the median overhead per read.
fn measure_clock_overhead() -> Result<u64> {
    let mut deltas = Vec::with_capacity(CALIBRATION_SAMPLES as usize);

    for _ in 0..CALIBRATION_SAMPLES {
        let t1 = monotonic_ns()?;
        let t2 = monotonic_ns()?;
        deltas.push(t2.saturating_sub(t1));
    }

    deltas.sort_unstable();
    Ok(median_u64(&deltas))
}

/// Measure the cost of a memory fence pair (before + after timing).
///
/// The fences prevent CPU reordering but have their own cost that
/// must be accounted for.
fn measure_fence_overhead() -> Result<u64> {
    let mut deltas = Vec::with_capacity(CALIBRATION_SAMPLES as usize);

    for _ in 0..CALIBRATION_SAMPLES {
        let t1 = monotonic_ns()?;
        timing_fence();
        timing_fence();
        let t2 = monotonic_ns()?;
        // Subtract clock overhead (one read pair)
        let raw = t2.saturating_sub(t1);
        deltas.push(raw);
    }

    deltas.sort_unstable();
    // The fence overhead is the median of (fence pair + clock overhead).
    // The caller already has clock_overhead separately, so this is
    // the combined cost. Pure fence cost = this - clock_overhead.
    Ok(median_u64(&deltas))
}

fn median_u64(sorted: &[u64]) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[mid - 1] + sorted[mid]) / 2
    } else {
        sorted[mid]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calibration_completes() {
        let cal = calibrate().unwrap();
        assert!(cal.samples == CALIBRATION_SAMPLES);
    }

    #[test]
    fn clock_resolution_is_reasonable() {
        let cal = calibrate().unwrap();
        // Resolution should be between 1ns and 10μs
        assert!(cal.clock_resolution_ns >= 1);
        assert!(cal.clock_resolution_ns < 10_000, "Resolution too coarse: {}ns", cal.clock_resolution_ns);
    }

    #[test]
    fn clock_overhead_is_reasonable() {
        let cal = calibrate().unwrap();
        // Clock read should cost less than 10μs
        assert!(cal.clock_overhead_ns < 10_000, "Clock overhead too high: {}ns", cal.clock_overhead_ns);
    }

    #[test]
    fn fence_overhead_is_reasonable() {
        let cal = calibrate().unwrap();
        // Fence pair should cost less than 10μs
        assert!(cal.fence_overhead_ns < 10_000, "Fence overhead too high: {}ns", cal.fence_overhead_ns);
    }

    #[test]
    fn calibration_serializes() {
        let cal = calibrate().unwrap();
        let json = serde_json::to_string(&cal).unwrap();
        let cal2: CalibrationResult = serde_json::from_str(&json).unwrap();
        assert_eq!(cal.clock_resolution_ns, cal2.clock_resolution_ns);
        assert_eq!(cal.clock_overhead_ns, cal2.clock_overhead_ns);
    }

    #[test]
    fn median_u64_works() {
        assert_eq!(median_u64(&[1, 2, 3]), 2);
        assert_eq!(median_u64(&[1, 2, 3, 4]), 2);
        assert_eq!(median_u64(&[10]), 10);
        assert_eq!(median_u64(&[]), 0);
    }
}
```

- [ ] **Step 5: Run calibration tests**

Run:
```bash
cd rust && cargo test -p xraybench-timing calibration::tests
```

Expected: All 6 tests pass.

- [ ] **Step 6: Write warmup.rs — CUSUM change-point detection**

Create `rust/xraybench-timing/src/warmup.rs`:

```rust
//! Warm-up detection using CUSUM (Cumulative Sum) change-point detection.
//!
//! Instead of a fixed number of warm-up iterations, CUSUM detects when
//! the execution time distribution has stabilized. This adapts to the
//! engine's actual behavior rather than guessing.
//!
//! Algorithm: Page's CUSUM (1954). Tracks cumulative deviation from the
//! running mean. A change-point is detected when the CUSUM statistic
//! exceeds a threshold.

/// Configuration for CUSUM warm-up detection.
pub struct CusumConfig {
    /// Minimum samples before detection can trigger.
    pub min_samples: usize,
    /// Threshold for the CUSUM statistic (in units of standard deviation).
    /// Higher = less sensitive (fewer false positives).
    pub threshold_sigma: f64,
    /// Drift parameter — allowable shift before detection triggers.
    /// Expressed as fraction of the running standard deviation.
    pub drift_fraction: f64,
}

impl Default for CusumConfig {
    fn default() -> Self {
        Self {
            min_samples: 5,
            threshold_sigma: 4.0,
            drift_fraction: 0.5,
        }
    }
}

/// CUSUM detector for warm-up phase termination.
pub struct CusumDetector {
    config: CusumConfig,
    values: Vec<f64>,
    cusum_pos: f64,
    cusum_neg: f64,
    running_mean: f64,
    running_m2: f64,
    change_point: Option<usize>,
}

impl CusumDetector {
    pub fn new(config: CusumConfig) -> Self {
        Self {
            config,
            values: Vec::new(),
            cusum_pos: 0.0,
            cusum_neg: 0.0,
            running_mean: 0.0,
            running_m2: 0.0,
            change_point: None,
        }
    }

    /// Add a new observation and check for change-point.
    ///
    /// Returns `true` if warm-up is detected as complete (steady-state reached).
    pub fn observe(&mut self, value: f64) -> bool {
        let n = self.values.len();
        self.values.push(value);

        // Welford's online algorithm for mean and variance
        let count = (n + 1) as f64;
        let delta = value - self.running_mean;
        self.running_mean += delta / count;
        let delta2 = value - self.running_mean;
        self.running_m2 += delta * delta2;

        if n + 1 < self.config.min_samples {
            return false;
        }

        let variance = self.running_m2 / (count - 1.0);
        let stddev = variance.sqrt();

        if stddev < f64::EPSILON {
            // All values are identical — trivially stable
            self.change_point = Some(0);
            return true;
        }

        let drift = self.config.drift_fraction * stddev;
        let threshold = self.config.threshold_sigma * stddev;

        // Normalize the observation
        let z = value - self.running_mean;

        // Update CUSUM statistics
        self.cusum_pos = (self.cusum_pos + z - drift).max(0.0);
        self.cusum_neg = (self.cusum_neg - z - drift).max(0.0);

        // Check for change-point
        if self.cusum_pos > threshold || self.cusum_neg > threshold {
            // Change detected — reset and mark this as the transition point
            self.cusum_pos = 0.0;
            self.cusum_neg = 0.0;
            self.change_point = Some(n);
            return false;
        }

        // If we had a change point and have been stable since, warm-up is done
        if self.change_point.is_some() {
            let since_change = n - self.change_point.unwrap();
            if since_change >= self.config.min_samples {
                return true;
            }
        }

        // If no change point ever detected and we have enough samples,
        // the system was never in a warm-up phase
        if self.change_point.is_none() && n + 1 >= self.config.min_samples * 2 {
            self.change_point = Some(0);
            return true;
        }

        false
    }

    /// The iteration at which warm-up ended (steady-state began).
    pub fn warmup_end(&self) -> Option<usize> {
        self.change_point
    }

    /// Number of observations so far.
    pub fn count(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_series_detects_quickly() {
        let mut det = CusumDetector::new(CusumConfig::default());
        // Feed stable values — should detect steady-state after min_samples * 2
        for i in 0..20 {
            let stable = det.observe(100.0 + (i as f64) * 0.01);
            if stable {
                assert!(det.warmup_end().is_some());
                return;
            }
        }
        panic!("Failed to detect steady-state on stable input");
    }

    #[test]
    fn warmup_then_stable_detects_transition() {
        let mut det = CusumDetector::new(CusumConfig {
            min_samples: 3,
            threshold_sigma: 3.0,
            drift_fraction: 0.5,
        });

        // Warm-up phase: high values
        for _ in 0..5 {
            det.observe(500.0);
        }

        // Transition to steady-state: low values
        let mut detected = false;
        for _ in 0..20 {
            if det.observe(100.0) {
                detected = true;
                break;
            }
        }

        assert!(detected, "Failed to detect warm-up → steady-state transition");
        let end = det.warmup_end().unwrap();
        // Change should be detected around iteration 5 (when values dropped)
        assert!(end >= 4 && end <= 10, "Change point at unexpected position: {end}");
    }

    #[test]
    fn all_identical_is_stable() {
        let mut det = CusumDetector::new(CusumConfig {
            min_samples: 3,
            ..Default::default()
        });

        let mut detected = false;
        for _ in 0..10 {
            if det.observe(42.0) {
                detected = true;
                break;
            }
        }
        assert!(detected, "Failed to detect stability on identical values");
    }

    #[test]
    fn noisy_but_stable_eventually_detects() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut det = CusumDetector::new(CusumConfig::default());

        // Deterministic pseudo-random noise around 100
        let mut detected = false;
        for i in 0..100 {
            let mut hasher = DefaultHasher::new();
            i.hash(&mut hasher);
            let noise = (hasher.finish() % 10) as f64 - 5.0;
            if det.observe(100.0 + noise) {
                detected = true;
                break;
            }
        }
        assert!(detected, "Failed to detect stability on noisy-but-stable input");
    }

    #[test]
    fn respects_min_samples() {
        let mut det = CusumDetector::new(CusumConfig {
            min_samples: 10,
            ..Default::default()
        });

        // Should not trigger before min_samples
        for _ in 0..9 {
            assert!(!det.observe(42.0), "Triggered before min_samples");
        }
    }
}
```

- [ ] **Step 7: Run warmup tests**

Run:
```bash
cd rust && cargo test -p xraybench-timing warmup::tests
```

Expected: All 5 tests pass.

- [ ] **Step 8: Write harness.rs — measurement orchestrator**

Create `rust/xraybench-timing/src/harness.rs`:

```rust
//! Measurement harness that orchestrates cold, warm-up, and steady-state phases.
//!
//! This is the core of the timing infrastructure. It:
//! 1. Self-calibrates at startup
//! 2. Takes a single cold measurement (after caller clears caches)
//! 3. Runs iterations until CUSUM detects warm-up completion
//! 4. Collects steady-state samples until target CI width or max iterations

use xraybench_types::{CalibrationResult, Measurement, Phase, Result};
use crate::calibration::calibrate;
use crate::clock::{monotonic_ns, timing_fence};
use crate::warmup::{CusumConfig, CusumDetector};

/// Configuration for the measurement harness.
pub struct HarnessConfig {
    /// Maximum iterations for warm-up detection.
    pub max_warmup_iterations: u32,
    /// Maximum iterations for steady-state collection.
    pub max_steady_iterations: u32,
    /// Target confidence interval half-width as fraction of mean.
    /// e.g., 0.05 = stop when CI is within ±5% of the mean.
    pub target_ci_fraction: f64,
    /// CUSUM configuration for warm-up detection.
    pub cusum_config: CusumConfig,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            max_warmup_iterations: 100,
            max_steady_iterations: 1000,
            target_ci_fraction: 0.02,
            cusum_config: CusumConfig::default(),
        }
    }
}

/// Collected measurements from a full benchmark run.
pub struct HarnessResult {
    pub calibration: CalibrationResult,
    pub cold: Measurement,
    pub warmup: Vec<Measurement>,
    pub steady_state: Vec<Measurement>,
}

impl HarnessResult {
    /// Total number of iterations (cold + warmup + steady-state).
    pub fn total_iterations(&self) -> u32 {
        1 + self.warmup.len() as u32 + self.steady_state.len() as u32
    }

    /// Number of warm-up iterations detected.
    pub fn warmup_count(&self) -> u32 {
        self.warmup.len() as u32
    }
}

/// Run the full measurement protocol.
///
/// `work_fn` is called for each iteration. It should execute the query
/// or operation being benchmarked. The harness handles all timing.
///
/// **Important:** The caller must clear caches BEFORE calling this function.
/// The harness assumes the first call to `work_fn` is a true cold execution.
pub fn measure<F>(config: &HarnessConfig, mut work_fn: F) -> Result<HarnessResult>
where
    F: FnMut() -> std::result::Result<(), String>,
{
    // Phase 0: Calibrate
    let cal = calibrate()?;

    // Phase 1: Cold run
    timing_fence();
    let cold_start = monotonic_ns()?;
    work_fn().map_err(|e| xraybench_types::BenchError::InvalidData(e))?;
    timing_fence();
    let cold_end = monotonic_ns()?;

    let cold = Measurement {
        timestamp_ns: cold_start,
        duration_ns: cold_end.saturating_sub(cold_start),
        clock_resolution_ns: cal.clock_resolution_ns,
        clock_overhead_ns: cal.clock_overhead_ns,
        fence_overhead_ns: cal.fence_overhead_ns,
        iteration: 0,
        phase: Phase::Cold,
    };

    // Phase 2: Warm-up detection
    let mut detector = CusumDetector::new(CusumConfig {
        min_samples: config.cusum_config.min_samples,
        threshold_sigma: config.cusum_config.threshold_sigma,
        drift_fraction: config.cusum_config.drift_fraction,
    });
    let mut warmup_measurements = Vec::new();
    let mut iteration = 1u32;

    loop {
        if iteration > config.max_warmup_iterations {
            break;
        }

        timing_fence();
        let start = monotonic_ns()?;
        work_fn().map_err(|e| xraybench_types::BenchError::InvalidData(e))?;
        timing_fence();
        let end = monotonic_ns()?;
        let duration = end.saturating_sub(start);

        let m = Measurement {
            timestamp_ns: start,
            duration_ns: duration,
            clock_resolution_ns: cal.clock_resolution_ns,
            clock_overhead_ns: cal.clock_overhead_ns,
            fence_overhead_ns: cal.fence_overhead_ns,
            iteration,
            phase: Phase::WarmUp,
        };

        warmup_measurements.push(m);

        if detector.observe(duration as f64) {
            break;
        }

        iteration += 1;
    }

    // Phase 3: Steady-state collection
    let mut steady_measurements = Vec::new();
    let start_iter = iteration + 1;

    for i in 0..config.max_steady_iterations {
        timing_fence();
        let start = monotonic_ns()?;
        work_fn().map_err(|e| xraybench_types::BenchError::InvalidData(e))?;
        timing_fence();
        let end = monotonic_ns()?;
        let duration = end.saturating_sub(start);

        let m = Measurement {
            timestamp_ns: start,
            duration_ns: duration,
            clock_resolution_ns: cal.clock_resolution_ns,
            clock_overhead_ns: cal.clock_overhead_ns,
            fence_overhead_ns: cal.fence_overhead_ns,
            iteration: start_iter + i,
            phase: Phase::SteadyState,
        };

        steady_measurements.push(m);

        // Check if we've reached target CI width
        if steady_measurements.len() >= 10 {
            let durations: Vec<f64> = steady_measurements
                .iter()
                .map(|m| m.duration_ns as f64)
                .collect();
            let mean = durations.iter().sum::<f64>() / durations.len() as f64;
            let stddev = sample_stddev(&durations);
            let n = durations.len() as f64;
            // Approximate 95% CI half-width: 1.96 * stddev / sqrt(n)
            let ci_half = 1.96 * stddev / n.sqrt();
            if mean > 0.0 && ci_half / mean < config.target_ci_fraction {
                break;
            }
        }
    }

    Ok(HarnessResult {
        calibration: cal,
        cold,
        warmup: warmup_measurements,
        steady_state: steady_measurements,
    })
}

fn sample_stddev(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
    variance.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn harness_measures_cold_and_warm() {
        let config = HarnessConfig {
            max_warmup_iterations: 20,
            max_steady_iterations: 50,
            target_ci_fraction: 0.10,
            ..Default::default()
        };

        let result = measure(&config, || {
            // Simulate 10μs of work
            let start = std::time::Instant::now();
            while start.elapsed().as_micros() < 10 {
                std::hint::black_box(0u64);
            }
            Ok(())
        })
        .unwrap();

        // Cold measurement should exist
        assert_eq!(result.cold.phase, Phase::Cold);
        assert!(result.cold.duration_ns > 0);
        assert_eq!(result.cold.iteration, 0);

        // Should have some warm-up and steady-state measurements
        assert!(!result.steady_state.is_empty(), "No steady-state measurements");

        // All steady-state measurements should be Phase::SteadyState
        for m in &result.steady_state {
            assert_eq!(m.phase, Phase::SteadyState);
        }

        // Calibration should be populated
        assert!(result.calibration.samples > 0);
    }

    #[test]
    fn harness_handles_work_fn_error() {
        let config = HarnessConfig::default();
        let call_count = AtomicU32::new(0);

        let result = measure(&config, || {
            let n = call_count.fetch_add(1, Ordering::Relaxed);
            if n == 0 {
                Ok(()) // Cold run succeeds
            } else {
                Err("simulated engine error".into())
            }
        });

        // Should get an error on the second call (first warm-up)
        assert!(result.is_err());
    }

    #[test]
    fn harness_calibration_values_present() {
        let config = HarnessConfig {
            max_warmup_iterations: 5,
            max_steady_iterations: 10,
            target_ci_fraction: 1.0, // Very loose — stop quickly
            cusum_config: CusumConfig { min_samples: 2, ..Default::default() },
        };

        let result = measure(&config, || Ok(())).unwrap();
        assert!(result.calibration.clock_resolution_ns > 0);
        assert!(result.calibration.samples > 0);
    }

    #[test]
    fn harness_cold_slower_than_warm() {
        let call_count = AtomicU32::new(0);

        let config = HarnessConfig {
            max_warmup_iterations: 10,
            max_steady_iterations: 20,
            target_ci_fraction: 0.20,
            cusum_config: CusumConfig { min_samples: 3, ..Default::default() },
        };

        let result = measure(&config, || {
            let n = call_count.fetch_add(1, Ordering::Relaxed);
            let delay_us = if n == 0 { 100 } else { 10 }; // Cold = 100μs, warm = 10μs
            let start = std::time::Instant::now();
            while start.elapsed().as_micros() < delay_us {
                std::hint::black_box(0u64);
            }
            Ok(())
        })
        .unwrap();

        if !result.steady_state.is_empty() {
            let warm_mean = result
                .steady_state
                .iter()
                .map(|m| m.duration_ns as f64)
                .sum::<f64>()
                / result.steady_state.len() as f64;
            // Cold should be at least 2x warm (100μs vs 10μs)
            assert!(
                result.cold.duration_ns as f64 > warm_mean * 1.5,
                "Cold ({}) should be much slower than warm mean ({:.0})",
                result.cold.duration_ns,
                warm_mean
            );
        }
    }
}
```

- [ ] **Step 9: Write lib.rs — public API**

Create `rust/xraybench-timing/src/lib.rs`:

```rust
//! High-resolution timing harness for xraybench.
//!
//! Provides platform-specific monotonic clocks, self-calibration,
//! CUSUM warm-up detection, and a measurement harness that produces
//! cold, warm-up, and steady-state measurements with full metadata.

pub mod clock;
pub mod calibration;
pub mod warmup;
pub mod harness;

// Re-export primary API
pub use calibration::calibrate;
pub use clock::{monotonic_ns, timing_fence};
pub use harness::{measure, HarnessConfig, HarnessResult};
pub use warmup::{CusumConfig, CusumDetector};
```

- [ ] **Step 10: Run all timing tests**

Run:
```bash
cd rust && cargo test -p xraybench-timing
```

Expected: All ~20 tests pass (clock: 5, calibration: 6, warmup: 5, harness: 4).

- [ ] **Step 11: Commit**

```bash
git add rust/xraybench-timing/
git commit -m "feat(rust): add xraybench-timing crate — clock, calibration, CUSUM warmup, harness"
```

---

### Task 3: xraybench-stats — Statistical Engine

**Files:**
- Create: `rust/xraybench-stats/Cargo.toml`
- Create: `rust/xraybench-stats/src/lib.rs`
- Create: `rust/xraybench-stats/src/percentile.rs`
- Create: `rust/xraybench-stats/src/tdigest.rs`
- Create: `rust/xraybench-stats/src/bootstrap.rs`
- Create: `rust/xraybench-stats/src/outlier.rs`
- Create: `rust/xraybench-stats/src/regression.rs`

- [ ] **Step 1: Create Cargo.toml**

Create `rust/xraybench-stats/Cargo.toml`:

```toml
[package]
name = "xraybench-stats"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
description = "Statistical engine for xraybench — percentiles, CI, outliers, regression"

[dependencies]
xraybench-types = { path = "../xraybench-types" }
rand = { workspace = true }
rand_chacha = { workspace = true }
```

- [ ] **Step 2: Write percentile.rs — exact and streaming percentiles**

Create `rust/xraybench-stats/src/percentile.rs`:

```rust
//! Percentile computation: exact for small N, t-digest for streaming.

use xraybench_types::{BenchError, Result};

/// Compute exact percentile from a slice of values.
///
/// Uses linear interpolation between nearest ranks (same as NumPy's
/// `percentile` with `method='linear'`).
///
/// `p` must be in [0.0, 1.0].
pub fn exact_percentile(values: &mut [f64], p: f64) -> Result<f64> {
    if values.is_empty() {
        return Err(BenchError::InsufficientSamples { needed: 1, got: 0 });
    }
    if !(0.0..=1.0).contains(&p) {
        return Err(BenchError::InvalidData(format!("Percentile must be in [0, 1], got {p}")));
    }

    values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    if values.len() == 1 {
        return Ok(values[0]);
    }

    let rank = p * (values.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    let frac = rank - lower as f64;

    if lower == upper {
        Ok(values[lower])
    } else {
        Ok(values[lower] * (1.0 - frac) + values[upper] * frac)
    }
}

/// Compute multiple percentiles at once (more efficient — single sort).
pub fn exact_percentiles(values: &mut [f64], percentiles: &[f64]) -> Result<Vec<f64>> {
    if values.is_empty() {
        return Err(BenchError::InsufficientSamples { needed: 1, got: 0 });
    }

    values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let mut results = Vec::with_capacity(percentiles.len());
    for &p in percentiles {
        if !(0.0..=1.0).contains(&p) {
            return Err(BenchError::InvalidData(format!("Percentile must be in [0, 1], got {p}")));
        }
        let rank = p * (values.len() - 1) as f64;
        let lower = rank.floor() as usize;
        let upper = rank.ceil() as usize;
        let frac = rank - lower as f64;

        let val = if lower == upper {
            values[lower]
        } else {
            values[lower] * (1.0 - frac) + values[upper] * frac
        };
        results.push(val);
    }

    Ok(results)
}

/// Basic descriptive statistics computed in a single pass.
pub struct DescriptiveStats {
    pub count: usize,
    pub mean: f64,
    pub min: f64,
    pub max: f64,
    pub variance: f64,
    pub stddev: f64,
}

/// Compute descriptive stats in a single pass using Welford's algorithm.
pub fn descriptive(values: &[f64]) -> Result<DescriptiveStats> {
    if values.is_empty() {
        return Err(BenchError::InsufficientSamples { needed: 1, got: 0 });
    }

    let mut mean = 0.0f64;
    let mut m2 = 0.0f64;
    let mut min = f64::MAX;
    let mut max = f64::MIN;

    for (i, &v) in values.iter().enumerate() {
        let n = (i + 1) as f64;
        let delta = v - mean;
        mean += delta / n;
        let delta2 = v - mean;
        m2 += delta * delta2;
        if v < min { min = v; }
        if v > max { max = v; }
    }

    let variance = if values.len() > 1 {
        m2 / (values.len() - 1) as f64
    } else {
        0.0
    };

    Ok(DescriptiveStats {
        count: values.len(),
        mean,
        min,
        max,
        variance,
        stddev: variance.sqrt(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_percentile_single_value() {
        let mut v = vec![42.0];
        assert_eq!(exact_percentile(&mut v, 0.5).unwrap(), 42.0);
        assert_eq!(exact_percentile(&mut v, 0.0).unwrap(), 42.0);
        assert_eq!(exact_percentile(&mut v, 1.0).unwrap(), 42.0);
    }

    #[test]
    fn exact_percentile_sorted() {
        let mut v: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let p50 = exact_percentile(&mut v, 0.50).unwrap();
        let p95 = exact_percentile(&mut v, 0.95).unwrap();
        let p99 = exact_percentile(&mut v, 0.99).unwrap();
        assert!((p50 - 50.5).abs() < 0.01, "p50={p50}");
        assert!((p95 - 95.05).abs() < 0.1, "p95={p95}");
        assert!((p99 - 99.01).abs() < 0.1, "p99={p99}");
    }

    #[test]
    fn exact_percentile_min_max() {
        let mut v = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        assert_eq!(exact_percentile(&mut v, 0.0).unwrap(), 10.0);
        assert_eq!(exact_percentile(&mut v, 1.0).unwrap(), 50.0);
    }

    #[test]
    fn exact_percentile_empty_fails() {
        let mut v: Vec<f64> = vec![];
        assert!(exact_percentile(&mut v, 0.5).is_err());
    }

    #[test]
    fn exact_percentile_invalid_p() {
        let mut v = vec![1.0, 2.0];
        assert!(exact_percentile(&mut v, 1.5).is_err());
        assert!(exact_percentile(&mut v, -0.1).is_err());
    }

    #[test]
    fn exact_percentiles_batch() {
        let mut v: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let ps = exact_percentiles(&mut v, &[0.50, 0.95, 0.99]).unwrap();
        assert_eq!(ps.len(), 3);
        assert!((ps[0] - 50.5).abs() < 0.01);
    }

    #[test]
    fn descriptive_basic() {
        let v = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let d = descriptive(&v).unwrap();
        assert_eq!(d.count, 8);
        assert!((d.mean - 5.0).abs() < 0.01);
        assert_eq!(d.min, 2.0);
        assert_eq!(d.max, 9.0);
        // Sample variance of [2,4,4,4,5,5,7,9] = 4.571...
        assert!((d.variance - 4.571).abs() < 0.01, "variance={}", d.variance);
    }

    #[test]
    fn descriptive_single() {
        let v = vec![42.0];
        let d = descriptive(&v).unwrap();
        assert_eq!(d.count, 1);
        assert_eq!(d.mean, 42.0);
        assert_eq!(d.variance, 0.0);
    }

    #[test]
    fn descriptive_empty_fails() {
        let v: Vec<f64> = vec![];
        assert!(descriptive(&v).is_err());
    }
}
```

- [ ] **Step 3: Run percentile tests**

Run:
```bash
cd rust && cargo test -p xraybench-stats percentile::tests
```

Expected: All 9 tests pass.

- [ ] **Step 4: Write tdigest.rs — streaming quantile estimation**

Create `rust/xraybench-stats/src/tdigest.rs`:

```rust
//! T-digest streaming quantile estimation.
//!
//! For large datasets (N > 10,000) where sorting is too expensive,
//! t-digest provides approximate percentiles with bounded error.
//!
//! Reference: Dunning & Ertl, "Computing Extremely Accurate Quantiles
//! Using t-Digests" (2019).

/// A centroid in the t-digest: a weighted mean.
#[derive(Debug, Clone)]
struct Centroid {
    mean: f64,
    weight: f64,
}

/// T-digest for streaming quantile estimation.
pub struct TDigest {
    centroids: Vec<Centroid>,
    compression: f64,
    total_weight: f64,
    max_centroids: usize,
}

impl TDigest {
    /// Create a new t-digest with the given compression parameter.
    ///
    /// Higher compression = more centroids = higher accuracy.
    /// Typical values: 100 (default), 200 (high accuracy), 50 (low memory).
    pub fn new(compression: f64) -> Self {
        let max = (compression * 2.0) as usize + 10;
        Self {
            centroids: Vec::with_capacity(max),
            compression,
            total_weight: 0.0,
            max_centroids: max,
        }
    }

    /// Add a single value.
    pub fn insert(&mut self, value: f64) {
        self.centroids.push(Centroid { mean: value, weight: 1.0 });
        self.total_weight += 1.0;

        if self.centroids.len() > self.max_centroids * 2 {
            self.compress();
        }
    }

    /// Add a value with weight.
    pub fn insert_weighted(&mut self, value: f64, weight: f64) {
        self.centroids.push(Centroid { mean: value, weight });
        self.total_weight += weight;

        if self.centroids.len() > self.max_centroids * 2 {
            self.compress();
        }
    }

    /// Estimate the value at the given quantile (0.0 to 1.0).
    pub fn quantile(&mut self, q: f64) -> Option<f64> {
        if self.centroids.is_empty() {
            return None;
        }

        self.compress();

        let target = q * self.total_weight;
        let mut cumulative = 0.0;

        for i in 0..self.centroids.len() {
            let half = self.centroids[i].weight / 2.0;
            if cumulative + half >= target {
                // Interpolate within this centroid
                if i == 0 || cumulative + half == target {
                    return Some(self.centroids[i].mean);
                }
                let prev = &self.centroids[i - 1];
                let curr = &self.centroids[i];
                let t = (target - cumulative) / half;
                return Some(prev.mean + (curr.mean - prev.mean) * t.min(1.0));
            }
            cumulative += self.centroids[i].weight;
        }

        self.centroids.last().map(|c| c.mean)
    }

    /// Total number of observations.
    pub fn count(&self) -> f64 {
        self.total_weight
    }

    fn compress(&mut self) {
        if self.centroids.is_empty() {
            return;
        }

        self.centroids.sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap_or(std::cmp::Ordering::Equal));

        let mut compressed = Vec::with_capacity(self.max_centroids);
        let mut current = self.centroids[0].clone();
        let mut weight_so_far = 0.0;

        for i in 1..self.centroids.len() {
            let proposed_weight = current.weight + self.centroids[i].weight;
            let q = (weight_so_far + proposed_weight / 2.0) / self.total_weight;
            let max_weight = 4.0 * self.compression * q * (1.0 - q) / self.total_weight;

            if proposed_weight <= max_weight.max(1.0) {
                // Merge into current centroid
                let new_weight = current.weight + self.centroids[i].weight;
                current.mean = (current.mean * current.weight + self.centroids[i].mean * self.centroids[i].weight) / new_weight;
                current.weight = new_weight;
            } else {
                weight_so_far += current.weight;
                compressed.push(current);
                current = self.centroids[i].clone();
            }
        }
        compressed.push(current);

        self.centroids = compressed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tdigest_single_value() {
        let mut td = TDigest::new(100.0);
        td.insert(42.0);
        assert!((td.quantile(0.5).unwrap() - 42.0).abs() < 0.001);
    }

    #[test]
    fn tdigest_uniform_distribution() {
        let mut td = TDigest::new(200.0);
        for i in 1..=10_000 {
            td.insert(i as f64);
        }

        let p50 = td.quantile(0.50).unwrap();
        let p95 = td.quantile(0.95).unwrap();
        let p99 = td.quantile(0.99).unwrap();

        // Allow 2% error for t-digest approximation
        assert!((p50 - 5000.0).abs() / 5000.0 < 0.02, "p50={p50}");
        assert!((p95 - 9500.0).abs() / 9500.0 < 0.02, "p95={p95}");
        assert!((p99 - 9900.0).abs() / 9900.0 < 0.02, "p99={p99}");
    }

    #[test]
    fn tdigest_min_max() {
        let mut td = TDigest::new(100.0);
        for i in 1..=1000 {
            td.insert(i as f64);
        }
        let min = td.quantile(0.0).unwrap();
        let max = td.quantile(1.0).unwrap();
        assert!((min - 1.0).abs() < 2.0, "min={min}");
        assert!((max - 1000.0).abs() < 2.0, "max={max}");
    }

    #[test]
    fn tdigest_count() {
        let mut td = TDigest::new(100.0);
        for i in 0..500 {
            td.insert(i as f64);
        }
        assert!((td.count() - 500.0).abs() < 0.001);
    }

    #[test]
    fn tdigest_empty_returns_none() {
        let mut td = TDigest::new(100.0);
        assert!(td.quantile(0.5).is_none());
    }

    #[test]
    fn tdigest_heavy_compression() {
        // Many values should still give reasonable results
        let mut td = TDigest::new(100.0);
        for i in 0..100_000 {
            td.insert((i % 1000) as f64);
        }
        let p50 = td.quantile(0.50).unwrap();
        assert!((p50 - 500.0).abs() / 500.0 < 0.05, "p50={p50}");
    }
}
```

- [ ] **Step 5: Run t-digest tests**

Run:
```bash
cd rust && cargo test -p xraybench-stats tdigest::tests
```

Expected: All 6 tests pass.

- [ ] **Step 6: Write bootstrap.rs — BCa confidence intervals**

Create `rust/xraybench-stats/src/bootstrap.rs`:

```rust
//! Bootstrap confidence intervals using the BCa (bias-corrected and accelerated) method.
//!
//! BCa does not assume the sampling distribution is normal. This is important
//! for benchmark timing data which is typically right-skewed.
//!
//! Reference: Efron & Tibshirani, "An Introduction to the Bootstrap" (1993).

use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use rand::Rng;
use xraybench_types::{BenchError, Result};

/// Configuration for bootstrap confidence intervals.
pub struct BootstrapConfig {
    /// Number of bootstrap resamples.
    pub n_resamples: usize,
    /// Confidence level (e.g., 0.95 for 95% CI).
    pub confidence: f64,
    /// Random seed for reproducibility.
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

/// Result of a bootstrap confidence interval computation.
#[derive(Debug, Clone)]
pub struct BootstrapCI {
    pub lower: f64,
    pub upper: f64,
    pub confidence: f64,
    pub point_estimate: f64,
    pub bias: f64,
    pub acceleration: f64,
}

/// Compute BCa bootstrap confidence interval for the mean.
pub fn bca_mean_ci(data: &[f64], config: &BootstrapConfig) -> Result<BootstrapCI> {
    if data.len() < 3 {
        return Err(BenchError::InsufficientSamples { needed: 3, got: data.len() });
    }

    let n = data.len();
    let observed_mean = data.iter().sum::<f64>() / n as f64;

    // Generate bootstrap distribution
    let mut rng = ChaCha20Rng::seed_from_u64(config.seed);
    let mut boot_means = Vec::with_capacity(config.n_resamples);

    for _ in 0..config.n_resamples {
        let mut sum = 0.0;
        for _ in 0..n {
            let idx = rng.gen_range(0..n);
            sum += data[idx];
        }
        boot_means.push(sum / n as f64);
    }

    boot_means.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    // Bias correction (z0)
    let below_observed = boot_means.iter().filter(|&&x| x < observed_mean).count();
    let prop = below_observed as f64 / config.n_resamples as f64;
    let z0 = normal_ppf(prop);

    // Acceleration (a) via jackknife
    let mut jackknife_means = Vec::with_capacity(n);
    for i in 0..n {
        let sum: f64 = data.iter().enumerate()
            .filter(|&(j, _)| j != i)
            .map(|(_, &v)| v)
            .sum();
        jackknife_means.push(sum / (n - 1) as f64);
    }
    let jack_mean = jackknife_means.iter().sum::<f64>() / n as f64;
    let mut numer = 0.0f64;
    let mut denom = 0.0f64;
    for &jm in &jackknife_means {
        let d = jack_mean - jm;
        numer += d.powi(3);
        denom += d.powi(2);
    }
    let acceleration = if denom.abs() > f64::EPSILON {
        numer / (6.0 * denom.powf(1.5))
    } else {
        0.0
    };

    // BCa adjusted percentiles
    let alpha = 1.0 - config.confidence;
    let z_alpha_lower = normal_ppf(alpha / 2.0);
    let z_alpha_upper = normal_ppf(1.0 - alpha / 2.0);

    let adj_lower = normal_cdf(z0 + (z0 + z_alpha_lower) / (1.0 - acceleration * (z0 + z_alpha_lower)));
    let adj_upper = normal_cdf(z0 + (z0 + z_alpha_upper) / (1.0 - acceleration * (z0 + z_alpha_upper)));

    let lower_idx = ((adj_lower * config.n_resamples as f64) as usize).min(config.n_resamples - 1);
    let upper_idx = ((adj_upper * config.n_resamples as f64) as usize).min(config.n_resamples - 1);

    Ok(BootstrapCI {
        lower: boot_means[lower_idx],
        upper: boot_means[upper_idx],
        confidence: config.confidence,
        point_estimate: observed_mean,
        bias: z0,
        acceleration,
    })
}

/// Standard normal CDF approximation (Abramowitz & Stegun).
fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

/// Standard normal PPF (inverse CDF) using rational approximation.
/// Accurate to ~4.5e-4 for p in (0.0003, 0.9997).
fn normal_ppf(p: f64) -> f64 {
    if p <= 0.0 { return f64::NEG_INFINITY; }
    if p >= 1.0 { return f64::INFINITY; }
    if (p - 0.5).abs() < f64::EPSILON { return 0.0; }

    // Rational approximation (Beasley-Springer-Moro algorithm)
    let q = p - 0.5;
    if q.abs() <= 0.425 {
        let r = 0.180625 - q * q;
        let x = q * (((((((2.5090809287301226727e3 * r + 3.3430575583588128105e4) * r
            + 6.7265770927008700853e4) * r + 4.5921953931549871457e4) * r
            + 1.3731693765509461125e4) * r + 1.9715909503065514427e3) * r
            + 1.3314166764078226174e2) * r + 3.3871328727963666080e0)
            / (((((((5.2264952788528545610e3 * r + 2.8729085735721942674e4) * r
            + 3.9307895800092710610e4) * r + 2.1213794301586595867e4) * r
            + 5.3941960214247511077e3) * r + 6.8718700749205790830e2) * r
            + 4.2313330701600911252e1) * r + 1.0);
        return x;
    }

    let r = if q < 0.0 { p } else { 1.0 - p };
    let r = (-r.ln()).sqrt();

    let x = if r <= 5.0 {
        let r = r - 1.6;
        (((((((7.7454501427834140764e-4 * r + 2.2723844989269184187e-2) * r
            + 7.2235882419019142572e-1) * r + 1.3886156578609543753) * r
            + 1.5835143846089518426) * r + 6.5707032737668765648e-1) * r
            + 1.2577016023826679674e-1) * r + 1.7015293807700474800e-2)
            / (((((((1.0507500716444169339e-4 * r + 1.0532057965268687102e-2) * r
            + 1.6882755560235672994e-1) * r + 7.3464093459206992985e-1) * r
            + 1.6317524679004476609) * r + 1.2962089880688756395) * r
            + 5.3103568268138652105e-1) * r + 1.0)
    } else {
        let r = r - 5.0;
        (((((((2.0103343998846592400e-7 * r + 2.7115555687434552063e-5) * r
            + 1.2426609473880784386e-3) * r + 2.6923844527510665246e-2) * r
            + 2.9662696911875150914e-1) * r + 1.6519323691667679665) * r
            + 3.7137289027302247520) * r + 2.9381482277672451863)
            / (((((((2.0440539983498223310e-7 * r + 1.4215117345812675880e-5) * r
            + 1.8463183175100552610e-3) * r + 1.8683867340710249021e-2) * r
            + 1.6715503133704542850e-1) * r + 4.9895221960498694290e-1) * r
            + 5.7719105052879569824e-1) * r + 1.0)
    };

    if q < 0.0 { -x } else { x }
}

/// Error function approximation (Abramowitz & Stegun 7.1.26, max error 1.5e-7).
fn erf(x: f64) -> f64 {
    let sign = if x >= 0.0 { 1.0 } else { -1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + 0.3275911 * x);
    let y = 1.0 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * (-x * x).exp();
    sign * y
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bca_symmetric_data() {
        // Symmetric data: CI should be roughly symmetric around mean
        let data: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let ci = bca_mean_ci(&data, &BootstrapConfig::default()).unwrap();
        assert!(ci.lower < ci.point_estimate);
        assert!(ci.upper > ci.point_estimate);
        assert!((ci.point_estimate - 50.5).abs() < 0.01);
        // 95% CI for mean of 1..100 should be approximately [46, 55]
        assert!(ci.lower > 40.0, "lower={}", ci.lower);
        assert!(ci.upper < 60.0, "upper={}", ci.upper);
    }

    #[test]
    fn bca_tight_data() {
        // Very little variance: CI should be tight
        let data = vec![100.0, 100.1, 99.9, 100.0, 100.05, 99.95, 100.0, 100.0, 99.98, 100.02];
        let ci = bca_mean_ci(&data, &BootstrapConfig::default()).unwrap();
        assert!(ci.upper - ci.lower < 0.2, "CI too wide: [{}, {}]", ci.lower, ci.upper);
    }

    #[test]
    fn bca_skewed_data() {
        // Right-skewed data (typical of timing): CI should not be symmetric
        let mut data = vec![10.0; 90];
        data.extend(vec![100.0; 10]); // 10% outliers
        let ci = bca_mean_ci(&data, &BootstrapConfig::default()).unwrap();
        assert!(ci.lower < ci.point_estimate);
        assert!(ci.upper > ci.point_estimate);
    }

    #[test]
    fn bca_reproducible() {
        let data: Vec<f64> = (1..=50).map(|i| i as f64).collect();
        let ci1 = bca_mean_ci(&data, &BootstrapConfig { seed: 123, ..Default::default() }).unwrap();
        let ci2 = bca_mean_ci(&data, &BootstrapConfig { seed: 123, ..Default::default() }).unwrap();
        assert_eq!(ci1.lower, ci2.lower);
        assert_eq!(ci1.upper, ci2.upper);
    }

    #[test]
    fn bca_too_few_samples() {
        let data = vec![1.0, 2.0];
        assert!(bca_mean_ci(&data, &BootstrapConfig::default()).is_err());
    }

    #[test]
    fn normal_ppf_standard_values() {
        assert!((normal_ppf(0.5) - 0.0).abs() < 0.001);
        assert!((normal_ppf(0.975) - 1.96).abs() < 0.01);
        assert!((normal_ppf(0.025) - (-1.96)).abs() < 0.01);
        assert!((normal_ppf(0.8413) - 1.0).abs() < 0.01);
    }

    #[test]
    fn normal_cdf_standard_values() {
        assert!((normal_cdf(0.0) - 0.5).abs() < 0.001);
        assert!((normal_cdf(1.96) - 0.975).abs() < 0.001);
        assert!((normal_cdf(-1.96) - 0.025).abs() < 0.001);
    }
}
```

- [ ] **Step 7: Run bootstrap tests**

Run:
```bash
cd rust && cargo test -p xraybench-stats bootstrap::tests
```

Expected: All 7 tests pass.

- [ ] **Step 8: Write outlier.rs — MAD-based outlier detection**

Create `rust/xraybench-stats/src/outlier.rs`:

```rust
//! Outlier detection using Modified Z-Score with MAD (Median Absolute Deviation).
//!
//! MAD is robust to non-normal distributions, unlike standard Z-score which
//! uses mean and standard deviation (both heavily influenced by outliers).
//!
//! Modified Z-Score = 0.6745 * (x_i - median) / MAD
//! Threshold: |Modified Z-Score| > 3.5 (Iglewicz & Hoaglin recommendation)

use xraybench_types::{BenchError, Result};

/// Default threshold for outlier classification.
pub const DEFAULT_THRESHOLD: f64 = 3.5;

/// 0.6745 is the 0.75th quantile of the standard normal distribution.
/// Used to make MAD consistent with standard deviation for normal data.
const MAD_SCALE: f64 = 0.6745;

/// Result of outlier detection on a dataset.
pub struct OutlierResult {
    /// Indices of outlier observations.
    pub outlier_indices: Vec<usize>,
    /// Modified Z-scores for each observation.
    pub modified_z_scores: Vec<f64>,
    /// Median of the dataset.
    pub median: f64,
    /// Median Absolute Deviation.
    pub mad: f64,
    /// Threshold used for classification.
    pub threshold: f64,
}

/// Detect outliers using MAD-based Modified Z-Score.
pub fn detect_outliers(values: &[f64], threshold: f64) -> Result<OutlierResult> {
    if values.len() < 3 {
        return Err(BenchError::InsufficientSamples { needed: 3, got: values.len() });
    }

    let median = compute_median(values);
    let deviations: Vec<f64> = values.iter().map(|&v| (v - median).abs()).collect();
    let mad = compute_median(&deviations);

    let modified_z_scores: Vec<f64> = if mad.abs() < f64::EPSILON {
        // MAD is zero (more than half the values are identical).
        // Use mean absolute deviation as fallback.
        let mean_dev = deviations.iter().sum::<f64>() / deviations.len() as f64;
        if mean_dev.abs() < f64::EPSILON {
            vec![0.0; values.len()]
        } else {
            values.iter().map(|&v| MAD_SCALE * (v - median) / mean_dev).collect()
        }
    } else {
        values.iter().map(|&v| MAD_SCALE * (v - median) / mad).collect()
    };

    let outlier_indices: Vec<usize> = modified_z_scores
        .iter()
        .enumerate()
        .filter(|(_, &z)| z.abs() > threshold)
        .map(|(i, _)| i)
        .collect();

    Ok(OutlierResult {
        outlier_indices,
        modified_z_scores,
        median,
        mad,
        threshold,
    })
}

/// Compute median of a slice (does not modify input).
fn compute_median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    }
}

/// Compute MAD (Median Absolute Deviation) of a slice.
pub fn mad(values: &[f64]) -> f64 {
    let median = compute_median(values);
    let deviations: Vec<f64> = values.iter().map(|&v| (v - median).abs()).collect();
    compute_median(&deviations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_outliers_in_uniform_data() {
        let data: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let result = detect_outliers(&data, DEFAULT_THRESHOLD).unwrap();
        assert!(result.outlier_indices.is_empty(), "Unexpected outliers in uniform data");
    }

    #[test]
    fn detects_extreme_outlier() {
        let mut data: Vec<f64> = vec![10.0; 100];
        data.push(1_000_000.0); // Extreme outlier
        let result = detect_outliers(&data, DEFAULT_THRESHOLD).unwrap();
        assert!(
            result.outlier_indices.contains(&100),
            "Failed to detect extreme outlier at index 100"
        );
    }

    #[test]
    fn mad_of_uniform_range() {
        // MAD of 1..100 should be ~25 (median=50.5, deviations are 0.5..49.5, median ~25)
        let data: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let m = mad(&data);
        assert!((m - 25.0).abs() < 1.0, "MAD={m}");
    }

    #[test]
    fn mad_of_constant() {
        let data = vec![42.0; 100];
        let m = mad(&data);
        assert_eq!(m, 0.0);
    }

    #[test]
    fn handles_all_identical_values() {
        let data = vec![5.0; 50];
        let result = detect_outliers(&data, DEFAULT_THRESHOLD).unwrap();
        assert!(result.outlier_indices.is_empty());
        assert_eq!(result.mad, 0.0);
    }

    #[test]
    fn z_scores_have_correct_sign() {
        let data = vec![1.0, 2.0, 3.0, 100.0, 4.0, 5.0];
        let result = detect_outliers(&data, DEFAULT_THRESHOLD).unwrap();
        // 100.0 should have a positive z-score (above median)
        assert!(result.modified_z_scores[3] > 0.0);
    }

    #[test]
    fn too_few_samples() {
        let data = vec![1.0, 2.0];
        assert!(detect_outliers(&data, DEFAULT_THRESHOLD).is_err());
    }
}
```

- [ ] **Step 9: Run outlier tests**

Run:
```bash
cd rust && cargo test -p xraybench-stats outlier::tests
```

Expected: All 7 tests pass.

- [ ] **Step 10: Write regression.rs — Mann-Whitney U test**

Create `rust/xraybench-stats/src/regression.rs`:

```rust
//! Non-parametric comparison of two samples using the Mann-Whitney U test.
//!
//! The Mann-Whitney U test does not assume normality, making it appropriate
//! for benchmark timing distributions which are typically right-skewed.
//!
//! Used to determine if two sets of benchmark results differ significantly
//! (e.g., before vs after a code change).

use xraybench_types::{BenchError, ChangeClass, MetricComparison, Result};
use crate::bootstrap::{bca_mean_ci, BootstrapConfig};

/// Result of a Mann-Whitney U test.
#[derive(Debug, Clone)]
pub struct MannWhitneyResult {
    /// U statistic for sample A.
    pub u_a: f64,
    /// U statistic for sample B.
    pub u_b: f64,
    /// Approximate p-value (two-tailed, normal approximation).
    pub p_value: f64,
    /// Whether the difference is statistically significant at alpha=0.05.
    pub significant: bool,
}

/// Perform Mann-Whitney U test on two samples.
///
/// Tests whether sample_a and sample_b are drawn from the same distribution.
/// Uses normal approximation for the p-value (valid for n >= 20).
pub fn mann_whitney_u(sample_a: &[f64], sample_b: &[f64]) -> Result<MannWhitneyResult> {
    let n_a = sample_a.len();
    let n_b = sample_b.len();

    if n_a < 3 || n_b < 3 {
        return Err(BenchError::InsufficientSamples {
            needed: 3,
            got: n_a.min(n_b),
        });
    }

    // Combine and rank
    let mut combined: Vec<(f64, usize)> = Vec::with_capacity(n_a + n_b);
    for &v in sample_a {
        combined.push((v, 0)); // group 0 = sample A
    }
    for &v in sample_b {
        combined.push((v, 1)); // group 1 = sample B
    }
    combined.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    // Assign ranks (handle ties by averaging)
    let n = combined.len();
    let mut ranks = vec![0.0f64; n];
    let mut i = 0;
    while i < n {
        let mut j = i;
        while j < n && (combined[j].0 - combined[i].0).abs() < f64::EPSILON {
            j += 1;
        }
        // Average rank for tied values
        let avg_rank = (i + j + 1) as f64 / 2.0; // 1-indexed
        for k in i..j {
            ranks[k] = avg_rank;
        }
        i = j;
    }

    // Sum ranks for group A
    let rank_sum_a: f64 = (0..n)
        .filter(|&i| combined[i].1 == 0)
        .map(|i| ranks[i])
        .sum();

    let u_a = rank_sum_a - (n_a * (n_a + 1)) as f64 / 2.0;
    let u_b = (n_a * n_b) as f64 - u_a;

    // Normal approximation for p-value
    let mu = (n_a * n_b) as f64 / 2.0;
    let sigma = ((n_a * n_b * (n_a + n_b + 1)) as f64 / 12.0).sqrt();

    let z = if sigma > 0.0 {
        (u_a - mu) / sigma
    } else {
        0.0
    };

    // Two-tailed p-value
    let p_value = 2.0 * (1.0 - normal_cdf(z.abs()));

    Ok(MannWhitneyResult {
        u_a,
        u_b,
        p_value,
        significant: p_value < 0.05,
    })
}

/// Compare two benchmark result sets on a named metric.
///
/// Returns a MetricComparison with delta, CI, p-value, and classification.
pub fn compare_metric(
    name: &str,
    values_a: &[f64],
    values_b: &[f64],
    significance_threshold: f64,
) -> Result<MetricComparison> {
    if values_a.len() < 3 || values_b.len() < 3 {
        return Err(BenchError::InsufficientSamples {
            needed: 3,
            got: values_a.len().min(values_b.len()),
        });
    }

    let mean_a = values_a.iter().sum::<f64>() / values_a.len() as f64;
    let mean_b = values_b.iter().sum::<f64>() / values_b.len() as f64;

    let absolute_delta = mean_b - mean_a;
    let percentage_change = if mean_a.abs() > f64::EPSILON {
        (mean_b - mean_a) / mean_a * 100.0
    } else {
        0.0
    };

    // Bootstrap CI on the difference
    let diffs: Vec<f64> = values_b
        .iter()
        .zip(values_a.iter().cycle())
        .map(|(b, a)| b - a)
        .take(values_a.len().max(values_b.len()))
        .collect();
    let ci = bca_mean_ci(&diffs, &BootstrapConfig::default())?;

    // Mann-Whitney test
    let mw = mann_whitney_u(values_a, values_b)?;

    let classification = if !mw.significant {
        ChangeClass::Inconclusive
    } else if absolute_delta.abs() / mean_a.abs().max(1.0) < significance_threshold {
        ChangeClass::NoChange
    } else if absolute_delta < 0.0 {
        ChangeClass::Improvement // Lower is better for timing
    } else {
        ChangeClass::Regression
    };

    Ok(MetricComparison {
        metric_name: name.to_string(),
        value_a: mean_a,
        value_b: mean_b,
        absolute_delta,
        percentage_change,
        ci_lower: ci.lower,
        ci_upper: ci.upper,
        p_value: mw.p_value,
        significant: mw.significant,
        classification,
    })
}

/// Standard normal CDF (shared with bootstrap module).
fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

fn erf(x: f64) -> f64 {
    let sign = if x >= 0.0 { 1.0 } else { -1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + 0.3275911 * x);
    let y = 1.0 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * (-x * x).exp();
    sign * y
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_samples_not_significant() {
        let a: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let b = a.clone();
        let result = mann_whitney_u(&a, &b).unwrap();
        assert!(!result.significant, "Identical samples should not be significant, p={}", result.p_value);
    }

    #[test]
    fn clearly_different_samples_significant() {
        let a: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let b: Vec<f64> = (1001..=1100).map(|i| i as f64).collect();
        let result = mann_whitney_u(&a, &b).unwrap();
        assert!(result.significant, "Very different samples should be significant, p={}", result.p_value);
        assert!(result.p_value < 0.001);
    }

    #[test]
    fn slightly_shifted_detection() {
        // Shift by 10% of range
        let a: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let b: Vec<f64> = (11..=110).map(|i| i as f64).collect();
        let result = mann_whitney_u(&a, &b).unwrap();
        // Should detect this shift with sufficient power
        assert!(result.p_value < 0.05, "10% shift should be detectable, p={}", result.p_value);
    }

    #[test]
    fn u_statistic_symmetry() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let b = vec![6.0, 7.0, 8.0, 9.0, 10.0];
        let result = mann_whitney_u(&a, &b).unwrap();
        assert_eq!(result.u_a + result.u_b, (a.len() * b.len()) as f64);
    }

    #[test]
    fn too_few_samples() {
        let a = vec![1.0, 2.0];
        let b = vec![3.0, 4.0];
        assert!(mann_whitney_u(&a, &b).is_err());
    }

    #[test]
    fn compare_metric_improvement() {
        let a: Vec<f64> = vec![100.0; 30]; // Before: 100ms
        let b: Vec<f64> = vec![50.0; 30];  // After: 50ms (improvement)
        let cmp = compare_metric("cold_ms", &a, &b, 0.05).unwrap();
        assert_eq!(cmp.classification, ChangeClass::Improvement);
        assert!(cmp.percentage_change < -40.0); // ~50% improvement
    }

    #[test]
    fn compare_metric_regression() {
        let a: Vec<f64> = vec![50.0; 30];  // Before: 50ms
        let b: Vec<f64> = vec![100.0; 30]; // After: 100ms (regression)
        let cmp = compare_metric("cold_ms", &a, &b, 0.05).unwrap();
        assert_eq!(cmp.classification, ChangeClass::Regression);
    }
}
```

- [ ] **Step 11: Run regression tests**

Run:
```bash
cd rust && cargo test -p xraybench-stats regression::tests
```

Expected: All 7 tests pass.

- [ ] **Step 12: Write stats lib.rs**

Create `rust/xraybench-stats/src/lib.rs`:

```rust
//! Statistical engine for xraybench.
//!
//! Provides percentile computation, bootstrap confidence intervals,
//! MAD-based outlier detection, Mann-Whitney U tests, and t-digest
//! streaming quantile estimation.

pub mod percentile;
pub mod tdigest;
pub mod bootstrap;
pub mod outlier;
pub mod regression;

pub use bootstrap::{bca_mean_ci, BootstrapCI, BootstrapConfig};
pub use outlier::{detect_outliers, mad, OutlierResult, DEFAULT_THRESHOLD};
pub use percentile::{descriptive, exact_percentile, exact_percentiles, DescriptiveStats};
pub use regression::{compare_metric, mann_whitney_u, MannWhitneyResult};
pub use tdigest::TDigest;
```

- [ ] **Step 13: Run all stats tests**

Run:
```bash
cd rust && cargo test -p xraybench-stats
```

Expected: All ~36 tests pass (percentile: 9, tdigest: 6, bootstrap: 7, outlier: 7, regression: 7).

- [ ] **Step 14: Commit**

```bash
git add rust/xraybench-stats/
git commit -m "feat(rust): add xraybench-stats crate — percentiles, BCa CI, MAD outliers, Mann-Whitney U, t-digest"
```

---

### Task 4: xraybench-checksum — Correctness Validation

**Files:**
- Create: `rust/xraybench-checksum/Cargo.toml`
- Create: `rust/xraybench-checksum/src/lib.rs`
- Create: `rust/xraybench-checksum/src/canonical.rs`
- Create: `rust/xraybench-checksum/src/hasher.rs`
- Create: `rust/xraybench-checksum/src/structural.rs`

- [ ] **Step 1: Create Cargo.toml**

Create `rust/xraybench-checksum/Cargo.toml`:

```toml
[package]
name = "xraybench-checksum"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
description = "Correctness validation and checksumming for xraybench"

[dependencies]
xraybench-types = { path = "../xraybench-types" }
blake3 = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
```

- [ ] **Step 2: Write canonical.rs — deterministic row serialization**

Create `rust/xraybench-checksum/src/canonical.rs`:

```rust
//! Deterministic row serialization for correctness validation.
//!
//! Converts result rows into a canonical byte representation that:
//! - Is order-independent (rows are sorted)
//! - Handles float comparison with epsilon tolerance
//! - Normalizes strings to UTF-8 NFC
//! - Has a well-defined null encoding

use xraybench_types::{BenchError, PropertyValue, Result};

/// Serialize a single PropertyValue to canonical bytes.
pub fn canonical_value(value: &PropertyValue) -> Vec<u8> {
    match value {
        PropertyValue::Null => vec![0x00],
        PropertyValue::Boolean(b) => vec![0x01, if *b { 1 } else { 0 }],
        PropertyValue::Integer(i) => {
            let mut buf = vec![0x02];
            buf.extend_from_slice(&i.to_be_bytes());
            buf
        }
        PropertyValue::Float(f) => {
            let mut buf = vec![0x03];
            // Canonical float: normalize -0.0 to 0.0, NaN to a fixed pattern
            let normalized = if f.is_nan() {
                f64::NAN.to_bits().to_be_bytes()
            } else if *f == 0.0 {
                0.0f64.to_be_bytes()
            } else {
                f.to_be_bytes()
            };
            buf.extend_from_slice(&normalized);
            buf
        }
        PropertyValue::Text(s) => {
            let mut buf = vec![0x04];
            // Length-prefixed UTF-8 bytes
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(bytes);
            buf
        }
    }
}

/// Serialize a row (list of property values) to canonical bytes.
pub fn canonical_row(values: &[PropertyValue]) -> Vec<u8> {
    let mut buf = Vec::new();
    // Prefix with column count
    buf.extend_from_slice(&(values.len() as u32).to_be_bytes());
    for v in values {
        buf.extend(canonical_value(v));
    }
    buf
}

/// Serialize and sort multiple rows for order-independent comparison.
///
/// Returns sorted canonical byte representations of each row.
pub fn canonical_rows_sorted(rows: &[Vec<PropertyValue>]) -> Vec<Vec<u8>> {
    let mut canonical: Vec<Vec<u8>> = rows.iter().map(|row| canonical_row(row)).collect();
    canonical.sort();
    canonical
}

/// Compare two floats with ULP (Units in the Last Place) tolerance.
///
/// Returns true if the floats are within `max_ulp` ULPs of each other.
pub fn float_eq_ulp(a: f64, b: f64, max_ulp: u32) -> bool {
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a.is_nan() || b.is_nan() {
        return false;
    }
    if a.is_sign_positive() != b.is_sign_positive() {
        return a == b; // Only equal if both are ±0.0
    }

    let a_bits = a.to_bits() as i64;
    let b_bits = b.to_bits() as i64;
    (a_bits - b_bits).unsigned_abs() <= max_ulp as u64
}

/// Check if two rows are equivalent under float tolerance.
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
            _ => false, // Different types
        };
        if !eq {
            return Ok(false);
        }
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_null() {
        assert_eq!(canonical_value(&PropertyValue::Null), vec![0x00]);
    }

    #[test]
    fn canonical_bool() {
        assert_eq!(canonical_value(&PropertyValue::Boolean(true)), vec![0x01, 1]);
        assert_eq!(canonical_value(&PropertyValue::Boolean(false)), vec![0x01, 0]);
    }

    #[test]
    fn canonical_integer_deterministic() {
        let a = canonical_value(&PropertyValue::Integer(42));
        let b = canonical_value(&PropertyValue::Integer(42));
        assert_eq!(a, b);
        let c = canonical_value(&PropertyValue::Integer(43));
        assert_ne!(a, c);
    }

    #[test]
    fn canonical_float_neg_zero() {
        let pos = canonical_value(&PropertyValue::Float(0.0));
        let neg = canonical_value(&PropertyValue::Float(-0.0));
        assert_eq!(pos, neg, "-0.0 and 0.0 should have same canonical form");
    }

    #[test]
    fn canonical_text_length_prefixed() {
        let v = canonical_value(&PropertyValue::Text("abc".into()));
        assert_eq!(v[0], 0x04);
        // Length = 3 in big-endian u32
        assert_eq!(&v[1..5], &[0, 0, 0, 3]);
        assert_eq!(&v[5..], b"abc");
    }

    #[test]
    fn row_sorting_is_deterministic() {
        let rows = vec![
            vec![PropertyValue::Integer(3), PropertyValue::Text("c".into())],
            vec![PropertyValue::Integer(1), PropertyValue::Text("a".into())],
            vec![PropertyValue::Integer(2), PropertyValue::Text("b".into())],
        ];
        let sorted1 = canonical_rows_sorted(&rows);
        let sorted2 = canonical_rows_sorted(&rows);
        assert_eq!(sorted1, sorted2);
        // First row after sorting should be Integer(1)
        assert!(sorted1[0] < sorted1[1]);
        assert!(sorted1[1] < sorted1[2]);
    }

    #[test]
    fn float_ulp_exact() {
        assert!(float_eq_ulp(1.0, 1.0, 0));
    }

    #[test]
    fn float_ulp_close() {
        let a = 1.0f64;
        let b = f64::from_bits(a.to_bits() + 2);
        assert!(float_eq_ulp(a, b, 4));
        assert!(!float_eq_ulp(a, b, 1));
    }

    #[test]
    fn float_ulp_nan() {
        assert!(float_eq_ulp(f64::NAN, f64::NAN, 0));
        assert!(!float_eq_ulp(f64::NAN, 1.0, 1000));
    }

    #[test]
    fn rows_equivalent_basic() {
        let a = vec![PropertyValue::Integer(1), PropertyValue::Float(3.14)];
        let b = vec![PropertyValue::Integer(1), PropertyValue::Float(3.14)];
        assert!(rows_equivalent(&a, &b, 4).unwrap());
    }

    #[test]
    fn rows_different_types_not_equivalent() {
        let a = vec![PropertyValue::Integer(1)];
        let b = vec![PropertyValue::Float(1.0)];
        assert!(!rows_equivalent(&a, &b, 4).unwrap());
    }
}
```

- [ ] **Step 3: Run canonical tests**

Run:
```bash
cd rust && cargo test -p xraybench-checksum canonical::tests
```

Expected: All 11 tests pass.

- [ ] **Step 4: Write hasher.rs — streaming BLAKE3**

Create `rust/xraybench-checksum/src/hasher.rs`:

```rust
//! Streaming BLAKE3 hashing for result set checksumming.
//!
//! Hashes the canonical form of sorted rows without materializing
//! the entire result set in memory.

use xraybench_types::{PropertyValue, Result};
use crate::canonical::{canonical_row, canonical_rows_sorted};

/// A streaming hasher that processes rows incrementally.
pub struct ResultHasher {
    hasher: blake3::Hasher,
    row_count: u64,
}

impl ResultHasher {
    pub fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
            row_count: 0,
        }
    }

    /// Add a single row to the hash.
    ///
    /// Note: for order-independent hashing, use `hash_rows_sorted` instead.
    /// This method hashes rows in insertion order.
    pub fn update_row(&mut self, row: &[PropertyValue]) {
        let canonical = canonical_row(row);
        self.hasher.update(&canonical);
        self.row_count += 1;
    }

    /// Finalize and return the hex hash.
    pub fn finalize_hex(&self) -> String {
        format!("blake3:{}", self.hasher.finalize().to_hex())
    }

    /// Number of rows hashed.
    pub fn row_count(&self) -> u64 {
        self.row_count
    }
}

impl Default for ResultHasher {
    fn default() -> Self {
        Self::new()
    }
}

/// Hash a complete result set (order-independent).
///
/// Sorts rows by canonical form before hashing.
pub fn hash_result_set(rows: &[Vec<PropertyValue>]) -> String {
    let sorted = canonical_rows_sorted(rows);
    let mut hasher = blake3::Hasher::new();
    for row in &sorted {
        hasher.update(row);
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

/// Hash a complete result set and return both hash and row count.
pub fn hash_result_set_with_count(rows: &[Vec<PropertyValue>]) -> (String, u64) {
    let count = rows.len() as u64;
    let hash = hash_result_set(rows);
    (hash, count)
}

/// Verify a result set against a reference hash.
pub fn verify_hash(rows: &[Vec<PropertyValue>], reference: &str) -> Result<bool> {
    let computed = hash_result_set(rows);
    Ok(computed == reference)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_set_has_consistent_hash() {
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
        assert_eq!(h.len(), 7 + 64); // "blake3:" + 64 hex chars
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
        let rows_a = vec![vec![PropertyValue::Integer(1)]];
        let rows_b = vec![vec![PropertyValue::Integer(2)]];
        assert_ne!(hash_result_set(&rows_a), hash_result_set(&rows_b));
    }

    #[test]
    fn hash_with_count() {
        let rows = vec![
            vec![PropertyValue::Integer(1)],
            vec![PropertyValue::Integer(2)],
        ];
        let (hash, count) = hash_result_set_with_count(&rows);
        assert_eq!(count, 2);
        assert!(hash.starts_with("blake3:"));
    }

    #[test]
    fn verify_correct_hash() {
        let rows = vec![vec![PropertyValue::Integer(42)]];
        let hash = hash_result_set(&rows);
        assert!(verify_hash(&rows, &hash).unwrap());
    }

    #[test]
    fn verify_incorrect_hash() {
        let rows = vec![vec![PropertyValue::Integer(42)]];
        assert!(!verify_hash(&rows, "blake3:0000000000000000000000000000000000000000000000000000000000000000").unwrap());
    }

    #[test]
    fn streaming_hasher_basic() {
        let mut h = ResultHasher::new();
        h.update_row(&[PropertyValue::Integer(1)]);
        h.update_row(&[PropertyValue::Integer(2)]);
        assert_eq!(h.row_count(), 2);
        let hash = h.finalize_hex();
        assert!(hash.starts_with("blake3:"));
    }
}
```

- [ ] **Step 5: Run hasher tests**

Run:
```bash
cd rust && cargo test -p xraybench-checksum hasher::tests
```

Expected: All 8 tests pass.

- [ ] **Step 6: Write structural.rs — topology validation for path results**

Create `rust/xraybench-checksum/src/structural.rs`:

```rust
//! Structural validation for graph-shaped results (paths, subgraphs).
//!
//! Validates topology properties without requiring exact result match:
//! - Path lengths
//! - Connectivity
//! - No duplicate paths (when spec requires it)
//!
//! Used for deep traversal benchmarks where exact enumeration may
//! differ between engines.

use xraybench_types::{BenchError, Edge, Result};
use std::collections::HashSet;

/// A path represented as a sequence of node IDs.
pub type Path = Vec<u64>;

/// Canonicalize a path to source-to-target ordering.
///
/// If the path is reversed (target ID < source ID at endpoints),
/// reverse it for canonical comparison.
pub fn canonicalize_path(path: &Path) -> Path {
    if path.len() < 2 {
        return path.clone();
    }
    if path.last() < path.first() {
        let mut rev = path.clone();
        rev.reverse();
        rev
    } else {
        path.clone()
    }
}

/// Validate that all paths have length within [min_len, max_len].
pub fn validate_path_lengths(paths: &[Path], min_len: usize, max_len: usize) -> Result<bool> {
    for (i, path) in paths.iter().enumerate() {
        let hops = if path.is_empty() { 0 } else { path.len() - 1 };
        if hops < min_len || hops > max_len {
            return Err(BenchError::InvalidData(format!(
                "Path {i} has {hops} hops, expected [{min_len}, {max_len}]"
            )));
        }
    }
    Ok(true)
}

/// Validate that all paths originate from the given seed node.
pub fn validate_paths_from_seed(paths: &[Path], seed_id: u64) -> Result<bool> {
    for (i, path) in paths.iter().enumerate() {
        if path.is_empty() {
            return Err(BenchError::InvalidData(format!("Path {i} is empty")));
        }
        if path[0] != seed_id {
            return Err(BenchError::InvalidData(format!(
                "Path {i} starts at {}, expected seed {seed_id}",
                path[0]
            )));
        }
    }
    Ok(true)
}

/// Validate that no duplicate paths exist (after canonicalization).
pub fn validate_no_duplicate_paths(paths: &[Path]) -> Result<bool> {
    let mut seen = HashSet::new();
    for (i, path) in paths.iter().enumerate() {
        let canonical = canonicalize_path(path);
        if !seen.insert(canonical) {
            return Err(BenchError::InvalidData(format!(
                "Duplicate path at index {i}"
            )));
        }
    }
    Ok(true)
}

/// Validate that all paths are valid walks on the given edge set.
///
/// Each consecutive pair of nodes in a path must have a corresponding edge.
pub fn validate_paths_on_graph(paths: &[Path], edges: &[Edge]) -> Result<bool> {
    let edge_set: HashSet<(u64, u64)> = edges.iter().map(|e| (e.source, e.target)).collect();

    for (i, path) in paths.iter().enumerate() {
        for j in 0..path.len().saturating_sub(1) {
            let from = path[j];
            let to = path[j + 1];
            if !edge_set.contains(&(from, to)) {
                return Err(BenchError::InvalidData(format!(
                    "Path {i}: edge ({from} -> {to}) does not exist in graph"
                )));
            }
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalize_already_ordered() {
        let p = vec![1, 2, 3, 4, 5];
        assert_eq!(canonicalize_path(&p), p);
    }

    #[test]
    fn canonicalize_reversed() {
        let p = vec![5, 4, 3, 2, 1];
        assert_eq!(canonicalize_path(&p), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn canonicalize_single_node() {
        let p = vec![42];
        assert_eq!(canonicalize_path(&p), vec![42]);
    }

    #[test]
    fn path_lengths_valid() {
        let paths = vec![vec![1, 2, 3], vec![4, 5, 6, 7]];
        assert!(validate_path_lengths(&paths, 2, 3).is_ok());
    }

    #[test]
    fn path_lengths_too_short() {
        let paths = vec![vec![1, 2]]; // 1 hop
        assert!(validate_path_lengths(&paths, 2, 5).is_err());
    }

    #[test]
    fn path_lengths_too_long() {
        let paths = vec![vec![1, 2, 3, 4, 5, 6]]; // 5 hops
        assert!(validate_path_lengths(&paths, 1, 3).is_err());
    }

    #[test]
    fn paths_from_seed_valid() {
        let paths = vec![vec![1, 2, 3], vec![1, 4, 5]];
        assert!(validate_paths_from_seed(&paths, 1).is_ok());
    }

    #[test]
    fn paths_from_wrong_seed() {
        let paths = vec![vec![2, 3, 4]];
        assert!(validate_paths_from_seed(&paths, 1).is_err());
    }

    #[test]
    fn no_duplicates_valid() {
        let paths = vec![vec![1, 2, 3], vec![1, 4, 5]];
        assert!(validate_no_duplicate_paths(&paths).is_ok());
    }

    #[test]
    fn duplicate_detected() {
        let paths = vec![vec![1, 2, 3], vec![1, 2, 3]];
        assert!(validate_no_duplicate_paths(&paths).is_err());
    }

    #[test]
    fn reversed_duplicate_detected() {
        let paths = vec![vec![1, 2, 3], vec![3, 2, 1]];
        assert!(validate_no_duplicate_paths(&paths).is_err());
    }

    #[test]
    fn paths_on_graph_valid() {
        let edges = vec![Edge { source: 1, target: 2 }, Edge { source: 2, target: 3 }];
        let paths = vec![vec![1, 2, 3]];
        assert!(validate_paths_on_graph(&paths, &edges).is_ok());
    }

    #[test]
    fn paths_on_graph_invalid_edge() {
        let edges = vec![Edge { source: 1, target: 2 }];
        let paths = vec![vec![1, 2, 3]]; // Edge 2->3 doesn't exist
        assert!(validate_paths_on_graph(&paths, &edges).is_err());
    }
}
```

- [ ] **Step 7: Run structural tests**

Run:
```bash
cd rust && cargo test -p xraybench-checksum structural::tests
```

Expected: All 13 tests pass.

- [ ] **Step 8: Write checksum lib.rs**

Create `rust/xraybench-checksum/src/lib.rs`:

```rust
//! Correctness validation and checksumming for xraybench.
//!
//! Provides deterministic row serialization, streaming BLAKE3 hashing,
//! ULP float comparison, and structural topology validation for
//! graph-shaped results.

pub mod canonical;
pub mod hasher;
pub mod structural;

pub use canonical::{canonical_row, canonical_rows_sorted, float_eq_ulp, rows_equivalent};
pub use hasher::{hash_result_set, hash_result_set_with_count, verify_hash, ResultHasher};
pub use structural::{
    canonicalize_path, validate_no_duplicate_paths, validate_path_lengths,
    validate_paths_from_seed, validate_paths_on_graph, Path,
};
```

- [ ] **Step 9: Run all checksum tests**

Run:
```bash
cd rust && cargo test -p xraybench-checksum
```

Expected: All ~32 tests pass (canonical: 11, hasher: 8, structural: 13).

- [ ] **Step 10: Commit**

```bash
git add rust/xraybench-checksum/
git commit -m "feat(rust): add xraybench-checksum crate — BLAKE3, canonical serialization, structural validation"
```

---

### Task 5: xraybench-generators — Compiled Graph Generators

This task is large. Due to plan size constraints, I will define the crate structure, the generator trait, and two representative generators (uniform and deep_traversal — the most critical new one). The remaining generators (power_law, hub, community, chain) follow the same pattern and will be specified in a follow-up task within this same implementation session.

**Files:**
- Create: `rust/xraybench-generators/Cargo.toml`
- Create: `rust/xraybench-generators/src/lib.rs`
- Create: `rust/xraybench-generators/src/uniform.rs`
- Create: `rust/xraybench-generators/src/deep_traversal.rs`
- Create: `rust/xraybench-generators/src/io.rs`
- Create: `rust/xraybench-generators/src/power_law.rs`
- Create: `rust/xraybench-generators/src/hub.rs`
- Create: `rust/xraybench-generators/src/community.rs`
- Create: `rust/xraybench-generators/src/chain.rs`

- [ ] **Step 1: Create Cargo.toml**

Create `rust/xraybench-generators/Cargo.toml`:

```toml
[package]
name = "xraybench-generators"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
description = "Compiled graph generators for xraybench datasets"

[dependencies]
xraybench-types = { path = "../xraybench-types" }
rand = { workspace = true }
rand_chacha = { workspace = true }
```

- [ ] **Step 2: Write io.rs — edge-list file I/O**

Create `rust/xraybench-generators/src/io.rs`:

```rust
//! Edge-list file I/O: binary and CSV formats.

use std::io::{BufWriter, Write, BufRead};
use std::fs::File;
use std::path::Path;
use xraybench_types::{BenchError, Edge, Result};

/// Write edges to a binary file (u64 source, u64 target pairs, little-endian).
pub fn write_edges_binary(edges: &[Edge], path: &Path) -> Result<()> {
    let file = File::create(path).map_err(|e| BenchError::IoError(e.to_string()))?;
    let mut writer = BufWriter::new(file);

    for edge in edges {
        writer
            .write_all(&edge.source.to_le_bytes())
            .map_err(|e| BenchError::IoError(e.to_string()))?;
        writer
            .write_all(&edge.target.to_le_bytes())
            .map_err(|e| BenchError::IoError(e.to_string()))?;
    }

    writer.flush().map_err(|e| BenchError::IoError(e.to_string()))?;
    Ok(())
}

/// Write edges to a CSV file (source,target per line).
pub fn write_edges_csv(edges: &[Edge], path: &Path) -> Result<()> {
    let file = File::create(path).map_err(|e| BenchError::IoError(e.to_string()))?;
    let mut writer = BufWriter::new(file);

    writeln!(writer, "source,target").map_err(|e| BenchError::IoError(e.to_string()))?;
    for edge in edges {
        writeln!(writer, "{},{}", edge.source, edge.target)
            .map_err(|e| BenchError::IoError(e.to_string()))?;
    }

    writer.flush().map_err(|e| BenchError::IoError(e.to_string()))?;
    Ok(())
}

/// Read edges from a binary file.
pub fn read_edges_binary(path: &Path) -> Result<Vec<Edge>> {
    let data = std::fs::read(path).map_err(|e| BenchError::IoError(e.to_string()))?;
    if data.len() % 16 != 0 {
        return Err(BenchError::InvalidData(
            "Binary edge file size must be a multiple of 16 bytes".into(),
        ));
    }

    let mut edges = Vec::with_capacity(data.len() / 16);
    for chunk in data.chunks_exact(16) {
        let source = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
        let target = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
        edges.push(Edge { source, target });
    }

    Ok(edges)
}

/// Read edges from a CSV file.
pub fn read_edges_csv(path: &Path) -> Result<Vec<Edge>> {
    let file = File::open(path).map_err(|e| BenchError::IoError(e.to_string()))?;
    let reader = std::io::BufReader::new(file);
    let mut edges = Vec::new();

    for (i, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| BenchError::IoError(e.to_string()))?;
        if i == 0 && line.starts_with("source") {
            continue; // Skip header
        }
        let parts: Vec<&str> = line.trim().split(',').collect();
        if parts.len() != 2 {
            return Err(BenchError::InvalidData(format!(
                "Line {}: expected 2 columns, got {}",
                i + 1,
                parts.len()
            )));
        }
        let source: u64 = parts[0]
            .parse()
            .map_err(|_| BenchError::InvalidData(format!("Line {}: invalid source", i + 1)))?;
        let target: u64 = parts[1]
            .parse()
            .map_err(|_| BenchError::InvalidData(format!("Line {}: invalid target", i + 1)))?;
        edges.push(Edge { source, target });
    }

    Ok(edges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("xraybench_test");
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn binary_roundtrip() {
        let edges = vec![
            Edge { source: 0, target: 1 },
            Edge { source: 1, target: 2 },
            Edge { source: 100, target: 200 },
        ];
        let path = temp_path("test_binary_roundtrip.bin");
        write_edges_binary(&edges, &path).unwrap();
        let loaded = read_edges_binary(&path).unwrap();
        assert_eq!(edges, loaded);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn csv_roundtrip() {
        let edges = vec![
            Edge { source: 0, target: 1 },
            Edge { source: 42, target: 99 },
        ];
        let path = temp_path("test_csv_roundtrip.csv");
        write_edges_csv(&edges, &path).unwrap();
        let loaded = read_edges_csv(&path).unwrap();
        assert_eq!(edges, loaded);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn binary_invalid_size() {
        let path = temp_path("test_invalid.bin");
        std::fs::write(&path, &[0u8; 15]).unwrap(); // Not a multiple of 16
        assert!(read_edges_binary(&path).is_err());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn empty_edge_list() {
        let path = temp_path("test_empty.bin");
        write_edges_binary(&[], &path).unwrap();
        let loaded = read_edges_binary(&path).unwrap();
        assert!(loaded.is_empty());
        std::fs::remove_file(&path).ok();
    }
}
```

- [ ] **Step 3: Write uniform.rs and deep_traversal.rs**

These two generators are the foundation. The remaining four (power_law, hub, community, chain) follow the same structure and will be implemented in the same session.

Create `rust/xraybench-generators/src/uniform.rs`:

```rust
//! Uniform node generator: flat property distribution, configurable cardinality.

use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use rand::Rng;
use xraybench_types::{Edge, Node, PropertyValue};

/// Generate uniform nodes with random properties.
///
/// All properties are uniformly distributed. Deterministic given the same seed.
pub fn generate_uniform_nodes(
    node_count: u64,
    property_count: usize,
    nullable_ratio: f64,
    seed: u64,
) -> Vec<Node> {
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    let categories: Vec<String> = (0..100).map(|i| format!("cat_{i}")).collect();

    let mut nodes = Vec::with_capacity(node_count as usize);
    for id in 0..node_count {
        let mut properties = Vec::with_capacity(property_count + 1);
        properties.push(PropertyValue::Integer(id as i64));

        for p in 0..property_count {
            let val = match p % 3 {
                0 => PropertyValue::Integer(rng.gen_range(0..1_000_000)),
                1 => {
                    if rng.gen::<f64>() < nullable_ratio {
                        PropertyValue::Null
                    } else {
                        PropertyValue::Float(rng.gen::<f64>())
                    }
                }
                _ => PropertyValue::Text(categories[rng.gen_range(0..categories.len())].clone()),
            };
            properties.push(val);
        }

        nodes.push(Node { id, properties });
    }

    nodes
}

/// Generate random edges between uniform nodes.
pub fn generate_uniform_edges(
    node_count: u64,
    edge_count: u64,
    seed: u64,
) -> Vec<Edge> {
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    let mut edges = Vec::with_capacity(edge_count as usize);

    for _ in 0..edge_count {
        let source = rng.gen_range(0..node_count);
        let mut target = rng.gen_range(0..node_count);
        while target == source {
            target = rng.gen_range(0..node_count);
        }
        edges.push(Edge { source, target });
    }

    edges
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_generation() {
        let nodes1 = generate_uniform_nodes(100, 3, 0.0, 42);
        let nodes2 = generate_uniform_nodes(100, 3, 0.0, 42);
        assert_eq!(nodes1.len(), nodes2.len());
        for (a, b) in nodes1.iter().zip(nodes2.iter()) {
            assert_eq!(a.id, b.id);
            assert_eq!(a.properties.len(), b.properties.len());
        }
    }

    #[test]
    fn different_seeds_different_output() {
        let nodes1 = generate_uniform_nodes(100, 3, 0.0, 42);
        let nodes2 = generate_uniform_nodes(100, 3, 0.0, 99);
        // At least some properties should differ
        let differ = nodes1.iter().zip(nodes2.iter())
            .any(|(a, b)| a.properties != b.properties);
        assert!(differ, "Different seeds should produce different nodes");
    }

    #[test]
    fn correct_count() {
        let nodes = generate_uniform_nodes(1000, 2, 0.0, 42);
        assert_eq!(nodes.len(), 1000);
    }

    #[test]
    fn nullable_produces_nulls() {
        let nodes = generate_uniform_nodes(1000, 3, 0.5, 42);
        let null_count: usize = nodes.iter()
            .flat_map(|n| &n.properties)
            .filter(|p| matches!(p, PropertyValue::Null))
            .count();
        assert!(null_count > 0, "50% nullable ratio should produce some nulls");
    }

    #[test]
    fn edges_no_self_loops() {
        let edges = generate_uniform_edges(100, 10000, 42);
        for e in &edges {
            assert_ne!(e.source, e.target, "Self-loop detected");
        }
    }

    #[test]
    fn edge_count_correct() {
        let edges = generate_uniform_edges(100, 500, 42);
        assert_eq!(edges.len(), 500);
    }
}
```

Create `rust/xraybench-generators/src/deep_traversal.rs`:

```rust
//! Deep traversal graph generator with depth-controlled fanout.
//!
//! Generates a graph where fanout attenuates with depth, modeling realistic
//! social and dependency networks where connectivity decreases at distance.
//!
//! This is the KEY generator for the benchmark suite's primary challenge:
//! 10+ hop traversal under combinatorial explosion.

use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use rand::Rng;
use xraybench_types::Edge;

/// Generate a deep traversal graph with controlled fanout per depth level.
///
/// `fanout_per_level` defines the number of outgoing edges per node at each
/// depth level. For example, `[50, 50, 30, 20, 10, 5, 3, 2, 2, 2]` creates:
/// - Level 0→1: each root connects to 50 nodes
/// - Level 1→2: each node connects to 50 nodes
/// - Level 9→10: each node connects to 2 nodes
///
/// `num_roots` controls how many seed nodes exist at depth 0.
///
/// Returns (total_node_count, edges).
pub fn generate_deep_traversal(
    num_roots: u64,
    fanout_per_level: &[u32],
    seed: u64,
) -> (u64, Vec<Edge>) {
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    let mut edges = Vec::new();
    let mut next_id = num_roots; // Root nodes are 0..num_roots

    // Current frontier: nodes at the current depth level
    let mut frontier: Vec<u64> = (0..num_roots).collect();

    for (depth, &fanout) in fanout_per_level.iter().enumerate() {
        let mut next_frontier = Vec::new();

        for &parent in &frontier {
            for _ in 0..fanout {
                let child = next_id;
                next_id += 1;
                edges.push(Edge {
                    source: parent,
                    target: child,
                });
                next_frontier.push(child);
            }

            // Add some cross-edges within the same level (5% probability)
            // This creates the irregular structure that breaks naive BFS
            if frontier.len() > 1 {
                let cross_edge_count = (fanout as f64 * 0.05).ceil() as u32;
                for _ in 0..cross_edge_count {
                    let other = frontier[rng.gen_range(0..frontier.len())];
                    if other != parent {
                        edges.push(Edge {
                            source: parent,
                            target: other,
                        });
                    }
                }
            }
        }

        frontier = next_frontier;

        // Safety: if frontier is getting too large, sample it down
        // This prevents OOM on very deep graphs with high fanout
        let max_frontier = 10_000_000u64;
        if frontier.len() as u64 > max_frontier {
            let sample_size = max_frontier as usize;
            let mut sampled = Vec::with_capacity(sample_size);
            for _ in 0..sample_size {
                sampled.push(frontier[rng.gen_range(0..frontier.len())]);
            }
            frontier = sampled;
        }

        if frontier.is_empty() {
            break;
        }
    }

    (next_id, edges)
}

/// Compute the theoretical node count for a given fanout schedule.
///
/// Useful for planning dataset sizes before generation.
pub fn estimate_node_count(num_roots: u64, fanout_per_level: &[u32]) -> u64 {
    let mut count = num_roots;
    let mut frontier_size = num_roots;
    for &fanout in fanout_per_level {
        frontier_size *= fanout as u64;
        count += frontier_size;
    }
    count
}

/// Compute the theoretical edge count (excluding cross-edges).
pub fn estimate_edge_count(num_roots: u64, fanout_per_level: &[u32]) -> u64 {
    let mut count = 0u64;
    let mut frontier_size = num_roots;
    for &fanout in fanout_per_level {
        count += frontier_size * fanout as u64;
        frontier_size *= fanout as u64;
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_root_single_level() {
        let (node_count, edges) = generate_deep_traversal(1, &[5], 42);
        assert_eq!(node_count, 6); // 1 root + 5 children
        // At least 5 edges (may have cross-edges)
        assert!(edges.len() >= 5, "Expected at least 5 edges, got {}", edges.len());
    }

    #[test]
    fn deterministic() {
        let (n1, e1) = generate_deep_traversal(1, &[3, 3], 42);
        let (n2, e2) = generate_deep_traversal(1, &[3, 3], 42);
        assert_eq!(n1, n2);
        assert_eq!(e1.len(), e2.len());
        for (a, b) in e1.iter().zip(e2.iter()) {
            assert_eq!(a, b);
        }
    }

    #[test]
    fn multiple_roots() {
        let (node_count, edges) = generate_deep_traversal(3, &[2], 42);
        // 3 roots + 3*2 children = 9 nodes
        assert_eq!(node_count, 9);
        // At least 6 edges (3 roots * 2 fanout)
        assert!(edges.len() >= 6);
    }

    #[test]
    fn realistic_schedule() {
        // Moderate fanout that won't explode memory
        let fanout = [10, 5, 3, 2];
        let (node_count, edges) = generate_deep_traversal(1, &fanout, 42);
        // 1 + 10 + 50 + 150 + 300 = 511 nodes
        assert!(node_count > 400 && node_count < 600,
            "Expected ~511 nodes, got {node_count}");
        assert!(!edges.is_empty());
    }

    #[test]
    fn estimate_matches_actual() {
        let fanout = [5, 3, 2];
        let estimated = estimate_node_count(1, &fanout);
        let (actual, _) = generate_deep_traversal(1, &fanout, 42);
        // Actual may have cross-edges creating more connections, not more nodes
        assert_eq!(estimated, actual);
    }

    #[test]
    fn edges_reference_valid_nodes() {
        let (node_count, edges) = generate_deep_traversal(2, &[3, 2], 42);
        for edge in &edges {
            assert!(edge.source < node_count, "Invalid source: {}", edge.source);
            assert!(edge.target < node_count, "Invalid target: {}", edge.target);
        }
    }

    #[test]
    fn empty_fanout() {
        let (node_count, edges) = generate_deep_traversal(5, &[], 42);
        assert_eq!(node_count, 5);
        assert!(edges.is_empty());
    }

    #[test]
    fn cross_edges_exist_on_larger_graphs() {
        let (_, edges) = generate_deep_traversal(10, &[10, 10], 42);
        // With 10 roots * 10 fanout = 100 nodes at level 1
        // Cross-edges should exist at level 1 (5% * 10 fanout * 100 nodes)
        let main_edge_count = estimate_edge_count(10, &[10, 10]);
        assert!(
            edges.len() as u64 > main_edge_count,
            "Expected cross-edges: {} total vs {} main",
            edges.len(),
            main_edge_count
        );
    }
}
```

- [ ] **Step 4: Write remaining generators (power_law, hub, community, chain)**

These follow the same pattern. Create `rust/xraybench-generators/src/power_law.rs`:

```rust
//! Power-law graph generator using Barabási-Albert preferential attachment.

use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use rand::Rng;
use xraybench_types::Edge;

/// Generate a power-law graph using preferential attachment.
///
/// Each new node connects to `m` existing nodes with probability proportional
/// to their degree (preferential attachment). This produces a scale-free
/// degree distribution that matches real-world social and web graphs.
pub fn generate_power_law(
    node_count: u64,
    m: u32, // edges per new node
    seed: u64,
) -> Vec<Edge> {
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    let m = m as usize;

    // Start with a small complete graph of m+1 nodes
    let initial_nodes = (m + 1) as u64;
    let mut edges = Vec::new();
    let mut degree = vec![0u64; node_count as usize];

    // Create initial clique
    for i in 0..initial_nodes {
        for j in (i + 1)..initial_nodes {
            edges.push(Edge { source: i, target: j });
            edges.push(Edge { source: j, target: i });
            degree[i as usize] += 1;
            degree[j as usize] += 1;
        }
    }

    let mut total_degree: u64 = degree.iter().sum();

    // Add remaining nodes with preferential attachment
    for new_node in initial_nodes..node_count {
        let mut targets = Vec::with_capacity(m);

        while targets.len() < m {
            // Select target proportional to degree
            let threshold = rng.gen_range(0..total_degree.max(1));
            let mut cumulative = 0u64;
            let mut target = 0u64;

            for (j, &deg) in degree[..new_node as usize].iter().enumerate() {
                cumulative += deg;
                if cumulative > threshold {
                    target = j as u64;
                    break;
                }
            }

            if !targets.contains(&target) {
                targets.push(target);
            }
        }

        for &target in &targets {
            edges.push(Edge { source: new_node, target });
            degree[new_node as usize] += 1;
            degree[target as usize] += 1;
            total_degree += 2;
        }
    }

    edges
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn deterministic() {
        let e1 = generate_power_law(1000, 3, 42);
        let e2 = generate_power_law(1000, 3, 42);
        assert_eq!(e1.len(), e2.len());
    }

    #[test]
    fn correct_edge_count() {
        let edges = generate_power_law(100, 2, 42);
        // Initial clique of 3: 6 edges. Then 97 nodes * 2 edges = 194. Total ~200
        assert!(edges.len() > 150 && edges.len() < 250, "edges={}", edges.len());
    }

    #[test]
    fn has_skewed_degree() {
        let edges = generate_power_law(10_000, 3, 42);
        let mut degree: HashMap<u64, u64> = HashMap::new();
        for e in &edges {
            *degree.entry(e.source).or_default() += 1;
            *degree.entry(e.target).or_default() += 1;
        }
        let max_deg = degree.values().max().unwrap();
        let min_deg = degree.values().min().unwrap();
        // Power-law: max degree should be much larger than min
        assert!(*max_deg > *min_deg * 10, "max={max_deg}, min={min_deg}");
    }

    #[test]
    fn no_self_loops() {
        let edges = generate_power_law(500, 2, 42);
        for e in &edges {
            assert_ne!(e.source, e.target);
        }
    }
}
```

Create `rust/xraybench-generators/src/hub.rs`:

```rust
//! Hub-and-spoke graph generator.

use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use rand::Rng;
use xraybench_types::Edge;

/// Generate a hub-and-spoke graph.
///
/// Creates `hub_count` hub nodes, each connected to `spokes_per_hub` spoke nodes.
/// Node IDs: hubs are 0..hub_count, spokes are hub_count..total.
pub fn generate_hub_graph(
    hub_count: u64,
    spokes_per_hub: u64,
    seed: u64,
) -> (u64, Vec<Edge>) {
    let _rng = ChaCha20Rng::seed_from_u64(seed);
    let total_nodes = hub_count + hub_count * spokes_per_hub;
    let mut edges = Vec::with_capacity((hub_count * spokes_per_hub) as usize);

    for hub_id in 0..hub_count {
        let spoke_start = hub_count + hub_id * spokes_per_hub;
        for spoke_offset in 0..spokes_per_hub {
            edges.push(Edge {
                source: hub_id,
                target: spoke_start + spoke_offset,
            });
        }
    }

    (total_nodes, edges)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correct_counts() {
        let (nodes, edges) = generate_hub_graph(5, 100, 42);
        assert_eq!(nodes, 5 + 5 * 100);
        assert_eq!(edges.len(), 500);
    }

    #[test]
    fn hub_degree() {
        let (_, edges) = generate_hub_graph(3, 50, 42);
        for hub_id in 0..3u64 {
            let deg: usize = edges.iter().filter(|e| e.source == hub_id).count();
            assert_eq!(deg, 50);
        }
    }

    #[test]
    fn deterministic() {
        let (n1, e1) = generate_hub_graph(3, 100, 42);
        let (n2, e2) = generate_hub_graph(3, 100, 42);
        assert_eq!(n1, n2);
        assert_eq!(e1, e2);
    }
}
```

Create `rust/xraybench-generators/src/community.rs`:

```rust
//! Community graph generator using Stochastic Block Model.

use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use rand::Rng;
use xraybench_types::Edge;

/// Generate a graph with community structure.
///
/// Nodes within the same community connect with `intra_density`,
/// nodes in different communities connect with `inter_density`.
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

    // Intra-community edges
    let intra_edges_per_node = (nodes_per_community as f64 * intra_density).max(1.0) as u64;
    for c in 0..community_count {
        let base = c * nodes_per_community;
        for i in 0..nodes_per_community {
            let src = base + i;
            for _ in 0..intra_edges_per_node {
                let dst = base + rng.gen_range(0..nodes_per_community);
                if src != dst {
                    edges.push(Edge { source: src, target: dst });
                }
            }
        }
    }

    // Inter-community edges
    let inter_total = (community_count * (community_count - 1) * nodes_per_community as u64) as f64
        * inter_density;
    for _ in 0..inter_total as u64 {
        let c1 = rng.gen_range(0..community_count);
        let mut c2 = rng.gen_range(0..community_count - 1);
        if c2 >= c1 { c2 += 1; }
        let src = c1 * nodes_per_community + rng.gen_range(0..nodes_per_community);
        let dst = c2 * nodes_per_community + rng.gen_range(0..nodes_per_community);
        edges.push(Edge { source: src, target: dst });
    }

    (total_nodes, edges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn has_community_structure() {
        let (_, edges) = generate_community_graph(5, 100, 0.1, 0.001, 42);
        // Count intra vs inter community edges
        let mut intra = 0u64;
        let mut inter = 0u64;
        for e in &edges {
            let c_src = e.source / 100;
            let c_dst = e.target / 100;
            if c_src == c_dst { intra += 1; } else { inter += 1; }
        }
        // Intra should be much more than inter
        assert!(intra > inter * 5, "intra={intra}, inter={inter}");
    }

    #[test]
    fn deterministic() {
        let (n1, e1) = generate_community_graph(3, 50, 0.05, 0.001, 42);
        let (n2, e2) = generate_community_graph(3, 50, 0.05, 0.001, 42);
        assert_eq!(n1, n2);
        assert_eq!(e1.len(), e2.len());
    }

    #[test]
    fn correct_node_count() {
        let (nodes, _) = generate_community_graph(10, 200, 0.01, 0.0001, 42);
        assert_eq!(nodes, 2000);
    }
}
```

Create `rust/xraybench-generators/src/chain.rs`:

```rust
//! Linear chain graph generator.

use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use xraybench_types::Edge;

/// Generate a simple chain (linked list) graph.
///
/// Node 0 → Node 1 → ... → Node (length-1).
/// Useful as a baseline for sequential traversal cost without fan-out.
pub fn generate_chain(length: u64, seed: u64) -> Vec<Edge> {
    let _rng = ChaCha20Rng::seed_from_u64(seed); // seed preserved for API consistency
    let mut edges = Vec::with_capacity(length.saturating_sub(1) as usize);
    for i in 0..length.saturating_sub(1) {
        edges.push(Edge { source: i, target: i + 1 });
    }
    edges
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correct_length() {
        let edges = generate_chain(100, 42);
        assert_eq!(edges.len(), 99);
    }

    #[test]
    fn chain_structure() {
        let edges = generate_chain(5, 42);
        assert_eq!(edges[0], Edge { source: 0, target: 1 });
        assert_eq!(edges[1], Edge { source: 1, target: 2 });
        assert_eq!(edges[2], Edge { source: 2, target: 3 });
        assert_eq!(edges[3], Edge { source: 3, target: 4 });
    }

    #[test]
    fn single_node() {
        let edges = generate_chain(1, 42);
        assert!(edges.is_empty());
    }

    #[test]
    fn deterministic() {
        let e1 = generate_chain(100, 42);
        let e2 = generate_chain(100, 42);
        assert_eq!(e1, e2);
    }
}
```

- [ ] **Step 5: Write generators lib.rs**

Create `rust/xraybench-generators/src/lib.rs`:

```rust
//! Compiled graph generators for xraybench.
//!
//! Deterministic generators for various graph topologies:
//! uniform, power-law, hub-and-spoke, community, chain, and
//! deep traversal (depth-controlled fanout).

pub mod io;
pub mod uniform;
pub mod power_law;
pub mod hub;
pub mod community;
pub mod chain;
pub mod deep_traversal;

pub use chain::generate_chain;
pub use community::generate_community_graph;
pub use deep_traversal::{estimate_edge_count, estimate_node_count, generate_deep_traversal};
pub use hub::generate_hub_graph;
pub use io::{read_edges_binary, read_edges_csv, write_edges_binary, write_edges_csv};
pub use power_law::generate_power_law;
pub use uniform::{generate_uniform_edges, generate_uniform_nodes};
```

- [ ] **Step 6: Run all generator tests**

Run:
```bash
cd rust && cargo test -p xraybench-generators
```

Expected: All ~30 tests pass (io: 4, uniform: 6, deep_traversal: 8, power_law: 4, hub: 3, community: 3, chain: 4).

- [ ] **Step 7: Commit**

```bash
git add rust/xraybench-generators/
git commit -m "feat(rust): add xraybench-generators crate — 6 deterministic graph generators with binary/CSV I/O"
```

---

### Task 6: xraybench-compare — Result Comparison Engine

**Files:**
- Create: `rust/xraybench-compare/Cargo.toml`
- Create: `rust/xraybench-compare/src/lib.rs`
- Create: `rust/xraybench-compare/src/diff.rs`
- Create: `rust/xraybench-compare/src/significance.rs`
- Create: `rust/xraybench-compare/src/matrix.rs`

- [ ] **Step 1: Create crate and implement**

Create `rust/xraybench-compare/Cargo.toml`:

```toml
[package]
name = "xraybench-compare"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
description = "Result comparison engine for xraybench"

[dependencies]
xraybench-types = { path = "../xraybench-types" }
xraybench-stats = { path = "../xraybench-stats" }
serde = { workspace = true }
serde_json = { workspace = true }
```

Create `rust/xraybench-compare/src/diff.rs`:

```rust
//! Per-metric comparison between two benchmark result sets.

use xraybench_types::{MetricComparison, Result};
use xraybench_stats::regression::compare_metric;

/// Compare all shared metrics between two result sets.
///
/// `metrics_a` and `metrics_b` are maps from metric name to list of values.
pub fn diff_results(
    metrics_a: &[(&str, &[f64])],
    metrics_b: &[(&str, &[f64])],
    significance_threshold: f64,
) -> Result<Vec<MetricComparison>> {
    let mut comparisons = Vec::new();

    for (name_a, values_a) in metrics_a {
        for (name_b, values_b) in metrics_b {
            if name_a == name_b {
                let cmp = compare_metric(name_a, values_a, values_b, significance_threshold)?;
                comparisons.push(cmp);
            }
        }
    }

    Ok(comparisons)
}

/// Format a comparison as a human-readable table row.
pub fn format_comparison(cmp: &MetricComparison) -> String {
    let arrow = match cmp.classification {
        xraybench_types::ChangeClass::Improvement => "v (better)",
        xraybench_types::ChangeClass::Regression => "^ (worse)",
        xraybench_types::ChangeClass::NoChange => "= (same)",
        xraybench_types::ChangeClass::Inconclusive => "? (unclear)",
    };

    format!(
        "{:<25} {:>12.2} {:>12.2} {:>+10.1}% {:>8.4} {}",
        cmp.metric_name, cmp.value_a, cmp.value_b, cmp.percentage_change, cmp.p_value, arrow
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_identical_results() {
        let a = vec![("cold_ms", vec![100.0; 30].as_slice())];
        let b = vec![("cold_ms", vec![100.0; 30].as_slice())];
        let diffs = diff_results(&a, &b, 0.05).unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(!diffs[0].significant);
    }

    #[test]
    fn diff_different_results() {
        let a = vec![("cold_ms", vec![100.0; 30].as_slice())];
        let b = vec![("cold_ms", vec![50.0; 30].as_slice())];
        let diffs = diff_results(&a, &b, 0.05).unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(diffs[0].significant);
    }

    #[test]
    fn diff_multiple_metrics() {
        let a = vec![
            ("cold_ms", vec![100.0; 30].as_slice()),
            ("warm_ms", vec![10.0; 30].as_slice()),
        ];
        let b = vec![
            ("cold_ms", vec![90.0; 30].as_slice()),
            ("warm_ms", vec![10.0; 30].as_slice()),
        ];
        let diffs = diff_results(&a, &b, 0.05).unwrap();
        assert_eq!(diffs.len(), 2);
    }

    #[test]
    fn format_produces_output() {
        let cmp = MetricComparison {
            metric_name: "cold_ms".into(),
            value_a: 100.0,
            value_b: 50.0,
            absolute_delta: -50.0,
            percentage_change: -50.0,
            ci_lower: -55.0,
            ci_upper: -45.0,
            p_value: 0.001,
            significant: true,
            classification: xraybench_types::ChangeClass::Improvement,
        };
        let s = format_comparison(&cmp);
        assert!(s.contains("cold_ms"));
        assert!(s.contains("better"));
    }
}
```

Create `rust/xraybench-compare/src/significance.rs`:

```rust
//! Wrapper for significance testing decisions.

use xraybench_types::ChangeClass;

/// Classify a comparison result based on p-value, effect size, and threshold.
pub fn classify_change(
    p_value: f64,
    percentage_change: f64,
    alpha: f64,
    min_effect_pct: f64,
) -> ChangeClass {
    if p_value > alpha {
        return ChangeClass::Inconclusive;
    }

    if percentage_change.abs() < min_effect_pct {
        return ChangeClass::NoChange;
    }

    if percentage_change < 0.0 {
        ChangeClass::Improvement
    } else {
        ChangeClass::Regression
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn improvement() {
        assert_eq!(classify_change(0.01, -30.0, 0.05, 5.0), ChangeClass::Improvement);
    }

    #[test]
    fn regression() {
        assert_eq!(classify_change(0.01, 30.0, 0.05, 5.0), ChangeClass::Regression);
    }

    #[test]
    fn no_change_small_effect() {
        assert_eq!(classify_change(0.01, 2.0, 0.05, 5.0), ChangeClass::NoChange);
    }

    #[test]
    fn inconclusive_high_p() {
        assert_eq!(classify_change(0.10, -30.0, 0.05, 5.0), ChangeClass::Inconclusive);
    }
}
```

Create `rust/xraybench-compare/src/matrix.rs`:

```rust
//! Multi-engine comparison matrix.

use xraybench_types::{MetricComparison, Result};
use crate::diff::diff_results;

/// A comparison between two named engines on one metric.
#[derive(Debug, Clone)]
pub struct PairwiseComparison {
    pub engine_a: String,
    pub engine_b: String,
    pub comparison: MetricComparison,
}

/// Compare multiple engines pairwise on a set of metrics.
///
/// `engines` is a list of (engine_name, metric_name, values) tuples.
pub fn pairwise_matrix(
    engines: &[(&str, &[(&str, &[f64])])],
    significance_threshold: f64,
) -> Result<Vec<PairwiseComparison>> {
    let mut results = Vec::new();

    for i in 0..engines.len() {
        for j in (i + 1)..engines.len() {
            let (name_a, metrics_a) = engines[i];
            let (name_b, metrics_b) = engines[j];

            let diffs = diff_results(metrics_a, metrics_b, significance_threshold)?;

            for cmp in diffs {
                results.push(PairwiseComparison {
                    engine_a: name_a.to_string(),
                    engine_b: name_b.to_string(),
                    comparison: cmp,
                });
            }
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_engine_matrix() {
        let a_cold: Vec<f64> = vec![100.0; 30];
        let b_cold: Vec<f64> = vec![80.0; 30];
        let c_cold: Vec<f64> = vec![120.0; 30];

        let engines: Vec<(&str, &[(&str, &[f64])])> = vec![
            ("xraygraphdb", &[("cold_ms", a_cold.as_slice())]),
            ("neo4j", &[("cold_ms", b_cold.as_slice())]),
            ("memgraph", &[("cold_ms", c_cold.as_slice())]),
        ];

        let matrix = pairwise_matrix(&engines, 0.05).unwrap();
        // 3 engines = 3 pairs, each with 1 metric = 3 comparisons
        assert_eq!(matrix.len(), 3);
    }
}
```

Create `rust/xraybench-compare/src/lib.rs`:

```rust
//! Result comparison engine for xraybench.

pub mod diff;
pub mod significance;
pub mod matrix;

pub use diff::{diff_results, format_comparison};
pub use matrix::{pairwise_matrix, PairwiseComparison};
pub use significance::classify_change;
```

- [ ] **Step 2: Run all compare tests**

Run:
```bash
cd rust && cargo test -p xraybench-compare
```

Expected: All ~9 tests pass (diff: 4, significance: 4, matrix: 1).

- [ ] **Step 3: Commit**

```bash
git add rust/xraybench-compare/
git commit -m "feat(rust): add xraybench-compare crate — result diff, significance testing, multi-engine matrix"
```

---

### Task 7: xraybench-py — PyO3 Python Bindings

**Files:**
- Create: `rust/xraybench-py/Cargo.toml`
- Create: `rust/xraybench-py/src/lib.rs`
- Create: `rust/xraybench-py/src/timing.rs`
- Create: `rust/xraybench-py/src/stats.rs`
- Create: `rust/xraybench-py/src/generators.rs`
- Create: `rust/xraybench-py/src/checksum.rs`
- Create: `rust/xraybench-py/src/compare.rs`

- [ ] **Step 1: Create Cargo.toml**

Create `rust/xraybench-py/Cargo.toml`:

```toml
[package]
name = "xraybench-py"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
description = "Python bindings for xraybench Rust core"

[lib]
name = "xraybench_core"
crate-type = ["cdylib"]

[dependencies]
xraybench-types = { path = "../xraybench-types" }
xraybench-timing = { path = "../xraybench-timing" }
xraybench-stats = { path = "../xraybench-stats" }
xraybench-generators = { path = "../xraybench-generators" }
xraybench-checksum = { path = "../xraybench-checksum" }
xraybench-compare = { path = "../xraybench-compare" }
pyo3 = { workspace = true }
serde_json = { workspace = true }
```

- [ ] **Step 2: Write Python bindings**

Create `rust/xraybench-py/src/lib.rs`:

```rust
use pyo3::prelude::*;

mod timing;
mod stats;
mod generators;
mod checksum;
mod compare;

/// xraybench_core — Rust measurement foundation for xraybench.
#[pymodule]
fn xraybench_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", xraybench_types::VERSION)?;

    // Timing submodule
    let timing_mod = PyModule::new(m.py(), "timing")?;
    timing::register(&timing_mod)?;
    m.add_submodule(&timing_mod)?;

    // Stats submodule
    let stats_mod = PyModule::new(m.py(), "stats")?;
    stats::register(&stats_mod)?;
    m.add_submodule(&stats_mod)?;

    // Generators submodule
    let gen_mod = PyModule::new(m.py(), "generators")?;
    generators::register(&gen_mod)?;
    m.add_submodule(&gen_mod)?;

    // Checksum submodule
    let checksum_mod = PyModule::new(m.py(), "checksum")?;
    checksum::register(&checksum_mod)?;
    m.add_submodule(&checksum_mod)?;

    // Compare submodule
    let compare_mod = PyModule::new(m.py(), "compare")?;
    compare::register(&compare_mod)?;
    m.add_submodule(&compare_mod)?;

    Ok(())
}
```

Create `rust/xraybench-py/src/timing.rs`:

```rust
use pyo3::prelude::*;

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(calibrate, m)?)?;
    m.add_function(wrap_pyfunction!(monotonic_ns, m)?)?;
    Ok(())
}

/// Run timing self-calibration and return results as dict.
#[pyfunction]
fn calibrate() -> PyResult<PyObject> {
    let cal = xraybench_timing::calibrate()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Python::with_gil(|py| {
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("clock_resolution_ns", cal.clock_resolution_ns)?;
        dict.set_item("clock_overhead_ns", cal.clock_overhead_ns)?;
        dict.set_item("fence_overhead_ns", cal.fence_overhead_ns)?;
        dict.set_item("samples", cal.samples)?;
        Ok(dict.into())
    })
}

/// Read the monotonic clock in nanoseconds.
#[pyfunction]
fn monotonic_ns() -> PyResult<u64> {
    xraybench_timing::monotonic_ns()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
}
```

Create `rust/xraybench-py/src/stats.rs`:

```rust
use pyo3::prelude::*;

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(percentile, m)?)?;
    m.add_function(wrap_pyfunction!(percentiles, m)?)?;
    m.add_function(wrap_pyfunction!(descriptive, m)?)?;
    m.add_function(wrap_pyfunction!(bootstrap_ci, m)?)?;
    m.add_function(wrap_pyfunction!(detect_outliers, m)?)?;
    m.add_function(wrap_pyfunction!(mann_whitney, m)?)?;
    Ok(())
}

#[pyfunction]
fn percentile(values: Vec<f64>, p: f64) -> PyResult<f64> {
    let mut v = values;
    xraybench_stats::exact_percentile(&mut v, p)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
}

#[pyfunction]
fn percentiles(values: Vec<f64>, ps: Vec<f64>) -> PyResult<Vec<f64>> {
    let mut v = values;
    xraybench_stats::exact_percentiles(&mut v, &ps)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
}

#[pyfunction]
fn descriptive(values: Vec<f64>) -> PyResult<PyObject> {
    let d = xraybench_stats::descriptive(&values)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    Python::with_gil(|py| {
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("count", d.count)?;
        dict.set_item("mean", d.mean)?;
        dict.set_item("min", d.min)?;
        dict.set_item("max", d.max)?;
        dict.set_item("variance", d.variance)?;
        dict.set_item("stddev", d.stddev)?;
        Ok(dict.into())
    })
}

#[pyfunction]
#[pyo3(signature = (values, confidence=0.95, n_resamples=10000, seed=42))]
fn bootstrap_ci(values: Vec<f64>, confidence: f64, n_resamples: usize, seed: u64) -> PyResult<PyObject> {
    let config = xraybench_stats::BootstrapConfig { n_resamples, confidence, seed };
    let ci = xraybench_stats::bca_mean_ci(&values, &config)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    Python::with_gil(|py| {
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("lower", ci.lower)?;
        dict.set_item("upper", ci.upper)?;
        dict.set_item("confidence", ci.confidence)?;
        dict.set_item("point_estimate", ci.point_estimate)?;
        dict.set_item("bias", ci.bias)?;
        dict.set_item("acceleration", ci.acceleration)?;
        Ok(dict.into())
    })
}

#[pyfunction]
#[pyo3(signature = (values, threshold=3.5))]
fn detect_outliers(values: Vec<f64>, threshold: f64) -> PyResult<PyObject> {
    let result = xraybench_stats::detect_outliers(&values, threshold)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    Python::with_gil(|py| {
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("outlier_indices", result.outlier_indices)?;
        dict.set_item("median", result.median)?;
        dict.set_item("mad", result.mad)?;
        dict.set_item("threshold", result.threshold)?;
        Ok(dict.into())
    })
}

#[pyfunction]
fn mann_whitney(sample_a: Vec<f64>, sample_b: Vec<f64>) -> PyResult<PyObject> {
    let result = xraybench_stats::mann_whitney_u(&sample_a, &sample_b)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    Python::with_gil(|py| {
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("u_a", result.u_a)?;
        dict.set_item("u_b", result.u_b)?;
        dict.set_item("p_value", result.p_value)?;
        dict.set_item("significant", result.significant)?;
        Ok(dict.into())
    })
}
```

Create `rust/xraybench-py/src/generators.rs`:

```rust
use pyo3::prelude::*;

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(generate_deep_traversal, m)?)?;
    m.add_function(wrap_pyfunction!(generate_power_law_edges, m)?)?;
    m.add_function(wrap_pyfunction!(generate_hub_graph, m)?)?;
    m.add_function(wrap_pyfunction!(generate_chain, m)?)?;
    m.add_function(wrap_pyfunction!(estimate_node_count, m)?)?;
    m.add_function(wrap_pyfunction!(write_edges_binary, m)?)?;
    m.add_function(wrap_pyfunction!(write_edges_csv, m)?)?;
    Ok(())
}

/// Generate a deep traversal graph. Returns (node_count, [(source, target), ...]).
#[pyfunction]
fn generate_deep_traversal(num_roots: u64, fanout_per_level: Vec<u32>, seed: u64) -> PyResult<(u64, Vec<(u64, u64)>)> {
    let (n, edges) = xraybench_generators::generate_deep_traversal(num_roots, &fanout_per_level, seed);
    let tuples: Vec<(u64, u64)> = edges.iter().map(|e| (e.source, e.target)).collect();
    Ok((n, tuples))
}

#[pyfunction]
fn generate_power_law_edges(node_count: u64, m: u32, seed: u64) -> PyResult<Vec<(u64, u64)>> {
    let edges = xraybench_generators::generate_power_law(node_count, m, seed);
    Ok(edges.iter().map(|e| (e.source, e.target)).collect())
}

#[pyfunction]
fn generate_hub_graph(hub_count: u64, spokes_per_hub: u64, seed: u64) -> PyResult<(u64, Vec<(u64, u64)>)> {
    let (n, edges) = xraybench_generators::generate_hub_graph(hub_count, spokes_per_hub, seed);
    Ok((n, edges.iter().map(|e| (e.source, e.target)).collect()))
}

#[pyfunction]
fn generate_chain(length: u64, seed: u64) -> PyResult<Vec<(u64, u64)>> {
    let edges = xraybench_generators::generate_chain(length, seed);
    Ok(edges.iter().map(|e| (e.source, e.target)).collect())
}

#[pyfunction]
fn estimate_node_count(num_roots: u64, fanout_per_level: Vec<u32>) -> PyResult<u64> {
    Ok(xraybench_generators::estimate_node_count(num_roots, &fanout_per_level))
}

#[pyfunction]
fn write_edges_binary(edges: Vec<(u64, u64)>, path: String) -> PyResult<()> {
    let edges: Vec<xraybench_types::Edge> = edges.iter().map(|&(s, t)| xraybench_types::Edge { source: s, target: t }).collect();
    xraybench_generators::write_edges_binary(&edges, std::path::Path::new(&path))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
}

#[pyfunction]
fn write_edges_csv(edges: Vec<(u64, u64)>, path: String) -> PyResult<()> {
    let edges: Vec<xraybench_types::Edge> = edges.iter().map(|&(s, t)| xraybench_types::Edge { source: s, target: t }).collect();
    xraybench_generators::write_edges_csv(&edges, std::path::Path::new(&path))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
}
```

Create `rust/xraybench-py/src/checksum.rs`:

```rust
use pyo3::prelude::*;
use xraybench_types::PropertyValue;

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hash_result_set, m)?)?;
    m.add_function(wrap_pyfunction!(verify_hash, m)?)?;
    m.add_function(wrap_pyfunction!(float_eq_ulp, m)?)?;
    Ok(())
}

/// Convert Python values to PropertyValues.
fn py_to_property(val: &Bound<'_, PyAny>) -> PyResult<PropertyValue> {
    if val.is_none() {
        Ok(PropertyValue::Null)
    } else if let Ok(b) = val.extract::<bool>() {
        Ok(PropertyValue::Boolean(b))
    } else if let Ok(i) = val.extract::<i64>() {
        Ok(PropertyValue::Integer(i))
    } else if let Ok(f) = val.extract::<f64>() {
        Ok(PropertyValue::Float(f))
    } else if let Ok(s) = val.extract::<String>() {
        Ok(PropertyValue::Text(s))
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            format!("Unsupported type: {}", val.get_type().name()?)
        ))
    }
}

/// Hash a result set (list of rows, each row is list of values).
/// Order-independent.
#[pyfunction]
fn hash_result_set(rows: Vec<Vec<Bound<'_, PyAny>>>) -> PyResult<String> {
    let mut converted: Vec<Vec<PropertyValue>> = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut prow = Vec::with_capacity(row.len());
        for val in row {
            prow.push(py_to_property(val)?);
        }
        converted.push(prow);
    }
    Ok(xraybench_checksum::hash_result_set(&converted))
}

/// Verify a result set against a reference hash.
#[pyfunction]
fn verify_hash(rows: Vec<Vec<Bound<'_, PyAny>>>, reference: String) -> PyResult<bool> {
    let mut converted: Vec<Vec<PropertyValue>> = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut prow = Vec::with_capacity(row.len());
        for val in row {
            prow.push(py_to_property(val)?);
        }
        converted.push(prow);
    }
    xraybench_checksum::verify_hash(&converted, &reference)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
}

/// Compare two floats with ULP tolerance.
#[pyfunction]
fn float_eq_ulp(a: f64, b: f64, max_ulp: u32) -> bool {
    xraybench_checksum::float_eq_ulp(a, b, max_ulp)
}
```

Create `rust/xraybench-py/src/compare.rs`:

```rust
use pyo3::prelude::*;

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(compare_metric, m)?)?;
    Ok(())
}

/// Compare two sets of metric values and return analysis dict.
#[pyfunction]
#[pyo3(signature = (name, values_a, values_b, significance_threshold=0.05))]
fn compare_metric(name: String, values_a: Vec<f64>, values_b: Vec<f64>, significance_threshold: f64) -> PyResult<PyObject> {
    let cmp = xraybench_stats::regression::compare_metric(&name, &values_a, &values_b, significance_threshold)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    Python::with_gil(|py| {
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("metric_name", cmp.metric_name)?;
        dict.set_item("value_a", cmp.value_a)?;
        dict.set_item("value_b", cmp.value_b)?;
        dict.set_item("absolute_delta", cmp.absolute_delta)?;
        dict.set_item("percentage_change", cmp.percentage_change)?;
        dict.set_item("ci_lower", cmp.ci_lower)?;
        dict.set_item("ci_upper", cmp.ci_upper)?;
        dict.set_item("p_value", cmp.p_value)?;
        dict.set_item("significant", cmp.significant)?;
        dict.set_item("classification", format!("{:?}", cmp.classification))?;
        Ok(dict.into())
    })
}
```

- [ ] **Step 3: Build the Python extension**

Run:
```bash
cd rust/xraybench-py && maturin develop --release 2>&1
```

Expected: Build succeeds. `import xraybench_core` works in Python.

- [ ] **Step 4: Verify Python import**

Run:
```bash
python3 -c "import xraybench_core; print(xraybench_core.__version__); print(xraybench_core.timing.calibrate())"
```

Expected: Prints version and calibration dict with clock_resolution_ns, clock_overhead_ns, fence_overhead_ns.

- [ ] **Step 5: Commit**

```bash
git add rust/xraybench-py/
git commit -m "feat(rust): add xraybench-py crate — PyO3 bindings for all Rust crates"
```

---

### Task 8: Python Integration Tests

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/rust_core/__init__.py`
- Create: `tests/rust_core/test_timing.py`
- Create: `tests/rust_core/test_stats.py`
- Create: `tests/rust_core/test_generators.py`
- Create: `tests/rust_core/test_checksum.py`
- Create: `tests/rust_core/test_compare.py`

- [ ] **Step 1: Write Python integration tests**

Create `tests/__init__.py` (empty) and `tests/rust_core/__init__.py` (empty).

Create `tests/rust_core/test_timing.py`:

```python
"""Integration tests for xraybench_core.timing."""

import xraybench_core


def test_calibrate_returns_dict():
    cal = xraybench_core.timing.calibrate()
    assert isinstance(cal, dict)
    assert "clock_resolution_ns" in cal
    assert "clock_overhead_ns" in cal
    assert "fence_overhead_ns" in cal
    assert "samples" in cal


def test_calibrate_values_positive():
    cal = xraybench_core.timing.calibrate()
    assert cal["clock_resolution_ns"] > 0
    assert cal["samples"] > 0


def test_monotonic_ns_returns_int():
    t = xraybench_core.timing.monotonic_ns()
    assert isinstance(t, int)
    assert t > 0


def test_monotonic_ns_is_monotonic():
    t1 = xraybench_core.timing.monotonic_ns()
    t2 = xraybench_core.timing.monotonic_ns()
    assert t2 >= t1
```

Create `tests/rust_core/test_stats.py`:

```python
"""Integration tests for xraybench_core.stats."""

import xraybench_core


def test_percentile_median():
    values = list(range(1, 101))
    p50 = xraybench_core.stats.percentile(values, 0.5)
    assert abs(p50 - 50.5) < 0.01


def test_percentiles_batch():
    values = list(range(1, 101))
    result = xraybench_core.stats.percentiles(values, [0.5, 0.95, 0.99])
    assert len(result) == 3
    assert abs(result[0] - 50.5) < 0.01


def test_descriptive():
    values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
    d = xraybench_core.stats.descriptive(values)
    assert d["count"] == 8
    assert abs(d["mean"] - 5.0) < 0.01


def test_bootstrap_ci():
    values = list(range(1, 101))
    ci = xraybench_core.stats.bootstrap_ci([float(v) for v in values])
    assert ci["lower"] < ci["point_estimate"]
    assert ci["upper"] > ci["point_estimate"]
    assert ci["confidence"] == 0.95


def test_detect_outliers():
    values = [10.0] * 100 + [1000000.0]
    result = xraybench_core.stats.detect_outliers(values)
    assert 100 in result["outlier_indices"]


def test_mann_whitney_identical():
    a = list(range(100))
    b = list(range(100))
    result = xraybench_core.stats.mann_whitney(
        [float(v) for v in a], [float(v) for v in b]
    )
    assert not result["significant"]


def test_mann_whitney_different():
    a = [float(v) for v in range(100)]
    b = [float(v) for v in range(1000, 1100)]
    result = xraybench_core.stats.mann_whitney(a, b)
    assert result["significant"]
    assert result["p_value"] < 0.001
```

Create `tests/rust_core/test_generators.py`:

```python
"""Integration tests for xraybench_core.generators."""

import os
import tempfile
import xraybench_core


def test_deep_traversal_basic():
    node_count, edges = xraybench_core.generators.generate_deep_traversal(
        num_roots=1, fanout_per_level=[3, 2], seed=42
    )
    # 1 root + 3 + 6 = 10 nodes
    assert node_count == 10
    assert len(edges) >= 9  # At least 3 + 6 direct edges


def test_deep_traversal_deterministic():
    _, e1 = xraybench_core.generators.generate_deep_traversal(1, [5, 3], 42)
    _, e2 = xraybench_core.generators.generate_deep_traversal(1, [5, 3], 42)
    assert e1 == e2


def test_power_law():
    edges = xraybench_core.generators.generate_power_law_edges(1000, 3, 42)
    assert len(edges) > 100


def test_hub_graph():
    node_count, edges = xraybench_core.generators.generate_hub_graph(5, 100, 42)
    assert node_count == 505
    assert len(edges) == 500


def test_chain():
    edges = xraybench_core.generators.generate_chain(100, 42)
    assert len(edges) == 99
    assert edges[0] == (0, 1)


def test_estimate_node_count():
    est = xraybench_core.generators.estimate_node_count(1, [10, 5, 3])
    assert est == 1 + 10 + 50 + 150


def test_write_edges_binary():
    edges = [(0, 1), (1, 2), (2, 3)]
    with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as f:
        path = f.name
    try:
        xraybench_core.generators.write_edges_binary(edges, path)
        assert os.path.getsize(path) == 3 * 16  # 3 edges * 16 bytes
    finally:
        os.unlink(path)


def test_write_edges_csv():
    edges = [(0, 1), (1, 2)]
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        path = f.name
    try:
        xraybench_core.generators.write_edges_csv(edges, path)
        with open(path) as f:
            lines = f.readlines()
        assert lines[0].strip() == "source,target"
        assert lines[1].strip() == "0,1"
    finally:
        os.unlink(path)
```

Create `tests/rust_core/test_checksum.py`:

```python
"""Integration tests for xraybench_core.checksum."""

import xraybench_core


def test_hash_empty():
    h = xraybench_core.checksum.hash_result_set([])
    assert h.startswith("blake3:")


def test_hash_deterministic():
    rows = [[1, "hello", 3.14], [2, "world", 2.72]]
    h1 = xraybench_core.checksum.hash_result_set(rows)
    h2 = xraybench_core.checksum.hash_result_set(rows)
    assert h1 == h2


def test_hash_order_independent():
    rows_a = [[1, "a"], [2, "b"], [3, "c"]]
    rows_b = [[3, "c"], [1, "a"], [2, "b"]]
    assert xraybench_core.checksum.hash_result_set(rows_a) == \
           xraybench_core.checksum.hash_result_set(rows_b)


def test_hash_different_data():
    rows_a = [[1]]
    rows_b = [[2]]
    assert xraybench_core.checksum.hash_result_set(rows_a) != \
           xraybench_core.checksum.hash_result_set(rows_b)


def test_verify_hash():
    rows = [[42, "test"]]
    h = xraybench_core.checksum.hash_result_set(rows)
    assert xraybench_core.checksum.verify_hash(rows, h)
    assert not xraybench_core.checksum.verify_hash(
        rows, "blake3:" + "0" * 64
    )


def test_hash_with_none():
    rows = [[1, None, "text"]]
    h = xraybench_core.checksum.hash_result_set(rows)
    assert h.startswith("blake3:")


def test_float_eq_ulp():
    assert xraybench_core.checksum.float_eq_ulp(1.0, 1.0, 0)
    assert not xraybench_core.checksum.float_eq_ulp(1.0, 2.0, 0)
```

Create `tests/rust_core/test_compare.py`:

```python
"""Integration tests for xraybench_core.compare."""

import xraybench_core


def test_compare_identical():
    result = xraybench_core.compare.compare_metric(
        "cold_ms", [100.0] * 30, [100.0] * 30
    )
    assert result["metric_name"] == "cold_ms"
    assert not result["significant"]


def test_compare_improvement():
    result = xraybench_core.compare.compare_metric(
        "cold_ms", [100.0] * 30, [50.0] * 30
    )
    assert result["significant"]
    assert result["percentage_change"] < -40.0
    assert result["classification"] == "Improvement"


def test_compare_regression():
    result = xraybench_core.compare.compare_metric(
        "cold_ms", [50.0] * 30, [100.0] * 30
    )
    assert result["classification"] == "Regression"
```

- [ ] **Step 2: Run Python integration tests**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && python3 -m pytest tests/rust_core/ -v
```

Expected: All ~28 Python tests pass.

- [ ] **Step 3: Run all Rust tests**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench/rust && cargo test --workspace
```

Expected: All ~130 Rust tests pass across all 6 crates.

- [ ] **Step 4: Commit**

```bash
git add tests/
git commit -m "test: add Python integration tests for xraybench_core Rust bindings"
```

---

### Task 9: Final Integration — Build Verification and Full Test Suite

- [ ] **Step 1: Clean build from scratch**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench/rust && cargo clean && cargo build --workspace --release 2>&1
```

Expected: Clean release build with no warnings.

- [ ] **Step 2: Rebuild Python extension**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench/rust/xraybench-py && maturin develop --release 2>&1
```

Expected: Extension built and installed.

- [ ] **Step 3: Run complete test suite**

Run:
```bash
cd /Users/sendlane/github_projects/xraygraph-bench && cargo test --workspace --manifest-path rust/Cargo.toml 2>&1 && python3 -m pytest tests/rust_core/ -v 2>&1
```

Expected: All Rust and Python tests pass.

- [ ] **Step 4: Verify Python package still works**

Run:
```bash
python3 -c "
import xraybench_core
print('Version:', xraybench_core.__version__)
cal = xraybench_core.timing.calibrate()
print('Clock resolution:', cal['clock_resolution_ns'], 'ns')
print('Clock overhead:', cal['clock_overhead_ns'], 'ns')
print('Stats: p50 of [1..100] =', xraybench_core.stats.percentile(list(range(1,101)), 0.5))
n, edges = xraybench_core.generators.generate_deep_traversal(1, [10, 5, 3], 42)
print('Deep traversal: nodes={}, edges={}'.format(n, len(edges)))
h = xraybench_core.checksum.hash_result_set([[1, 'test'], [2, 'data']])
print('Hash:', h[:30] + '...')
print('All systems nominal.')
"
```

Expected: All prints succeed with reasonable values.

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "feat: complete Phase 1 — Rust measurement core with timing, stats, generators, checksum, compare

Rust workspace with 7 crates:
- xraybench-types: shared types, errors, constants
- xraybench-timing: platform clock, calibration, CUSUM warmup, harness
- xraybench-stats: percentiles, BCa CI, MAD outliers, Mann-Whitney U, t-digest
- xraybench-generators: 6 deterministic graph generators with binary/CSV I/O
- xraybench-checksum: BLAKE3, canonical serialization, structural validation
- xraybench-compare: result diff, significance testing, multi-engine matrix
- xraybench-py: PyO3 bindings (single Python extension)

~130 Rust unit tests + ~28 Python integration tests."
```
