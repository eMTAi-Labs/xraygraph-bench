use serde::{Deserialize, Serialize};
use std::fmt;

// ── Version ──────────────────────────────────────────────────────────────────

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// ── Phase ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    Cold,
    WarmUp,
    SteadyState,
}

// ── Measurement ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Measurement {
    pub timestamp_ns: u64,
    pub duration_ns: u64,
    pub clock_resolution_ns: u64,
    pub clock_overhead_ns: u64,
    pub fence_overhead_ns: u64,
    pub iteration: u32,
    pub phase: Phase,
}

// ── CalibrationResult ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationResult {
    pub clock_resolution_ns: u64,
    pub clock_overhead_ns: u64,
    pub fence_overhead_ns: u64,
    pub samples: u32,
}

// ── StatsSummary ──────────────────────────────────────────────────────────────

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

// ── Outcome ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

// ── CorrectnessResult ─────────────────────────────────────────────────────────

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

// ── Graph primitives ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Edge {
    pub source: u64,
    pub target: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: u64,
    pub properties: Vec<PropertyValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropertyValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
}

// ── MetricComparison / ChangeClass ────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeClass {
    Improvement,
    Regression,
    NoChange,
    Inconclusive,
}

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

// ── BenchError ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum BenchError {
    ClockUnavailable(String),
    CalibrationFailed(String),
    InsufficientSamples { needed: usize, got: usize },
    IoError(String),
    InvalidData(String),
}

impl fmt::Display for BenchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BenchError::ClockUnavailable(msg) => {
                write!(f, "clock unavailable: {msg}")
            }
            BenchError::CalibrationFailed(msg) => {
                write!(f, "calibration failed: {msg}")
            }
            BenchError::InsufficientSamples { needed, got } => {
                write!(f, "insufficient samples: needed {needed}, got {got}")
            }
            BenchError::IoError(msg) => {
                write!(f, "I/O error: {msg}")
            }
            BenchError::InvalidData(msg) => {
                write!(f, "invalid data: {msg}")
            }
        }
    }
}

impl std::error::Error for BenchError {}

// ── Result alias ──────────────────────────────────────────────────────────────

pub type Result<T> = std::result::Result<T, BenchError>;

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measurement_serialization_roundtrip() {
        let m = Measurement {
            timestamp_ns: 1_000_000,
            duration_ns: 42_000,
            clock_resolution_ns: 100,
            clock_overhead_ns: 50,
            fence_overhead_ns: 25,
            iteration: 7,
            phase: Phase::SteadyState,
        };
        let json = serde_json::to_string(&m).expect("serialize");
        let m2: Measurement = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(m.timestamp_ns, m2.timestamp_ns);
        assert_eq!(m.duration_ns, m2.duration_ns);
        assert_eq!(m.iteration, m2.iteration);
        assert_eq!(m.phase, m2.phase);
    }

    #[test]
    fn stats_summary_serialization_roundtrip() {
        let s = StatsSummary {
            count: 1000,
            mean_ns: 1234.5,
            median_ns: 1200.0,
            min_ns: 800,
            max_ns: 2000,
            p50_ns: 1200.0,
            p95_ns: 1900.0,
            p99_ns: 1980.0,
            stddev_ns: 120.3,
            ci_lower_ns: 1230.0,
            ci_upper_ns: 1238.0,
            ci_confidence: 0.95,
            mad_ns: 50.0,
            outlier_count: 3,
            warmup_iterations: 100,
        };
        let json = serde_json::to_string(&s).expect("serialize");
        let s2: StatsSummary = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(s.count, s2.count);
        assert_eq!(s.min_ns, s2.min_ns);
        assert_eq!(s.max_ns, s2.max_ns);
        assert_eq!(s.ci_confidence, s2.ci_confidence);
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
        let variants: Vec<PropertyValue> = vec![
            PropertyValue::Null,
            PropertyValue::Integer(-42),
            PropertyValue::Float(3.14),
            PropertyValue::Text("hello".to_string()),
            PropertyValue::Boolean(true),
        ];
        for v in &variants {
            let json = serde_json::to_string(v).expect("serialize");
            let v2: PropertyValue = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(*v, v2);
        }
    }

    #[test]
    fn outcome_variants() {
        let outcomes = vec![
            Outcome::Success,
            Outcome::CorrectnessMismatch,
            Outcome::EngineError,
            Outcome::Timeout,
            Outcome::Unsupported,
            Outcome::DatasetVerificationFailed,
            Outcome::HarnessFailure,
            Outcome::ConnectionFailure,
            Outcome::OutOfMemory,
        ];
        for o in &outcomes {
            let json = serde_json::to_string(o).expect("serialize");
            assert!(!json.is_empty());
        }
    }

    #[test]
    fn error_display() {
        let e1 = BenchError::ClockUnavailable("HPET missing".to_string());
        assert!(e1.to_string().contains("HPET missing"));

        let e2 = BenchError::CalibrationFailed("variance too high".to_string());
        assert!(e2.to_string().contains("variance too high"));

        let e3 = BenchError::InsufficientSamples { needed: 100, got: 12 };
        let msg = e3.to_string();
        assert!(msg.contains("100"));
        assert!(msg.contains("12"));

        let e4 = BenchError::IoError("permission denied".to_string());
        assert!(e4.to_string().contains("permission denied"));

        let e5 = BenchError::InvalidData("NaN in column".to_string());
        assert!(e5.to_string().contains("NaN in column"));
    }

    #[test]
    fn version_is_set() {
        assert!(!VERSION.is_empty());
    }
}
