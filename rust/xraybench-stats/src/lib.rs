pub mod bootstrap;
pub mod outlier;
pub mod percentile;
pub mod regression;
pub mod tdigest;

pub use bootstrap::{bca_mean_ci, BootstrapCI, BootstrapConfig};
pub use outlier::{detect_outliers, mad, OutlierResult, DEFAULT_THRESHOLD};
pub use percentile::{descriptive, exact_percentile, exact_percentiles, DescriptiveStats};
pub use regression::{compare_metric, mann_whitney_u, MannWhitneyResult};
pub use tdigest::TDigest;
