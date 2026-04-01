pub mod diff;
pub mod significance;
pub mod matrix;

pub use diff::{diff_results, format_comparison};
pub use matrix::{pairwise_matrix, PairwiseComparison};
pub use significance::classify_change;
