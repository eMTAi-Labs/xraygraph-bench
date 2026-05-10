pub mod diff;
pub mod matrix;
pub mod significance;

pub use diff::{diff_results, format_comparison};
pub use matrix::{pairwise_matrix, PairwiseComparison};
pub use significance::classify_change;
