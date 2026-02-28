pub mod collector;
pub mod histogram;
pub mod error;

#[cfg(test)]
mod edge_tests;

pub use collector::{Analyzer, TableStats, ColumnStats};
pub use histogram::Histogram;
pub use error::{StatisticsError, Result};
