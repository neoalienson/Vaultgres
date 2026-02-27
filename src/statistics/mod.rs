pub mod collector;
pub mod histogram;
pub mod error;

pub use collector::{Analyzer, TableStats, ColumnStats};
pub use histogram::Histogram;
pub use error::{StatisticsError, Result};
