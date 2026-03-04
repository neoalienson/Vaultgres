//! Volcano-style query executors
//!
//! This module contains the core executors that follow the Volcano model,
//! where each operator implements the Executor trait and pulls tuples from
//! its children on demand.

pub use distinct::DistinctExecutor;
pub use hash_agg::HashAggExecutor;
pub use limit::LimitExecutor;
pub use sort::SortExecutor;

mod distinct;
mod hash_agg;
mod limit;
mod sort;

// Re-export commonly used types
pub use crate::executor::operators::filter::FilterExecutor;
pub use crate::executor::operators::project::ProjectExecutor;
pub use crate::executor::operators::seq_scan::SeqScanExecutor;
pub use crate::executor::operators::subquery_scan::SubqueryScanExecutor;
