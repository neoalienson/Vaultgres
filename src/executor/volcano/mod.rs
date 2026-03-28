//! Volcano-style executors (pull-based, single next() method)
//!
//! This module contains executors that follow the Volcano model,
//! where each executor only implements `next()` and buffers data
//! in the constructor as needed.

mod aggregate;
mod aggregate_state;
mod case;
mod cte;
mod distinct;
mod except;
mod filter;
mod group_by;
mod hash_agg;
mod hash_join;
mod hashing;
mod having;
mod intersect;
mod join;
mod limit;
mod merge_join;
mod nested_loop_join;
mod project;
mod seq_scan;
mod sort;
mod sql_function;
mod subquery;
mod subquery_scan;
mod union;
mod window;

pub use aggregate::{AggregateExecutor, AggregateFunction};
pub use aggregate_state::{AggregateState, CustomAggregateState, hash_value};
pub use case::CaseExecutor;
pub use cte::{
    CTEColumns, CTEExecutor, CTEPlanner, VolcanoRecursiveCTEExecutor, VolcanoRecursiveCTEState,
};
pub use distinct::DistinctExecutor;
pub use except::ExceptExecutor;
pub use filter::FilterExecutor;
pub use group_by::GroupByExecutor;
pub use hash_agg::HashAggExecutor;
pub use hash_join::HashJoinExecutor;
pub use hashing::compute_group_key;
pub use having::HavingExecutor;
pub use intersect::IntersectExecutor;
pub use join::{JoinExecutor, JoinType};
pub use limit::LimitExecutor;
pub use merge_join::MergeJoinExecutor;
pub use nested_loop_join::NestedLoopJoinExecutor;
pub use project::ProjectExecutor;
// Re-export SeqScanExecutor from operators for compatibility with planner
pub use crate::executor::operators::seq_scan::SeqScanExecutor;
pub use sort::SortExecutor;
pub use sql_function::SqlFunctionExecutor;
pub use subquery::SubqueryExecutor;
pub use subquery_scan::SubqueryScanExecutor;
pub use union::{UnionExecutor, UnionType};
pub use window::{WindowExecutor, create_window_info};
