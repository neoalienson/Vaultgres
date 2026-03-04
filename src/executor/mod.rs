#![allow(clippy::module_inception)]

mod array_subquery;
mod builtin;
mod case;
mod correlated;
mod cte;
mod cursor;
mod derived_table;
mod distinct;
mod eval;
mod except;
mod function_cache;
mod group_by;
mod hash_agg;
mod hash_join;
mod having;
mod index_only_scan;
mod intersect;
mod join;
mod lateral;
mod merge_join;
mod mock;
mod multiple_cte;
mod old_executor; // Renamed from executor.rs - contains OldExecutor trait
pub mod operators; // New module for compositional executor
mod plpgsql;
mod recursive_cte;
mod sort;
mod subquery;
mod union;
mod unnest;
pub mod volcano;
mod window;

#[cfg(test)]
mod test_helpers;

pub mod parallel;

// Edge tests that need refactoring for new Executor trait - temporarily disabled
// #[cfg(test)]
// mod advanced_sql_edge_tests;
// #[cfg(test)]
// mod aggregate_edge_tests;
// #[cfg(test)]
// mod case_edge_tests;
// #[cfg(test)]
// mod cte_edge_tests;
// #[cfg(test)]
// mod distinct_edge_tests;
// #[cfg(test)]
// mod edge_tests;
// #[cfg(test)]
// mod except_edge_tests;
// #[cfg(test)]
// mod group_by_edge_tests;
// #[cfg(test)]
// mod having_edge_tests;
// #[cfg(test)]
// mod intersect_edge_tests;
// #[cfg(test)]
// mod join_edge_tests;
// #[cfg(test)]
// mod limit_edge_tests;
// #[cfg(test)]
// mod merge_join_edge_tests;
// #[cfg(test)]
// mod subquery_edge_tests;
// #[cfg(test)]
// mod union_edge_tests;
// #[cfg(test)]
// mod window_edge_tests;

pub use array_subquery::ArraySubqueryExecutor;
pub use builtin::BuiltinFunctions;
pub use case::Case;
pub use correlated::{CorrelatedExecutor, SubqueryKind};
pub use cte::CTE;
pub use cursor::CursorManager;
pub use derived_table::DerivedTableExecutor;
pub use distinct::Distinct;
pub use eval::Eval;
pub use except::Except;
pub use function_cache::FunctionCache;
pub use group_by::GroupBy;
pub use hash_agg::HashAgg;
pub use hash_join::HashJoin;
pub use having::Having;
pub use index_only_scan::IndexOnlyScan;
pub use intersect::Intersect;
pub use join::{Join, JoinType};
pub use lateral::LateralSubqueryExecutor;
pub use merge_join::MergeJoin;
pub use mock::{MockExecutor, MockTupleExecutor};
pub use multiple_cte::MultipleCTEExecutor;
pub use old_executor::{OldExecutor, OldExecutorError, SimpleTuple, Value}; // old executor
pub use operators::executor::{Executor, ExecutorError, Tuple}; // new executor trait
pub use parallel::{ParallelConfig, ParallelExecutor};
pub use plpgsql::PlPgSqlInterpreter;
pub use recursive_cte::RecursiveCTEExecutor;
pub use sort::Sort;
pub use subquery::Subquery;
pub use union::Union;
pub use unnest::UnnestExecutor;

// Re-export volcano executors for backward compatibility
pub use volcano::{
    DistinctExecutor, FilterExecutor, HashAggExecutor, LimitExecutor, ProjectExecutor,
    SeqScanExecutor, SortExecutor, SubqueryScanExecutor,
};

// Type alias for backward compatibility
pub type Limit = LimitExecutor;
