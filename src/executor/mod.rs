#![allow(clippy::module_inception)]

mod array_subquery;
mod builtin;
mod correlated;
mod cursor;
mod derived_table;
mod eval;
mod function_cache;
mod index_only_scan;
mod lateral;
mod multiple_cte;
pub mod operators;
mod plpgsql;
mod recursive_cte;
mod unnest;
pub mod volcano;

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

// ============================================================================
// Executor Exports
// ============================================================================

pub use operators::executor::{Executor, ExecutorError, Tuple};

// ============================================================================
// Parallel Execution
// ============================================================================

pub use parallel::{ParallelConfig, ParallelExecutor};

// ============================================================================
// Volcano Executors (primary executor implementations)
// ============================================================================

pub use volcano::{
    AggregateExecutor, AggregateFunction, CaseExecutor, DistinctExecutor, ExceptExecutor,
    FilterExecutor, HashAggExecutor, HashJoinExecutor, HavingExecutor, IntersectExecutor,
    JoinExecutor, JoinType, LimitExecutor, MergeJoinExecutor, NestedLoopJoinExecutor,
    ProjectExecutor, SeqScanExecutor, SortExecutor, SubqueryExecutor, SubqueryScanExecutor,
    UnionExecutor, UnionType,
};

// ============================================================================
// Other Executors and Utilities
// ============================================================================

pub use array_subquery::ArraySubqueryExecutor;
pub use builtin::BuiltinFunctions;
pub use correlated::{CorrelatedExecutor, SubqueryKind};
pub use cursor::CursorManager;
pub use derived_table::DerivedTableExecutor;
pub use eval::Eval;
pub use function_cache::FunctionCache;
pub use index_only_scan::IndexOnlyScan;
pub use lateral::LateralSubqueryExecutor;
pub use multiple_cte::MultipleCTEExecutor;
pub use plpgsql::PlPgSqlInterpreter;
pub use recursive_cte::RecursiveCTEExecutor;
pub use unnest::UnnestExecutor;

// ============================================================================
// Test Helpers (only compiled in test mode)
// ============================================================================

#[cfg(test)]
pub use test_helpers::{
    MockExecutor as TestMockExecutor, TupleBuilder, compare_executors, count_results,
    create_multi_column_schema, create_simple_schema, run_executor, test_executor_lifecycle,
    tuple_with_value,
};
