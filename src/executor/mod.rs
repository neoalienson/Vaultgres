#![allow(clippy::module_inception)]

mod aggregate;
mod array_subquery;
mod builtin;
mod case;
mod correlated;
mod cte;
mod cursor;
mod derived_table;
mod distinct;
mod except;
mod executor;
mod filter;
mod function_cache;
mod group_by;
mod hash_agg;
mod hash_join;
mod having;
mod intersect;
mod join;
mod lateral;
mod limit;
mod merge_join;
mod mock;
mod multiple_cte;
mod nested_loop;
mod plpgsql;
mod project;
mod recursive_cte;
mod seq_scan;
mod sort;
mod subquery;
mod table_function;
mod union;
mod unnest;
mod window;

pub mod parallel;

#[cfg(test)]
mod advanced_sql_edge_tests;
#[cfg(test)]
mod aggregate_edge_tests;
#[cfg(test)]
mod case_edge_tests;
#[cfg(test)]
mod cte_edge_tests;
#[cfg(test)]
mod distinct_edge_tests;
#[cfg(test)]
mod edge_tests;
#[cfg(test)]
mod except_edge_tests;
#[cfg(test)]
mod group_by_edge_tests;
#[cfg(test)]
mod having_edge_tests;
#[cfg(test)]
mod intersect_edge_tests;
#[cfg(test)]
mod join_edge_tests;
#[cfg(test)]
mod limit_edge_tests;
#[cfg(test)]
mod merge_join_edge_tests;
#[cfg(test)]
mod subquery_edge_tests;
#[cfg(test)]
mod union_edge_tests;
#[cfg(test)]
mod window_edge_tests;

pub use aggregate::{Aggregate, AggregateFunction};
pub use array_subquery::ArraySubqueryExecutor;
pub use builtin::BuiltinFunctions;
pub use case::Case;
pub use correlated::{CorrelatedExecutor, SubqueryKind};
pub use cte::CTE;
pub use cursor::CursorManager;
pub use derived_table::DerivedTableExecutor;
pub use distinct::Distinct;
pub use except::Except;
pub use executor::{Executor, ExecutorError, SimpleExecutor, SimpleTuple, Tuple, Value};
pub use filter::Filter;
pub use function_cache::FunctionCache;
pub use group_by::GroupBy;
pub use hash_agg::HashAgg;
pub use hash_join::HashJoin;
pub use having::Having;
pub use intersect::Intersect;
pub use join::{Join, JoinType};
pub use lateral::LateralSubqueryExecutor;
pub use limit::Limit;
pub use merge_join::MergeJoin;
pub use mock::{MockExecutor, MockTupleExecutor};
pub use multiple_cte::MultipleCTEExecutor;
pub use nested_loop::NestedLoopJoin;
pub use parallel::{ParallelConfig, ParallelExecutor};
pub use plpgsql::PlPgSqlInterpreter;
pub use project::Project;
pub use recursive_cte::RecursiveCTEExecutor;
pub use seq_scan::SeqScan;
pub use sort::Sort;
pub use subquery::Subquery;
pub use table_function::{SetReturningFunctionExecutor, TableValuedFunctionExecutor};
pub use union::Union;
pub use unnest::UnnestExecutor;
