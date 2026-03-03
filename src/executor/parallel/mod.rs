pub mod config;
pub mod coordinator;
pub mod hash_agg;
pub mod hash_join;
pub mod morsel;
pub mod operator;
pub mod partition;
pub mod seq_scan;
pub mod sort;
pub mod work_stealing;
pub mod worker_pool;

pub use config::{ParallelConfig, ParallelExecutor};
pub use work_stealing::{WorkStealingExecutor, WorkStealingScheduler};

#[cfg(test)]
mod parallel_edge_tests;
