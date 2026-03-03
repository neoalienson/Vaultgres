//! VaultGres - A PostgreSQL-compatible RDBMS written in Rust
//!
//! # Architecture
//!
//! VaultGres is organized into layers:
//! - Storage: Page-based storage, buffer pool, indexes
//! - Transaction: MVCC, locking, snapshots
//! - WAL: Write-ahead logging and recovery
//! - Parser: SQL parsing
//! - Executor: Query execution engine
//! - Protocol: PostgreSQL wire protocol

pub mod catalog;
pub mod config;
pub mod executor;
pub mod lock_monitor;
pub mod metrics;
pub mod optimizer;
pub mod parser;
pub mod prepared;
pub mod protocol;
pub mod query_stats;
pub mod slow_query_log;
pub mod statistics;
pub mod storage;
pub mod transaction;
pub mod wal;

#[cfg(test)]
mod config_edge_tests;

pub use config::Config;
pub use executor::{Executor, ExecutorError, Filter, NestedLoopJoin, Project, SeqScan, Tuple};
pub use parser::{Parser, Statement};
pub use prepared::PreparedStatementManager;
pub use protocol::{Connection, Message, Response, Server};
pub use storage::{BufferPool, Page, PageId, StorageError};
pub use transaction::{Transaction, TransactionId, TransactionManager};
pub use wal::{RecoveryManager, WALRecord, WALWriter};
