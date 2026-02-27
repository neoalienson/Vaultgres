//! RustGres - A PostgreSQL-compatible RDBMS written in Rust
//!
//! # Architecture
//!
//! RustGres is organized into layers:
//! - Storage: Page-based storage, buffer pool, indexes
//! - Transaction: MVCC, locking, snapshots
//! - WAL: Write-ahead logging and recovery
//! - Parser: SQL parsing
//! - Executor: Query execution engine
//! - Protocol: PostgreSQL wire protocol

pub mod storage;
pub mod transaction;
pub mod wal;
pub mod parser;
pub mod executor;
pub mod protocol;
pub mod config;
pub mod statistics;

pub use storage::{BufferPool, Page, PageId, StorageError};
pub use transaction::{TransactionManager, Transaction, TransactionId};
pub use wal::{WALWriter, WALRecord, RecoveryManager};
pub use parser::{Parser, Statement};
pub use executor::{Executor, ExecutorError, Tuple, SeqScan, Filter, Project, NestedLoopJoin};
pub use protocol::{Server, Connection, Message, Response};
pub use config::Config;
