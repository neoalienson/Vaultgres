//! Transaction management with MVCC.
//!
//! This module provides:
//! - Transaction ID generation
//! - MVCC tuple visibility
//! - Snapshot isolation
//! - Basic lock manager

pub mod error;
pub mod lock;
pub mod manager;
pub mod mvcc;
pub mod snapshot;

pub use error::{Result, TransactionError};
pub use lock::{LockKey, LockManager, LockMode};
pub use manager::{
    IsolationLevel, Transaction, TransactionId, TransactionManager, TransactionState,
};
pub use mvcc::TupleHeader;
pub use snapshot::Snapshot;

#[cfg(test)]
mod edge_tests;
