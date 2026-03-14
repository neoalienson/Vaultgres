//! Write-Ahead Logging (WAL) and recovery.
//!
//! This module provides:
//! - WAL record format
//! - WAL writing and flushing
//! - ARIES recovery protocol
//! - Checkpoint mechanism

pub mod checkpoint;
pub mod disk;
pub mod error;
pub mod recovery;
pub mod writer;

pub use checkpoint::CheckpointManager;
pub use disk::WALDiskWriter;
pub use error::{Result, WALError};
pub use recovery::{RecoveryManager, RecoveryState};
pub use writer::{LSN, RecordType, WALRecord, WALWriter};

#[cfg(test)]
mod edge_tests;
