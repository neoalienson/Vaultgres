//! Write-Ahead Logging (WAL) and recovery.
//!
//! This module provides:
//! - WAL record format
//! - WAL writing and flushing
//! - ARIES recovery protocol
//! - Checkpoint mechanism

pub mod error;
pub mod writer;
pub mod recovery;
pub mod checkpoint;
pub mod disk;

pub use error::{WALError, Result};
pub use writer::{WALWriter, WALRecord, RecordType, LSN};
pub use recovery::{RecoveryManager, RecoveryState};
pub use checkpoint::CheckpointManager;
pub use disk::WALDiskWriter;
