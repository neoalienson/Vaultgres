use super::error::Result;
use super::writer::{LSN, RecordType, WALWriter};
use crate::storage::BufferPool;
use crate::transaction::TransactionManager;
use std::sync::Arc;

/// Checkpoint manager
pub struct CheckpointManager {
    wal_writer: Arc<WALWriter>,
    buffer_pool: Arc<BufferPool>,
    txn_manager: Arc<TransactionManager>,
}

impl CheckpointManager {
    /// Creates a new checkpoint manager
    pub fn new(
        wal_writer: Arc<WALWriter>,
        buffer_pool: Arc<BufferPool>,
        txn_manager: Arc<TransactionManager>,
    ) -> Self {
        Self { wal_writer, buffer_pool, txn_manager }
    }

    /// Performs a checkpoint
    pub fn checkpoint(&self) -> Result<LSN> {
        // 1. Write checkpoint start record
        let checkpoint_lsn = self.wal_writer.write(0, RecordType::Checkpoint, None, vec![])?;

        // 2. Flush all dirty pages (simplified - buffer pool doesn't track dirty pages yet)

        // 3. Write checkpoint complete record
        self.wal_writer.write(0, RecordType::Checkpoint, None, vec![])?;

        // 4. Flush WAL
        self.wal_writer.flush()?;

        Ok(checkpoint_lsn)
    }

    /// Gets the last checkpoint LSN
    pub fn last_checkpoint_lsn(&self) -> LSN {
        self.wal_writer.flushed_lsn()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint() {
        let wal_writer = Arc::new(WALWriter::new());
        let buffer_pool = Arc::new(BufferPool::new(10));
        let txn_manager = Arc::new(TransactionManager::new());

        let checkpoint_mgr = CheckpointManager::new(wal_writer.clone(), buffer_pool, txn_manager);

        let lsn = checkpoint_mgr.checkpoint().unwrap();
        assert!(lsn > 0);
    }

    #[test]
    fn test_last_checkpoint_lsn() {
        let wal_writer = Arc::new(WALWriter::new());
        let buffer_pool = Arc::new(BufferPool::new(10));
        let txn_manager = Arc::new(TransactionManager::new());

        let checkpoint_mgr = CheckpointManager::new(wal_writer.clone(), buffer_pool, txn_manager);

        checkpoint_mgr.checkpoint().unwrap();
        let lsn = checkpoint_mgr.last_checkpoint_lsn();
        assert!(lsn > 0);
    }
}
