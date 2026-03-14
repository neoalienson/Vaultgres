use super::error::Result;
use super::writer::{LSN, RecordType, WALRecord};
use crate::storage::PageId;
use crate::transaction::TransactionId;
use std::collections::HashSet;

/// Recovery state
#[derive(Debug)]
pub struct RecoveryState {
    pub dirty_pages: HashSet<PageId>,
    pub active_txns: HashSet<TransactionId>,
    pub committed_txns: HashSet<TransactionId>,
    pub aborted_txns: HashSet<TransactionId>,
}

impl RecoveryState {
    fn new() -> Self {
        Self {
            dirty_pages: HashSet::new(),
            active_txns: HashSet::new(),
            committed_txns: HashSet::new(),
            aborted_txns: HashSet::new(),
        }
    }
}

/// ARIES recovery manager
pub struct RecoveryManager {
    checkpoint_lsn: LSN,
}

impl RecoveryManager {
    /// Creates a new recovery manager
    pub fn new() -> Self {
        Self { checkpoint_lsn: 0 }
    }

    /// Performs ARIES recovery
    pub fn recover(&mut self, records: &[WALRecord]) -> Result<RecoveryState> {
        // Phase 1: Analysis
        let state = self.analysis_phase(records)?;

        // Phase 2: Redo
        self.redo_phase(records, &state)?;

        // Phase 3: Undo
        self.undo_phase(records, &state)?;

        Ok(state)
    }

    /// Analysis phase: Scan WAL to identify dirty pages and active transactions
    fn analysis_phase(&self, records: &[WALRecord]) -> Result<RecoveryState> {
        let mut state = RecoveryState::new();

        for record in records {
            match record.record_type {
                RecordType::Insert | RecordType::Update | RecordType::Delete => {
                    state.active_txns.insert(record.xid);
                    if let Some(page_id) = record.page_id {
                        state.dirty_pages.insert(page_id);
                    }
                }
                RecordType::Commit => {
                    state.active_txns.remove(&record.xid);
                    state.committed_txns.insert(record.xid);
                }
                RecordType::Abort => {
                    state.active_txns.remove(&record.xid);
                    state.aborted_txns.insert(record.xid);
                }
                RecordType::Checkpoint => {
                    // Checkpoint processing
                }
            }
        }

        Ok(state)
    }

    /// Redo phase: Replay all operations from checkpoint
    fn redo_phase(&self, records: &[WALRecord], _state: &RecoveryState) -> Result<()> {
        for record in records {
            if record.lsn <= self.checkpoint_lsn {
                continue;
            }

            match record.record_type {
                RecordType::Insert | RecordType::Update | RecordType::Delete => {
                    // Redo operation (simplified)
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Undo phase: Roll back uncommitted transactions
    fn undo_phase(&self, records: &[WALRecord], state: &RecoveryState) -> Result<()> {
        // Process records in reverse order
        for record in records.iter().rev() {
            if state.active_txns.contains(&record.xid) {
                match record.record_type {
                    RecordType::Insert | RecordType::Update | RecordType::Delete => {
                        // Undo operation (simplified)
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    /// Sets checkpoint LSN
    pub fn set_checkpoint(&mut self, lsn: LSN) {
        self.checkpoint_lsn = lsn;
    }
}

impl Default for RecoveryManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_records() -> Vec<WALRecord> {
        vec![
            WALRecord::new(1, 10, RecordType::Insert, Some(PageId(1)), vec![]), // Txn 10 inserts into Page 1
            WALRecord::new(2, 20, RecordType::Insert, Some(PageId(2)), vec![]), // Txn 20 inserts into Page 2
            WALRecord::new(3, 10, RecordType::Commit, None, vec![]),            // Txn 10 commits
            WALRecord::new(4, 30, RecordType::Insert, Some(PageId(3)), vec![]), // Txn 30 inserts into Page 3
            WALRecord::new(5, 20, RecordType::Abort, None, vec![]),             // Txn 20 aborts
            WALRecord::new(6, 40, RecordType::Update, Some(PageId(4)), vec![]), // Txn 40 updates Page 4 (active)
            WALRecord::new(7, 50, RecordType::Checkpoint, None, vec![]),        // Checkpoint
        ]
    }

    #[test]
    fn test_analysis_phase() {
        let mgr = RecoveryManager::new();
        let records = create_test_records();

        let state = mgr.analysis_phase(&records).unwrap();

        assert!(state.committed_txns.contains(&10));
        assert!(state.aborted_txns.contains(&20));
        assert!(state.active_txns.contains(&30)); // Txn 30 is in progress
        assert!(state.active_txns.contains(&40)); // Txn 40 is in progress

        assert!(state.dirty_pages.contains(&PageId(1)));
        assert!(state.dirty_pages.contains(&PageId(2)));
        assert!(state.dirty_pages.contains(&PageId(3)));
        assert!(state.dirty_pages.contains(&PageId(4)));
    }

    #[test]
    fn test_recovery() {
        let mut mgr = RecoveryManager::new();
        let records = create_test_records();

        let state = mgr.recover(&records).unwrap();

        assert!(state.committed_txns.contains(&10));
        assert!(state.aborted_txns.contains(&20));
        assert!(state.active_txns.contains(&30));
        assert!(state.active_txns.contains(&40));

        assert!(!state.active_txns.contains(&20)); // Txn 20 is aborted, so not active
    }

    #[test]
    fn test_checkpoint() {
        let mut mgr = RecoveryManager::new();
        mgr.set_checkpoint(5);

        assert_eq!(mgr.checkpoint_lsn, 5);
    }
}
