use super::error::{Result, TransactionError};
use super::snapshot::Snapshot;
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Transaction ID type
pub type TransactionId = u64;

/// Special transaction IDs
pub const INVALID_XID: TransactionId = 0;
pub const FROZEN_XID: TransactionId = 1;
pub const FIRST_NORMAL_XID: TransactionId = 2;

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    InProgress,
    Committed,
    Aborted,
}

/// Transaction isolation level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// Transaction context
pub struct Transaction {
    pub xid: TransactionId,
    pub snapshot: Snapshot,
    pub state: TransactionState,
    pub isolation_level: IsolationLevel,
}

impl From<crate::parser::ast::IsolationLevel> for IsolationLevel {
    fn from(level: crate::parser::ast::IsolationLevel) -> Self {
        match level {
            crate::parser::ast::IsolationLevel::ReadCommitted => IsolationLevel::ReadCommitted,
            crate::parser::ast::IsolationLevel::RepeatableRead => IsolationLevel::RepeatableRead,
            crate::parser::ast::IsolationLevel::Serializable => IsolationLevel::Serializable,
        }
    }
}

/// Transaction manager
pub struct TransactionManager {
    next_xid: Arc<AtomicU64>,
    active_txns: Arc<DashMap<TransactionId, TransactionState>>,
    default_isolation: IsolationLevel,
}

impl TransactionManager {
    /// Creates a new transaction manager
    pub fn new() -> Self {
        Self {
            next_xid: Arc::new(AtomicU64::new(FIRST_NORMAL_XID)),
            active_txns: Arc::new(DashMap::new()),
            default_isolation: IsolationLevel::ReadCommitted,
        }
    }

    /// Begins a new transaction with default isolation level
    pub fn begin(&self) -> Transaction {
        self.begin_with_isolation(self.default_isolation)
    }

    /// Begins a new transaction with specified isolation level
    pub fn begin_with_isolation(&self, isolation_level: IsolationLevel) -> Transaction {
        let xid = self.next_xid.fetch_add(1, Ordering::SeqCst);
        self.active_txns.insert(xid, TransactionState::InProgress);

        let snapshot = match isolation_level {
            IsolationLevel::ReadCommitted => Snapshot::new(xid, xid + 1, vec![]),
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => self.get_snapshot(),
        };

        log::debug!("Transaction {} started with {:?}", xid, isolation_level);

        Transaction { xid, snapshot, state: TransactionState::InProgress, isolation_level }
    }

    /// Commits a transaction
    pub fn commit(&self, xid: TransactionId) -> Result<()> {
        let mut entry = self.active_txns.get_mut(&xid).ok_or(TransactionError::NotFound(xid))?;

        if *entry == TransactionState::Committed {
            return Err(TransactionError::AlreadyCommitted(xid));
        }

        if *entry == TransactionState::Aborted {
            return Err(TransactionError::AlreadyAborted(xid));
        }

        *entry = TransactionState::Committed;
        log::debug!("Transaction {} committed", xid);
        Ok(())
    }

    /// Aborts a transaction
    pub fn abort(&self, xid: TransactionId) -> Result<()> {
        let mut entry = self.active_txns.get_mut(&xid).ok_or(TransactionError::NotFound(xid))?;

        *entry = TransactionState::Aborted;
        log::debug!("Transaction {} aborted", xid);
        Ok(())
    }

    /// Gets current snapshot
    pub fn get_snapshot(&self) -> Snapshot {
        let xmax = self.next_xid.load(Ordering::SeqCst);

        let active: Vec<TransactionId> = self
            .active_txns
            .iter()
            .filter(|entry| *entry.value() == TransactionState::InProgress)
            .map(|entry| *entry.key())
            .filter(|&xid| xid < xmax)
            .collect();

        let xmin = active.iter().min().copied().unwrap_or(xmax);

        Snapshot::new(xmin, xmax, active)
    }

    /// Checks if transaction is committed
    pub fn is_committed(&self, xid: TransactionId) -> bool {
        self.active_txns
            .get(&xid)
            .map(|entry| *entry == TransactionState::Committed)
            .unwrap_or(false)
    }

    /// Checks if transaction is in progress
    pub fn is_in_progress(&self, xid: TransactionId) -> bool {
        self.active_txns
            .get(&xid)
            .map(|entry| *entry == TransactionState::InProgress)
            .unwrap_or(false)
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_begin_transaction() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin();

        assert!(txn.xid >= FIRST_NORMAL_XID);
        assert_eq!(txn.state, TransactionState::InProgress);
    }

    #[test]
    fn test_commit_transaction() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin();

        mgr.commit(txn.xid).unwrap();
        assert!(mgr.is_committed(txn.xid));
    }

    #[test]
    fn test_abort_transaction() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin();

        mgr.abort(txn.xid).unwrap();
        assert!(!mgr.is_committed(txn.xid));
    }

    #[test]
    fn test_multiple_transactions() {
        let mgr = TransactionManager::new();
        let txn1 = mgr.begin();
        let txn2 = mgr.begin();

        assert_ne!(txn1.xid, txn2.xid);
        assert!(txn2.xid > txn1.xid);
    }

    #[test]
    fn test_snapshot() {
        let mgr = TransactionManager::new();
        let _txn1 = mgr.begin();
        let _txn2 = mgr.begin();

        let snapshot = mgr.get_snapshot();
        assert_eq!(snapshot.active.len(), 2);
    }

    #[test]
    fn test_commit_non_existent_transaction() {
        let mgr = TransactionManager::new();
        let result = mgr.commit(999); // Non-existent XID
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TransactionError::NotFound(_)));
    }

    #[test]
    fn test_commit_already_committed_transaction() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin();
        mgr.commit(txn.xid).unwrap(); // Commit once

        let result = mgr.commit(txn.xid); // Commit again
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TransactionError::AlreadyCommitted(_)));
    }

    #[test]
    fn test_commit_already_aborted_transaction() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin();
        mgr.abort(txn.xid).unwrap(); // Abort once

        let result = mgr.commit(txn.xid); // Commit aborted transaction
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TransactionError::AlreadyAborted(_)));
    }

    #[test]
    fn test_abort_non_existent_transaction() {
        let mgr = TransactionManager::new();
        let result = mgr.abort(999); // Non-existent XID
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TransactionError::NotFound(_)));
    }

    #[test]
    fn test_abort_already_committed_transaction() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin();
        mgr.commit(txn.xid).unwrap(); // Commit once

        let result = mgr.abort(txn.xid); // Abort committed transaction
        assert!(result.is_ok()); // Aborting a committed transaction should still succeed in marking it as aborted if the logic changes
    }

    #[test]
    fn test_abort_already_aborted_transaction() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin();
        mgr.abort(txn.xid).unwrap(); // Abort once

        let result = mgr.abort(txn.xid); // Abort again
        assert!(result.is_ok()); // Aborting an already aborted transaction should still succeed
    }
}
