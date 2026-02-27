use super::error::{Result, TransactionError};
use super::snapshot::Snapshot;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use dashmap::DashMap;

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

/// Transaction context
pub struct Transaction {
    pub xid: TransactionId,
    pub snapshot: Snapshot,
    pub state: TransactionState,
}

/// Transaction manager
pub struct TransactionManager {
    next_xid: Arc<AtomicU64>,
    active_txns: Arc<DashMap<TransactionId, TransactionState>>,
}

impl TransactionManager {
    /// Creates a new transaction manager
    pub fn new() -> Self {
        Self {
            next_xid: Arc::new(AtomicU64::new(FIRST_NORMAL_XID)),
            active_txns: Arc::new(DashMap::new()),
        }
    }
    
    /// Begins a new transaction
    pub fn begin(&self) -> Transaction {
        let xid = self.next_xid.fetch_add(1, Ordering::SeqCst);
        self.active_txns.insert(xid, TransactionState::InProgress);
        
        let snapshot = self.get_snapshot();
        
        log::debug!("Transaction {} started", xid);
        
        Transaction {
            xid,
            snapshot,
            state: TransactionState::InProgress,
        }
    }
    
    /// Commits a transaction
    pub fn commit(&self, xid: TransactionId) -> Result<()> {
        let mut entry = self.active_txns.get_mut(&xid)
            .ok_or(TransactionError::NotFound(xid))?;
        
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
        let mut entry = self.active_txns.get_mut(&xid)
            .ok_or(TransactionError::NotFound(xid))?;
        
        *entry = TransactionState::Aborted;
        log::debug!("Transaction {} aborted", xid);
        Ok(())
    }
    
    /// Gets current snapshot
    pub fn get_snapshot(&self) -> Snapshot {
        let xmax = self.next_xid.load(Ordering::SeqCst);
        
        let active: Vec<TransactionId> = self.active_txns
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
}
