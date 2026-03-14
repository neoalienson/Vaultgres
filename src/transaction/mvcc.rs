use super::manager::{FROZEN_XID, TransactionId, TransactionManager};
use super::snapshot::Snapshot;

/// Tuple header for MVCC
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TupleHeader {
    pub xmin: TransactionId, // Creating transaction
    pub xmax: TransactionId, // Deleting transaction (0 if active)
}

impl TupleHeader {
    /// Creates a new tuple header
    pub fn new(xmin: TransactionId) -> Self {
        Self { xmin, xmax: 0 }
    }

    /// Marks tuple as deleted
    pub fn delete(&mut self, xmax: TransactionId) {
        self.xmax = xmax;
    }

    /// Checks if tuple is visible to a snapshot
    pub fn is_visible(&self, snapshot: &Snapshot, txn_mgr: &TransactionManager) -> bool {
        // Check xmin (creating transaction)
        if self.xmin == FROZEN_XID {
            // Frozen tuples are always visible
        } else if self.xmin >= snapshot.xmax {
            // Created after snapshot
            return false;
        } else if snapshot.active.contains(&self.xmin) {
            // Creating transaction still in progress
            return false;
        } else if !txn_mgr.is_committed(self.xmin) {
            // Creating transaction aborted
            return false;
        }

        // Check xmax (deleting transaction)
        if self.xmax == 0 {
            // Not deleted
            return true;
        }

        if self.xmax >= snapshot.xmax {
            // Deleted after snapshot
            return true;
        }

        if snapshot.active.contains(&self.xmax) {
            // Deleting transaction still in progress
            return true;
        }

        // Visible if delete aborted
        !txn_mgr.is_committed(self.xmax)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple_creation() {
        let header = TupleHeader::new(10);
        assert_eq!(header.xmin, 10);
        assert_eq!(header.xmax, 0);
    }

    #[test]
    fn test_tuple_deletion() {
        let mut header = TupleHeader::new(10);
        header.delete(20);
        assert_eq!(header.xmax, 20);
    }

    #[test]
    fn test_tuple_visibility() {
        let mgr = TransactionManager::new();
        let txn = mgr.begin();
        mgr.commit(txn.xid).unwrap();

        let header = TupleHeader::new(txn.xid);
        let snapshot = mgr.get_snapshot();

        assert!(header.is_visible(&snapshot, &mgr));
    }

    #[test]
    fn test_deleted_tuple_visibility() {
        let mgr = TransactionManager::new();
        let txn1 = mgr.begin();
        mgr.commit(txn1.xid).unwrap();

        let mut header = TupleHeader::new(txn1.xid);

        let txn2 = mgr.begin();
        header.delete(txn2.xid);
        mgr.commit(txn2.xid).unwrap();

        let snapshot = mgr.get_snapshot();
        assert!(!header.is_visible(&snapshot, &mgr));
    }
}
