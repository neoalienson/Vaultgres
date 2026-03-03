use vaultgres::transaction::{TransactionManager, TransactionState};

const INVALID_XID: u64 = 0;
const FROZEN_XID: u64 = 1;
const FIRST_NORMAL_XID: u64 = 2;

#[test]
fn test_transaction_id_constants() {
    assert_eq!(INVALID_XID, 0);
    assert_eq!(FROZEN_XID, 1);
    assert_eq!(FIRST_NORMAL_XID, 2);
}

#[test]
fn test_begin_transaction() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin();

    assert!(txn.xid >= FIRST_NORMAL_XID);
    assert_eq!(txn.state, TransactionState::InProgress);
}

#[test]
fn test_sequential_transaction_ids() {
    let mgr = TransactionManager::new();
    let txn1 = mgr.begin();
    let txn2 = mgr.begin();
    let txn3 = mgr.begin();

    assert_eq!(txn2.xid, txn1.xid + 1);
    assert_eq!(txn3.xid, txn2.xid + 1);
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
fn test_commit_nonexistent_transaction() {
    let mgr = TransactionManager::new();
    let result = mgr.commit(999);

    assert!(result.is_err());
}

#[test]
fn test_abort_nonexistent_transaction() {
    let mgr = TransactionManager::new();
    let result = mgr.abort(999);

    assert!(result.is_err());
}

#[test]
fn test_double_commit() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin();

    mgr.commit(txn.xid).unwrap();
    let result = mgr.commit(txn.xid);

    assert!(result.is_err());
}

#[test]
fn test_commit_after_abort() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin();

    mgr.abort(txn.xid).unwrap();
    let result = mgr.commit(txn.xid);

    assert!(result.is_err());
}

#[test]
fn test_is_in_progress() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin();

    assert!(mgr.is_in_progress(txn.xid));

    mgr.commit(txn.xid).unwrap();
    assert!(!mgr.is_in_progress(txn.xid));
}

#[test]
fn test_snapshot_creation() {
    let mgr = TransactionManager::new();
    let _txn1 = mgr.begin();
    let _txn2 = mgr.begin();

    let snapshot = mgr.get_snapshot();
    assert_eq!(snapshot.active.len(), 2);
}

#[test]
fn test_snapshot_excludes_committed() {
    let mgr = TransactionManager::new();
    let txn1 = mgr.begin();
    let _txn2 = mgr.begin();

    mgr.commit(txn1.xid).unwrap();

    let snapshot = mgr.get_snapshot();
    assert_eq!(snapshot.active.len(), 1);
}

#[test]
fn test_many_concurrent_transactions() {
    let mgr = TransactionManager::new();
    let mut txns = vec![];

    for _ in 0..100 {
        txns.push(mgr.begin());
    }

    assert_eq!(txns.len(), 100);
    for i in 1..txns.len() {
        assert!(txns[i].xid > txns[i - 1].xid);
    }
}

#[test]
fn test_transaction_state_transitions() {
    let mgr = TransactionManager::new();
    let txn = mgr.begin();

    assert_eq!(txn.state, TransactionState::InProgress);

    mgr.commit(txn.xid).unwrap();
    assert!(mgr.is_committed(txn.xid));
}
