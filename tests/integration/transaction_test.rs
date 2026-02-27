use rustgres::transaction::{TransactionManager, LockManager, LockKey, LockMode};
use rustgres::transaction::mvcc::TupleHeader;

#[test]
fn test_transaction_lifecycle() {
    let mgr = TransactionManager::new();
    
    // Begin transaction
    let txn = mgr.begin();
    assert!(txn.xid >= 2);
    
    // Commit transaction
    mgr.commit(txn.xid).unwrap();
    assert!(mgr.is_committed(txn.xid));
}

#[test]
fn test_concurrent_transactions() {
    let mgr = TransactionManager::new();
    
    let txn1 = mgr.begin();
    let txn2 = mgr.begin();
    let txn3 = mgr.begin();
    
    // All transactions should have unique IDs
    assert_ne!(txn1.xid, txn2.xid);
    assert_ne!(txn2.xid, txn3.xid);
    
    // Commit in different order
    mgr.commit(txn2.xid).unwrap();
    mgr.commit(txn1.xid).unwrap();
    mgr.abort(txn3.xid).unwrap();
    
    assert!(mgr.is_committed(txn1.xid));
    assert!(mgr.is_committed(txn2.xid));
    assert!(!mgr.is_committed(txn3.xid));
}

#[test]
fn test_mvcc_visibility() {
    let mgr = TransactionManager::new();
    
    // Create and commit a tuple
    let txn1 = mgr.begin();
    let header = TupleHeader::new(txn1.xid);
    mgr.commit(txn1.xid).unwrap();
    
    // New transaction should see the tuple
    let txn2 = mgr.begin();
    assert!(header.is_visible(&txn2.snapshot, &mgr));
}

#[test]
fn test_mvcc_deleted_tuple() {
    let mgr = TransactionManager::new();
    
    // Create tuple
    let txn1 = mgr.begin();
    let mut header = TupleHeader::new(txn1.xid);
    mgr.commit(txn1.xid).unwrap();
    
    // Delete tuple
    let txn2 = mgr.begin();
    header.delete(txn2.xid);
    mgr.commit(txn2.xid).unwrap();
    
    // New transaction should not see the tuple
    let txn3 = mgr.begin();
    assert!(!header.is_visible(&txn3.snapshot, &mgr));
}

#[test]
fn test_snapshot_isolation() {
    let mgr = TransactionManager::new();
    
    // Start two concurrent transactions
    let txn1 = mgr.begin();
    let txn2 = mgr.begin();
    
    // txn1's snapshot should not see txn2
    assert!(!txn1.snapshot.is_visible(txn2.xid));
    
    // Commit txn2
    mgr.commit(txn2.xid).unwrap();
    
    // txn1's snapshot still shouldn't see txn2 (snapshot isolation)
    assert!(!txn1.snapshot.is_visible(txn2.xid));
    
    // New transaction should see txn2
    let txn3 = mgr.begin();
    assert!(txn3.snapshot.is_visible(txn2.xid));
}

#[test]
fn test_lock_manager() {
    let lock_mgr = LockManager::new();
    let key = LockKey::table(1);
    
    // Acquire shared locks
    lock_mgr.acquire(1, key, LockMode::Shared).unwrap();
    lock_mgr.acquire(2, key, LockMode::Shared).unwrap();
    
    // Release locks
    lock_mgr.release(1, key).unwrap();
    lock_mgr.release(2, key).unwrap();
    
    // Acquire exclusive lock
    lock_mgr.acquire(3, key, LockMode::Exclusive).unwrap();
}

#[test]
fn test_lock_conflict() {
    let lock_mgr = LockManager::new();
    let key = LockKey::table(1);
    
    // Acquire exclusive lock
    lock_mgr.acquire(1, key, LockMode::Exclusive).unwrap();
    
    // Try to acquire conflicting lock
    assert!(lock_mgr.acquire(2, key, LockMode::Exclusive).is_err());
    assert!(lock_mgr.acquire(2, key, LockMode::Shared).is_err());
}

#[test]
fn test_transaction_with_locks() {
    let txn_mgr = TransactionManager::new();
    let lock_mgr = LockManager::new();
    
    let txn = txn_mgr.begin();
    let key = LockKey::table(1);
    
    // Acquire lock
    lock_mgr.acquire(txn.xid, key, LockMode::Exclusive).unwrap();
    
    // Commit and release locks
    txn_mgr.commit(txn.xid).unwrap();
    lock_mgr.release_all(txn.xid);
    
    // Another transaction can now acquire the lock
    let txn2 = txn_mgr.begin();
    lock_mgr.acquire(txn2.xid, key, LockMode::Exclusive).unwrap();
}
