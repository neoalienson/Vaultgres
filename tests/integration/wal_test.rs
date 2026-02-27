use rustgres::wal::{WALWriter, RecordType, RecoveryManager, CheckpointManager};
use rustgres::storage::{BufferPool, PageId};
use rustgres::transaction::TransactionManager;
use std::sync::Arc;

#[test]
fn test_wal_write_and_flush() {
    let writer = WALWriter::new();
    
    // Write records
    let lsn1 = writer.write(10, RecordType::Insert, Some(PageId(1)), vec![1, 2, 3])
        .unwrap();
    let lsn2 = writer.write(10, RecordType::Update, Some(PageId(2)), vec![4, 5, 6])
        .unwrap();
    
    assert_eq!(lsn1, 1);
    assert_eq!(lsn2, 2);
    
    // Flush
    let flushed_lsn = writer.flush().unwrap();
    assert_eq!(flushed_lsn, 2);
}

#[test]
fn test_transaction_with_wal() {
    let txn_mgr = TransactionManager::new();
    let wal_writer = WALWriter::new();
    
    // Begin transaction
    let txn = txn_mgr.begin();
    
    // Write WAL records
    wal_writer.write(txn.xid, RecordType::Insert, Some(PageId(1)), vec![])
        .unwrap();
    wal_writer.write(txn.xid, RecordType::Update, Some(PageId(2)), vec![])
        .unwrap();
    
    // Commit transaction
    txn_mgr.commit(txn.xid).unwrap();
    wal_writer.write(txn.xid, RecordType::Commit, None, vec![])
        .unwrap();
    
    // Flush WAL
    wal_writer.flush().unwrap();
}

#[test]
fn test_recovery_committed_transaction() {
    let wal_writer = WALWriter::new();
    
    // Simulate transaction
    wal_writer.write(10, RecordType::Insert, Some(PageId(1)), vec![])
        .unwrap();
    wal_writer.write(10, RecordType::Commit, None, vec![])
        .unwrap();
    
    // Recover
    let mut recovery_mgr = RecoveryManager::new();
    let records = wal_writer.get_records();
    let state = recovery_mgr.recover(&records).unwrap();
    
    assert!(state.committed_txns.contains(&10));
    assert!(!state.active_txns.contains(&10));
}

#[test]
fn test_recovery_aborted_transaction() {
    let wal_writer = WALWriter::new();
    
    // Simulate transaction
    wal_writer.write(10, RecordType::Insert, Some(PageId(1)), vec![])
        .unwrap();
    wal_writer.write(10, RecordType::Abort, None, vec![])
        .unwrap();
    
    // Recover
    let mut recovery_mgr = RecoveryManager::new();
    let records = wal_writer.get_records();
    let state = recovery_mgr.recover(&records).unwrap();
    
    assert!(state.aborted_txns.contains(&10));
    assert!(!state.active_txns.contains(&10));
}

#[test]
fn test_recovery_active_transaction() {
    let wal_writer = WALWriter::new();
    
    // Simulate incomplete transaction
    wal_writer.write(10, RecordType::Insert, Some(PageId(1)), vec![])
        .unwrap();
    wal_writer.write(10, RecordType::Update, Some(PageId(2)), vec![])
        .unwrap();
    
    // Recover (no commit/abort)
    let mut recovery_mgr = RecoveryManager::new();
    let records = wal_writer.get_records();
    let state = recovery_mgr.recover(&records).unwrap();
    
    assert!(state.active_txns.contains(&10));
    assert!(!state.committed_txns.contains(&10));
}

#[test]
fn test_checkpoint() {
    let wal_writer = Arc::new(WALWriter::new());
    let buffer_pool = Arc::new(BufferPool::new(10));
    let txn_manager = Arc::new(TransactionManager::new());
    
    let checkpoint_mgr = CheckpointManager::new(
        wal_writer.clone(),
        buffer_pool,
        txn_manager,
    );
    
    // Write some records
    wal_writer.write(10, RecordType::Insert, Some(PageId(1)), vec![])
        .unwrap();
    
    // Checkpoint
    let checkpoint_lsn = checkpoint_mgr.checkpoint().unwrap();
    assert!(checkpoint_lsn > 0);
    
    // Verify checkpoint was flushed
    assert_eq!(checkpoint_mgr.last_checkpoint_lsn(), wal_writer.flushed_lsn());
}

#[test]
fn test_recovery_with_checkpoint() {
    let wal_writer = WALWriter::new();
    
    // Write records before checkpoint
    wal_writer.write(10, RecordType::Insert, Some(PageId(1)), vec![])
        .unwrap();
    wal_writer.write(10, RecordType::Commit, None, vec![])
        .unwrap();
    
    // Checkpoint
    let checkpoint_lsn = wal_writer.write(0, RecordType::Checkpoint, None, vec![])
        .unwrap();
    
    // Write records after checkpoint
    wal_writer.write(20, RecordType::Insert, Some(PageId(2)), vec![])
        .unwrap();
    
    // Recover
    let mut recovery_mgr = RecoveryManager::new();
    recovery_mgr.set_checkpoint(checkpoint_lsn);
    
    let records = wal_writer.get_records();
    let state = recovery_mgr.recover(&records).unwrap();
    
    assert!(state.committed_txns.contains(&10));
    assert!(state.active_txns.contains(&20));
}

#[test]
fn test_multiple_concurrent_transactions() {
    let wal_writer = WALWriter::new();
    
    // Transaction 1
    wal_writer.write(10, RecordType::Insert, Some(PageId(1)), vec![])
        .unwrap();
    
    // Transaction 2
    wal_writer.write(20, RecordType::Insert, Some(PageId(2)), vec![])
        .unwrap();
    
    // Transaction 1 commits
    wal_writer.write(10, RecordType::Commit, None, vec![])
        .unwrap();
    
    // Transaction 3
    wal_writer.write(30, RecordType::Update, Some(PageId(3)), vec![])
        .unwrap();
    
    // Transaction 2 commits
    wal_writer.write(20, RecordType::Commit, None, vec![])
        .unwrap();
    
    // Recover
    let mut recovery_mgr = RecoveryManager::new();
    let records = wal_writer.get_records();
    let state = recovery_mgr.recover(&records).unwrap();
    
    assert!(state.committed_txns.contains(&10));
    assert!(state.committed_txns.contains(&20));
    assert!(state.active_txns.contains(&30));
}
