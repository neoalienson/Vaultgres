use std::sync::Arc;
use vaultgres::executor::Executor;
use vaultgres::parser::parse;
use vaultgres::storage::heap::HeapFile;
use vaultgres::storage::{BufferPool, PageId};
use vaultgres::transaction::TransactionManager;
use vaultgres::wal::WALWriter;

#[test]
fn test_end_to_end_transaction_flow() {
    // Setup components
    let pool = Arc::new(BufferPool::new(10));
    let heap = Arc::new(HeapFile::new(pool.clone()));
    let txn_mgr = TransactionManager::new();
    let _wal = WALWriter::new();

    // Begin transaction
    let txn = txn_mgr.begin();
    assert!(txn.xid >= 2);

    // Insert data
    heap.insert_tuple(PageId(0), vec![1, 2, 3]).unwrap();
    heap.insert_tuple(PageId(0), vec![4, 5, 6]).unwrap();

    // Commit transaction
    txn_mgr.commit(txn.xid).unwrap();
    assert!(txn_mgr.is_committed(txn.xid));
}

#[test]
fn test_end_to_end_query_execution() {
    // Parse query
    let stmt = parse("SELECT * FROM users").unwrap();
    assert!(matches!(stmt, vaultgres::parser::Statement::Select(_)));

    // Setup storage
    let pool = Arc::new(BufferPool::new(10));
    let heap = Arc::new(HeapFile::new(pool));
    let txn_mgr = TransactionManager::new();
    let _wal = WALWriter::new();

    // Begin transaction
    let txn = txn_mgr.begin();

    // Insert test data
    heap.insert_tuple(PageId(0), vec![1, 2, 3]).unwrap();
    heap.insert_tuple(PageId(0), vec![4, 5, 6]).unwrap();

    // Commit transaction
    txn_mgr.commit(txn.xid).unwrap();
    assert!(txn_mgr.is_committed(txn.xid));

    // Note: SeqScanExecutor test disabled - uses old executor API
    // Execute scan
    // let mut scan = SeqScanExecutor::new(heap, "users".to_string());
    // scan.open().unwrap();
    // let mut count = 0;
    // while scan.next().unwrap().is_some() {
    //     count += 1;
    // }
    // assert_eq!(count, 2);
    // scan.close().unwrap();
}

#[test]
fn test_end_to_end_parse_and_validate() {
    // Test all statement types
    let queries = vec![
        "SELECT * FROM users",
        "SELECT id, name FROM users WHERE id = 1",
        "INSERT INTO users VALUES (1, 'Alice')",
        "UPDATE users SET name = 'Bob' WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
    ];

    for query in queries {
        let result = parse(query);
        assert!(result.is_ok(), "Failed to parse: {}", query);
    }
}

#[test]
fn test_end_to_end_transaction_abort() {
    let txn_mgr = TransactionManager::new();

    // Begin and abort transaction
    let txn = txn_mgr.begin();
    txn_mgr.abort(txn.xid).unwrap();

    assert!(!txn_mgr.is_committed(txn.xid));
}

#[test]
fn test_end_to_end_buffer_pool_eviction() {
    let pool = Arc::new(BufferPool::new(2));

    // Fill buffer pool
    pool.fetch(PageId(1)).unwrap();
    pool.unpin(PageId(1), false).unwrap();

    pool.fetch(PageId(2)).unwrap();
    pool.unpin(PageId(2), false).unwrap();

    // Trigger eviction
    pool.fetch(PageId(3)).unwrap();

    assert_eq!(pool.size(), 2);
}

#[test]
fn test_end_to_end_wal_operations() {
    use vaultgres::wal::RecordType;

    let wal = WALWriter::new();

    // Write multiple records
    let lsn1 = wal.write(1, RecordType::Insert, Some(PageId(1)), vec![1, 2, 3]).unwrap();
    let lsn2 = wal.write(1, RecordType::Update, Some(PageId(1)), vec![4, 5, 6]).unwrap();
    let lsn3 = wal.write(1, RecordType::Commit, None, vec![]).unwrap();

    assert_eq!(lsn1, 1);
    assert_eq!(lsn2, 2);
    assert_eq!(lsn3, 3);

    // Flush
    let flushed = wal.flush().unwrap();
    assert_eq!(flushed, 3);
}

#[test]
fn test_end_to_end_concurrent_transactions() {
    let txn_mgr = TransactionManager::new();

    // Start multiple transactions
    let txn1 = txn_mgr.begin();
    let txn2 = txn_mgr.begin();
    let txn3 = txn_mgr.begin();

    assert_ne!(txn1.xid, txn2.xid);
    assert_ne!(txn2.xid, txn3.xid);

    // Commit in different order
    txn_mgr.commit(txn2.xid).unwrap();
    txn_mgr.commit(txn1.xid).unwrap();
    txn_mgr.abort(txn3.xid).unwrap();

    assert!(txn_mgr.is_committed(txn1.xid));
    assert!(txn_mgr.is_committed(txn2.xid));
    assert!(!txn_mgr.is_committed(txn3.xid));
}
