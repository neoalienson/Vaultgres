use rustgres::storage::{BufferPool, PageId};
use rustgres::storage::btree::{BTree, TupleId};
use rustgres::storage::heap::HeapFile;
use std::sync::Arc;

#[test]
fn test_storage_integration() {
    // Create buffer pool
    let pool = Arc::new(BufferPool::new(100));
    
    // Create heap file
    let mut heap = HeapFile::new(pool.clone());
    
    // Insert data
    let data = b"test data";
    let (page_id, slot) = heap.insert(data).unwrap();
    
    assert_eq!(page_id, PageId(1));
    assert_eq!(slot, 0);
}

#[test]
fn test_btree_with_buffer_pool() {
    let _pool = Arc::new(BufferPool::new(100));
    let mut tree = BTree::new();
    
    // Insert multiple entries
    for i in 0..100 {
        let key = vec![i];
        let value = TupleId {
            page_id: PageId(i as u32),
            slot: 0,
        };
        tree.insert(key, value).unwrap();
    }
    
    // Verify retrieval
    let key = vec![50];
    let value = tree.get(&key).unwrap();
    assert_eq!(value.page_id, PageId(50));
}

#[test]
fn test_buffer_pool_concurrent_access() {
    let pool = Arc::new(BufferPool::new(10));
    
    // Simulate concurrent access
    let page_ids: Vec<PageId> = (1..=5).map(PageId).collect();
    
    for &page_id in &page_ids {
        pool.fetch(page_id).unwrap();
        pool.unpin(page_id, false).unwrap();
    }
    
    assert_eq!(pool.size(), 5);
}
