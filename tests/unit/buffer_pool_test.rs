use rustgres::storage::{BufferPool, PageId};

#[test]
fn test_buffer_pool_creation() {
    let pool = BufferPool::new(10);
    assert_eq!(pool.size(), 0);
}

#[test]
#[should_panic(expected = "capacity must be positive")]
fn test_buffer_pool_zero_capacity() {
    BufferPool::new(0);
}

#[test]
fn test_fetch_page() {
    let pool = BufferPool::new(10);
    pool.fetch(PageId(1)).unwrap();
    
    assert_eq!(pool.size(), 1);
}

#[test]
fn test_fetch_same_page_twice() {
    let pool = BufferPool::new(10);
    
    pool.fetch(PageId(1)).unwrap();
    pool.fetch(PageId(1)).unwrap();
    
    assert_eq!(pool.size(), 1);
}

#[test]
fn test_unpin_page() {
    let pool = BufferPool::new(10);
    
    pool.fetch(PageId(1)).unwrap();
    pool.unpin(PageId(1), false).unwrap();
}

#[test]
fn test_unpin_nonexistent_page() {
    let pool = BufferPool::new(10);
    let result = pool.unpin(PageId(999), false);
    
    assert!(result.is_err());
}

#[test]
fn test_unpin_dirty_page() {
    let pool = BufferPool::new(10);
    
    pool.fetch(PageId(1)).unwrap();
    pool.unpin(PageId(1), true).unwrap();
}

#[test]
fn test_buffer_pool_full() {
    let pool = BufferPool::new(2);
    
    pool.fetch(PageId(1)).unwrap();
    pool.unpin(PageId(1), false).unwrap();
    
    pool.fetch(PageId(2)).unwrap();
    pool.unpin(PageId(2), false).unwrap();
    
    // Should evict and succeed
    pool.fetch(PageId(3)).unwrap();
    assert_eq!(pool.size(), 2);
}

#[test]
fn test_lru_eviction() {
    let pool = BufferPool::new(2);
    
    // Fill pool
    pool.fetch(PageId(1)).unwrap();
    pool.unpin(PageId(1), false).unwrap();
    
    pool.fetch(PageId(2)).unwrap();
    pool.unpin(PageId(2), false).unwrap();
    
    // Access page 1 again (makes it more recent)
    pool.fetch(PageId(1)).unwrap();
    pool.unpin(PageId(1), false).unwrap();
    
    // Fetch new page should evict page 2 (least recently used)
    pool.fetch(PageId(3)).unwrap();
    
    assert_eq!(pool.size(), 2);
}

#[test]
fn test_pinned_page_not_evicted() {
    let pool = BufferPool::new(2);
    
    pool.fetch(PageId(1)).unwrap();
    // Don't unpin page 1
    
    pool.fetch(PageId(2)).unwrap();
    pool.unpin(PageId(2), false).unwrap();
    
    // Should succeed, evicting page 2
    pool.fetch(PageId(3)).unwrap();
}

#[test]
fn test_multiple_pins() {
    let pool = BufferPool::new(10);
    
    pool.fetch(PageId(1)).unwrap();
    pool.fetch(PageId(1)).unwrap();
    pool.fetch(PageId(1)).unwrap();
    
    // Should need 3 unpins
    pool.unpin(PageId(1), false).unwrap();
    pool.unpin(PageId(1), false).unwrap();
    pool.unpin(PageId(1), false).unwrap();
}

#[test]
fn test_many_pages() {
    let pool = BufferPool::new(100);
    
    for i in 0..50 {
        pool.fetch(PageId(i)).unwrap();
        pool.unpin(PageId(i), false).unwrap();
    }
    
    assert_eq!(pool.size(), 50);
}

#[test]
fn test_buffer_pool_capacity_limit() {
    let pool = BufferPool::new(5);
    
    for i in 0..10 {
        pool.fetch(PageId(i)).unwrap();
        pool.unpin(PageId(i), false).unwrap();
    }
    
    assert_eq!(pool.size(), 5);
}

#[test]
fn test_page_id_equality() {
    let id1 = PageId(42);
    let id2 = PageId(42);
    let id3 = PageId(43);
    
    assert_eq!(id1, id2);
    assert_ne!(id1, id3);
}

#[test]
fn test_page_id_zero() {
    let pool = BufferPool::new(10);
    pool.fetch(PageId(0)).unwrap();
    
    assert_eq!(pool.size(), 1);
}

#[test]
fn test_page_id_max() {
    let pool = BufferPool::new(10);
    pool.fetch(PageId(u32::MAX)).unwrap();
    
    assert_eq!(pool.size(), 1);
}
