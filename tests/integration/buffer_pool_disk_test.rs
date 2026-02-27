use rustgres::storage::{BufferPool, DiskManager, Page, PageId};
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn test_buffer_pool_with_disk() {
    let temp_dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(temp_dir.path()).unwrap());
    let pool = BufferPool::with_disk(2, dm.clone());
    
    let page_id = PageId(1);
    pool.fetch(page_id).unwrap();
    pool.unpin(page_id, true).unwrap();
    
    pool.flush_all().unwrap();
}

#[test]
fn test_buffer_pool_eviction_writes_to_disk() {
    let temp_dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(temp_dir.path()).unwrap());
    let pool = BufferPool::with_disk(2, dm.clone());
    
    // Fill buffer pool
    pool.fetch(PageId(1)).unwrap();
    pool.unpin(PageId(1), true).unwrap();
    
    pool.fetch(PageId(2)).unwrap();
    pool.unpin(PageId(2), true).unwrap();
    
    // Trigger eviction - should write dirty page to disk
    pool.fetch(PageId(3)).unwrap();
    pool.unpin(PageId(3), false).unwrap();
}

#[test]
fn test_buffer_pool_reads_from_disk() {
    let temp_dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(temp_dir.path()).unwrap());
    
    // Write page to disk
    let page_id = PageId(42);
    let page = Page::new(page_id);
    dm.write_page(page_id, &page).unwrap();
    dm.sync().unwrap();
    
    // Create buffer pool and read page
    let pool = BufferPool::with_disk(10, dm.clone());
    pool.fetch(page_id).unwrap();
    pool.unpin(page_id, false).unwrap();
}

#[test]
fn test_flush_all_dirty_pages() {
    let temp_dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(temp_dir.path()).unwrap());
    let pool = BufferPool::with_disk(5, dm.clone());
    
    // Create multiple dirty pages
    for i in 0..5 {
        pool.fetch(PageId(i)).unwrap();
        pool.unpin(PageId(i), true).unwrap();
    }
    
    // Flush all
    pool.flush_all().unwrap();
}
