use super::error::{Result, StorageError};
use super::page::{Page, PageId};
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// Frame ID in buffer pool
type FrameId = usize;

/// Buffer pool entry
struct Frame {
    page: Page,
    pin_count: usize,
    dirty: bool,
}

/// Buffer pool with LRU eviction policy
pub struct BufferPool {
    frames: Vec<RwLock<Frame>>,
    page_table: RwLock<HashMap<PageId, FrameId>>,
    free_list: RwLock<VecDeque<FrameId>>,
    lru_list: RwLock<VecDeque<FrameId>>,
    capacity: usize,
}

impl BufferPool {
    /// Creates a new buffer pool with the specified capacity.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum number of pages to cache
    ///
    /// # Panics
    ///
    /// Panics if capacity is 0.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be positive");
        
        let mut frames = Vec::with_capacity(capacity);
        let mut free_list = VecDeque::with_capacity(capacity);
        
        for i in 0..capacity {
            frames.push(RwLock::new(Frame {
                page: Page::new(PageId(0)),
                pin_count: 0,
                dirty: false,
            }));
            free_list.push_back(i);
        }
        
        Self {
            frames,
            page_table: RwLock::new(HashMap::new()),
            free_list: RwLock::new(free_list),
            lru_list: RwLock::new(VecDeque::new()),
            capacity,
        }
    }
    
    /// Fetches a page from the buffer pool.
    ///
    /// If the page is not in the pool, it will be loaded.
    /// The page is pinned and must be unpinned when done.
    pub fn fetch(&self, page_id: PageId) -> Result<()> {
        // Check if page is already in buffer pool
        {
            let page_table = self.page_table.read();
            if let Some(&frame_id) = page_table.get(&page_id) {
                let frame = &self.frames[frame_id];
                frame.write().pin_count += 1;
                self.update_lru(frame_id);
                log::trace!("Buffer pool hit: page {}", page_id.0);
                return Ok(());
            }
        }
        
        // Page not in pool, need to load it
        log::debug!("Buffer pool miss: loading page {}", page_id.0);
        let frame_id = self.get_free_frame()?;
        
        // Load page (for now, just create new page)
        let page = Page::new(page_id);
        
        {
            let mut frame = self.frames[frame_id].write();
            frame.page = page;
            frame.pin_count = 1;
            frame.dirty = false;
        }
        
        // Update page table
        self.page_table.write().insert(page_id, frame_id);
        self.update_lru(frame_id);
        
        Ok(())
    }
    
    /// Unpins a page, allowing it to be evicted
    pub fn unpin(&self, page_id: PageId, is_dirty: bool) -> Result<()> {
        let page_table = self.page_table.read();
        let frame_id = page_table.get(&page_id)
            .ok_or(StorageError::PageNotFound(page_id.0))?;
        
        let mut frame = self.frames[*frame_id].write();
        if frame.pin_count > 0 {
            frame.pin_count -= 1;
        }
        if is_dirty {
            frame.dirty = true;
        }
        
        Ok(())
    }
    
    /// Returns the number of pages currently in the buffer pool
    pub fn size(&self) -> usize {
        self.page_table.read().len()
    }
    
    /// Gets a free frame, evicting if necessary
    fn get_free_frame(&self) -> Result<FrameId> {
        // Try free list first
        if let Some(frame_id) = self.free_list.write().pop_front() {
            return Ok(frame_id);
        }
        
        // Need to evict a page using LRU
        log::debug!("Buffer pool full, evicting page");
        let mut lru_list = self.lru_list.write();
        
        while let Some(frame_id) = lru_list.pop_front() {
            let frame = self.frames[frame_id].read();
            if frame.pin_count == 0 {
                let page_id = frame.page.id();
                drop(frame);
                
                // Remove from page table
                self.page_table.write().remove(&page_id);
                
                log::trace!("Evicted page {}", page_id.0);
                return Ok(frame_id);
            }
            // Page is pinned, put it back
            lru_list.push_back(frame_id);
        }
        
        Err(StorageError::BufferPoolFull)
    }
    
    /// Updates LRU list
    fn update_lru(&self, frame_id: FrameId) {
        let mut lru_list = self.lru_list.write();
        lru_list.retain(|&id| id != frame_id);
        lru_list.push_back(frame_id);
    }
}

impl Clone for Frame {
    fn clone(&self) -> Self {
        Self {
            page: Page::new(self.page.id()),
            pin_count: self.pin_count,
            dirty: self.dirty,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_creation() {
        let pool = BufferPool::new(10);
        assert_eq!(pool.capacity, 10);
        assert_eq!(pool.size(), 0);
    }
    
    #[test]
    fn test_buffer_pool_fetch() {
        let pool = BufferPool::new(10);
        let page_id = PageId(1);
        
        pool.fetch(page_id).unwrap();
        assert_eq!(pool.size(), 1);
    }
    
    #[test]
    fn test_buffer_pool_unpin() {
        let pool = BufferPool::new(10);
        let page_id = PageId(1);
        
        pool.fetch(page_id).unwrap();
        pool.unpin(page_id, false).unwrap();
    }
    
    #[test]
    fn test_buffer_pool_eviction() {
        let pool = BufferPool::new(2);
        
        // Fill buffer pool
        pool.fetch(PageId(1)).unwrap();
        pool.unpin(PageId(1), false).unwrap();
        
        pool.fetch(PageId(2)).unwrap();
        pool.unpin(PageId(2), false).unwrap();
        
        // This should trigger eviction
        pool.fetch(PageId(3)).unwrap();
        assert_eq!(pool.size(), 2);
    }
    
    #[test]
    #[should_panic(expected = "capacity must be positive")]
    fn test_buffer_pool_zero_capacity() {
        BufferPool::new(0);
    }
}
