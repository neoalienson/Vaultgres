use super::disk::DiskManager;
use super::error::{Result, StorageError};
use super::page::{Page, PageId};
use crate::storage::compression::{CompressionAlgorithm, CompressionStats};
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

type FrameId = usize;

struct Frame {
    page: Page,
    pin_count: usize,
    dirty: bool,
    compressed: bool,
    compression_algorithm: CompressionAlgorithm,
}

pub struct BufferPool {
    frames: Vec<RwLock<Frame>>,
    page_table: RwLock<HashMap<PageId, FrameId>>,
    free_list: RwLock<VecDeque<FrameId>>,
    lru_list: RwLock<VecDeque<FrameId>>,
    capacity: usize,
    disk_manager: Option<Arc<DiskManager>>,
    default_compression: CompressionAlgorithm,
    compression_stats: RwLock<CompressionStats>,
}

impl BufferPool {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be positive");

        let mut frames = Vec::with_capacity(capacity);
        let mut free_list = VecDeque::with_capacity(capacity);

        for i in 0..capacity {
            frames.push(RwLock::new(Frame {
                page: Page::new(PageId(0)),
                pin_count: 0,
                dirty: false,
                compressed: false,
                compression_algorithm: CompressionAlgorithm::None,
            }));
            free_list.push_back(i);
        }

        Self {
            frames,
            page_table: RwLock::new(HashMap::new()),
            free_list: RwLock::new(free_list),
            lru_list: RwLock::new(VecDeque::new()),
            capacity,
            disk_manager: None,
            default_compression: CompressionAlgorithm::Lz4,
            compression_stats: RwLock::new(CompressionStats::new()),
        }
    }

    pub fn with_disk(capacity: usize, disk_manager: Arc<DiskManager>) -> Self {
        let mut pool = Self::new(capacity);
        pool.disk_manager = Some(disk_manager);
        pool
    }

    pub fn with_compression(capacity: usize, algorithm: CompressionAlgorithm) -> Self {
        let mut pool = Self::new(capacity);
        pool.default_compression = algorithm;
        pool
    }

    pub fn fetch(&self, page_id: PageId) -> Result<()> {
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

        log::debug!("Buffer pool miss: loading page {}", page_id.0);
        let frame_id = self.get_free_frame()?;

        let page = if let Some(ref dm) = self.disk_manager {
            dm.read_page(page_id).unwrap_or_else(|_| Page::new(page_id))
        } else {
            Page::new(page_id)
        };

        {
            let mut frame = self.frames[frame_id].write();
            frame.page = page;
            frame.pin_count = 1;
            frame.dirty = false;
            frame.compressed = false;
            frame.compression_algorithm = CompressionAlgorithm::None;
        }

        self.page_table.write().insert(page_id, frame_id);
        self.update_lru(frame_id);

        Ok(())
    }

    pub fn fetch_decompressed(
        &self,
        page_id: PageId,
        algorithm: CompressionAlgorithm,
    ) -> Result<()> {
        self.fetch(page_id)?;

        let page_table = self.page_table.read();
        if let Some(&frame_id) = page_table.get(&page_id) {
            let mut frame = self.frames[frame_id].write();
            if frame.compressed && frame.page.is_compressed() {
                let mut page = frame.page.clone();
                if page.decompress(algorithm).is_ok() {
                    frame.page = page;
                    frame.compressed = false;
                    frame.compression_algorithm = CompressionAlgorithm::None;
                }
            }
        }

        Ok(())
    }

    pub fn unpin(&self, page_id: PageId, is_dirty: bool) -> Result<()> {
        let page_table = self.page_table.read();
        let frame_id = page_table.get(&page_id).ok_or(StorageError::PageNotFound(page_id.0))?;

        let mut frame = self.frames[*frame_id].write();
        if frame.pin_count > 0 {
            frame.pin_count -= 1;
        }
        if is_dirty {
            frame.dirty = true;
        }

        Ok(())
    }

    pub fn unpin_and_compress(
        &self,
        page_id: PageId,
        algorithm: CompressionAlgorithm,
    ) -> Result<()> {
        let page_table = self.page_table.read();
        let frame_id = page_table.get(&page_id).ok_or(StorageError::PageNotFound(page_id.0))?;

        let mut frame = self.frames[*frame_id].write();
        if frame.pin_count > 0 {
            frame.pin_count -= 1;
        }
        frame.dirty = true;
        frame.compressed = true;
        frame.compression_algorithm = algorithm;

        if !frame.page.is_compressed() {
            let mut page = frame.page.clone();
            if page.compress(algorithm).is_ok() {
                frame.page = page;
            }
        }

        Ok(())
    }

    pub fn size(&self) -> usize {
        self.page_table.read().len()
    }

    fn get_free_frame(&self) -> Result<FrameId> {
        if let Some(frame_id) = self.free_list.write().pop_front() {
            return Ok(frame_id);
        }

        log::debug!("Buffer pool full, evicting page");
        let mut lru_list = self.lru_list.write();

        while let Some(frame_id) = lru_list.pop_front() {
            let frame = self.frames[frame_id].read();
            if frame.pin_count == 0 {
                let page_id = frame.page.id();
                let dirty = frame.dirty;
                let compressed = frame.compressed;
                let algorithm = frame.compression_algorithm;
                let page = frame.page.clone();
                drop(frame);

                if dirty {
                    if let Some(ref dm) = self.disk_manager {
                        dm.write_page(page_id, &page)?;
                        log::trace!("Flushed dirty page {} to disk", page_id.0);
                    }
                }

                self.page_table.write().remove(&page_id);

                if compressed {
                    self.compression_stats.write().bytes_compressed += page.data().len();
                }

                log::trace!("Evicted page {}", page_id.0);
                return Ok(frame_id);
            }
            lru_list.push_back(frame_id);
        }

        Err(StorageError::BufferPoolFull)
    }

    fn update_lru(&self, frame_id: FrameId) {
        let mut lru_list = self.lru_list.write();
        lru_list.retain(|&id| id != frame_id);
        lru_list.push_back(frame_id);
    }

    pub fn flush_all(&self) -> Result<()> {
        if let Some(ref dm) = self.disk_manager {
            let page_table = self.page_table.read();
            for &frame_id in page_table.values() {
                let frame = self.frames[frame_id].read();
                if frame.dirty {
                    dm.write_page(frame.page.id(), &frame.page)?;
                }
            }
            dm.sync()?;
            log::debug!("Flushed all dirty pages to disk");
        }
        Ok(())
    }

    pub fn get_compression_stats(&self) -> parking_lot::RwLockReadGuard<'_, CompressionStats> {
        self.compression_stats.read()
    }

    pub fn set_default_compression(&mut self, algorithm: CompressionAlgorithm) {
        self.default_compression = algorithm;
    }

    pub fn get_page(&self, page_id: PageId) -> Option<Page> {
        let page_table = self.page_table.read();
        if let Some(&frame_id) = page_table.get(&page_id) {
            let frame = self.frames[frame_id].read();
            Some(frame.page.clone())
        } else {
            None
        }
    }

    pub fn is_page_compressed(&self, page_id: PageId) -> bool {
        let page_table = self.page_table.read();
        if let Some(&frame_id) = page_table.get(&page_id) {
            let frame = self.frames[frame_id].read();
            frame.compressed
        } else {
            false
        }
    }

    pub fn evict_compressed_pages(&self) -> Result<usize> {
        let mut evicted = 0;
        let mut lru_list = self.lru_list.write();

        let frame_ids: Vec<FrameId> = lru_list.iter().cloned().collect();

        for frame_id in frame_ids {
            let frame = self.frames[frame_id].read();
            if frame.pin_count == 0 && frame.compressed {
                let page_id = frame.page.id();
                let dirty = frame.dirty;
                let page = frame.page.clone();
                drop(frame);

                if dirty {
                    if let Some(ref dm) = self.disk_manager {
                        dm.write_page(page_id, &page)?;
                    }
                }

                self.page_table.write().remove(&page_id);
                self.free_list.write().push_back(frame_id);
                evicted += 1;
            }
        }

        lru_list.retain(|&id| {
            let frame = self.frames[id].read();
            if frame.compressed { frame.pin_count > 0 } else { true }
        });

        Ok(evicted)
    }
}

impl Clone for Frame {
    fn clone(&self) -> Self {
        Self {
            page: Page::new(self.page.id()),
            pin_count: self.pin_count,
            dirty: self.dirty,
            compressed: self.compressed,
            compression_algorithm: self.compression_algorithm,
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

        pool.fetch(PageId(1)).unwrap();
        pool.unpin(PageId(1), false).unwrap();

        pool.fetch(PageId(2)).unwrap();
        pool.unpin(PageId(2), false).unwrap();

        pool.fetch(PageId(3)).unwrap();
        assert_eq!(pool.size(), 2);
    }

    #[test]
    #[should_panic(expected = "capacity must be positive")]
    fn test_buffer_pool_zero_capacity() {
        BufferPool::new(0);
    }

    #[test]
    fn test_buffer_pool_eviction_dirty() {
        use std::fs;
        let db_dir = "test_bp_evict_dirty_dir";
        let _ = fs::remove_dir_all(db_dir);
        fs::create_dir_all(db_dir).unwrap();

        let dm = Arc::new(DiskManager::new(db_dir).unwrap());
        let pool = BufferPool::with_disk(2, dm.clone());

        pool.fetch(PageId(1)).unwrap();
        pool.unpin(PageId(1), true).unwrap();

        pool.fetch(PageId(2)).unwrap();
        pool.unpin(PageId(2), false).unwrap();

        pool.fetch(PageId(3)).unwrap();

        dm.read_page(PageId(1)).unwrap();

        fs::remove_dir_all(db_dir).unwrap();
    }

    #[test]
    fn test_buffer_pool_with_compression() {
        let pool = BufferPool::with_compression(10, CompressionAlgorithm::Lz4);

        pool.fetch(PageId(1)).unwrap();
        pool.unpin(PageId(1), false).unwrap();

        assert!(!pool.is_page_compressed(PageId(1)));
    }

    #[test]
    fn test_buffer_pool_unpin_and_compress() {
        let pool = BufferPool::with_compression(10, CompressionAlgorithm::Lz4);

        pool.fetch(PageId(1)).unwrap();
        pool.unpin_and_compress(PageId(1), CompressionAlgorithm::Lz4).unwrap();

        assert!(pool.is_page_compressed(PageId(1)));
    }

    #[test]
    fn test_get_page() {
        let pool = BufferPool::new(10);
        let page_id = PageId(42);

        pool.fetch(page_id).unwrap();

        let page = pool.get_page(page_id);
        assert!(page.is_some());
        assert_eq!(page.unwrap().id(), page_id);
    }

    #[test]
    fn test_get_page_not_found() {
        let pool = BufferPool::new(10);
        let page = pool.get_page(PageId(999));
        assert!(page.is_none());
    }

    #[test]
    fn test_set_default_compression() {
        let mut pool = BufferPool::new(10);
        pool.set_default_compression(CompressionAlgorithm::Zstd);

        assert_eq!(pool.default_compression, CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_compression_stats() {
        let pool = BufferPool::new(10);
        let stats = pool.get_compression_stats();
        assert_eq!(stats.bytes_compressed, 0);
        assert_eq!(stats.compression_count, 0);
    }

    #[test]
    fn test_fetch_decompressed() {
        let pool = BufferPool::new(10);
        let page_id = PageId(1);

        pool.fetch(page_id).unwrap();
        pool.unpin_and_compress(page_id, CompressionAlgorithm::Lz4).unwrap();
        assert!(pool.is_page_compressed(page_id));

        pool.fetch_decompressed(page_id, CompressionAlgorithm::Lz4).unwrap();
        assert!(!pool.is_page_compressed(page_id));
    }

    #[test]
    fn test_buffer_pool_multiple_pages() {
        let pool = BufferPool::new(5);

        for i in 1..=5 {
            pool.fetch(PageId(i)).unwrap();
        }

        assert_eq!(pool.size(), 5);

        for i in 1..=5 {
            pool.unpin(PageId(i), false).unwrap();
        }
    }

    #[test]
    fn test_buffer_pool_pin_count() {
        let pool = BufferPool::new(5);
        let page_id = PageId(1);

        pool.fetch(page_id).unwrap();
        assert_eq!(pool.get_page(page_id).unwrap().id(), page_id);

        pool.fetch(page_id).unwrap();
        pool.unpin(page_id, false).unwrap();
    }
}
