use super::buffer_pool::BufferPool;
use super::error::Result;
use super::page::PageId;
use crate::catalog::TableSchema;
use crate::storage::compression::{CompressionAlgorithm, compress, decompress, should_compress};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct HeapFile {
    buffer_pool: Arc<BufferPool>,
    next_page_id: u32,
    tuples: Arc<Mutex<HashMap<(PageId, u16), Vec<u8>>>>,
    compression_algorithm: CompressionAlgorithm,
}

impl HeapFile {
    pub fn new(buffer_pool: Arc<BufferPool>) -> Self {
        Self {
            buffer_pool,
            next_page_id: 1,
            tuples: Arc::new(Mutex::new(HashMap::new())),
            compression_algorithm: CompressionAlgorithm::Lz4,
        }
    }

    pub fn with_compression(buffer_pool: Arc<BufferPool>, algorithm: CompressionAlgorithm) -> Self {
        Self {
            buffer_pool,
            next_page_id: 1,
            tuples: Arc::new(Mutex::new(HashMap::new())),
            compression_algorithm: algorithm,
        }
    }

    pub fn with_schema(buffer_pool: Arc<BufferPool>, schema: &TableSchema) -> Self {
        Self {
            buffer_pool,
            next_page_id: 1,
            tuples: Arc::new(Mutex::new(HashMap::new())),
            compression_algorithm: schema.compression_algorithm(),
        }
    }

    pub fn set_compression_algorithm(&mut self, algorithm: CompressionAlgorithm) {
        self.compression_algorithm = algorithm;
    }

    pub fn compression_algorithm(&self) -> CompressionAlgorithm {
        self.compression_algorithm
    }

    pub fn insert_tuple(&self, page_id: PageId, data: Vec<u8>) -> Result<u16> {
        self.buffer_pool.fetch(page_id)?;
        let mut tuples = self.tuples.lock().unwrap();
        let slot = tuples.len() as u16;
        tuples.insert((page_id, slot), data);
        self.buffer_pool.unpin(page_id, true)?;
        Ok(slot)
    }

    pub fn insert_compressed_tuple(
        &self,
        page_id: PageId,
        data: &[u8],
        algorithm: CompressionAlgorithm,
    ) -> Result<u16> {
        self.buffer_pool.fetch(page_id)?;

        let data_to_store = if should_compress(data.len()) {
            let compressed = compress(data, algorithm)?;
            if compressed.len() < data.len() { compressed } else { data.to_vec() }
        } else {
            data.to_vec()
        };

        let mut tuples = self.tuples.lock().unwrap();
        let slot = tuples.len() as u16;
        tuples.insert((page_id, slot), data_to_store);
        self.buffer_pool.unpin(page_id, true)?;
        Ok(slot)
    }

    pub fn get_tuple(&self, page_id: PageId, slot: u16) -> Result<Vec<u8>> {
        self.buffer_pool.fetch(page_id)?;
        let tuples = self.tuples.lock().unwrap();
        let data = tuples.get(&(page_id, slot)).cloned().unwrap_or_default();
        self.buffer_pool.unpin(page_id, false)?;
        Ok(data)
    }

    pub fn get_tuple_decompressed(&self, page_id: PageId, slot: u16) -> Result<Vec<u8>> {
        self.buffer_pool.fetch(page_id)?;
        let tuples = self.tuples.lock().unwrap();
        let data = tuples.get(&(page_id, slot)).cloned().unwrap_or_default();
        self.buffer_pool.unpin(page_id, false)?;
        Ok(data)
    }

    pub fn get_tuple_with_algorithm(
        &self,
        page_id: PageId,
        slot: u16,
        algorithm: CompressionAlgorithm,
    ) -> Result<Vec<u8>> {
        self.buffer_pool.fetch(page_id)?;
        let tuples = self.tuples.lock().unwrap();
        let data = tuples.get(&(page_id, slot)).cloned().unwrap_or_default();
        self.buffer_pool.unpin(page_id, false)?;

        if should_compress(data.len()) {
            let header = &data[..5];
            let algo_bits = header[0] & 0x03;

            if algo_bits != 0 {
                let stored_algo = match algo_bits {
                    0x01 => CompressionAlgorithm::Lz4,
                    0x02 => CompressionAlgorithm::Zstd,
                    _ => return Ok(data),
                };

                if stored_algo == algorithm {
                    let compressed_data = &data[5..data.len() - 4];
                    let original_size_bytes = &data[data.len() - 4..];
                    let original_size =
                        u32::from_le_bytes(original_size_bytes.try_into().unwrap()) as usize;
                    return decompress(compressed_data, algorithm, original_size).map_err(|e| {
                        crate::storage::error::StorageError::Compression(e.to_string())
                    });
                }
            }
        }

        Ok(data)
    }

    pub fn insert(&mut self, data: &[u8]) -> Result<(PageId, u16)> {
        let page_id = self.find_page_with_space(data.len())?;

        let mut tuples = self.tuples.lock().unwrap();
        let slot = tuples.len() as u16;
        tuples.insert((page_id, slot), data.to_vec());

        Ok((page_id, slot))
    }

    pub fn insert_compressed(&mut self, data: &[u8]) -> Result<(PageId, u16)> {
        let page_id = self.find_page_with_space(data.len())?;

        let data_to_store = if should_compress(data.len()) {
            let compressed = compress(data, self.compression_algorithm)?;
            if compressed.len() < data.len() {
                let mut full_data = compressed;
                full_data.extend_from_slice(&(data.len() as u32).to_le_bytes());
                full_data
            } else {
                data.to_vec()
            }
        } else {
            data.to_vec()
        };

        let mut tuples = self.tuples.lock().unwrap();
        let slot = tuples.len() as u16;
        tuples.insert((page_id, slot), data_to_store);

        Ok((page_id, slot))
    }

    pub fn read(&self, page_id: PageId, _slot: u16) -> Result<Vec<u8>> {
        self.buffer_pool.fetch(page_id)?;
        Ok(vec![])
    }

    pub fn delete(&mut self, page_id: PageId, _slot: u16) -> Result<()> {
        self.buffer_pool.fetch(page_id)?;
        self.buffer_pool.unpin(page_id, true)?;
        Ok(())
    }

    fn find_page_with_space(&mut self, _required: usize) -> Result<PageId> {
        let page_id = PageId(self.next_page_id);
        self.next_page_id += 1;

        self.buffer_pool.fetch(page_id)?;
        self.buffer_pool.unpin(page_id, false)?;

        Ok(page_id)
    }

    pub fn tuple_count(&self) -> usize {
        self.tuples.lock().unwrap().len()
    }

    pub fn is_compressed(&self, page_id: PageId, slot: u16) -> bool {
        let tuples = self.tuples.lock().unwrap();
        if let Some(data) = tuples.get(&(page_id, slot)) {
            if data.len() < 5 {
                return false;
            }
            let header = &data[0];
            (header & 0x80) != 0
        } else {
            false
        }
    }

    pub fn compressed_size(&self, page_id: PageId, slot: u16) -> Option<usize> {
        let tuples = self.tuples.lock().unwrap();
        tuples.get(&(page_id, slot)).map(|data| data.len())
    }

    pub fn estimated_original_size(&self, page_id: PageId, slot: u16) -> Option<usize> {
        let tuples = self.tuples.lock().unwrap();
        if let Some(data) = tuples.get(&(page_id, slot)) {
            if data.len() >= 9 {
                let header = &data[0];
                if (header & 0x80) != 0 {
                    let size_bytes = &data[data.len() - 4..];
                    return Some(u32::from_le_bytes(size_bytes.try_into().unwrap()) as usize);
                }
            }
            Some(data.len())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heap_file_insert() {
        let pool = Arc::new(BufferPool::new(10));
        let mut heap = HeapFile::new(pool);

        let data = vec![1, 2, 3, 4];
        let (page_id, slot) = heap.insert(&data).unwrap();

        assert_eq!(page_id, PageId(1));
        assert_eq!(slot, 0);
    }

    #[test]
    fn test_heap_file_delete() {
        let pool = Arc::new(BufferPool::new(10));
        let mut heap = HeapFile::new(pool);

        let data = vec![1, 2, 3, 4];
        let (page_id, slot) = heap.insert(&data).unwrap();

        heap.delete(page_id, slot).unwrap();
    }

    #[test]
    fn test_heap_with_compression() {
        let pool = Arc::new(BufferPool::new(10));
        let heap = HeapFile::with_compression(pool, CompressionAlgorithm::Lz4);

        assert_eq!(heap.compression_algorithm(), CompressionAlgorithm::Lz4);
    }

    #[test]
    fn test_heap_set_compression() {
        let pool = Arc::new(BufferPool::new(10));
        let mut heap = HeapFile::new(pool);

        assert_eq!(heap.compression_algorithm(), CompressionAlgorithm::Lz4);

        heap.set_compression_algorithm(CompressionAlgorithm::Zstd);
        assert_eq!(heap.compression_algorithm(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_insert_compressed() {
        let pool = Arc::new(BufferPool::new(10));
        let mut heap = HeapFile::with_compression(pool, CompressionAlgorithm::Lz4);

        let data: Vec<u8> = vec![0u8; 3000];
        let (page_id, slot) = heap.insert_compressed(&data).unwrap();

        assert_eq!(page_id, PageId(1));
        assert_eq!(slot, 0);
    }

    #[test]
    fn test_tuple_count() {
        let pool = Arc::new(BufferPool::new(10));
        let mut heap = HeapFile::new(pool);

        assert_eq!(heap.tuple_count(), 0);

        heap.insert(&vec![1, 2, 3]).unwrap();
        assert_eq!(heap.tuple_count(), 1);

        heap.insert(&vec![4, 5, 6]).unwrap();
        assert_eq!(heap.tuple_count(), 2);
    }

    #[test]
    fn test_is_compressed_small_data() {
        let pool = Arc::new(BufferPool::new(10));
        let mut heap = HeapFile::with_compression(pool, CompressionAlgorithm::Lz4);

        let small_data = vec![1, 2, 3];
        let (_, slot) = heap.insert_compressed(&small_data).unwrap();

        assert!(!heap.is_compressed(PageId(1), slot));
    }

    #[test]
    fn test_is_compressed_large_data() {
        let pool = Arc::new(BufferPool::new(10));
        let mut heap = HeapFile::with_compression(pool, CompressionAlgorithm::Lz4);

        let large_data: Vec<u8> = (0..3000).map(|i| (i % 256) as u8).collect();
        let (_, slot) = heap.insert_compressed(&large_data).unwrap();

        assert!(heap.is_compressed(PageId(1), slot));
    }

    #[test]
    fn test_compressed_size() {
        let pool = Arc::new(BufferPool::new(10));
        let mut heap = HeapFile::with_compression(pool, CompressionAlgorithm::Lz4);

        let large_data: Vec<u8> = vec![0u8; 3000];
        let (_, slot) = heap.insert_compressed(&large_data).unwrap();

        let compressed_sz = heap.compressed_size(PageId(1), slot);
        assert!(compressed_sz.is_some());
        assert!(compressed_sz.unwrap() < 3000);
    }

    #[test]
    fn test_estimated_original_size() {
        let pool = Arc::new(BufferPool::new(10));
        let mut heap = HeapFile::with_compression(pool, CompressionAlgorithm::Lz4);

        let large_data: Vec<u8> = vec![0u8; 3000];
        let (_, slot) = heap.insert_compressed(&large_data).unwrap();

        let original_sz = heap.estimated_original_size(PageId(1), slot);
        assert!(original_sz.is_some());
        assert_eq!(original_sz.unwrap(), 3000);
    }

    #[test]
    fn test_small_data_not_compressed() {
        let pool = Arc::new(BufferPool::new(10));
        let mut heap = HeapFile::with_compression(pool, CompressionAlgorithm::Lz4);

        let small_data = vec![1u8; 100];
        let (_, slot) = heap.insert_compressed(&small_data).unwrap();

        let size = heap.compressed_size(PageId(1), slot);
        assert_eq!(size, Some(100));
    }
}
