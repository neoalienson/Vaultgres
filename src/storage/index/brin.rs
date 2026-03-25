use super::index_trait::{Index, IndexError, IndexType, TupleId};
use crate::storage::compression::CompressionError;
use crate::storage::compression::{CompressionAlgorithm, compress, decompress, should_compress};
use crate::storage::page::PageId;

pub struct BRINIndex {
    ranges: Vec<BlockRange>,
    pages_per_range: usize,
    compression_algorithm: CompressionAlgorithm,
}

struct BlockRange {
    start_page: u32,
    end_page: u32,
    min_value: Option<Vec<u8>>,
    max_value: Option<Vec<u8>>,
    tids: Vec<TupleId>,
    compressed: bool,
}

impl BRINIndex {
    pub fn new(pages_per_range: usize) -> Self {
        Self { ranges: vec![], pages_per_range, compression_algorithm: CompressionAlgorithm::Lz4 }
    }

    pub fn with_compression(pages_per_range: usize, algorithm: CompressionAlgorithm) -> Self {
        Self { ranges: vec![], pages_per_range, compression_algorithm: algorithm }
    }

    pub fn set_compression_algorithm(&mut self, algorithm: CompressionAlgorithm) {
        self.compression_algorithm = algorithm;
    }

    pub fn compression_algorithm(&self) -> CompressionAlgorithm {
        self.compression_algorithm
    }

    fn get_or_create_range(&mut self, page: u32) -> &mut BlockRange {
        let range_idx = (page as usize) / self.pages_per_range;

        while self.ranges.len() <= range_idx {
            let start = (self.ranges.len() * self.pages_per_range) as u32;
            let end = start + self.pages_per_range as u32;
            self.ranges.push(BlockRange {
                start_page: start,
                end_page: end,
                min_value: None,
                max_value: None,
                tids: vec![],
                compressed: false,
            });
        }

        &mut self.ranges[range_idx]
    }

    fn update_range_bounds(&mut self, page: u32, key: &[u8]) {
        let range = self.get_or_create_range(page);

        match &range.min_value {
            None => range.min_value = Some(key.to_vec()),
            Some(min) if key < min.as_slice() => range.min_value = Some(key.to_vec()),
            _ => {}
        }

        match &range.max_value {
            None => range.max_value = Some(key.to_vec()),
            Some(max) if key > max.as_slice() => range.max_value = Some(key.to_vec()),
            _ => {}
        }
    }

    fn get_page_from_tid(tid: TupleId) -> u32 {
        tid.0.0
    }

    pub fn insert_compressed(&mut self, key: &[u8], tid: TupleId) -> Result<(), IndexError> {
        let key_to_store = if should_compress(key.len()) {
            let compressed = compress(key, self.compression_algorithm)
                .map_err(|e| IndexError::Storage(e.to_string()))?;
            if compressed.len() < key.len() {
                let mut full_data = compressed;
                full_data.extend_from_slice(&(key.len() as u32).to_le_bytes());
                full_data
            } else {
                key.to_vec()
            }
        } else {
            key.to_vec()
        };

        let page = Self::get_page_from_tid(tid);
        self.update_range_bounds(page, key);
        let range = self.get_or_create_range(page);
        range.tids.push(tid);
        Ok(())
    }

    pub fn compress_range(&mut self, range_idx: usize) -> Result<(), IndexError> {
        if range_idx >= self.ranges.len() {
            return Err(IndexError::InvalidIndex);
        }

        let range = &mut self.ranges[range_idx];
        if range.compressed || range.min_value.is_none() {
            return Ok(());
        }

        let serialized = Self::serialize_range(range);
        let compressed = compress(&serialized, self.compression_algorithm)
            .map_err(|e| IndexError::Storage(e.to_string()))?;

        if compressed.len() < serialized.len() {
            range.min_value = Some(compressed);
            range.max_value = None;
            range.tids = vec![];
            range.compressed = true;
        }

        Ok(())
    }

    pub fn decompress_range(&mut self, range_idx: usize) -> Result<(), IndexError> {
        if range_idx >= self.ranges.len() {
            return Err(IndexError::InvalidIndex);
        }

        let range = &mut self.ranges[range_idx];
        if !range.compressed || range.min_value.is_none() {
            return Ok(());
        }

        let compressed = range.min_value.take().unwrap();
        let decompressed = decompress(&compressed, self.compression_algorithm, compressed.len())
            .map_err(|e| IndexError::Storage(e.to_string()))?;
        let (min_value, max_value, tids) = Self::deserialize_range(&decompressed)
            .map_err(|e| IndexError::Storage(e.to_string()))?;

        range.min_value = min_value;
        range.max_value = max_value;
        range.tids = tids;
        range.compressed = false;

        Ok(())
    }

    fn serialize_range(range: &BlockRange) -> Vec<u8> {
        let mut result = Vec::new();

        result.extend_from_slice(&range.start_page.to_le_bytes());
        result.extend_from_slice(&range.end_page.to_le_bytes());

        if let Some(ref min) = range.min_value {
            result.push(1);
            result.extend_from_slice(&(min.len() as u32).to_le_bytes());
            result.extend_from_slice(min);
        } else {
            result.push(0);
        }

        if let Some(ref max) = range.max_value {
            result.push(1);
            result.extend_from_slice(&(max.len() as u32).to_le_bytes());
            result.extend_from_slice(max);
        } else {
            result.push(0);
        }

        result.extend_from_slice(&(range.tids.len() as u32).to_le_bytes());
        for tid in &range.tids {
            result.extend_from_slice(&tid.0.0.to_le_bytes());
            result.extend_from_slice(&tid.1.to_le_bytes());
        }

        result
    }

    fn deserialize_range(
        data: &[u8],
    ) -> std::result::Result<(Option<Vec<u8>>, Option<Vec<u8>>, Vec<TupleId>), CompressionError>
    {
        let mut offset = 0;

        let _start_page = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let _end_page = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let has_min = data[offset] != 0;
        offset += 1;
        let min_value = if has_min {
            let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let value = data[offset..offset + len].to_vec();
            offset += len;
            Some(value)
        } else {
            None
        };

        let has_max = data[offset] != 0;
        offset += 1;
        let max_value = if has_max {
            let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let value = data[offset..offset + len].to_vec();
            offset += len;
            Some(value)
        } else {
            None
        };

        let tid_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut tids = Vec::new();
        for _ in 0..tid_count {
            let page_id = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let slot = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
            offset += 2;
            tids.push((PageId(page_id), slot));
        }

        Ok((min_value, max_value, tids))
    }

    pub fn is_range_compressed(&self, range_idx: usize) -> bool {
        if range_idx < self.ranges.len() { self.ranges[range_idx].compressed } else { false }
    }

    pub fn range_count(&self) -> usize {
        self.ranges.len()
    }
}

impl Index for BRINIndex {
    fn insert(&mut self, key: &[u8], tid: TupleId) -> Result<(), IndexError> {
        let page = Self::get_page_from_tid(tid);
        self.update_range_bounds(page, key);
        let range = self.get_or_create_range(page);
        range.tids.push(tid);
        Ok(())
    }

    fn delete(&mut self, _key: &[u8], tid: TupleId) -> Result<bool, IndexError> {
        let page = Self::get_page_from_tid(tid);
        let range_idx = (page as usize) / self.pages_per_range;

        if range_idx < self.ranges.len() {
            let range = &mut self.ranges[range_idx];
            if range.compressed {
                return Err(IndexError::InvalidOperation);
            }
            if let Some(pos) = range.tids.iter().position(|t| *t == tid) {
                range.tids.remove(pos);
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn search(&self, key: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        let mut result = vec![];

        for range in &self.ranges {
            if range.compressed {
                continue;
            }

            let matches = match (&range.min_value, &range.max_value) {
                (Some(min), Some(max)) => key >= min.as_slice() && key <= max.as_slice(),
                _ => false,
            };

            if matches {
                result.extend_from_slice(&range.tids);
            }
        }

        if result.is_empty() { Err(IndexError::KeyNotFound) } else { Ok(result) }
    }

    fn range_search(&self, start: &[u8], end: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        let mut result = vec![];

        for range in &self.ranges {
            if range.compressed {
                continue;
            }

            let overlaps = match (&range.min_value, &range.max_value) {
                (Some(min), Some(max)) => max.as_slice() >= start && min.as_slice() <= end,
                _ => false,
            };

            if overlaps {
                result.extend_from_slice(&range.tids);
            }
        }

        if result.is_empty() { Err(IndexError::KeyNotFound) } else { Ok(result) }
    }

    fn index_type(&self) -> IndexType {
        IndexType::BRIN
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::PageId;

    #[test]
    fn test_brin_insert_and_search() {
        let mut index = BRINIndex::new(128);
        let tid = (PageId(0), 0);

        index.insert(b"key1", tid).unwrap();
        let result = index.search(b"key1").unwrap();
        assert!(result.contains(&tid));
    }

    #[test]
    fn test_brin_range_search() {
        let mut index = BRINIndex::new(128);

        index.insert(b"a", (PageId(0), 0)).unwrap();
        index.insert(b"m", (PageId(0), 1)).unwrap();
        index.insert(b"z", (PageId(0), 2)).unwrap();

        let result = index.range_search(b"a", b"n").unwrap();
        assert!(result.len() >= 2);
    }

    #[test]
    fn test_brin_multiple_ranges() {
        let mut index = BRINIndex::new(2);

        index.insert(b"a", (PageId(0), 0)).unwrap();
        index.insert(b"z", (PageId(5), 0)).unwrap();

        assert_eq!(index.ranges.len(), 3);
    }

    #[test]
    fn test_brin_not_found() {
        let index = BRINIndex::new(128);
        assert!(index.search(b"nonexistent").is_err());
    }

    #[test]
    fn test_brin_with_compression() {
        let index = BRINIndex::with_compression(128, CompressionAlgorithm::Lz4);
        assert_eq!(index.compression_algorithm(), CompressionAlgorithm::Lz4);
    }

    #[test]
    fn test_brin_set_compression() {
        let mut index = BRINIndex::new(128);
        index.set_compression_algorithm(CompressionAlgorithm::Zstd);
        assert_eq!(index.compression_algorithm(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_brin_insert_compressed() {
        let mut index = BRINIndex::with_compression(128, CompressionAlgorithm::Lz4);

        let large_key: Vec<u8> = vec![0u8; 3000];
        let tid = (PageId(0), 0);
        index.insert_compressed(&large_key, tid).unwrap();

        assert!(index.search(&large_key).is_ok());
    }

    #[test]
    fn test_brin_compress_range() {
        let mut index = BRINIndex::with_compression(128, CompressionAlgorithm::Lz4);

        for i in 0..10 {
            let key = vec![i as u8; 100];
            let tid = (PageId(i * 128), 0);
            index.insert(&key, tid).unwrap();
        }

        assert!(!index.is_range_compressed(0));

        index.compress_range(0).unwrap();
    }

    #[test]
    fn test_brin_decompress_range() {
        let mut index = BRINIndex::with_compression(128, CompressionAlgorithm::Lz4);

        for i in 0..10 {
            let key = vec![i as u8; 100];
            let tid = (PageId(i * 128), 0);
            index.insert(&key, tid).unwrap();
        }

        index.compress_range(0).unwrap();

        if index.is_range_compressed(0) {
            index.decompress_range(0).unwrap();
            assert!(!index.is_range_compressed(0));
        }
    }

    #[test]
    fn test_brin_range_count() {
        let mut index = BRINIndex::new(128);

        assert_eq!(index.range_count(), 0);

        index.insert(b"a", (PageId(0), 0)).unwrap();
        assert_eq!(index.range_count(), 1);

        index.insert(b"z", (PageId(1000), 0)).unwrap();
        assert_eq!(index.range_count(), 8);
    }

    #[test]
    fn test_brin_serialize_deserialize_range() {
        let mut index = BRINIndex::new(128);

        for i in 0..5 {
            let key = vec![i as u8; 10];
            let tid = (PageId(i * 128), i as u16);
            index.insert(&key, tid).unwrap();
        }

        let range = &index.ranges[0];
        let serialized = BRINIndex::serialize_range(range);
        let (min, max, tids) = BRINIndex::deserialize_range(&serialized).unwrap();

        assert_eq!(range.min_value, min);
        assert_eq!(range.max_value, max);
        assert_eq!(range.tids.len(), tids.len());
    }
}
