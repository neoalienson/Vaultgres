use super::index_trait::{Index, IndexError, IndexType, TupleId};
use crate::storage::compression::{CompressionAlgorithm, compress, should_compress};
use crate::storage::page::PageId;
use std::collections::HashMap;

pub struct GINIndex {
    posting_lists: HashMap<Vec<u8>, PostingList>,
    compression_algorithm: CompressionAlgorithm,
}

struct PostingList {
    tids: Vec<TupleId>,
    compressed: bool,
}

impl Default for GINIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl GINIndex {
    pub fn new() -> Self {
        Self { posting_lists: HashMap::new(), compression_algorithm: CompressionAlgorithm::Lz4 }
    }

    pub fn with_compression(algorithm: CompressionAlgorithm) -> Self {
        Self { posting_lists: HashMap::new(), compression_algorithm: algorithm }
    }

    pub fn set_compression_algorithm(&mut self, algorithm: CompressionAlgorithm) {
        self.compression_algorithm = algorithm;
    }

    pub fn compression_algorithm(&self) -> CompressionAlgorithm {
        self.compression_algorithm
    }

    fn extract_keys(&self, value: &[u8]) -> Vec<Vec<u8>> {
        if value.is_empty() {
            return vec![];
        }

        if value.contains(&0) {
            let keys: Vec<Vec<u8>> =
                value.split(|&b| b == 0).filter(|s| !s.is_empty()).map(|s| s.to_vec()).collect();
            if keys.is_empty() { vec![value.to_vec()] } else { keys }
        } else {
            vec![value.to_vec()]
        }
    }

    pub fn insert_compressed(&mut self, key: &[u8], tid: TupleId) -> Result<(), IndexError> {
        self.insert(key, tid)
    }

    pub fn compress_posting_list(&mut self, key: &[u8]) -> Result<(), IndexError> {
        let key_to_find = key.to_vec();

        if let Some(posting_list) = self.posting_lists.get_mut(&key_to_find) {
            if posting_list.compressed || posting_list.tids.is_empty() {
                return Ok(());
            }

            let serialized = Self::serialize_posting_list(&posting_list.tids);
            let compressed = compress(&serialized, self.compression_algorithm)
                .map_err(|e| IndexError::Storage(e.to_string()))?;

            if compressed.len() < serialized.len() {
                posting_list.tids = vec![];
                posting_list.tids = Self::deserialize_posting_list(&compressed)
                    .map_err(|e| IndexError::Storage(e.to_string()))?;
                posting_list.compressed = true;
            }
        }

        Ok(())
    }

    pub fn decompress_posting_list(&mut self, key: &[u8]) -> Result<(), IndexError> {
        let key_to_find = key.to_vec();

        if let Some(posting_list) = self.posting_lists.get_mut(&key_to_find) {
            if !posting_list.compressed {
                return Ok(());
            }

            if posting_list.tids.is_empty() {
                posting_list.compressed = false;
            }
        }

        Ok(())
    }

    fn serialize_posting_list(tids: &[TupleId]) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&(tids.len() as u32).to_le_bytes());

        for tid in tids {
            result.extend_from_slice(&tid.0.0.to_le_bytes());
            result.extend_from_slice(&tid.1.to_le_bytes());
        }

        result
    }

    fn deserialize_posting_list(
        data: &[u8],
    ) -> std::result::Result<Vec<TupleId>, crate::storage::compression::CompressionError> {
        let mut tids = Vec::new();
        let mut offset = 0;

        let count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        for _ in 0..count {
            let page_id = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let slot = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
            offset += 2;
            tids.push((PageId(page_id), slot));
        }

        Ok(tids)
    }

    pub fn is_posting_list_compressed(&self, key: &[u8]) -> bool {
        if let Some(posting_list) = self.posting_lists.get(key) {
            posting_list.compressed
        } else {
            false
        }
    }

    pub fn posting_list_size(&self, key: &[u8]) -> Option<usize> {
        if let Some(posting_list) = self.posting_lists.get(key) {
            Some(posting_list.tids.len())
        } else {
            None
        }
    }

    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.posting_lists.keys().cloned().collect()
    }
}

impl Index for GINIndex {
    fn insert(&mut self, key: &[u8], tid: TupleId) -> Result<(), IndexError> {
        let keys = self.extract_keys(key);

        for k in keys {
            let posting_list = self
                .posting_lists
                .entry(k)
                .or_insert_with(|| PostingList { tids: vec![], compressed: false });

            if !posting_list.tids.contains(&tid) {
                posting_list.tids.push(tid);
            }
        }

        Ok(())
    }

    fn delete(&mut self, key: &[u8], tid: TupleId) -> Result<bool, IndexError> {
        let keys = self.extract_keys(key);
        let mut deleted = false;

        for k in keys {
            if let Some(posting_list) = self.posting_lists.get_mut(&k) {
                if posting_list.compressed {
                    return Err(IndexError::InvalidOperation);
                }
                if let Some(pos) = posting_list.tids.iter().position(|t| *t == tid) {
                    posting_list.tids.remove(pos);
                    deleted = true;
                }
            }
        }

        self.posting_lists.retain(|_, pl| !pl.tids.is_empty());

        Ok(deleted)
    }

    fn search(&self, key: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        let keys = self.extract_keys(key);

        if keys.is_empty() {
            return Err(IndexError::KeyNotFound);
        }

        let mut result: Option<Vec<TupleId>> = None;

        for k in keys {
            if let Some(posting_list) = self.posting_lists.get(&k) {
                if posting_list.compressed {
                    return Err(IndexError::InvalidOperation);
                }
                match result {
                    None => result = Some(posting_list.tids.clone()),
                    Some(ref mut tids) => {
                        tids.retain(|tid| posting_list.tids.contains(tid));
                    }
                }
            } else {
                return Err(IndexError::KeyNotFound);
            }
        }

        result.ok_or(IndexError::KeyNotFound)
    }

    fn range_search(&self, _start: &[u8], _end: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        Err(IndexError::InvalidOperation)
    }

    fn index_type(&self) -> IndexType {
        IndexType::GIN
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::PageId;

    #[test]
    fn test_gin_insert_and_search() {
        let mut index = GINIndex::new();
        let tid = (PageId(1), 0);

        index.insert(b"key1", tid).unwrap();
        let result = index.search(b"key1").unwrap();
        assert_eq!(result, vec![tid]);
    }

    #[test]
    fn test_gin_multiple_keys() {
        let mut index = GINIndex::new();
        let tid = (PageId(1), 0);

        index.insert(b"a\0b\0c", tid).unwrap();

        let result = index.search(b"a").unwrap();
        assert!(result.contains(&tid));

        let result = index.search(b"b").unwrap();
        assert!(result.contains(&tid));
    }

    #[test]
    fn test_gin_containment() {
        let mut index = GINIndex::new();
        let tid1 = (PageId(1), 0);
        let tid2 = (PageId(2), 0);

        index.insert(b"a\0b", tid1).unwrap();
        index.insert(b"b\0c", tid2).unwrap();

        let result = index.search(b"a\0b").unwrap();
        assert_eq!(result, vec![tid1]);
    }

    #[test]
    fn test_gin_not_found() {
        let index = GINIndex::new();
        assert!(index.search(b"nonexistent").is_err());
    }

    #[test]
    fn test_gin_delete() {
        let mut index = GINIndex::new();
        let tid = (PageId(1), 0);

        index.insert(b"key1", tid).unwrap();
        assert!(index.delete(b"key1", tid).unwrap());
        assert!(index.search(b"key1").is_err());
    }

    #[test]
    fn test_gin_with_compression() {
        let index = GINIndex::with_compression(CompressionAlgorithm::Lz4);
        assert_eq!(index.compression_algorithm(), CompressionAlgorithm::Lz4);
    }

    #[test]
    fn test_gin_set_compression() {
        let mut index = GINIndex::new();
        index.set_compression_algorithm(CompressionAlgorithm::Zstd);
        assert_eq!(index.compression_algorithm(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_gin_insert_compressed() {
        let mut index = GINIndex::with_compression(CompressionAlgorithm::Lz4);
        let tid = (PageId(1), 0);

        let large_key: Vec<u8> = vec![0u8; 3000];
        index.insert_compressed(&large_key, tid).unwrap();

        assert!(index.search(&large_key).is_ok());
    }

    #[test]
    fn test_gin_posting_list_size() {
        let mut index = GINIndex::new();
        let tid = (PageId(1), 0);

        index.insert(b"key1", tid).unwrap();

        let size = index.posting_list_size(b"key1");
        assert!(size.is_some());
        assert_eq!(size.unwrap(), 1);
    }

    #[test]
    fn test_gin_keys() {
        let mut index = GINIndex::new();

        index.insert(b"a", (PageId(1), 0)).unwrap();
        index.insert(b"b", (PageId(2), 0)).unwrap();

        let keys = index.keys();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_gin_serialize_deserialize_posting_list() {
        let tids = vec![(PageId(1), 0), (PageId(2), 1), (PageId(3), 2)];

        let serialized = GINIndex::serialize_posting_list(&tids);
        let deserialized = GINIndex::deserialize_posting_list(&serialized).unwrap();

        assert_eq!(tids.len(), deserialized.len());
        for (orig, deser) in tids.iter().zip(deserialized.iter()) {
            assert_eq!(orig.0, deser.0);
            assert_eq!(orig.1, deser.1);
        }
    }
}
