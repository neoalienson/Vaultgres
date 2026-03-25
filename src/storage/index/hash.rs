use super::index_trait::{Index, IndexError, IndexType, TupleId};
use crate::storage::compression::{CompressionAlgorithm, compress, decompress, should_compress};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct HashIndex {
    buckets: Vec<Bucket>,
    num_buckets: usize,
    compression_algorithm: CompressionAlgorithm,
}

struct Bucket {
    entries: Vec<HashEntry>,
    compressed: bool,
}

struct HashEntry {
    key: Vec<u8>,
    tids: Vec<TupleId>,
    compressed: bool,
}

impl HashIndex {
    pub fn new(num_buckets: usize) -> Self {
        let buckets =
            (0..num_buckets).map(|_| Bucket { entries: vec![], compressed: false }).collect();
        Self { buckets, num_buckets, compression_algorithm: CompressionAlgorithm::Lz4 }
    }

    pub fn with_compression(num_buckets: usize, algorithm: CompressionAlgorithm) -> Self {
        let buckets =
            (0..num_buckets).map(|_| Bucket { entries: vec![], compressed: false }).collect();
        Self { buckets, num_buckets, compression_algorithm: algorithm }
    }

    pub fn set_compression_algorithm(&mut self, algorithm: CompressionAlgorithm) {
        self.compression_algorithm = algorithm;
    }

    pub fn compression_algorithm(&self) -> CompressionAlgorithm {
        self.compression_algorithm
    }

    fn hash_key(&self, key: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn bucket_index(&self, hash: u64) -> usize {
        (hash as usize) % self.num_buckets
    }

    pub fn insert_compressed(&mut self, key: &[u8], tid: TupleId) -> Result<(), IndexError> {
        let (key_to_store, is_compressed) = if should_compress(key.len()) {
            let compressed = compress(key, self.compression_algorithm)
                .map_err(|e| IndexError::Storage(e.to_string()))?;
            if compressed.len() < key.len() {
                let mut full_data = compressed;
                full_data.extend_from_slice(&(key.len() as u32).to_le_bytes());
                (full_data, true)
            } else {
                (key.to_vec(), false)
            }
        } else {
            (key.to_vec(), false)
        };

        let hash = self.hash_key(key);
        let bucket_idx = self.bucket_index(hash);
        let bucket = &mut self.buckets[bucket_idx];

        if bucket.compressed {
            return Err(IndexError::InvalidOperation);
        }

        for entry in &mut bucket.entries {
            if entry.key == key_to_store {
                entry.tids.push(tid);
                return Ok(());
            }
        }

        bucket.entries.push(HashEntry {
            key: key_to_store,
            tids: vec![tid],
            compressed: is_compressed,
        });
        Ok(())
    }

    pub fn compress_bucket(&mut self, bucket_idx: usize) -> Result<(), IndexError> {
        if bucket_idx >= self.num_buckets {
            return Err(IndexError::InvalidIndex);
        }

        let bucket = &mut self.buckets[bucket_idx];
        if bucket.compressed || bucket.entries.is_empty() {
            return Ok(());
        }

        let serialized = Self::serialize_bucket(bucket);
        let compressed = compress(&serialized, self.compression_algorithm)
            .map_err(|e| IndexError::Storage(e.to_string()))?;

        if compressed.len() < serialized.len() {
            bucket.entries = vec![HashEntry { key: compressed, tids: vec![], compressed: true }];
            bucket.compressed = true;
        }

        Ok(())
    }

    pub fn decompress_bucket(&mut self, bucket_idx: usize) -> Result<(), IndexError> {
        if bucket_idx >= self.num_buckets {
            return Err(IndexError::InvalidIndex);
        }

        let bucket = &mut self.buckets[bucket_idx];
        if !bucket.compressed || bucket.entries.is_empty() {
            return Ok(());
        }

        let compressed_key = &bucket.entries[0].key;
        let decompressed =
            decompress(compressed_key, self.compression_algorithm, compressed_key.len())
                .map_err(|e| IndexError::Storage(e.to_string()))?;
        let entries = Self::deserialize_bucket(&decompressed)?;

        bucket.entries = entries;
        bucket.compressed = false;

        Ok(())
    }

    fn serialize_bucket(bucket: &Bucket) -> Vec<u8> {
        let mut result = Vec::new();

        result.extend_from_slice(&(bucket.entries.len() as u32).to_le_bytes());

        for entry in &bucket.entries {
            result.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
            result.extend_from_slice(&entry.key);
            result.extend_from_slice(&(entry.tids.len() as u32).to_le_bytes());
            for tid in &entry.tids {
                result.extend_from_slice(&tid.0.0.to_le_bytes());
                result.extend_from_slice(&tid.1.to_le_bytes());
            }
        }

        result
    }

    fn deserialize_bucket(data: &[u8]) -> Result<Vec<HashEntry>, IndexError> {
        let mut entries = Vec::new();
        let mut offset = 0;

        let entry_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        for _ in 0..entry_count {
            let key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let key = data[offset..offset + key_len].to_vec();
            offset += key_len;

            let tid_count =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;

            let mut tids = Vec::new();
            for _ in 0..tid_count {
                let page_id = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                offset += 4;
                let slot = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
                offset += 2;
                tids.push((super::super::page::PageId(page_id), slot));
            }

            entries.push(HashEntry { key, tids, compressed: false });
        }

        Ok(entries)
    }

    pub fn is_bucket_compressed(&self, bucket_idx: usize) -> bool {
        if bucket_idx < self.num_buckets { self.buckets[bucket_idx].compressed } else { false }
    }

    pub fn bucket_size(&self, bucket_idx: usize) -> Option<usize> {
        if bucket_idx < self.num_buckets {
            let bucket = &self.buckets[bucket_idx];
            if bucket.compressed {
                bucket.entries.first().map(|e| e.key.len())
            } else {
                let mut size = 0;
                for entry in &bucket.entries {
                    size += entry.key.len();
                    size += entry.tids.len() * 6;
                }
                Some(size)
            }
        } else {
            None
        }
    }
}

impl Index for HashIndex {
    fn insert(&mut self, key: &[u8], tid: TupleId) -> Result<(), IndexError> {
        let hash = self.hash_key(key);
        let bucket_idx = self.bucket_index(hash);
        let bucket = &mut self.buckets[bucket_idx];

        if bucket.compressed {
            return Err(IndexError::InvalidOperation);
        }

        for entry in &mut bucket.entries {
            if entry.key == key {
                entry.tids.push(tid);
                return Ok(());
            }
        }

        bucket.entries.push(HashEntry { key: key.to_vec(), tids: vec![tid], compressed: false });
        Ok(())
    }

    fn delete(&mut self, key: &[u8], tid: TupleId) -> Result<bool, IndexError> {
        let hash = self.hash_key(key);
        let bucket_idx = self.bucket_index(hash);
        let bucket = &mut self.buckets[bucket_idx];

        if bucket.compressed {
            return Err(IndexError::InvalidOperation);
        }

        let mut entry_to_remove = None;
        let mut tid_found = false;

        for (i, entry) in bucket.entries.iter_mut().enumerate() {
            if entry.key == key {
                if let Some(pos) = entry.tids.iter().position(|&t| t == tid) {
                    entry.tids.remove(pos);
                    tid_found = true;
                    if entry.tids.is_empty() {
                        entry_to_remove = Some(i);
                    }
                    break;
                }
            }
        }

        if let Some(i) = entry_to_remove {
            bucket.entries.remove(i);
        }

        Ok(tid_found)
    }

    fn search(&self, key: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        let hash = self.hash_key(key);
        let bucket_idx = self.bucket_index(hash);
        let bucket = &self.buckets[bucket_idx];

        if bucket.compressed {
            return Err(IndexError::InvalidOperation);
        }

        for entry in &bucket.entries {
            let key_to_compare = if entry.compressed && entry.key.len() >= 4 {
                let original_size =
                    u32::from_le_bytes(entry.key[entry.key.len() - 4..].try_into().unwrap())
                        as usize;
                let compressed_data = &entry.key[..entry.key.len() - 4];
                let decompressed =
                    decompress(compressed_data, self.compression_algorithm, original_size)
                        .map_err(|e| IndexError::Storage(e.to_string()))?;
                decompressed
            } else {
                entry.key.clone()
            };

            if key_to_compare == key {
                return Ok(entry.tids.clone());
            }
        }
        Err(IndexError::KeyNotFound)
    }

    fn range_search(&self, _start: &[u8], _end: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        Err(IndexError::InvalidOperation)
    }

    fn index_type(&self) -> IndexType {
        IndexType::Hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::PageId;

    #[test]
    fn test_hash_insert_and_search() {
        let mut index = HashIndex::new(16);
        let tid = (PageId(1), 0);

        index.insert(b"key1", tid).unwrap();
        let result = index.search(b"key1").unwrap();
        assert_eq!(result, vec![tid]);
    }

    #[test]
    fn test_hash_not_found() {
        let index = HashIndex::new(16);
        assert!(index.search(b"nonexistent").is_err());
    }

    #[test]
    fn test_hash_delete() {
        let mut index = HashIndex::new(16);
        let tid = (PageId(1), 0);

        index.insert(b"key1", tid).unwrap();
        assert!(index.delete(b"key1", tid).unwrap());
        assert!(index.search(b"key1").is_err());
    }

    #[test]
    fn test_hash_collisions() {
        let mut index = HashIndex::new(4);
        let tid1 = (PageId(1), 0);
        let tid2 = (PageId(2), 0);

        index.insert(b"key1", tid1).unwrap();
        index.insert(b"key2", tid2).unwrap();

        assert_eq!(index.search(b"key1").unwrap(), vec![tid1]);
        assert_eq!(index.search(b"key2").unwrap(), vec![tid2]);
    }

    #[test]
    fn test_hash_range_not_supported() {
        let index = HashIndex::new(16);
        assert!(index.range_search(b"a", b"z").is_err());
    }

    #[test]
    fn test_hash_index_with_compression() {
        let index = HashIndex::with_compression(16, CompressionAlgorithm::Lz4);
        assert_eq!(index.compression_algorithm(), CompressionAlgorithm::Lz4);
    }

    #[test]
    fn test_hash_set_compression() {
        let mut index = HashIndex::new(16);
        index.set_compression_algorithm(CompressionAlgorithm::Zstd);
        assert_eq!(index.compression_algorithm(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_hash_insert_compressed() {
        let mut index = HashIndex::with_compression(16, CompressionAlgorithm::Lz4);
        let tid = (PageId(1), 0);

        let large_key: Vec<u8> = vec![0u8; 3000];
        index.insert_compressed(&large_key, tid).unwrap();

        assert!(index.search(&large_key).is_ok());
    }

    #[test]
    fn test_hash_compress_bucket() {
        let mut index = HashIndex::with_compression(4, CompressionAlgorithm::Lz4);

        for i in 0..4 {
            let key = vec![i as u8; 100];
            let tid = (PageId(i), 0);
            index.insert(&key, tid).unwrap();
        }

        assert!(!index.is_bucket_compressed(0));

        index.compress_bucket(0).unwrap();
    }

    #[test]
    fn test_hash_decompress_bucket() {
        let mut index = HashIndex::with_compression(4, CompressionAlgorithm::Lz4);

        for i in 0..4 {
            let key = vec![i as u8; 100];
            let tid = (PageId(i), 0);
            index.insert(&key, tid).unwrap();
        }

        index.compress_bucket(0).unwrap();

        if index.is_bucket_compressed(0) {
            index.decompress_bucket(0).unwrap();
            assert!(!index.is_bucket_compressed(0));
        }
    }

    #[test]
    fn test_hash_bucket_size() {
        let index = HashIndex::new(4);

        let size = index.bucket_size(0);
        assert!(size.is_some());
        assert_eq!(size.unwrap(), 0);
    }

    #[test]
    fn test_hash_serialize_deserialize_bucket() {
        let mut index = HashIndex::new(4);

        for i in 0..3 {
            let key = vec![i as u8; 10];
            let tid = (PageId(i), i as u16);
            index.insert(&key, tid).unwrap();
        }

        let bucket = &index.buckets[0];
        let serialized = HashIndex::serialize_bucket(bucket);
        let deserialized = HashIndex::deserialize_bucket(&serialized).unwrap();

        assert_eq!(bucket.entries.len(), deserialized.len());
    }
}
