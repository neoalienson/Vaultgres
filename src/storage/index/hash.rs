use super::index_trait::{Index, IndexError, IndexType, TupleId};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct HashIndex {
    buckets: Vec<Bucket>,
    num_buckets: usize,
}

struct Bucket {
    entries: Vec<HashEntry>,
}

struct HashEntry {
    key: Vec<u8>,
    tids: Vec<TupleId>,
}

impl HashIndex {
    pub fn new(num_buckets: usize) -> Self {
        let buckets = (0..num_buckets).map(|_| Bucket { entries: vec![] }).collect();
        Self { buckets, num_buckets }
    }

    fn hash_key(&self, key: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn bucket_index(&self, hash: u64) -> usize {
        (hash as usize) % self.num_buckets
    }
}

impl Index for HashIndex {
    fn insert(&mut self, key: &[u8], tid: TupleId) -> Result<(), IndexError> {
        let hash = self.hash_key(key);
        let bucket_idx = self.bucket_index(hash);
        let bucket = &mut self.buckets[bucket_idx];

        for entry in &mut bucket.entries {
            if entry.key == key {
                entry.tids.push(tid);
                return Ok(());
            }
        }

        bucket.entries.push(HashEntry { key: key.to_vec(), tids: vec![tid] });
        Ok(())
    }

    fn delete(&mut self, key: &[u8], tid: TupleId) -> Result<bool, IndexError> {
        let hash = self.hash_key(key);
        let bucket_idx = self.bucket_index(hash);
        let bucket = &mut self.buckets[bucket_idx];

        for i in 0..bucket.entries.len() {
            if bucket.entries[i].key == key {
                if let Some(pos) = bucket.entries[i].tids.iter().position(|&t| t == tid) {
                    bucket.entries[i].tids.remove(pos);
                    if bucket.entries[i].tids.is_empty() {
                        bucket.entries.remove(i);
                    }
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn search(&self, key: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        let hash = self.hash_key(key);
        let bucket_idx = self.bucket_index(hash);
        let bucket = &self.buckets[bucket_idx];

        for entry in &bucket.entries {
            if entry.key == key {
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
}
