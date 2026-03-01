use super::index_trait::{Index, IndexError, IndexType, TupleId};
use std::collections::HashMap;

pub struct GINIndex {
    posting_lists: HashMap<Vec<u8>, PostingList>,
}

struct PostingList {
    tids: Vec<TupleId>,
}

impl GINIndex {
    pub fn new() -> Self {
        Self { posting_lists: HashMap::new() }
    }

    fn extract_keys(&self, value: &[u8]) -> Vec<Vec<u8>> {
        if value.is_empty() {
            return vec![];
        }

        // Simple extraction: split by null bytes or treat as single key
        if value.contains(&0) {
            value.split(|&b| b == 0).filter(|s| !s.is_empty()).map(|s| s.to_vec()).collect()
        } else {
            vec![value.to_vec()]
        }
    }
}

impl Index for GINIndex {
    fn insert(&mut self, key: &[u8], tid: TupleId) -> Result<(), IndexError> {
        let keys = self.extract_keys(key);

        for k in keys {
            let posting_list =
                self.posting_lists.entry(k).or_insert_with(|| PostingList { tids: vec![] });

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
                if let Some(pos) = posting_list.tids.iter().position(|&t| t == tid) {
                    posting_list.tids.remove(pos);
                    deleted = true;
                }
            }
        }

        // Remove empty posting lists
        self.posting_lists.retain(|_, pl| !pl.tids.is_empty());

        Ok(deleted)
    }

    fn search(&self, key: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        let keys = self.extract_keys(key);

        if keys.is_empty() {
            return Err(IndexError::KeyNotFound);
        }

        // Return TIDs that contain all keys (AND semantics)
        let mut result: Option<Vec<TupleId>> = None;

        for k in keys {
            if let Some(posting_list) = self.posting_lists.get(&k) {
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

        // Insert array-like value with null separator
        index.insert(b"a\0b\0c", tid).unwrap();

        // Search for individual elements
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

        // Search for documents containing both 'a' and 'b'
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
}
