use super::index_trait::{Index, IndexError, IndexType, TupleId};

pub struct PartialIndex {
    inner: Box<dyn Index>,
    predicate: Box<dyn Fn(&[u8]) -> bool + Send + Sync>,
}

impl PartialIndex {
    pub fn new<F>(inner: Box<dyn Index>, predicate: F) -> Self
    where
        F: Fn(&[u8]) -> bool + Send + Sync + 'static,
    {
        Self { inner, predicate: Box::new(predicate) }
    }

    fn should_index(&self, key: &[u8]) -> bool {
        (self.predicate)(key)
    }
}

impl Index for PartialIndex {
    fn insert(&mut self, key: &[u8], tid: TupleId) -> Result<(), IndexError> {
        if self.should_index(key) { self.inner.insert(key, tid) } else { Ok(()) }
    }

    fn delete(&mut self, key: &[u8], tid: TupleId) -> Result<bool, IndexError> {
        if self.should_index(key) { self.inner.delete(key, tid) } else { Ok(false) }
    }

    fn search(&self, key: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        self.inner.search(key)
    }

    fn range_search(&self, start: &[u8], end: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        self.inner.range_search(start, end)
    }

    fn index_type(&self) -> IndexType {
        self.inner.index_type()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::index::hash::HashIndex;
    use crate::storage::page::PageId;

    #[test]
    fn test_partial_index_filters() {
        let hash_index = Box::new(HashIndex::new(16));
        let mut index = PartialIndex::new(hash_index, |key| key[0] > b'a');

        let tid1 = (PageId(1), 0);
        let tid2 = (PageId(2), 0);

        index.insert(b"a", tid1).unwrap();
        index.insert(b"z", tid2).unwrap();

        // 'a' should not be indexed
        assert!(index.search(b"a").is_err());

        // 'z' should be indexed
        assert_eq!(index.search(b"z").unwrap(), vec![tid2]);
    }

    #[test]
    fn test_partial_index_delete() {
        let hash_index = Box::new(HashIndex::new(16));
        let mut index = PartialIndex::new(hash_index, |key| key[0] > b'm');

        let tid = (PageId(1), 0);

        index.insert(b"z", tid).unwrap();
        assert!(index.delete(b"z", tid).unwrap());

        // Deleting non-indexed key returns false
        assert!(!index.delete(b"a", tid).unwrap());
    }
}
