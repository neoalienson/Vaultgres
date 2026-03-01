use super::index_trait::{Index, IndexError, IndexType, TupleId};

pub struct ExpressionIndex {
    inner: Box<dyn Index>,
    expression: Box<dyn Fn(&[u8]) -> Vec<u8> + Send + Sync>,
}

impl ExpressionIndex {
    pub fn new<F>(inner: Box<dyn Index>, expression: F) -> Self
    where
        F: Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static,
    {
        Self { inner, expression: Box::new(expression) }
    }

    fn compute_key(&self, key: &[u8]) -> Vec<u8> {
        (self.expression)(key)
    }
}

impl Index for ExpressionIndex {
    fn insert(&mut self, key: &[u8], tid: TupleId) -> Result<(), IndexError> {
        let computed_key = self.compute_key(key);
        self.inner.insert(&computed_key, tid)
    }

    fn delete(&mut self, key: &[u8], tid: TupleId) -> Result<bool, IndexError> {
        let computed_key = self.compute_key(key);
        self.inner.delete(&computed_key, tid)
    }

    fn search(&self, key: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        let computed_key = self.compute_key(key);
        self.inner.search(&computed_key)
    }

    fn range_search(&self, start: &[u8], end: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        let computed_start = self.compute_key(start);
        let computed_end = self.compute_key(end);
        self.inner.range_search(&computed_start, &computed_end)
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
    fn test_expression_index_lowercase() {
        let hash_index = Box::new(HashIndex::new(16));
        let mut index = ExpressionIndex::new(hash_index, |key| key.to_ascii_lowercase());

        let tid = (PageId(1), 0);

        index.insert(b"HELLO", tid).unwrap();

        // Search with lowercase should find it
        let result = index.search(b"hello").unwrap();
        assert_eq!(result, vec![tid]);

        // Search with uppercase should also find it
        let result = index.search(b"HELLO").unwrap();
        assert_eq!(result, vec![tid]);
    }

    #[test]
    fn test_expression_index_transform() {
        let hash_index = Box::new(HashIndex::new(16));
        let mut index = ExpressionIndex::new(hash_index, |key| {
            // Simple transformation: add 1 to each byte
            key.iter().map(|&b| b.wrapping_add(1)).collect()
        });

        let tid = (PageId(1), 0);

        index.insert(b"abc", tid).unwrap();

        // Search with original key
        let result = index.search(b"abc").unwrap();
        assert_eq!(result, vec![tid]);
    }

    #[test]
    fn test_expression_index_delete() {
        let hash_index = Box::new(HashIndex::new(16));
        let mut index = ExpressionIndex::new(hash_index, |key| key.to_ascii_lowercase());

        let tid = (PageId(1), 0);

        index.insert(b"TEST", tid).unwrap();
        assert!(index.delete(b"test", tid).unwrap());
        assert!(index.search(b"test").is_err());
    }
}
