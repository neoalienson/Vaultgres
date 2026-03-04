use crate::catalog::Value;
use crate::storage::btree::BTree;
use crate::storage::hash_index::HashIndex;

pub struct IndexOnlyScan {
    index_type: IndexType,
    btree: Option<BTree>,
    hash: Option<HashIndex>,
    columns: Vec<String>,
}

enum IndexType {
    BTree,
    Hash,
}

impl IndexOnlyScan {
    pub fn new_btree(columns: Vec<String>) -> Self {
        Self { index_type: IndexType::BTree, btree: Some(BTree::new()), hash: None, columns }
    }

    pub fn new_hash(columns: Vec<String>) -> Self {
        Self { index_type: IndexType::Hash, btree: None, hash: Some(HashIndex::new()), columns }
    }

    pub fn scan(&self) -> Vec<Vec<Value>> {
        match self.index_type {
            IndexType::BTree => {
                if let Some(ref btree) = self.btree {
                    btree
                        .iter()
                        .map(|(key, _)| vec![Value::Text(String::from_utf8_lossy(key).to_string())])
                        .collect()
                } else {
                    Vec::new()
                }
            }
            IndexType::Hash => Vec::new(),
        }
    }

    pub fn scan_with_key(&self, key: &[u8]) -> Option<Vec<Value>> {
        let key_vec = key.to_vec();
        match self.index_type {
            IndexType::BTree => {
                if let Some(ref btree) = self.btree {
                    btree
                        .get(&key_vec)
                        .map(|_| vec![Value::Text(String::from_utf8_lossy(key).to_string())])
                } else {
                    None
                }
            }
            IndexType::Hash => {
                if let Some(ref hash) = self.hash {
                    hash.get(key)
                        .map(|_| vec![Value::Text(String::from_utf8_lossy(key).to_string())])
                } else {
                    None
                }
            }
        }
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::btree::TupleId;
    use crate::storage::page::PageId;

    #[test]
    fn test_index_only_scan_btree() {
        let mut scan = IndexOnlyScan::new_btree(vec!["id".to_string()]);
        if let Some(ref mut btree) = scan.btree {
            btree.insert(vec![1], TupleId { page_id: PageId(1), slot: 0 }).unwrap();
            btree.insert(vec![2], TupleId { page_id: PageId(1), slot: 1 }).unwrap();
        }

        let results = scan.scan();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_index_only_scan_with_key() {
        let mut scan = IndexOnlyScan::new_btree(vec!["email".to_string()]);
        if let Some(ref mut btree) = scan.btree {
            btree
                .insert(b"test@example.com".to_vec(), TupleId { page_id: PageId(1), slot: 0 })
                .unwrap();
        }

        let result = scan.scan_with_key(b"test@example.com");
        assert!(result.is_some());
    }

    #[test]
    fn test_index_only_scan_hash() {
        let scan = IndexOnlyScan::new_hash(vec!["id".to_string()]);
        assert_eq!(scan.columns().len(), 1);
    }

    #[test]
    fn test_index_only_scan_columns() {
        let scan = IndexOnlyScan::new_btree(vec!["col1".to_string(), "col2".to_string()]);
        assert_eq!(scan.columns(), &["col1", "col2"]);
    }
}
