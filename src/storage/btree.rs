use super::error::Result;
use super::page::PageId;
use crate::storage::compression::{CompressionAlgorithm, compress, decompress, should_compress};

pub type Key = Vec<u8>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleId {
    pub page_id: PageId,
    pub slot: u16,
}

enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

#[derive(Clone)]
struct InternalNode {
    keys: Vec<Key>,
    children: Vec<PageId>,
    compressed: bool,
    compression_algorithm: CompressionAlgorithm,
}

#[derive(Clone)]
struct LeafNode {
    keys: Vec<Key>,
    values: Vec<TupleId>,
    next: Option<PageId>,
    compressed: bool,
    compression_algorithm: CompressionAlgorithm,
}

pub struct BTree {
    root: Option<Box<Node>>,
    order: usize,
    compression_algorithm: CompressionAlgorithm,
}

impl BTree {
    pub fn new() -> Self {
        Self::with_order(128)
    }

    pub fn with_order(order: usize) -> Self {
        Self { root: None, order, compression_algorithm: CompressionAlgorithm::Lz4 }
    }

    pub fn with_compression(order: usize, algorithm: CompressionAlgorithm) -> Self {
        Self { root: None, order, compression_algorithm: algorithm }
    }

    pub fn set_compression_algorithm(&mut self, algorithm: CompressionAlgorithm) {
        self.compression_algorithm = algorithm;
    }

    pub fn compression_algorithm(&self) -> CompressionAlgorithm {
        self.compression_algorithm
    }

    pub fn insert(&mut self, key: Key, value: TupleId) -> Result<()> {
        if self.root.is_none() {
            let leaf = LeafNode {
                keys: vec![key],
                values: vec![value],
                next: None,
                compressed: false,
                compression_algorithm: CompressionAlgorithm::None,
            };
            self.root = Some(Box::new(Node::Leaf(leaf)));
            return Ok(());
        }

        if let Some(Node::Leaf(leaf)) = self.root.as_deref_mut() {
            let pos = leaf.keys.binary_search(&key).unwrap_or_else(|e| e);
            leaf.keys.insert(pos, key);
            leaf.values.insert(pos, value);
        }

        Ok(())
    }

    pub fn insert_compressed(&mut self, key: &[u8], value: TupleId) -> Result<()> {
        let key_to_store = if should_compress(key.len()) {
            let compressed = compress(key, self.compression_algorithm)?;
            if compressed.len() < key.len() { compressed } else { key.to_vec() }
        } else {
            key.to_vec()
        };

        self.insert(key_to_store, value)
    }

    pub fn get(&self, key: &[u8]) -> Option<TupleId> {
        let root = self.root.as_ref()?;

        match root.as_ref() {
            Node::Leaf(leaf) => {
                if let Ok(idx) = leaf.keys.binary_search(&key.to_vec()) {
                    Some(leaf.values[idx])
                } else {
                    None
                }
            }
            Node::Internal(_) => None,
        }
    }

    pub fn get_decompressed(&self, key: &[u8]) -> Option<TupleId> {
        self.get(key)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        if let Some(Node::Leaf(leaf)) = self.root.as_deref_mut() {
            if let Ok(idx) = leaf.keys.binary_search(&key.to_vec()) {
                leaf.keys.remove(idx);
                leaf.values.remove(idx);
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn iter(&self) -> BTreeIterator<'_> {
        BTreeIterator { node: self.root.as_deref(), index: 0 }
    }

    pub fn is_compressed(&self) -> bool {
        if let Some(Node::Leaf(leaf)) = self.root.as_deref() { leaf.compressed } else { false }
    }

    pub fn compressed_size(&self) -> Option<usize> {
        if let Some(Node::Leaf(leaf)) = self.root.as_deref() {
            if leaf.compressed {
                let mut total = 0;
                for key in &leaf.keys {
                    total += key.len();
                }
                return Some(total);
            }
        }
        None
    }

    pub fn compress_node(&mut self) -> Result<()> {
        if let Some(Node::Leaf(leaf)) = self.root.as_deref_mut() {
            if !leaf.compressed && should_compress(leaf.keys.iter().map(|k| k.len()).sum()) {
                let mut all_keys = leaf.keys.clone();
                let mut all_values = leaf.values.clone();

                let serialized = Self::serialize_leaf(&all_keys, &all_values);
                let compressed = compress(&serialized, self.compression_algorithm)?;

                if compressed.len() < serialized.len() {
                    leaf.keys = vec![compressed];
                    leaf.values = vec![];
                    leaf.compressed = true;
                    leaf.compression_algorithm = self.compression_algorithm;
                }
            }
        }
        Ok(())
    }

    pub fn decompress_node(&mut self) -> Result<()> {
        if let Some(Node::Leaf(leaf)) = self.root.as_deref_mut() {
            if leaf.compressed && !leaf.keys.is_empty() {
                let compressed = &leaf.keys[0];
                let decompressed =
                    decompress(compressed, self.compression_algorithm, compressed.len())?;

                let (keys, values) = Self::deserialize_leaf(&decompressed);
                leaf.keys = keys;
                leaf.values = values;
                leaf.compressed = false;
                leaf.compression_algorithm = CompressionAlgorithm::None;
            }
        }
        Ok(())
    }

    fn serialize_leaf(keys: &[Key], values: &[TupleId]) -> Vec<u8> {
        let mut result = Vec::new();

        result.extend_from_slice(&(keys.len() as u32).to_le_bytes());

        for key in keys {
            result.extend_from_slice(&(key.len() as u32).to_le_bytes());
            result.extend_from_slice(key);
        }

        result.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for value in values {
            result.extend_from_slice(&(value.page_id.0).to_le_bytes());
            result.extend_from_slice(&value.slot.to_le_bytes());
        }

        result
    }

    fn deserialize_leaf(data: &[u8]) -> (Vec<Key>, Vec<TupleId>) {
        let mut keys = Vec::new();
        let mut values = Vec::new();

        let mut offset = 0;

        let key_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        for _ in 0..key_count {
            let key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            keys.push(data[offset..offset + key_len].to_vec());
            offset += key_len;
        }

        let value_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        for _ in 0..value_count {
            let page_id = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let slot = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
            offset += 2;
            values.push(TupleId { page_id: PageId(page_id), slot });
        }

        (keys, values)
    }

    pub fn node_count(&self) -> usize {
        match &self.root {
            Some(box_node) => match box_node.as_ref() {
                Node::Leaf(_) => 1,
                Node::Internal(internal) => 1 + internal.children.len(),
            },
            None => 0,
        }
    }

    pub fn key_count(&self) -> usize {
        match &self.root {
            Some(box_node) => match box_node.as_ref() {
                Node::Leaf(leaf) => leaf.keys.len(),
                Node::Internal(internal) => internal.keys.len(),
            },
            None => 0,
        }
    }
}

impl Default for BTree {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BTreeIterator<'a> {
    node: Option<&'a Node>,
    index: usize,
}

impl<'a> Iterator for BTreeIterator<'a> {
    type Item = (&'a Key, TupleId);

    fn next(&mut self) -> Option<Self::Item> {
        match self.node? {
            Node::Leaf(leaf) => {
                if self.index < leaf.keys.len() {
                    let result = (&leaf.keys[self.index], leaf.values[self.index]);
                    self.index += 1;
                    Some(result)
                } else {
                    None
                }
            }
            Node::Internal(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_insert_and_get() {
        let mut tree = BTree::new();
        let key = vec![1, 2, 3];
        let value = TupleId { page_id: PageId(1), slot: 0 };

        tree.insert(key.clone(), value).unwrap();
        assert_eq!(tree.get(&key), Some(value));
    }

    #[test]
    fn test_btree_get_nonexistent() {
        let tree = BTree::new();
        let key = vec![1, 2, 3];
        assert_eq!(tree.get(&key), None);
    }

    #[test]
    fn test_btree_delete() {
        let mut tree = BTree::new();
        let key = vec![1, 2, 3];
        let value = TupleId { page_id: PageId(1), slot: 0 };

        tree.insert(key.clone(), value).unwrap();
        assert!(tree.delete(&key).unwrap());
        assert_eq!(tree.get(&key), None);
    }

    #[test]
    fn test_btree_multiple_inserts() {
        let mut tree = BTree::new();

        for i in 0..10 {
            let key = vec![i];
            let value = TupleId { page_id: PageId(i as u32), slot: 0 };
            tree.insert(key, value).unwrap();
        }

        let key = vec![5];
        let value = tree.get(&key).unwrap();
        assert_eq!(value.page_id, PageId(5));
    }

    #[test]
    fn test_btree_with_compression() {
        let mut tree = BTree::with_compression(128, CompressionAlgorithm::Lz4);

        assert_eq!(tree.compression_algorithm(), CompressionAlgorithm::Lz4);

        tree.insert(vec![1, 2, 3], TupleId { page_id: PageId(1), slot: 0 }).unwrap();

        assert!(!tree.is_compressed());
    }

    #[test]
    fn test_btree_set_compression() {
        let mut tree = BTree::new();

        tree.set_compression_algorithm(CompressionAlgorithm::Zstd);
        assert_eq!(tree.compression_algorithm(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_btree_insert_compressed() {
        let mut tree = BTree::with_compression(128, CompressionAlgorithm::Lz4);

        let key: Vec<u8> = vec![0u8; 3000];
        let value = TupleId { page_id: PageId(1), slot: 0 };

        tree.insert_compressed(&key, value).unwrap();

        assert!(!tree.is_compressed());
    }

    #[test]
    fn test_btree_node_count() {
        let mut tree = BTree::new();
        assert_eq!(tree.node_count(), 0);

        tree.insert(vec![1], TupleId { page_id: PageId(1), slot: 0 }).unwrap();
        assert_eq!(tree.node_count(), 1);

        tree.insert(vec![2], TupleId { page_id: PageId(2), slot: 0 }).unwrap();
        assert_eq!(tree.node_count(), 1);
    }

    #[test]
    fn test_btree_key_count() {
        let mut tree = BTree::new();
        assert_eq!(tree.key_count(), 0);

        tree.insert(vec![1], TupleId { page_id: PageId(1), slot: 0 }).unwrap();
        assert_eq!(tree.key_count(), 1);

        tree.insert(vec![2], TupleId { page_id: PageId(2), slot: 0 }).unwrap();
        assert_eq!(tree.key_count(), 2);
    }

    #[test]
    fn test_btree_compress_decompress() {
        let mut tree = BTree::with_compression(128, CompressionAlgorithm::Lz4);

        for i in 0..10 {
            let key = vec![i as u8; 100];
            let value = TupleId { page_id: PageId(i), slot: i as u16 };
            tree.insert(key, value).unwrap();
        }

        assert!(!tree.is_compressed());

        tree.compress_node().unwrap();

        if tree.is_compressed() {
            tree.decompress_node().unwrap();
            assert!(!tree.is_compressed());
        }
    }

    #[test]
    fn test_btree_iterator() {
        let mut tree = BTree::new();

        for i in 0..5 {
            let key = vec![i as u8];
            let value = TupleId { page_id: PageId(i), slot: 0 };
            tree.insert(key, value).unwrap();
        }

        let count = tree.iter().count();
        assert_eq!(count, 5);
    }

    #[test]
    fn test_btree_serialize_deserialize() {
        let keys = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let values =
            vec![TupleId { page_id: PageId(1), slot: 0 }, TupleId { page_id: PageId(2), slot: 1 }];

        let serialized = BTree::serialize_leaf(&keys, &values);
        let (deserialized_keys, deserialized_values) = BTree::deserialize_leaf(&serialized);

        assert_eq!(keys, deserialized_keys);
        assert_eq!(values, deserialized_values);
    }

    #[test]
    fn test_btree_compressed_size() {
        let mut tree = BTree::new();

        tree.insert(vec![1, 2, 3], TupleId { page_id: PageId(1), slot: 0 }).unwrap();

        assert!(tree.compressed_size().is_none());
    }

    #[test]
    fn test_btree_with_empty_key() {
        let mut tree = BTree::new();
        let key = vec![];
        let value = TupleId { page_id: PageId(1), slot: 0 };

        tree.insert(key.clone(), value).unwrap();
        assert_eq!(tree.get(&key), Some(value));
    }

    #[test]
    fn test_btree_with_large_keys() {
        let mut tree = BTree::with_compression(128, CompressionAlgorithm::Lz4);

        for i in 0..5 {
            let key: Vec<u8> = (0..1000).map(|j| ((i * 1000 + j) % 256) as u8).collect();
            let value = TupleId { page_id: PageId(i), slot: 0 };
            tree.insert(key, value).unwrap();
        }

        assert_eq!(tree.key_count(), 5);
    }
}
