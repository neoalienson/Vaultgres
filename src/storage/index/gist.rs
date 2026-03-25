use super::index_trait::{Index, IndexError, IndexType, TupleId};
use crate::storage::compression::{CompressionAlgorithm, compress, decompress, should_compress};
use crate::storage::page::PageId;

pub struct GiSTIndex {
    root: Option<Box<GiSTNode>>,
    max_entries: usize,
    compression_algorithm: CompressionAlgorithm,
}

enum GiSTNode {
    Internal(InternalNode),
    Leaf(LeafNode),
}

struct InternalNode {
    keys: Vec<BoundingBox>,
    children: Vec<Box<GiSTNode>>,
    compressed: bool,
}

struct LeafNode {
    keys: Vec<BoundingBox>,
    tids: Vec<TupleId>,
    compressed: bool,
}

#[derive(Debug, Clone)]
struct BoundingBox {
    min: Vec<u8>,
    max: Vec<u8>,
}

impl BoundingBox {
    fn new(key: &[u8]) -> Self {
        Self { min: key.to_vec(), max: key.to_vec() }
    }

    fn contains(&self, key: &[u8]) -> bool {
        key >= self.min.as_slice() && key <= self.max.as_slice()
    }

    fn overlaps(&self, other: &BoundingBox) -> bool {
        self.max.as_slice() >= other.min.as_slice() && self.min.as_slice() <= other.max.as_slice()
    }

    fn union(&self, other: &BoundingBox) -> BoundingBox {
        BoundingBox {
            min: std::cmp::min(&self.min, &other.min).clone(),
            max: std::cmp::max(&self.max, &other.max).clone(),
        }
    }
}

impl Default for GiSTIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl GiSTIndex {
    pub fn new() -> Self {
        Self { root: None, max_entries: 50, compression_algorithm: CompressionAlgorithm::Lz4 }
    }

    pub fn with_compression(algorithm: CompressionAlgorithm) -> Self {
        Self { root: None, max_entries: 50, compression_algorithm: algorithm }
    }

    pub fn set_compression_algorithm(&mut self, algorithm: CompressionAlgorithm) {
        self.compression_algorithm = algorithm;
    }

    pub fn compression_algorithm(&self) -> CompressionAlgorithm {
        self.compression_algorithm
    }

    fn insert_into_leaf(&mut self, leaf: &mut LeafNode, key: &[u8], tid: TupleId) {
        leaf.keys.push(BoundingBox::new(key));
        leaf.tids.push(tid);
    }

    fn search_node(&self, node: &GiSTNode, key: &[u8]) -> Vec<TupleId> {
        match node {
            GiSTNode::Leaf(leaf) => {
                let mut result = vec![];
                for (i, bbox) in leaf.keys.iter().enumerate() {
                    if bbox.contains(key) {
                        result.push(leaf.tids[i]);
                    }
                }
                result
            }
            GiSTNode::Internal(internal) => {
                let mut result = vec![];
                for (i, bbox) in internal.keys.iter().enumerate() {
                    if bbox.contains(key) {
                        result.extend(self.search_node(&internal.children[i], key));
                    }
                }
                result
            }
        }
    }

    pub fn insert_compressed(&mut self, key: &[u8], tid: TupleId) -> Result<(), IndexError> {
        if self.root.is_none() {
            self.root = Some(Box::new(GiSTNode::Leaf(LeafNode {
                keys: vec![],
                tids: vec![],
                compressed: false,
            })));
        }

        if let Some(GiSTNode::Leaf(leaf)) = self.root.as_deref_mut() {
            if leaf.compressed {
                return Err(IndexError::InvalidOperation);
            }
            leaf.keys.push(BoundingBox::new(key));
            leaf.tids.push(tid);
        }

        Ok(())
    }

    pub fn compress_entry(&mut self, key: &[u8]) -> Result<(), IndexError> {
        if let Some(GiSTNode::Leaf(leaf)) = self.root.as_deref_mut() {
            if !leaf.compressed {
                let serialized = Self::serialize_leaf(leaf);
                let compressed = compress(&serialized, self.compression_algorithm)
                    .map_err(|e| IndexError::Storage(e.to_string()))?;

                if compressed.len() < serialized.len() {
                    leaf.keys = vec![];
                    leaf.tids = vec![];
                    leaf.compressed = true;
                }
            }
        }
        Ok(())
    }

    pub fn decompress_entry(&mut self) -> Result<(), IndexError> {
        Ok(())
    }

    fn serialize_leaf(leaf: &LeafNode) -> Vec<u8> {
        let mut result = Vec::new();

        result.extend_from_slice(&(leaf.keys.len() as u32).to_le_bytes());

        for bbox in &leaf.keys {
            result.extend_from_slice(&(bbox.min.len() as u32).to_le_bytes());
            result.extend_from_slice(&bbox.min);
            result.extend_from_slice(&(bbox.max.len() as u32).to_le_bytes());
            result.extend_from_slice(&bbox.max);
        }

        result.extend_from_slice(&(leaf.tids.len() as u32).to_le_bytes());
        for tid in &leaf.tids {
            result.extend_from_slice(&tid.0.0.to_le_bytes());
            result.extend_from_slice(&tid.1.to_le_bytes());
        }

        result
    }

    fn deserialize_leaf(data: &[u8]) -> Result<LeafNode, IndexError> {
        let mut offset = 0;

        let key_count = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut keys = Vec::new();
        for _ in 0..key_count {
            let min_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let min = data[offset..offset + min_len].to_vec();
            offset += min_len;

            let max_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let max = data[offset..offset + max_len].to_vec();
            offset += max_len;

            keys.push(BoundingBox { min, max });
        }

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

        Ok(LeafNode { keys, tids, compressed: false })
    }

    pub fn is_compressed(&self) -> bool {
        if let Some(GiSTNode::Leaf(leaf)) = self.root.as_deref() { leaf.compressed } else { false }
    }

    pub fn node_count(&self) -> usize {
        match &self.root {
            Some(box_node) => match box_node.as_ref() {
                GiSTNode::Leaf(_) => 1,
                GiSTNode::Internal(internal) => 1 + internal.children.len(),
            },
            None => 0,
        }
    }
}

impl Index for GiSTIndex {
    fn insert(&mut self, key: &[u8], tid: TupleId) -> Result<(), IndexError> {
        if self.root.is_none() {
            self.root = Some(Box::new(GiSTNode::Leaf(LeafNode {
                keys: vec![],
                tids: vec![],
                compressed: false,
            })));
        }

        if let Some(GiSTNode::Leaf(leaf)) = self.root.as_deref_mut() {
            if leaf.compressed {
                return Err(IndexError::InvalidOperation);
            }
            leaf.keys.push(BoundingBox::new(key));
            leaf.tids.push(tid);
        }

        Ok(())
    }

    fn delete(&mut self, _key: &[u8], _tid: TupleId) -> Result<bool, IndexError> {
        Ok(false)
    }

    fn search(&self, key: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        match &self.root {
            Some(root) => {
                if let GiSTNode::Leaf(leaf) = root.as_ref() {
                    if leaf.compressed {
                        return Err(IndexError::InvalidOperation);
                    }
                }
                let result = self.search_node(root, key);
                if result.is_empty() { Err(IndexError::KeyNotFound) } else { Ok(result) }
            }
            None => Err(IndexError::KeyNotFound),
        }
    }

    fn range_search(&self, start: &[u8], end: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        let query_box = BoundingBox { min: start.to_vec(), max: end.to_vec() };

        fn search_range(node: &GiSTNode, query: &BoundingBox) -> Vec<TupleId> {
            match node {
                GiSTNode::Leaf(leaf) => {
                    let mut result = vec![];
                    for (i, bbox) in leaf.keys.iter().enumerate() {
                        if bbox.overlaps(query) {
                            result.push(leaf.tids[i]);
                        }
                    }
                    result
                }
                GiSTNode::Internal(internal) => {
                    let mut result = vec![];
                    for (i, bbox) in internal.keys.iter().enumerate() {
                        if bbox.overlaps(query) {
                            result.extend(search_range(&internal.children[i], query));
                        }
                    }
                    result
                }
            }
        }

        match &self.root {
            Some(root) => {
                if let GiSTNode::Leaf(leaf) = root.as_ref() {
                    if leaf.compressed {
                        return Err(IndexError::InvalidOperation);
                    }
                }
                let result = search_range(root, &query_box);
                if result.is_empty() { Err(IndexError::KeyNotFound) } else { Ok(result) }
            }
            None => Err(IndexError::KeyNotFound),
        }
    }

    fn index_type(&self) -> IndexType {
        IndexType::GiST
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::PageId;

    #[test]
    fn test_gist_insert_and_search() {
        let mut index = GiSTIndex::new();
        let tid = (PageId(1), 0);

        index.insert(b"key1", tid).unwrap();
        let result = index.search(b"key1").unwrap();
        assert_eq!(result, vec![tid]);
    }

    #[test]
    fn test_gist_range_search() {
        let mut index = GiSTIndex::new();

        index.insert(b"a", (PageId(1), 0)).unwrap();
        index.insert(b"m", (PageId(2), 0)).unwrap();
        index.insert(b"z", (PageId(3), 0)).unwrap();

        let result = index.range_search(b"a", b"n").unwrap();
        assert!(result.len() >= 2);
    }

    #[test]
    fn test_gist_not_found() {
        let index = GiSTIndex::new();
        assert!(index.search(b"nonexistent").is_err());
    }

    #[test]
    fn test_bounding_box() {
        let bbox1 = BoundingBox::new(b"a");
        let bbox2 = BoundingBox::new(b"z");

        assert!(bbox1.contains(b"a"));
        assert!(!bbox1.contains(b"z"));

        let union = bbox1.union(&bbox2);
        assert!(union.contains(b"a"));
        assert!(union.contains(b"m"));
        assert!(union.contains(b"z"));
    }

    #[test]
    fn test_gist_with_compression() {
        let index = GiSTIndex::with_compression(CompressionAlgorithm::Lz4);
        assert_eq!(index.compression_algorithm(), CompressionAlgorithm::Lz4);
    }

    #[test]
    fn test_gist_set_compression() {
        let mut index = GiSTIndex::new();
        index.set_compression_algorithm(CompressionAlgorithm::Zstd);
        assert_eq!(index.compression_algorithm(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_gist_insert_compressed() {
        let mut index = GiSTIndex::with_compression(CompressionAlgorithm::Lz4);
        let tid = (PageId(1), 0);

        let large_key: Vec<u8> = vec![0u8; 3000];
        index.insert_compressed(&large_key, tid).unwrap();

        assert!(index.search(&large_key).is_ok());
    }

    #[test]
    fn test_gist_is_compressed() {
        let mut index = GiSTIndex::new();
        index.insert(b"key", (PageId(1), 0)).unwrap();

        assert!(!index.is_compressed());
    }

    #[test]
    fn test_gist_node_count() {
        let mut index = GiSTIndex::new();
        assert_eq!(index.node_count(), 0);

        index.insert(b"key", (PageId(1), 0)).unwrap();
        assert_eq!(index.node_count(), 1);
    }

    #[test]
    fn test_gist_serialize_deserialize_leaf() {
        let leaf = LeafNode {
            keys: vec![BoundingBox::new(b"a"), BoundingBox::new(b"z")],
            tids: vec![(PageId(1), 0), (PageId(2), 1)],
            compressed: false,
        };

        let serialized = GiSTIndex::serialize_leaf(&leaf);
        let deserialized = GiSTIndex::deserialize_leaf(&serialized).unwrap();

        assert_eq!(leaf.keys.len(), deserialized.keys.len());
        assert_eq!(leaf.tids.len(), deserialized.tids.len());
    }
}
