use super::index_trait::{Index, IndexError, IndexType, TupleId};

pub struct GiSTIndex {
    root: Option<Box<GiSTNode>>,
    max_entries: usize,
}

enum GiSTNode {
    Internal(InternalNode),
    Leaf(LeafNode),
}

struct InternalNode {
    keys: Vec<BoundingBox>,
    children: Vec<Box<GiSTNode>>,
}

struct LeafNode {
    keys: Vec<BoundingBox>,
    tids: Vec<TupleId>,
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
        Self { root: None, max_entries: 50 }
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
}

impl Index for GiSTIndex {
    fn insert(&mut self, key: &[u8], tid: TupleId) -> Result<(), IndexError> {
        if self.root.is_none() {
            self.root = Some(Box::new(GiSTNode::Leaf(LeafNode { keys: vec![], tids: vec![] })));
        }

        if let Some(ref mut root) = self.root {
            if let GiSTNode::Leaf(ref mut leaf) = **root {
                leaf.keys.push(BoundingBox::new(key));
                leaf.tids.push(tid);
            }
        }

        Ok(())
    }

    fn delete(&mut self, _key: &[u8], _tid: TupleId) -> Result<bool, IndexError> {
        // Simplified: not implemented
        Ok(false)
    }

    fn search(&self, key: &[u8]) -> Result<Vec<TupleId>, IndexError> {
        match &self.root {
            Some(root) => {
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
}
