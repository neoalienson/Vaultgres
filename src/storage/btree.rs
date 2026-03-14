use super::error::Result;
use super::page::PageId;

/// B+Tree key type
pub type Key = Vec<u8>;

/// B+Tree value type (tuple identifier)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TupleId {
    pub page_id: PageId,
    pub slot: u16,
}

/// B+Tree node
#[allow(dead_code)]
enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

/// Internal node with keys and child pointers
struct InternalNode {
    keys: Vec<Key>,
    children: Vec<PageId>,
}

/// Leaf node with key-value pairs
struct LeafNode {
    keys: Vec<Key>,
    values: Vec<TupleId>,
    next: Option<PageId>,
}

/// B+Tree index
pub struct BTree {
    root: Option<Box<Node>>,
    order: usize,
}

impl BTree {
    /// Creates a new B+Tree with default order
    pub fn new() -> Self {
        Self::with_order(128)
    }

    /// Creates a new B+Tree with specified order
    pub fn with_order(order: usize) -> Self {
        Self { root: None, order }
    }

    /// Inserts a key-value pair into the tree
    pub fn insert(&mut self, key: Key, value: TupleId) -> Result<()> {
        if self.root.is_none() {
            self.root = Some(Box::new(Node::Leaf(LeafNode {
                keys: vec![key],
                values: vec![value],
                next: None,
            })));
            return Ok(());
        }

        // Simple insertion without splitting for now
        if let Some(Node::Leaf(leaf)) = self.root.as_deref_mut() {
            let pos = leaf.keys.binary_search(&key).unwrap_or_else(|e| e);
            leaf.keys.insert(pos, key);
            leaf.values.insert(pos, value);
        }

        Ok(())
    }

    /// Searches for a key in the tree
    pub fn get(&self, key: &Key) -> Option<TupleId> {
        let root = self.root.as_ref()?;

        match root.as_ref() {
            Node::Leaf(leaf) => leaf.keys.binary_search(key).ok().map(|idx| leaf.values[idx]),
            Node::Internal(_) => None, // Not implemented yet
        }
    }

    /// Deletes a key from the tree
    pub fn delete(&mut self, key: &Key) -> Result<bool> {
        if let Some(Node::Leaf(leaf)) = self.root.as_deref_mut() {
            if let Ok(idx) = leaf.keys.binary_search(key) {
                leaf.keys.remove(idx);
                leaf.values.remove(idx);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Returns an iterator over all key-value pairs
    pub fn iter(&self) -> BTreeIterator<'_> {
        BTreeIterator { node: self.root.as_deref(), index: 0 }
    }
}

impl Default for BTree {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator over B+Tree entries
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
}
