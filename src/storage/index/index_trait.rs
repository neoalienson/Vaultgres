use crate::storage::page::PageId;

pub type TupleId = (PageId, u16);

pub trait Index: Send + Sync {
    fn insert(&mut self, key: &[u8], tid: TupleId) -> Result<(), IndexError>;
    fn delete(&mut self, key: &[u8], tid: TupleId) -> Result<bool, IndexError>;
    fn search(&self, key: &[u8]) -> Result<Vec<TupleId>, IndexError>;
    fn range_search(&self, start: &[u8], end: &[u8]) -> Result<Vec<TupleId>, IndexError>;
    fn index_type(&self) -> IndexType;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    BTree,
    Hash,
    BRIN,
    GIN,
    GiST,
}

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("Key not found")]
    KeyNotFound,
    #[error("Duplicate key")]
    DuplicateKey,
    #[error("Invalid operation for index type")]
    InvalidOperation,
    #[error("Invalid index")]
    InvalidIndex,
    #[error("Storage error: {0}")]
    Storage(String),
}
