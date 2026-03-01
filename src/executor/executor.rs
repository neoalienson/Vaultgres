use std::collections::HashMap;

pub type Value = Vec<u8>;
pub type Tuple = HashMap<String, Value>;

#[derive(Debug, Clone)]
pub struct SimpleTuple {
    pub data: Vec<u8>,
}

pub trait SimpleExecutor: Send {
    fn open(&mut self) -> Result<(), ExecutorError>;
    fn next(&mut self) -> Result<Option<SimpleTuple>, ExecutorError>;
    fn close(&mut self) -> Result<(), ExecutorError>;
}

pub trait Executor: Send {
    fn open(&mut self) -> Result<(), ExecutorError>;
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError>;
    fn close(&mut self) -> Result<(), ExecutorError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    #[error("Type mismatch: {0}")]
    TypeMismatch(String),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
}
