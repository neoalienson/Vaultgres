//! Old executor model (legacy)
//!
//! This module contains the old executor model with open()/next()/close() methods.
//! New code should use the new Executor trait from `operators::executor`.

use std::collections::HashMap;

pub type Value = Vec<u8>;
pub type Tuple = HashMap<String, Value>;

#[derive(Debug, Clone)]
pub struct SimpleTuple {
    pub data: Vec<u8>,
}

pub trait OldExecutor: Send {
    fn open(&mut self) -> Result<(), OldExecutorError>;
    fn next(&mut self) -> Result<Option<SimpleTuple>, OldExecutorError>;
    fn close(&mut self) -> Result<(), OldExecutorError>;
}

#[derive(Debug, thiserror::Error)]
pub enum OldExecutorError {
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    #[error("Type mismatch: {0}")]
    TypeMismatch(String),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
}
