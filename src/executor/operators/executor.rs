use crate::catalog::Value;
use std::collections::HashMap;
use std::fmt; // Import fmt

pub type Tuple = HashMap<String, Value>;

#[derive(Debug, Clone)] // Added Clone
pub enum ExecutorError {
    EndOfData,
    ColumnNotFound(String),
    TypeMismatch(String),
    UnsupportedExpression(String),
    FunctionError(String),
    FunctionNotFound(String),
    IoError(String),
    StorageError(String),
    InternalError(String),
    InvalidInput(String),
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExecutorError::EndOfData => write!(f, "End of data"),
            ExecutorError::ColumnNotFound(col) => write!(f, "Column not found: {}", col),
            ExecutorError::TypeMismatch(msg) => write!(f, "Type mismatch: {}", msg),
            ExecutorError::UnsupportedExpression(msg) => {
                write!(f, "Unsupported expression: {}", msg)
            }
            ExecutorError::FunctionError(msg) => write!(f, "Function error: {}", msg),
            ExecutorError::FunctionNotFound(msg) => write!(f, "Function not found: {}", msg),
            ExecutorError::IoError(msg) => write!(f, "IO error: {}", msg),
            ExecutorError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            ExecutorError::InternalError(msg) => write!(f, "Internal error: {}", msg),
            ExecutorError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
        }
    }
}

pub trait Executor {
    fn next(&mut self) -> Result<Option<Tuple>, ExecutorError>;
}
