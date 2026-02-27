use thiserror::Error;

#[derive(Error, Debug)]
pub enum WALError {
    #[error("WAL write failed: {0}")]
    WriteFailed(String),
    
    #[error("WAL flush failed: {0}")]
    FlushFailed(String),
    
    #[error("invalid LSN: {0}")]
    InvalidLSN(u64),
    
    #[error("recovery failed: {0}")]
    RecoveryFailed(String),
    
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, WALError>;
