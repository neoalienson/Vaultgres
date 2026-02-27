use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("page {0} not found")]
    PageNotFound(u32),
    
    #[error("buffer pool full")]
    BufferPoolFull,
    
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("invalid page data")]
    InvalidPageData,
}

pub type Result<T> = std::result::Result<T, StorageError>;
