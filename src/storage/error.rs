use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("page {0} not found")]
    PageNotFound(u32),

    #[error("buffer pool full")]
    BufferPoolFull,

    #[error("I/O error: {0}")]
    Io(String),

    #[error("invalid page data")]
    InvalidPageData,

    #[error("compression error: {0}")]
    Compression(String),
}

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> Self {
        StorageError::Io(err.to_string())
    }
}

impl From<crate::storage::compression::CompressionError> for StorageError {
    fn from(err: crate::storage::compression::CompressionError) -> Self {
        StorageError::Compression(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;
