use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum TransactionError {
    #[error("transaction {0} not found")]
    NotFound(u64),

    #[error("transaction {0} already committed")]
    AlreadyCommitted(u64),

    #[error("transaction {0} already aborted")]
    AlreadyAborted(u64),

    #[error("deadlock detected")]
    Deadlock,

    #[error("lock timeout exceeded")]
    LockTimeout,

    #[error("serialization failure")]
    SerializationFailure,
}

pub type Result<T> = std::result::Result<T, TransactionError>;
