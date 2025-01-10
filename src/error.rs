use std::fmt;
use std::io;

#[derive(Debug, PartialEq)]
pub enum ReefDBError {
    TableNotFound(String),
    ColumnNotFound(String),
    SavepointNotFound(String),
    SavepointNotActive(String),
    TransactionNotActive,
    TransactionNotFound(u64),
    DuplicateKey(String),
    LockAcquisitionFailed(String),
    WALError(String),
    MVCCError(String),
    IoError(String),
    DeadlockDetected(String),
    Deadlock,
    LockConflict(String),
    InvalidIsolationLevel(String),
    Other(String),
    WriteConflict(String),
}

impl fmt::Display for ReefDBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReefDBError::TableNotFound(table) => write!(f, "Table not found: {}", table),
            ReefDBError::ColumnNotFound(column) => write!(f, "Column not found: {}", column),
            ReefDBError::SavepointNotFound(sp) => write!(f, "Savepoint not found: {}", sp),
            ReefDBError::SavepointNotActive(sp) => write!(f, "Savepoint is not active: {}", sp),
            ReefDBError::TransactionNotActive => write!(f, "Transaction is not active"),
            ReefDBError::TransactionNotFound(id) => write!(f, "Transaction not found: {}", id),
            ReefDBError::DuplicateKey(key) => write!(f, "Duplicate key violation: {}", key),
            ReefDBError::LockAcquisitionFailed(msg) => write!(f, "Failed to acquire lock: {}", msg),
            ReefDBError::WALError(msg) => write!(f, "WAL error: {}", msg),
            ReefDBError::MVCCError(msg) => write!(f, "MVCC error: {}", msg),
            ReefDBError::IoError(msg) => write!(f, "IO error: {}", msg),
            ReefDBError::DeadlockDetected(msg) => write!(f, "Deadlock detected: {}", msg),
            ReefDBError::Deadlock => write!(f, "Transaction aborted due to deadlock"),
            ReefDBError::LockConflict(msg) => write!(f, "Lock conflict: {}", msg),
            ReefDBError::InvalidIsolationLevel(level) => write!(f, "Invalid isolation level: {}", level),
            ReefDBError::Other(msg) => write!(f, "{}", msg),
            ReefDBError::WriteConflict(msg) => write!(f, "Write conflict: {}", msg),
        }
    }
}

impl std::error::Error for ReefDBError {}

impl From<io::Error> for ReefDBError {
    fn from(error: io::Error) -> Self {
        ReefDBError::IoError(error.to_string())
    }
}

impl From<bincode::Error> for ReefDBError {
    fn from(error: bincode::Error) -> Self {
        ReefDBError::IoError(error.to_string())
    }
}
