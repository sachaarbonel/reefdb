use std::any::Any;

use crate::error::ReefDBError;
use crate::fts::search::Search;
use crate::indexes::index_manager::IndexManager;
use crate::storage::Storage;
use crate::wal::{WALEntry, WALOperation};

use super::TransactionManager;

impl<S: Storage + IndexManager + Clone + Any, FTS: Search + Clone> TransactionManager<S, FTS>
where
    FTS::NewArgs: Clone,
{
    fn append_wal_entry(&self, entry: WALEntry) -> Result<(), ReefDBError> {
        self.wal
            .lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire WAL lock".to_string()))?
            .append_entry(entry)
    }

    pub(super) fn append_commit_entry(&self, transaction_id: u64) -> Result<(), ReefDBError> {
        let wal_entry = WALEntry {
            transaction_id,
            timestamp: std::time::SystemTime::now(),
            operation: WALOperation::Commit,
            table_name: String::new(),
            data: vec![],
        };
        self.append_wal_entry(wal_entry)
    }

    pub(super) fn append_rollback_entry(&self, transaction_id: u64) -> Result<(), ReefDBError> {
        let wal_entry = WALEntry {
            transaction_id,
            timestamp: std::time::SystemTime::now(),
            operation: WALOperation::Rollback,
            table_name: String::new(),
            data: vec![],
        };
        self.append_wal_entry(wal_entry)
    }
}
