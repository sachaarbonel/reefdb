use std::any::Any;

use crate::error::ReefDBError;
use crate::fts::search::Search;
use crate::indexes::index_manager::IndexManager;
use crate::storage::{Storage, TableStorage};
use crate::transaction::TransactionState;

use super::TransactionManager;

impl<S: Storage + IndexManager + Clone + Any, FTS: Search + Clone> TransactionManager<S, FTS>
where
    FTS::NewArgs: Clone,
{
    pub fn create_savepoint(&mut self, transaction_id: u64, name: String) -> Result<(), ReefDBError> {
        let transaction = self.active_transactions.get_mut(&transaction_id)
            .ok_or_else(|| ReefDBError::TransactionNotFound(transaction_id))?;

        if transaction.get_state() != &TransactionState::Active {
            return Err(ReefDBError::TransactionNotActive);
        }

        transaction.create_savepoint(name)?;
        Ok(())
    }

    pub fn rollback_to_savepoint(&mut self, transaction_id: u64, name: &str) -> Result<TableStorage, ReefDBError> {
        let transaction = self.active_transactions.get_mut(&transaction_id)
            .ok_or_else(|| ReefDBError::TransactionNotFound(transaction_id))?;

        if transaction.get_state() != &TransactionState::Active {
            return Err(ReefDBError::TransactionNotActive);
        }

        transaction.rollback_to_savepoint(name)?;
        let restored_state = transaction.get_table_state();

        // Update database state
        let mut reef_db = self.reef_db.lock()
            .map_err(|_| ReefDBError::LockAcquisitionFailed("Failed to acquire database lock".to_string()))?;
        reef_db.tables.restore_from(&restored_state);

        // Update storage state
        for (table_name, (columns, rows)) in restored_state.tables.iter() {
            reef_db.storage.insert_table(table_name.clone(), columns.clone(), rows.clone());
        }

        // Write WAL entry for rollback
        self.append_rollback_entry(transaction_id)?;

        Ok(restored_state)
    }

    pub fn release_savepoint(&mut self, transaction_id: u64, name: &str) -> Result<(), ReefDBError> {
        let transaction = self.active_transactions.get_mut(&transaction_id)
            .ok_or_else(|| ReefDBError::TransactionNotFound(transaction_id))?;

        if transaction.get_state() != &TransactionState::Active {
            return Err(ReefDBError::TransactionNotActive);
        }
        transaction.release_savepoint(name)
    }
}
