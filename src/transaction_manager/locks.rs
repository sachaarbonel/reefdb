use std::any::Any;

use crate::error::ReefDBError;
use crate::fts::search::Search;
use crate::indexes::index_manager::IndexManager;
use crate::locks::LockType;
use crate::storage::Storage;

use super::TransactionManager;

impl<S: Storage + IndexManager + Clone + Any, FTS: Search + Clone> TransactionManager<S, FTS>
where
    FTS::NewArgs: Clone,
{
    pub fn acquire_lock(&self, transaction_id: u64, table_name: &str, lock_type: LockType) -> Result<(), ReefDBError> {
        let mut lock_manager = self.lock_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire lock manager".to_string()))?;

        // Check for deadlocks before acquiring lock
        let mut deadlock_detector = self.deadlock_detector.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire deadlock detector".to_string()))?;

        // Get current lock holders for this table
        let lock_holders = lock_manager.get_lock_holders(table_name);

        // If there are existing locks and we don't already have a lock, add wait-for edges
        if !lock_holders.is_empty() && !lock_manager.has_lock(transaction_id, table_name) {
            for holder_id in lock_holders {
                if holder_id != transaction_id {
                    deadlock_detector.add_wait(transaction_id, holder_id, table_name.to_string());

                    // Check for deadlocks
                    let active_txs: Vec<&crate::transaction::Transaction<S, FTS>> = self.active_transactions.values().collect();
                    if let Some(victim_tx) = deadlock_detector.detect_deadlock(&active_txs) {
                        if victim_tx == transaction_id {
                            // Remove the wait edge since we're aborting
                            deadlock_detector.remove_transaction(transaction_id);
                            return Err(ReefDBError::Deadlock);
                        }
                    }
                }
            }
        }

        // Try to acquire the lock
        match lock_manager.acquire_lock(transaction_id, table_name, lock_type) {
            Ok(()) => {
                // Successfully acquired lock, remove any wait edges
                deadlock_detector.remove_transaction(transaction_id);
                Ok(())
            }
            Err(e) => {
                // Failed to acquire lock, remove any wait edges
                deadlock_detector.remove_transaction(transaction_id);
                Err(e)
            }
        }
    }
}
