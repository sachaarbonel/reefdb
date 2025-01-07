use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::TableStorage;
use crate::{
    error::ReefDBError,
    indexes::fts::search::Search,
    storage::Storage,
    ReefDB,
    transaction::{Transaction, TransactionState, IsolationLevel},
    wal::{WriteAheadLog, WALEntry, WALOperation},
    mvcc::MVCCManager,
    deadlock::DeadlockDetector,
    savepoint::SavepointManager,
    sql::statements::Statement,
    result::ReefDBResult,
};

#[derive(Debug)]
pub struct LockManager {
    table_locks: HashMap<String, Vec<(u64, LockType)>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LockType {
    Shared,
    Exclusive,
}

impl LockManager {
    pub fn new() -> Self {
        LockManager {
            table_locks: HashMap::new(),
        }
    }

    pub fn acquire_lock(&mut self, transaction_id: u64, table_name: &str, lock_type: LockType) -> Result<(), ReefDBError> {
        let locks = self.table_locks.entry(table_name.to_string()).or_insert_with(Vec::new);
        
        // Check for conflicts
        for (existing_id, existing_lock) in locks.iter() {
            if *existing_id != transaction_id {
                match (existing_lock, &lock_type) {
                    (LockType::Exclusive, _) | (_, LockType::Exclusive) => {
                        return Err(ReefDBError::Other(format!(
                            "Lock conflict: Transaction {} cannot acquire {:?} lock on table {}",
                            transaction_id, lock_type, table_name
                        )));
                    }
                    _ => {}
                }
            }
        }
        
        locks.push((transaction_id, lock_type));
        Ok(())
    }

    pub fn release_transaction_locks(&mut self, transaction_id: u64) {
        for locks in self.table_locks.values_mut() {
            locks.retain(|(id, _)| *id != transaction_id);
        }
    }
}

#[derive(Clone)]
pub struct TransactionManager<S: Storage + Clone, FTS: Search + Clone>
where
    FTS::NewArgs: Clone,
{
    active_transactions: HashMap<u64, Transaction<S, FTS>>,
    lock_manager: Arc<Mutex<LockManager>>,
    wal: Arc<Mutex<WriteAheadLog>>,
    reef_db: Arc<Mutex<ReefDB<S, FTS>>>,
    mvcc_manager: Arc<Mutex<MVCCManager>>,
    deadlock_detector: Arc<Mutex<DeadlockDetector>>,
    savepoint_manager: Arc<Mutex<SavepointManager>>,
}

impl<S: Storage + Clone, FTS: Search + Clone> TransactionManager<S, FTS>
where
    FTS::NewArgs: Clone,
{
    pub fn create(reef_db: ReefDB<S, FTS>, wal: WriteAheadLog) -> Self {
        TransactionManager {
            active_transactions: HashMap::new(),
            lock_manager: Arc::new(Mutex::new(LockManager::new())),
            wal: Arc::new(Mutex::new(wal)),
            reef_db: Arc::new(Mutex::new(reef_db)),
            mvcc_manager: Arc::new(Mutex::new(MVCCManager::new())),
            deadlock_detector: Arc::new(Mutex::new(DeadlockDetector::new())),
            savepoint_manager: Arc::new(Mutex::new(SavepointManager::new())),
        }
    }

    pub fn begin_transaction(&mut self, isolation_level: IsolationLevel) -> Result<u64, ReefDBError> {
        let reef_db = self.reef_db.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire database lock".to_string()))?;
        
        let transaction = Transaction::create((*reef_db).clone(), isolation_level);
        let id = transaction.get_id();
        
        // Initialize MVCC timestamp for the transaction
        self.mvcc_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire MVCC manager lock".to_string()))?
            .begin_transaction(id);
        
        self.active_transactions.insert(id, transaction);
        Ok(id)
    }

    pub fn commit_transaction(&mut self, id: u64) -> Result<(), ReefDBError> {
        let mut transaction = self.active_transactions.remove(&id)
            .ok_or_else(|| ReefDBError::Other("Transaction not found".to_string()))?;
        
        if transaction.get_state() != &TransactionState::Active {
            return Err(ReefDBError::Other("Transaction is not active".to_string()));
        }

        // Write to WAL
        let wal_entry = WALEntry {
            transaction_id: id,
            timestamp: std::time::SystemTime::now(),
            operation: WALOperation::Commit,
            table_name: String::new(),
            data: vec![],
        };

        self.wal.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire WAL lock".to_string()))?
            .append_entry(wal_entry)?;

        // Commit changes to database
        let mut reef_db = self.reef_db.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire database lock".to_string()))?;
        
        transaction.commit(&mut reef_db)?;

        // Commit MVCC changes
        self.mvcc_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire MVCC manager lock".to_string()))?
            .commit(id);

        // Release locks and remove from deadlock detector
        self.lock_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire lock manager".to_string()))?
            .release_transaction_locks(id);
        
        self.deadlock_detector.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire deadlock detector".to_string()))?
            .remove_transaction(id);

        // Clear savepoints for this transaction
        let mut savepoint_manager = self.savepoint_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire savepoint manager lock".to_string()))?;
        savepoint_manager.clear_transaction_savepoints(id);

        Ok(())
    }

    pub fn rollback_transaction(&mut self, id: u64) -> Result<(), ReefDBError> {
        let mut transaction = self.active_transactions.remove(&id)
            .ok_or_else(|| ReefDBError::Other("Transaction not found".to_string()))?;

        let mut reef_db = self.reef_db.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire database lock".to_string()))?;
        
        transaction.rollback(&mut reef_db)?;

        // Rollback MVCC changes
        self.mvcc_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire MVCC manager lock".to_string()))?
            .rollback(id);

        // Release locks and remove from deadlock detector
        self.lock_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire lock manager".to_string()))?
            .release_transaction_locks(id);
        
        self.deadlock_detector.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire deadlock detector".to_string()))?
            .remove_transaction(id);

        // Clear savepoints for this transaction
        let mut savepoint_manager = self.savepoint_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire savepoint manager lock".to_string()))?;
        savepoint_manager.clear_transaction_savepoints(id);

        Ok(())
    }

    pub fn acquire_lock(&self, transaction_id: u64, table_name: &str, lock_type: LockType) -> Result<(), ReefDBError> {
        let mut lock_manager = self.lock_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire lock manager".to_string()))?;
        
        // Check for deadlocks before acquiring lock
        let mut deadlock_detector = self.deadlock_detector.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire deadlock detector".to_string()))?;
        
        // Add wait-for edge
        if let Some(holding_tx) = lock_manager.table_locks.get(table_name)
            .and_then(|locks| locks.first())
            .map(|(id, _)| *id) {
            deadlock_detector.add_wait(transaction_id, holding_tx, table_name.to_string());
            
            // Check for deadlocks
            if let Some(victim_tx) = deadlock_detector.detect_deadlock() {
                if victim_tx == transaction_id {
                    return Err(ReefDBError::Other("Deadlock detected, transaction aborted".to_string()));
                }
            }
        }
        
        lock_manager.acquire_lock(transaction_id, table_name, lock_type)
    }

    pub fn create_savepoint(&mut self, transaction_id: u64, name: String) -> Result<(), ReefDBError> {
        let transaction = self.active_transactions.get(&transaction_id)
            .ok_or_else(|| ReefDBError::Other("Transaction not found".to_string()))?;
        
        // Get the transaction's current state
        let table_state = transaction.get_table_state();
        
        // Create the savepoint with this state
        self.savepoint_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire savepoint manager lock".to_string()))?
            .create_savepoint(transaction_id, name, table_state)?;
        
        Ok(())
    }

    pub fn rollback_to_savepoint(&mut self, transaction_id: u64, name: &str) -> Result<TableStorage, ReefDBError> {
        let transaction = self.active_transactions.get_mut(&transaction_id)
            .ok_or_else(|| ReefDBError::Other("Transaction not found".to_string()))?;
        
        // Get the savepoint state
        let restored_state = self.savepoint_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire savepoint manager lock".to_string()))?
            .rollback_to_savepoint(transaction_id, name)?;
        
        // Update transaction's state
        transaction.restore_table_state(&restored_state);
        
        // Update database state
        let mut reef_db = self.reef_db.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire database lock".to_string()))?;
        reef_db.tables.restore_from(&restored_state);
        
        Ok(restored_state)
    }

    pub fn release_savepoint(&mut self, transaction_id: u64, name: &str) -> Result<(), ReefDBError> {
        let mut savepoint_manager = self.savepoint_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire savepoint manager lock".to_string()))?;
        
        savepoint_manager.release_savepoint(transaction_id, name)
    }

    pub fn execute_statement(&mut self, transaction_id: u64, stmt: Statement) -> Result<ReefDBResult, ReefDBError> {
        // Get transaction first
        let transaction = self.active_transactions.get_mut(&transaction_id)
            .ok_or_else(|| ReefDBError::Other("Transaction not found".to_string()))?;

        // Execute the statement and get the result
        let result = match &stmt {
            Statement::Savepoint(savepoint_stmt) => {
                // Get current state before creating savepoint
                let table_state = transaction.get_table_state();
                
                // Create savepoint with current state
                self.savepoint_manager.lock()
                    .map_err(|_| ReefDBError::Other("Failed to acquire savepoint manager lock".to_string()))?
                    .create_savepoint(transaction_id, savepoint_stmt.name.clone(), table_state)?;
                
                Ok(ReefDBResult::Savepoint)
            },
            Statement::RollbackToSavepoint(name) => {
                // Get the savepoint state
                let restored_state = self.savepoint_manager.lock()
                    .map_err(|_| ReefDBError::Other("Failed to acquire savepoint manager lock".to_string()))?
                    .rollback_to_savepoint(transaction_id, name)?;
                
                // Update transaction's state
                transaction.restore_table_state(&restored_state);
                
                // Update database state
                let mut reef_db = self.reef_db.lock()
                    .map_err(|_| ReefDBError::Other("Failed to acquire database lock".to_string()))?;
                reef_db.tables = restored_state;
                
                Ok(ReefDBResult::RollbackToSavepoint)
            },
            Statement::ReleaseSavepoint(name) => {
                self.savepoint_manager.lock()
                    .map_err(|_| ReefDBError::Other("Failed to acquire savepoint manager lock".to_string()))?
                    .release_savepoint(transaction_id, name)?;
                
                Ok(ReefDBResult::ReleaseSavepoint)
            },
            _ => transaction.execute_statement(stmt.clone()),
        }?;
        
        // After executing the statement, update the database state
        // Only update if it's not a rollback (since we already updated the state)
        if !matches!(&stmt, Statement::RollbackToSavepoint(_)) {
            let mut reef_db = self.reef_db.lock()
                .map_err(|_| ReefDBError::Other("Failed to acquire database lock".to_string()))?;
            
            // Update database state with transaction's state
            let mut new_state = reef_db.tables.clone();
            new_state.restore_from(&transaction.get_table_state());
            reef_db.tables = new_state;
        }
        
        Ok(result)
    }

    pub fn get_transaction_state(&self, transaction_id: u64) -> Result<TableStorage, ReefDBError> {
        let transaction = self.active_transactions.get(&transaction_id)
            .ok_or_else(|| ReefDBError::Other("Transaction not found".to_string()))?;
        
        Ok(transaction.get_table_state())
    }

    pub fn update_database_state(&mut self, tables: TableStorage) -> Result<(), ReefDBError> {
        let mut reef_db = self.reef_db.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire database lock".to_string()))?;
        reef_db.tables = tables;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryReefDB;
    use tempfile::tempdir;

    #[test]
    fn test_transaction_manager() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal = WriteAheadLog::new(wal_path).unwrap();
        
        let db = InMemoryReefDB::create_in_memory();
        let mut tm = TransactionManager::create(db, wal);
        
        // Begin transaction
        let tx_id = tm.begin_transaction(IsolationLevel::Serializable).unwrap();
        
        // Acquire lock
        tm.acquire_lock(tx_id, "users", LockType::Exclusive).unwrap();
        
        // Try to acquire conflicting lock (should fail)
        let tx_id2 = tm.begin_transaction(IsolationLevel::Serializable).unwrap();
        assert!(tm.acquire_lock(tx_id2, "users", LockType::Shared).is_err());
        
        // Commit first transaction
        tm.commit_transaction(tx_id).unwrap();
        
        // Now second transaction should be able to acquire lock
        assert!(tm.acquire_lock(tx_id2, "users", LockType::Shared).is_ok());
    }
} 