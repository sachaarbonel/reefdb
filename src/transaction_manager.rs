use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::{
    error::ReefDBError,
    indexes::fts::search::Search,
    storage::Storage,
    ReefDB,
    transaction::{Transaction, TransactionState, IsolationLevel},
    wal::{WriteAheadLog, WALEntry, WALOperation},
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
        }
    }

    pub fn begin_transaction(&mut self, isolation_level: IsolationLevel) -> Result<u64, ReefDBError> {
        let reef_db = self.reef_db.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire database lock".to_string()))?;
        
        let transaction = Transaction::create((*reef_db).clone(), isolation_level);
        let id = transaction.get_id();
        
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

        // Release locks
        self.lock_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire lock manager".to_string()))?
            .release_transaction_locks(id);

        Ok(())
    }

    pub fn rollback_transaction(&mut self, id: u64) -> Result<(), ReefDBError> {
        let mut transaction = self.active_transactions.remove(&id)
            .ok_or_else(|| ReefDBError::Other("Transaction not found".to_string()))?;

        let mut reef_db = self.reef_db.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire database lock".to_string()))?;
        
        transaction.rollback(&mut reef_db)?;

        // Release locks
        self.lock_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire lock manager".to_string()))?
            .release_transaction_locks(id);

        Ok(())
    }

    pub fn acquire_lock(&self, transaction_id: u64, table_name: &str, lock_type: LockType) -> Result<(), ReefDBError> {
        self.lock_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire lock manager".to_string()))?
            .acquire_lock(transaction_id, table_name, lock_type)
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