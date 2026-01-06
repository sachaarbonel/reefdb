use std::collections::{HashMap, HashSet};
use std::time::SystemTime;
use crate::error::ReefDBError;
use crate::transaction::IsolationLevel;
use log::debug;

pub struct TransactionState {
    transaction_writes: HashMap<u64, HashSet<String>>,
    table_writes: HashMap<String, HashSet<String>>,
    committed_transactions: HashSet<u64>,
    active_transactions: HashSet<u64>,
    transaction_timestamps: HashMap<u64, SystemTime>,
    transaction_isolation_levels: HashMap<u64, IsolationLevel>,
}

impl TransactionState {
    pub fn new() -> Self {
        Self {
            transaction_writes: HashMap::new(),
            table_writes: HashMap::new(),
            committed_transactions: HashSet::new(),
            active_transactions: HashSet::new(),
            transaction_timestamps: HashMap::new(),
            transaction_isolation_levels: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.transaction_writes.is_empty() &&
        self.table_writes.is_empty() &&
        self.committed_transactions.is_empty() &&
        self.active_transactions.is_empty() &&
        self.transaction_timestamps.is_empty() &&
        self.transaction_isolation_levels.is_empty()
    }

    pub fn begin_transaction(&mut self, transaction_id: u64) {
        debug!("Beginning transaction: {}", transaction_id);
        self.active_transactions.insert(transaction_id);
        self.transaction_timestamps.insert(transaction_id, SystemTime::now());
        // Default to READ COMMITTED if not specified
        self.transaction_isolation_levels.insert(transaction_id, IsolationLevel::ReadCommitted);
        debug!(
            "Transaction {} started. Active transactions: {:?}",
            transaction_id, self.active_transactions
        );
    }

    pub fn set_isolation_level(&mut self, transaction_id: u64, isolation_level: IsolationLevel) {
        self.transaction_isolation_levels.insert(transaction_id, isolation_level);
    }

    pub fn get_isolation_level(&self, transaction_id: u64) -> Option<IsolationLevel> {
        self.transaction_isolation_levels.get(&transaction_id).cloned()
    }

    pub fn commit_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        debug!("Committing transaction: {}", transaction_id);
        if !self.active_transactions.contains(&transaction_id) {
            debug!("Error: Transaction {} not active", transaction_id);
            return Err(ReefDBError::TransactionNotActive);
        }

        self.active_transactions.remove(&transaction_id);
        self.committed_transactions.insert(transaction_id);
        debug!(
            "Transaction {} committed. Active transactions: {:?}, Committed transactions: {:?}",
            transaction_id, self.active_transactions, self.committed_transactions
        );
        Ok(())
    }

    pub fn rollback_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        if !self.active_transactions.contains(&transaction_id) {
            return Err(ReefDBError::Other("Transaction not found".to_string()));
        }

        // Remove from active transactions but keep the timestamp
        self.active_transactions.remove(&transaction_id);
        
        // Clean up writes
        self.transaction_writes.remove(&transaction_id);
        
        Ok(())
    }

    pub fn record_write(&mut self, transaction_id: u64, key: String, table_name: String, primary_key: String) {
        self.transaction_writes
            .entry(transaction_id)
            .or_insert_with(HashSet::new)
            .insert(key);
            
        self.table_writes
            .entry(table_name)
            .or_insert_with(HashSet::new)
            .insert(primary_key);
    }

    pub fn is_transaction_active(&self, transaction_id: u64) -> bool {
        self.active_transactions.contains(&transaction_id)
    }

    pub fn is_transaction_committed(&self, transaction_id: u64) -> bool {
        self.committed_transactions.contains(&transaction_id)
    }

    pub fn get_transaction_timestamp(&self, transaction_id: u64) -> Option<SystemTime> {
        self.transaction_timestamps.get(&transaction_id).cloned()
    }

    pub fn get_transaction_writes(&self, transaction_id: u64) -> Option<&HashSet<String>> {
        self.transaction_writes.get(&transaction_id)
    }

    pub fn get_committed_transactions(&self) -> &HashSet<u64> {
        debug!("Getting committed transactions: {:?}", self.committed_transactions);
        &self.committed_transactions
    }

    pub fn get_transaction_start_time(&self, transaction_id: u64) -> Option<SystemTime> {
        self.transaction_timestamps.get(&transaction_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_lifecycle() {
        let mut state = TransactionState::new();
        
        // Begin transaction
        state.begin_transaction(1);
        assert!(state.is_transaction_active(1));
        assert!(!state.is_transaction_committed(1));
        
        // Commit transaction
        state.commit_transaction(1).unwrap();
        assert!(!state.is_transaction_active(1));
        assert!(state.is_transaction_committed(1));
    }

    #[test]
    fn test_transaction_rollback() {
        let mut state = TransactionState::new();
        
        state.begin_transaction(1);
        state.record_write(1, "key1".to_string(), "table1".to_string(), "pk1".to_string());
        
        state.rollback_transaction(1).unwrap();
        assert!(!state.is_transaction_active(1));
        assert!(state.get_transaction_writes(1).is_none());
    }
} 
