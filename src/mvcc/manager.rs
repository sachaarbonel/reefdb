use std::collections::HashSet;
use std::time::SystemTime;
use crate::error::ReefDBError;
use crate::sql::data_value::DataValue;
use crate::key_format::KeyFormat;
use crate::transaction::IsolationLevel;
use crate::mvcc::version::{Version, VersionStore};
use crate::mvcc::transaction_state::TransactionState;

pub struct MVCCManager {
    version_store: VersionStore,
    transaction_state: TransactionState,
    tables: HashSet<String>,
}

impl MVCCManager {
    pub fn new() -> Self {
        Self {
            version_store: VersionStore::new(),
            transaction_state: TransactionState::new(),
            tables: HashSet::new(),
        }
    }

    pub fn begin_transaction(&mut self, transaction_id: u64) {
        self.transaction_state.begin_transaction(transaction_id);
    }

    pub fn set_isolation_level(&mut self, transaction_id: u64, isolation_level: IsolationLevel) {
        self.transaction_state.set_isolation_level(transaction_id, isolation_level);
    }

    pub fn commit(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        println!("[DEBUG] Committing transaction {}", transaction_id);
        // Update the timestamp for all versions of this transaction
        if let Some(keys) = self.transaction_state.get_transaction_writes(transaction_id) {
            println!("[DEBUG] Found keys to update for transaction {}: {:?}", transaction_id, keys);
            let commit_time = SystemTime::now();
            for key in keys {
                if let Some(versions) = self.version_store.get_versions_mut(&key) {
                    println!("[DEBUG] Updating versions for key {}", key);
                    // First, find and remove all versions for this transaction
                    let mut tx_versions: Vec<_> = versions.iter()
                        .filter(|v| v.transaction_id == transaction_id)
                        .cloned()
                        .collect();
                    
                    println!("[DEBUG] Found {} versions to update", tx_versions.len());
                    
                    // Update their timestamps
                    for version in tx_versions.iter_mut() {
                        version.timestamp = commit_time;
                        println!("[DEBUG] Updated version timestamp to {:?}", version.timestamp);
                    }
                    
                    // Remove old versions
                    versions.retain(|v| v.transaction_id != transaction_id);
                    
                    // Re-insert updated versions in the correct order
                    for version in tx_versions {
                        // We want newer timestamps first, and for equal timestamps, lower transaction IDs first
                        let insert_pos = versions.binary_search_by(|v| {
                            match version.timestamp.cmp(&v.timestamp) {
                                std::cmp::Ordering::Equal => v.transaction_id.cmp(&version.transaction_id),
                                ord => ord
                            }
                        }).unwrap_or_else(|pos| pos);
                        versions.insert(insert_pos, version);
                    }
                    println!("[DEBUG] Re-inserted versions in correct order");
                }
            }
        }
        // First commit the transaction to update its state
        self.transaction_state.commit_transaction(transaction_id)?;
        println!("[DEBUG] Transaction {} committed successfully", transaction_id);
        Ok(())
    }

    pub fn rollback(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        if let Some(keys) = self.transaction_state.get_transaction_writes(transaction_id) {
            self.version_store.remove_transaction_versions(keys, transaction_id);
        }
        self.transaction_state.rollback_transaction(transaction_id)
    }

    pub fn write(&mut self, transaction_id: u64, key: String, value: Vec<DataValue>) -> Result<(), ReefDBError> {
        println!("[DEBUG] Writing value {:?} for key {} in transaction {}", value, key, transaction_id);
        if !self.transaction_state.is_transaction_active(transaction_id) {
            return Err(ReefDBError::Other("Transaction not found".to_string()));
        }

        if let Some(KeyFormat::Row { table_name, version: _, primary_key }) = KeyFormat::parse(&key) {
            let base_key = KeyFormat::row(&table_name, 0, &primary_key);
            println!("[DEBUG] Using base key: {}", base_key);
            
            // Create a new version with current timestamp
            let version = Version::new(transaction_id, value);
            println!("[DEBUG] Created new version with timestamp {:?}", version.timestamp);
            
            // Store the version - the VersionStore will handle proper ordering
            self.version_store.store_version(base_key.clone(), version);
            
            // Record the write in the transaction state
            self.transaction_state.record_write(transaction_id, base_key, table_name, primary_key);
        }
        
        Ok(())
    }

    pub fn read_committed(&self, transaction_id: u64, key: &str) -> Result<Option<Vec<DataValue>>, ReefDBError> {
        println!("[DEBUG] Reading committed value for key {} in transaction {}", key, transaction_id);
        if let Some(KeyFormat::Row { table_name, version: _, primary_key }) = KeyFormat::parse(key) {
            let base_key = KeyFormat::row(&table_name, 0, &primary_key);
            println!("[DEBUG] Using base key: {}", base_key);
            
            // Get the committed transactions
            let committed_transactions = self.transaction_state.get_committed_transactions();
            println!("[DEBUG] Committed transactions: {:?}", committed_transactions);
            
            // Get the latest committed version
            if let Some(version) = self.version_store.get_latest_committed_version(&base_key, &committed_transactions) {
                println!("[DEBUG] Found committed version: tx_id={}, value={:?}, timestamp={:?}", 
                    version.transaction_id, version.value, version.timestamp);
                Ok(Some(version.value.clone()))
            } else {
                println!("[DEBUG] No committed version found");
                Ok(None)
            }
        } else {
            println!("[DEBUG] Invalid key format");
            Ok(None)
        }
    }

    pub fn read_uncommitted(&self, key: &str) -> Result<Option<Vec<DataValue>>, ReefDBError> {
        if let Some(KeyFormat::Row { table_name, version: _, primary_key }) = KeyFormat::parse(key) {
            let base_key = KeyFormat::row(&table_name, 0, &primary_key);
            
            // Get the latest version, regardless of transaction state
            if let Some(version) = self.version_store.get_latest_version(&base_key) {
                Ok(Some(version.value.clone()))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn is_active(&self, transaction_id: u64) -> bool {
        self.transaction_state.is_transaction_active(transaction_id)
    }

    pub fn table_exists(&self, _transaction_id: u64, table_name: &str) -> Result<bool, ReefDBError> {
        Ok(self.tables.contains(table_name))
    }

    pub fn create_table(&mut self, transaction_id: u64, table_name: String) -> Result<(), ReefDBError> {
        if !self.transaction_state.is_transaction_active(transaction_id) {
            return Err(ReefDBError::Other("Transaction not found".to_string()));
        }
        self.tables.insert(table_name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_mvcc_manager_new() {
        let manager = MVCCManager::new();
        assert!(manager.transaction_state.is_empty());
        assert!(manager.tables.is_empty());
    }

    #[test]
    fn test_begin_transaction() {
        let mut manager = MVCCManager::new();
        manager.begin_transaction(1);
        assert!(manager.transaction_state.is_transaction_active(1));
    }

    #[test]
    fn test_write_and_read() -> Result<(), ReefDBError> {
        let mut manager = MVCCManager::new();
        let data = vec![DataValue::Integer(42)];
        
        manager.begin_transaction(1);
        let key = KeyFormat::row("users", 1, "1");
        manager.write(1, key.clone(), data.clone())?;
        
        // Test read_uncommitted
        let result = manager.read_uncommitted(&key)?;
        assert_eq!(result, Some(data.clone()));

        // Test read_committed (should be None since not committed)
        let result = manager.read_committed(1, &key)?;
        assert_eq!(result, None);

        // Commit and test read_committed again
        manager.commit(1)?;
        let result = manager.read_committed(1, &key)?;
        assert_eq!(result, Some(data));

        Ok(())
    }

    #[test]
    fn test_write_conflict() -> Result<(), ReefDBError> {
        let mut manager = MVCCManager::new();
        let data = vec![DataValue::Integer(42)];
        
        manager.begin_transaction(1);
        manager.begin_transaction(2);
        
        let key = KeyFormat::row("users", 1, "1");
        manager.write(1, key.clone(), data.clone())?;

        // Second transaction should still be able to write (MVCC handles conflicts)
        let result = manager.write(2, key, data);
        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    fn test_concurrent_transactions() -> Result<(), ReefDBError> {
        let mut manager = MVCCManager::new();
        let data1 = vec![DataValue::Integer(42)];
        let data2 = vec![DataValue::Integer(43)];
        
        manager.begin_transaction(1);
        manager.begin_transaction(2);
        
        let key1 = KeyFormat::row("users", 1, "1");
        let key2 = KeyFormat::row("users", 2, "2");
        
        manager.write(1, key1.clone(), data1.clone())?;
        manager.write(2, key2.clone(), data2.clone())?;
        
        // Test visibility before commit
        assert_eq!(manager.read_uncommitted(&key1)?, Some(data1.clone()));
        assert_eq!(manager.read_uncommitted(&key2)?, Some(data2.clone()));
        assert_eq!(manager.read_committed(1, &key1)?, None);
        assert_eq!(manager.read_committed(1, &key2)?, None);
        
        // Commit transactions
        manager.commit(1)?;
        manager.commit(2)?;
        
        // Test visibility after commit
        assert_eq!(manager.read_committed(1, &key1)?, Some(data1));
        assert_eq!(manager.read_committed(1, &key2)?, Some(data2));
        
        Ok(())
    }

    #[test]
    fn test_concurrent_transactions_with_precise_timestamps() -> Result<(), ReefDBError> {
        let mut manager = MVCCManager::new();
        let data = vec![DataValue::Integer(42)];
        
        // Start two transactions in quick succession
        manager.begin_transaction(1);
        thread::sleep(Duration::from_millis(10));
        manager.begin_transaction(2);
        
        let key = KeyFormat::row("users", 1, "1");
        manager.write(1, key.clone(), data.clone())?;
        
        // Before commit, transaction 2 shouldn't see the data
        assert_eq!(manager.read_committed(1, &key)?, None);
        
        // Commit transaction 1
        manager.commit(1)?;
        
        // After commit, transaction 2 should see the data
        assert_eq!(manager.read_committed(1, &key)?, Some(data.clone()));
        
        Ok(())
    }

    #[test]
    fn test_rollback() -> Result<(), ReefDBError> {
        let mut manager = MVCCManager::new();
        let data = vec![DataValue::Integer(42)];
        
        manager.begin_transaction(1);
        let key = KeyFormat::row("users", 1, "1");
        manager.write(1, key.clone(), data)?;
        
        // Rollback the transaction
        manager.rollback(1)?;
        
        // Data should not be visible
        assert_eq!(manager.read_committed(1, &key)?, None);
        assert_eq!(manager.read_uncommitted(&key)?, None);
        
        Ok(())
    }

    #[test]
    fn test_table_operations() -> Result<(), ReefDBError> {
        let mut manager = MVCCManager::new();
        manager.begin_transaction(1);
        
        // Test table creation
        assert!(!manager.table_exists(1, "users")?);
        manager.create_table(1, "users".to_string())?;
        assert!(manager.table_exists(1, "users")?);
        
        Ok(())
    }
} 