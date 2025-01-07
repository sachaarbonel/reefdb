use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::sql::data_value::DataValue;
use crate::error::ReefDBError;

#[derive(Clone)]
struct Version {
    data: Vec<DataValue>,
    timestamp: u64,
    created_by: u64,
    deleted_by: Option<u64>,
}

pub struct MVCCManager {
    versions: HashMap<String, Vec<Version>>,
    transaction_timestamps: HashMap<u64, u64>,
    timestamp_generator: AtomicU64,
}

impl MVCCManager {
    pub fn new() -> Self {
        MVCCManager {
            versions: HashMap::new(),
            transaction_timestamps: HashMap::new(),
            timestamp_generator: AtomicU64::new(0),
        }
    }

    pub fn begin_transaction(&mut self, tx_id: u64) -> u64 {
        let timestamp = self.timestamp_generator.fetch_add(1, Ordering::SeqCst);
        self.transaction_timestamps.insert(tx_id, timestamp);
        timestamp
    }

    pub fn read(&self, tx_id: u64, key: &str) -> Option<Vec<DataValue>> {
        let tx_timestamp = self.transaction_timestamps.get(&tx_id)?;
        
        if let Some(versions) = self.versions.get(key) {
            // Find the latest version visible to this transaction
            for version in versions.iter().rev() {
                if version.timestamp <= *tx_timestamp && 
                   version.deleted_by.map_or(true, |ts| ts > *tx_timestamp) {
                    return Some(version.data.clone());
                }
            }
        }
        None
    }

    pub fn write(&mut self, tx_id: u64, key: String, data: Vec<DataValue>) -> Result<(), ReefDBError> {
        let timestamp = self.transaction_timestamps.get(&tx_id)
            .ok_or_else(|| ReefDBError::Other("Transaction not found".to_string()))?;

        let version = Version {
            data,
            timestamp: *timestamp,
            created_by: tx_id,
            deleted_by: None,
        };

        self.versions.entry(key)
            .or_insert_with(Vec::new)
            .push(version);

        Ok(())
    }

    pub fn commit(&mut self, tx_id: u64) {
        self.transaction_timestamps.remove(&tx_id);
    }

    pub fn rollback(&mut self, tx_id: u64) {
        // Remove all versions created by this transaction
        for versions in self.versions.values_mut() {
            versions.retain(|v| v.created_by != tx_id);
        }
        self.transaction_timestamps.remove(&tx_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mvcc_manager_new() {
        let manager = MVCCManager::new();
        assert!(manager.versions.is_empty());
        assert!(manager.transaction_timestamps.is_empty());
        assert_eq!(manager.timestamp_generator.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_begin_transaction() {
        let mut manager = MVCCManager::new();
        
        // Start first transaction
        let timestamp1 = manager.begin_transaction(1);
        assert_eq!(timestamp1, 0);
        assert_eq!(manager.transaction_timestamps.get(&1), Some(&0));
        
        // Start second transaction
        let timestamp2 = manager.begin_transaction(2);
        assert_eq!(timestamp2, 1);
        assert_eq!(manager.transaction_timestamps.get(&2), Some(&1));
    }

    #[test]
    fn test_write_and_read() {
        let mut manager = MVCCManager::new();
        
        // Begin transaction and write data
        let tx_id = 1;
        manager.begin_transaction(tx_id);
        
        let data = vec![DataValue::Integer(42), DataValue::Text("test".to_string())];
        manager.write(tx_id, "key1".to_string(), data.clone()).unwrap();
        
        // Read data from same transaction
        let read_data = manager.read(tx_id, "key1");
        assert_eq!(read_data, Some(data));
    }

    #[test]
    fn test_mvcc_isolation() {
        let mut manager = MVCCManager::new();
        
        // Transaction 1 writes data
        let tx1_id = 1;
        manager.begin_transaction(tx1_id);
        let data1 = vec![DataValue::Integer(42)];
        manager.write(tx1_id, "key1".to_string(), data1.clone()).unwrap();
        
        // Transaction 2 starts after Transaction 1's write
        let tx2_id = 2;
        manager.begin_transaction(tx2_id);
        
        // Transaction 2 writes different data
        let data2 = vec![DataValue::Integer(43)];
        manager.write(tx2_id, "key1".to_string(), data2.clone()).unwrap();
        
        // Each transaction should see its own version
        assert_eq!(manager.read(tx1_id, "key1"), Some(data1.clone()));
        assert_eq!(manager.read(tx2_id, "key1"), Some(data2));
    }

    #[test]
    fn test_commit_and_rollback() {
        let mut manager = MVCCManager::new();
        
        // Start and commit first transaction
        let tx1_id = 1;
        manager.begin_transaction(tx1_id);
        let data1 = vec![DataValue::Integer(42)];
        manager.write(tx1_id, "key1".to_string(), data1.clone()).unwrap();
        manager.commit(tx1_id);
        
        // Start second transaction and roll it back
        let tx2_id = 2;
        manager.begin_transaction(tx2_id);
        let data2 = vec![DataValue::Integer(43)];
        manager.write(tx2_id, "key1".to_string(), data2).unwrap();
        manager.rollback(tx2_id);
        
        // Start third transaction to verify state
        let tx3_id = 3;
        manager.begin_transaction(tx3_id);
        
        // Should see committed data from tx1, not rolled back data from tx2
        assert_eq!(manager.read(tx3_id, "key1"), Some(data1));
    }

    #[test]
    fn test_read_committed_isolation() {
        let mut manager = MVCCManager::new();
        
        // Transaction 1 writes and commits
        let tx1_id = 1;
        manager.begin_transaction(tx1_id);
        let data1 = vec![DataValue::Integer(42)];
        manager.write(tx1_id, "key1".to_string(), data1.clone()).unwrap();
        manager.commit(tx1_id);
        
        // Transaction 2 starts and reads
        let tx2_id = 2;
        manager.begin_transaction(tx2_id);
        assert_eq!(manager.read(tx2_id, "key1"), Some(data1.clone()));
        
        // Transaction 3 writes but doesn't commit
        let tx3_id = 3;
        manager.begin_transaction(tx3_id);
        let data3 = vec![DataValue::Integer(44)];
        manager.write(tx3_id, "key1".to_string(), data3).unwrap();
        
        // Transaction 2 should still see the committed data from Transaction 1
        assert_eq!(manager.read(tx2_id, "key1"), Some(data1));
    }

    #[test]
    fn test_multiple_versions() {
        let mut manager = MVCCManager::new();
        
        // Create multiple versions of the same key
        for i in 1..=3 {
            let tx_id = i;
            manager.begin_transaction(tx_id);
            let data = vec![DataValue::Integer(i as i32)];
            manager.write(tx_id, "key1".to_string(), data).unwrap();
            manager.commit(tx_id);
        }
        
        // Start a new transaction with a later timestamp
        let tx4_id = 4;
        manager.begin_transaction(tx4_id);
        
        // Should see the latest committed version
        let expected_data = vec![DataValue::Integer(3)];
        assert_eq!(manager.read(tx4_id, "key1"), Some(expected_data));
    }

    #[test]
    fn test_write_skew() {
        let mut manager = MVCCManager::new();
        
        // Initialize data
        let tx0_id = 0;
        manager.begin_transaction(tx0_id);
        manager.write(tx0_id, "balance1".to_string(), vec![DataValue::Integer(100)]).unwrap();
        manager.write(tx0_id, "balance2".to_string(), vec![DataValue::Integer(100)]).unwrap();
        manager.commit(tx0_id);
        
        // Transaction 1 reads both balances and updates balance1
        let tx1_id = 1;
        manager.begin_transaction(tx1_id);
        manager.read(tx1_id, "balance1");
        manager.read(tx1_id, "balance2");
        manager.write(tx1_id, "balance1".to_string(), vec![DataValue::Integer(-50)]).unwrap();
        
        // Transaction 2 reads both balances and updates balance2
        let tx2_id = 2;
        manager.begin_transaction(tx2_id);
        manager.read(tx2_id, "balance1");
        manager.read(tx2_id, "balance2");
        manager.write(tx2_id, "balance2".to_string(), vec![DataValue::Integer(-50)]).unwrap();
        
        // Both transactions can commit because MVCC doesn't prevent write skew
        manager.commit(tx1_id);
        manager.commit(tx2_id);
        
        // Start a new transaction to check final state
        let tx3_id = 3;
        manager.begin_transaction(tx3_id);
        let final_balance1 = manager.read(tx3_id, "balance1").unwrap();
        let final_balance2 = manager.read(tx3_id, "balance2").unwrap();
        
        assert_eq!(final_balance1, vec![DataValue::Integer(-50)]);
        assert_eq!(final_balance2, vec![DataValue::Integer(-50)]);
    }
} 