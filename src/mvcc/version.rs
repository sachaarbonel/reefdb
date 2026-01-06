use std::time::SystemTime;
use std::collections::{HashMap, HashSet};
use crate::sql::data_value::DataValue;
use crate::key_format::KeyFormat;
use log::debug;

#[derive(Debug, Clone)]
pub struct Version {
    pub transaction_id: u64,
    pub value: Vec<DataValue>,
    pub timestamp: SystemTime,
}

pub struct VersionStore {
    versions: HashMap<String, Vec<Version>>,
}

impl Version {
    pub fn new(transaction_id: u64, value: Vec<DataValue>) -> Self {
        Self {
            transaction_id,
            value,
            timestamp: SystemTime::now(),
        }
    }

    pub fn with_timestamp(transaction_id: u64, value: Vec<DataValue>, timestamp: SystemTime) -> Self {
        Self {
            transaction_id,
            value,
            timestamp,
        }
    }
}

impl PartialEq for Version {
    fn eq(&self, other: &Self) -> bool {
        self.transaction_id == other.transaction_id &&
        self.value == other.value &&
        self.timestamp == other.timestamp
    }
}

impl Eq for Version {}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // First compare by timestamp (newer first)
        match self.timestamp.cmp(&other.timestamp) {
            std::cmp::Ordering::Equal => {
                // If timestamps are equal, compare by transaction ID (older first)
                self.transaction_id.cmp(&other.transaction_id)
            },
            ord => ord.reverse()  // Reverse to get newer timestamps first
        }
    }
}

impl VersionStore {
    pub fn new() -> Self {
        Self {
            versions: HashMap::new(),
        }
    }

    pub fn store_version(&mut self, key: String, version: Version) {
        debug!("Storing version for key: {}, tx_id: {}", key, version.transaction_id);
        let versions = self.versions.entry(key.clone()).or_insert_with(Vec::new);
        
        // Remove any existing versions from this transaction
        versions.retain(|v| v.transaction_id != version.transaction_id);
        
        Self::insert_version_sorted(versions, version);
        debug!("Current versions for key {}: {:?}", key, versions);
    }

    pub fn retimestamp_transaction_versions(
        &mut self,
        keys: &HashSet<String>,
        transaction_id: u64,
        commit_time: SystemTime,
    ) {
        for key in keys {
            if let Some(versions) = self.versions.get_mut(key) {
                let mut tx_versions: Vec<_> = versions
                    .iter()
                    .filter(|v| v.transaction_id == transaction_id)
                    .cloned()
                    .collect();

                if tx_versions.is_empty() {
                    continue;
                }

                for version in tx_versions.iter_mut() {
                    version.timestamp = commit_time;
                }

                versions.retain(|v| v.transaction_id != transaction_id);
                for version in tx_versions {
                    Self::insert_version_sorted(versions, version);
                }
            }
        }
    }

    pub fn get_latest_committed_version(&self, key: &str, committed_transactions: &HashSet<u64>) -> Option<&Version> {
        self.versions.get(key).and_then(|versions| {
            versions
                .iter()
                .filter(|v| committed_transactions.contains(&v.transaction_id))
                .max_by(|a, b| {
                    // Compare by timestamp (newer first)
                    match a.timestamp.cmp(&b.timestamp) {
                        std::cmp::Ordering::Equal => {
                            // If timestamps are equal, compare by transaction ID (older first)
                            a.transaction_id.cmp(&b.transaction_id)
                        },
                        ord => ord
                    }
                })
        })
    }

    pub fn get_latest_version(&self, key: &str) -> Option<&Version> {
        if let Some(versions) = self.versions.get(key) {
            // Versions are already sorted by timestamp in store_version
            versions.first()
        } else {
            None
        }
    }

    pub fn get_original_version(&self, key: &str) -> Option<&Version> {
        if let Some(KeyFormat::Row { table_name, version: _, primary_key }) = KeyFormat::parse(key) {
            let mut earliest_version = None;
            let mut earliest_timestamp = SystemTime::now();
            
            // Find all versions for this row
            for (version_key, versions) in &self.versions {
                if let Some(KeyFormat::Row { table_name: ver_table, version: _, primary_key: ver_pk }) = KeyFormat::parse(version_key) {
                    if ver_table == table_name && ver_pk == primary_key {
                        // Look through all versions for this key
                        for version in versions {
                            // Take the version with the earliest timestamp
                            if earliest_version.is_none() || version.timestamp < earliest_timestamp {
                                earliest_version = Some(version);
                                earliest_timestamp = version.timestamp;
                            }
                        }
                    }
                }
            }
            earliest_version
        } else {
            None
        }
    }

    pub fn remove_transaction_versions(&mut self, keys: &HashSet<String>, transaction_id: u64) {
        for key in keys {
            if let Some(versions) = self.versions.get_mut(key) {
                versions.retain(|v| v.transaction_id != transaction_id);
                if versions.is_empty() {
                    self.versions.remove(key);
                }
            }
        }
    }

    pub fn get_versions(&self, key: &str) -> Option<&Vec<Version>> {
        self.versions.get(key)
    }

    pub fn get_versions_mut(&mut self, key: &str) -> Option<&mut Vec<Version>> {
        self.versions.get_mut(key)
    }

    pub fn get_version_for_transaction(&self, key: &str, transaction_id: u64) -> Option<&Version> {
        self.versions.get(key)
            .and_then(|versions| versions.iter()
                .find(|v| v.transaction_id == transaction_id))
    }

    pub fn get_latest_committed_version_before(&self, key: &str, committed_transactions: &HashSet<u64>, start_time: SystemTime) -> Option<&Version> {
        self.versions.get(key)
            .and_then(|versions| versions.iter()
                .find(|v| committed_transactions.contains(&v.transaction_id) && v.timestamp <= start_time))
    }

    fn insert_version_sorted(versions: &mut Vec<Version>, version: Version) {
        let insert_pos = versions
            .binary_search_by(|v| v.cmp(&version))
            .unwrap_or_else(|pos| pos);
        versions.insert(insert_pos, version);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_version_creation() {
        let data = vec![DataValue::Integer(42)];
        let version = Version::new(1, data.clone());
        
        assert_eq!(version.transaction_id, 1);
        assert_eq!(version.value, data);
    }

    #[test]
    fn test_version_timestamps() {
        let data = vec![DataValue::Integer(42)];
        
        // Create first version
        let version1 = Version::new(1, data.clone());
        
        // Wait a bit
        thread::sleep(Duration::from_millis(10));
        
        // Create second version
        let version2 = Version::new(2, data);
        
        // Second version should have later timestamp
        assert!(version2.timestamp > version1.timestamp);
    }

    #[test]
    fn test_version_clone() {
        let data = vec![DataValue::Integer(42)];
        let version1 = Version::new(1, data);
        let version2 = version1.clone();
        
        assert_eq!(version1.transaction_id, version2.transaction_id);
        assert_eq!(version1.value, version2.value);
        assert_eq!(version1.timestamp, version2.timestamp);
    }

    #[test]
    fn test_committed_with_newer_uncommitted() {
        let mut store = VersionStore::new();
        let mut committed = HashSet::new();
        let key = "test_key".to_string();
        
        // Create and commit an older version
        let old_version = Version::new(1, vec![DataValue::Integer(100)]);
        store.store_version(key.clone(), old_version);
        committed.insert(1);
        
        // Create a newer uncommitted version
        thread::sleep(Duration::from_millis(10));
        let new_version = Version::new(2, vec![DataValue::Integer(200)]);
        store.store_version(key.clone(), new_version);
        
        // Should return the committed version even though there's a newer uncommitted one
        let result = store.get_latest_committed_version(&key, &committed);
        assert!(result.is_some());
        assert_eq!(result.unwrap().transaction_id, 1);
        assert_eq!(result.unwrap().value, vec![DataValue::Integer(100)]);
    }

    #[test]
    fn test_multiple_uncommitted_versions() {
        let mut store = VersionStore::new();
        let mut committed = HashSet::new();
        let key = "test_key".to_string();
        
        // Create committed version
        let committed_version = Version::new(1, vec![DataValue::Integer(100)]);
        store.store_version(key.clone(), committed_version);
        committed.insert(1);
        
        // Create multiple uncommitted versions with different timestamps
        for i in 2u64..5u64 {
            thread::sleep(Duration::from_millis(10));
            let version = Version::new(i, vec![DataValue::Integer(((i * 100) as i32).into())]);
            store.store_version(key.clone(), version);
        }
        
        // Should still return the committed version
        let result = store.get_latest_committed_version(&key, &committed);
        assert!(result.is_some());
        assert_eq!(result.unwrap().transaction_id, 1);
        assert_eq!(result.unwrap().value, vec![DataValue::Integer(100)]);
    }

    #[test]
    fn test_remove_transaction_versions_partial() {
        let mut store = VersionStore::new();
        let key1 = "key1".to_string();
        let key2 = "key2".to_string();
        
        // Store versions for multiple keys with same transaction
        store.store_version(key1.clone(), Version::new(1, vec![DataValue::Integer(100)]));
        store.store_version(key2.clone(), Version::new(1, vec![DataValue::Integer(200)]));
        store.store_version(key1.clone(), Version::new(2, vec![DataValue::Integer(300)]));
        
        // Remove versions for only key1
        let mut keys_to_remove = HashSet::new();
        keys_to_remove.insert(key1.clone());
        store.remove_transaction_versions(&keys_to_remove, 1);
        
        // Verify key1's version was removed but key2's remains
        assert!(store.get_versions(&key1).unwrap().iter().all(|v| v.transaction_id != 1));
        assert!(store.get_versions(&key2).unwrap().iter().any(|v| v.transaction_id == 1));
    }

    #[test]
    fn test_concurrent_version_timestamps() {
        let mut store = VersionStore::new();
        let key = "test_key".to_string();
        let mut committed = HashSet::new();
        
        // Create first version
        let version1 = Version::new(1, vec![DataValue::Integer(100)]);
        store.store_version(key.clone(), version1.clone());
        
        // Wait a bit to ensure different timestamps
        thread::sleep(Duration::from_millis(10));
        
        // Create second version
        let version2 = Version::new(2, vec![DataValue::Integer(200)]);
        store.store_version(key.clone(), version2.clone());
        
        // Commit both transactions
        committed.insert(1);
        committed.insert(2);
        
        // Should return the version with the later timestamp (version2)
        let result = store.get_latest_committed_version(&key, &committed);
        assert!(result.is_some());
        assert_eq!(result.unwrap().transaction_id, 2);
        
        // Verify timestamps are ordered correctly
        assert!(version2.timestamp > version1.timestamp);
    }

    #[test]
    fn test_empty_version_list() {
        let store = VersionStore::new();
        let committed = HashSet::new();
        
        // Test with non-existent key
        assert!(store.get_latest_committed_version("nonexistent", &committed).is_none());
        assert!(store.get_latest_version("nonexistent").is_none());
        assert!(store.get_original_version("nonexistent").is_none());
    }

    #[test]
    fn test_version_cleanup() {
        let mut store = VersionStore::new();
        let key = "test_key".to_string();
        
        // Add multiple versions for same transaction
        store.store_version(key.clone(), Version::new(1, vec![DataValue::Integer(100)]));
        store.store_version(key.clone(), Version::new(1, vec![DataValue::Integer(200)]));
        
        // Remove all versions for transaction
        let mut keys = HashSet::new();
        keys.insert(key.clone());
        store.remove_transaction_versions(&keys, 1);
        
        // Verify key was completely removed since all versions were for transaction 1
        assert!(store.get_versions(&key).is_none());
    }
}
