use std::collections::HashMap;
use crate::{
    error::ReefDBError,
    storage::TableStorage,
};
use super::savepoint::{Savepoint, SavepointState};

#[derive(Clone)]
pub struct SavepointManager {
    savepoints: HashMap<u64, Vec<Savepoint>>,
}

impl SavepointManager {
    pub fn new() -> Self {
        SavepointManager {
            savepoints: HashMap::new(),
        }
    }

    pub(crate) fn create_savepoint(&mut self, transaction_id: u64, name: String, tables: TableStorage) -> Result<(), ReefDBError> {
        // Check if savepoint with same name already exists
        if let Some(transaction_savepoints) = self.savepoints.get(&transaction_id) {
            if transaction_savepoints.iter().any(|sp| sp.name == name) {
                return Err(ReefDBError::Other(format!("Savepoint {} already exists", name)));
            }
        }

        let savepoint = Savepoint {
            name,
            table_snapshot: tables,
            timestamp: std::time::SystemTime::now(),
            state: SavepointState::Active,
        };

        self.savepoints
            .entry(transaction_id)
            .or_insert_with(Vec::new)
            .push(savepoint);

        Ok(())
    }

    pub(crate) fn rollback_to_savepoint(&mut self, transaction_id: u64, name: &str) -> Result<TableStorage, ReefDBError> {
        let transaction_savepoints = self.savepoints.get_mut(&transaction_id)
            .ok_or_else(|| ReefDBError::SavepointNotFound(name.to_string()))?;

        let savepoint_index = transaction_savepoints.iter()
            .position(|sp| sp.name == name)
            .ok_or_else(|| ReefDBError::SavepointNotFound(name.to_string()))?;

        if transaction_savepoints[savepoint_index].state != SavepointState::Active {
            return Err(ReefDBError::SavepointNotActive(name.to_string()));
        }

        // Get the savepoint state
        let snapshot = transaction_savepoints[savepoint_index].table_snapshot.clone();

        // Remove all savepoints after this one
        transaction_savepoints.truncate(savepoint_index + 1);

        Ok(snapshot)
    }

    pub(crate) fn release_savepoint(&mut self, transaction_id: u64, name: &str) -> Result<(), ReefDBError> {
        let transaction_savepoints = self.savepoints.get_mut(&transaction_id)
            .ok_or_else(|| ReefDBError::SavepointNotFound(name.to_string()))?;

        let savepoint_index = transaction_savepoints.iter()
            .position(|sp| sp.name == name)
            .ok_or_else(|| ReefDBError::SavepointNotFound(name.to_string()))?;

        if transaction_savepoints[savepoint_index].state != SavepointState::Active {
            return Err(ReefDBError::SavepointNotActive(name.to_string()));
        }

        transaction_savepoints.remove(savepoint_index);
        Ok(())
    }

    pub(crate) fn clear_transaction_savepoints(&mut self, transaction_id: u64) {
        self.savepoints.remove(&transaction_id);
    }

    pub(crate) fn get_active_savepoints(&self, transaction_id: u64) -> Vec<String> {
        self.savepoints.get(&transaction_id)
            .map(|savepoints| {
                savepoints.iter()
                    .filter(|sp| sp.state == SavepointState::Active)
                    .map(|sp| sp.name.clone())
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_savepoint() {
        let mut manager = SavepointManager::new();
        let tables = TableStorage::new();
        
        assert!(manager.create_savepoint(1, "sp1".to_string(), tables.clone()).is_ok());
        assert_eq!(manager.get_active_savepoints(1), vec!["sp1"]);
        
        // Test duplicate savepoint
        assert!(manager.create_savepoint(1, "sp1".to_string(), tables).is_err());
    }

    #[test]
    fn test_rollback_to_savepoint() {
        let mut manager = SavepointManager::new();
        let mut tables = TableStorage::new();
        
        manager.create_savepoint(1, "sp1".to_string(), tables.clone()).unwrap();
        
        // Modify tables after savepoint
        tables = TableStorage::new(); // Simulating modification
        
        manager.create_savepoint(1, "sp2".to_string(), tables.clone()).unwrap();
        
        // Rollback to first savepoint
        let rolled_back_tables = manager.rollback_to_savepoint(1, "sp1").unwrap();
        
        // Check that sp2 is no longer active
        assert_eq!(manager.get_active_savepoints(1), vec!["sp1"]);
    }

    #[test]
    fn test_release_savepoint() {
        let mut manager = SavepointManager::new();
        let tables = TableStorage::new();
        
        manager.create_savepoint(1, "sp1".to_string(), tables.clone()).unwrap();
        manager.create_savepoint(1, "sp2".to_string(), tables).unwrap();
        
        assert!(manager.release_savepoint(1, "sp1").is_ok());
        assert_eq!(manager.get_active_savepoints(1), vec!["sp2"]);
    }

    #[test]
    fn test_clear_transaction_savepoints() {
        let mut manager = SavepointManager::new();
        let tables = TableStorage::new();
        
        manager.create_savepoint(1, "sp1".to_string(), tables.clone()).unwrap();
        manager.create_savepoint(1, "sp2".to_string(), tables).unwrap();
        
        manager.clear_transaction_savepoints(1);
        assert!(manager.get_active_savepoints(1).is_empty());
    }
} 
