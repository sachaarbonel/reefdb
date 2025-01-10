use std::collections::HashMap;
use crate::{
    error::ReefDBError,
    TableStorage,
    transaction::TransactionState,
    savepoint::{Savepoint, SavepointState},
};

#[derive(Clone)]
pub struct SavepointHandler {
    savepoints: HashMap<String, Savepoint>,
}

impl SavepointHandler {
    pub fn new() -> Self {
        Self {
            savepoints: HashMap::new(),
        }
    }

    pub fn create_savepoint(&mut self, name: String, tables: TableStorage) -> Result<(), ReefDBError> {
        if self.savepoints.contains_key(&name) {
            return Err(ReefDBError::Other(format!("Savepoint {} already exists", name)));
        }
        
        let savepoint = Savepoint {
            name: name.clone(),
            table_snapshot: tables,
            timestamp: std::time::SystemTime::now(),
            state: SavepointState::Active,
        };
        
        self.savepoints.insert(name, savepoint);
        Ok(())
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<(TableStorage, Vec<String>), ReefDBError> {
        let savepoint = self.savepoints.get(name)
            .ok_or_else(|| ReefDBError::SavepointNotFound(name.to_string()))?;
            
        if savepoint.state != SavepointState::Active {
            return Err(ReefDBError::SavepointNotActive(name.to_string()));
        }
        
        let snapshot = savepoint.table_snapshot.clone();
        
        // Get all savepoint names in order
        let mut savepoint_names: Vec<_> = self.savepoints.keys().cloned().collect();
        savepoint_names.sort();
        
        let savepoint_time = savepoint_names.iter()
            .position(|sp_name| sp_name == name)
            .unwrap();
            
        // Get names of savepoints to be removed
        let removed_savepoints: Vec<String> = savepoint_names.iter()
            .skip(savepoint_time + 1)
            .cloned()
            .collect();
            
        // Remove all savepoints created after this one
        for sp_name in &removed_savepoints {
            self.savepoints.remove(sp_name);
        }
        
        Ok((snapshot, removed_savepoints))
    }

    pub fn release_savepoint(&mut self, name: &str) -> Result<(), ReefDBError> {
        if !self.savepoints.contains_key(name) {
            return Err(ReefDBError::SavepointNotFound(name.to_string()));
        }
        
        let savepoint = self.savepoints.get(name)
            .ok_or_else(|| ReefDBError::SavepointNotFound(name.to_string()))?;
            
        if savepoint.state != SavepointState::Active {
            return Err(ReefDBError::SavepointNotActive(name.to_string()));
        }
        
        self.savepoints.remove(name);
        Ok(())
    }

    pub fn get_savepoints(&self) -> &HashMap<String, Savepoint> {
        &self.savepoints
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_savepoint() {
        let mut handler = SavepointHandler::new();
        let tables = TableStorage::new();
        
        // Test successful creation
        assert!(handler.create_savepoint("sp1".to_string(), tables.clone()).is_ok());
        
        // Test duplicate savepoint
        assert!(handler.create_savepoint("sp1".to_string(), tables.clone()).is_err());
    }

    #[test]
    fn test_rollback_to_savepoint() {
        let mut handler = SavepointHandler::new();
        let tables = TableStorage::new();
        
        // Create multiple savepoints
        handler.create_savepoint("sp1".to_string(), tables.clone()).unwrap();
        handler.create_savepoint("sp2".to_string(), tables.clone()).unwrap();
        handler.create_savepoint("sp3".to_string(), tables.clone()).unwrap();
        
        // Test rollback to middle savepoint
        let (_, removed) = handler.rollback_to_savepoint("sp2").unwrap();
        assert_eq!(removed, vec!["sp3".to_string()]);
        assert!(handler.get_savepoints().contains_key("sp1"));
        assert!(handler.get_savepoints().contains_key("sp2"));
        assert!(!handler.get_savepoints().contains_key("sp3"));
        
        // Test rollback to non-existent savepoint
        assert!(handler.rollback_to_savepoint("sp4").is_err());
    }

    #[test]
    fn test_release_savepoint() {
        let mut handler = SavepointHandler::new();
        let tables = TableStorage::new();
        
        // Create savepoint
        handler.create_savepoint("sp1".to_string(), tables.clone()).unwrap();
        
        // Test successful release
        assert!(handler.release_savepoint("sp1").is_ok());
        assert!(!handler.get_savepoints().contains_key("sp1"));
        
        // Test release of non-existent savepoint
        assert!(handler.release_savepoint("sp1").is_err());
    }

    #[test]
    fn test_get_savepoints() {
        let mut handler = SavepointHandler::new();
        let tables = TableStorage::new();
        
        assert!(handler.get_savepoints().is_empty());
        
        handler.create_savepoint("sp1".to_string(), tables.clone()).unwrap();
        assert_eq!(handler.get_savepoints().len(), 1);
        assert!(handler.get_savepoints().contains_key("sp1"));
    }
}