use std::collections::HashMap;
use crate::{
    error::ReefDBError,
    TableStorage,
    storage::Storage,
};

#[derive(Debug)]
pub(crate) struct Savepoint {
    name: String,
    table_snapshot: TableStorage,
}

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
        let savepoint = Savepoint {
            name,
            table_snapshot: tables,
        };

        self.savepoints
            .entry(transaction_id)
            .or_insert_with(Vec::new)
            .push(savepoint);

        Ok(())
    }

    pub(crate) fn rollback_to_savepoint(&mut self, transaction_id: u64, name: &str) -> Result<TableStorage, ReefDBError> {
        let savepoints = self.savepoints.get_mut(&transaction_id)
            .ok_or_else(|| ReefDBError::Other("No savepoints found for transaction".to_string()))?;

        let position = savepoints.iter()
            .position(|sp| sp.name == name)
            .ok_or_else(|| ReefDBError::Other(format!("Savepoint {} not found", name)))?;

        let snapshot = savepoints[position].table_snapshot.clone();
        savepoints.truncate(position + 1);

        Ok(snapshot)
    }

    pub(crate) fn release_savepoint(&mut self, transaction_id: u64, name: &str) -> Result<(), ReefDBError> {
        let savepoints = self.savepoints.get_mut(&transaction_id)
            .ok_or_else(|| ReefDBError::Other("No savepoints found for transaction".to_string()))?;

        let position = savepoints.iter()
            .position(|sp| sp.name == name)
            .ok_or_else(|| ReefDBError::Other(format!("Savepoint {} not found", name)))?;

        savepoints.remove(position);
        Ok(())
    }

    pub(crate) fn clear_transaction_savepoints(&mut self, transaction_id: u64) {
        self.savepoints.remove(&transaction_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::column_def::ColumnDef;

    #[test]
    fn test_create_savepoint() {
        let mut manager = SavepointManager::new();
        let tx_id = 1;
        let tables = TableStorage::default();
        
        assert!(manager.create_savepoint(tx_id, "sp1".to_string(), tables.clone()).is_ok());
        
        let savepoints = manager.savepoints.get(&tx_id).unwrap();
        assert_eq!(savepoints.len(), 1);
        assert_eq!(savepoints[0].name, "sp1");
    }

    #[test]
    fn test_rollback_to_savepoint() {
        let mut manager = SavepointManager::new();
        let tx_id = 1;
        let mut tables = TableStorage::default();
        
        // Create initial savepoint
        manager.create_savepoint(tx_id, "sp1".to_string(), tables.clone()).unwrap();
        
        // Create second savepoint with modified state
        tables.insert_table("test".to_string(), vec![], vec![]);
        manager.create_savepoint(tx_id, "sp2".to_string(), tables.clone()).unwrap();
        
        // Rollback to sp1
        let result = manager.rollback_to_savepoint(tx_id, "sp1");
        assert!(result.is_ok());
        
        // Verify that only sp1 remains
        let savepoints = manager.savepoints.get(&tx_id).unwrap();
        assert_eq!(savepoints.len(), 1, "Expected one savepoint to remain after rollback");
        assert_eq!(savepoints[0].name, "sp1", "Expected sp1 to be the remaining savepoint");
        
        // Verify the table state was restored correctly
        let restored_state = result.unwrap();
        assert!(!restored_state.table_exists("test"), "Table should not exist in restored state");
    }

    #[test]
    fn test_release_savepoint() {
        let mut manager = SavepointManager::new();
        let tx_id = 1;
        let tables = TableStorage::default();

        manager.create_savepoint(tx_id, "sp1".to_string(), tables.clone()).unwrap();
        manager.create_savepoint(tx_id, "sp2".to_string(), tables.clone()).unwrap();

        assert!(manager.release_savepoint(tx_id, "sp1").is_ok());

        let savepoints = manager.savepoints.get(&tx_id).unwrap();
        assert_eq!(savepoints.len(), 1);
        assert_eq!(savepoints[0].name, "sp2");
    }

    #[test]
    fn test_clear_transaction_savepoints() {
        let mut manager = SavepointManager::new();
        let tx_id = 1;
        let tables = TableStorage::default();

        manager.create_savepoint(tx_id, "sp1".to_string(), tables.clone()).unwrap();
        manager.create_savepoint(tx_id, "sp2".to_string(), tables.clone()).unwrap();

        manager.clear_transaction_savepoints(tx_id);
        assert!(manager.savepoints.get(&tx_id).is_none());
    }
}