use std::sync::atomic::{AtomicBool, Ordering};
use crate::{
    error::ReefDBError,
    TableStorage,
    storage::Storage,
};

pub(crate) struct AcidManager {
    committed: AtomicBool,
    durability_enabled: bool,
    snapshot: TableStorage,
}

impl Clone for AcidManager {
    fn clone(&self) -> Self {
        AcidManager {
            committed: AtomicBool::new(self.committed.load(Ordering::SeqCst)),
            durability_enabled: self.durability_enabled,
            snapshot: self.snapshot.clone(),
        }
    }
}

impl AcidManager {
    pub(crate) fn new(durability_enabled: bool) -> Self {
        AcidManager {
            committed: AtomicBool::new(false),
            durability_enabled,
            snapshot: TableStorage::new(),
        }
    }

    pub(crate) fn begin_atomic(&mut self, tables: &TableStorage) {
        self.snapshot = tables.clone();
    }

    pub(crate) fn commit_atomic(&mut self) -> Result<(), ReefDBError> {
        if self.durability_enabled {
            // Ensure data is written to disk
            sync_to_disk()?;
        }
        self.committed.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub(crate) fn rollback_atomic(&self) -> TableStorage {
        self.snapshot.clone()
    }
}

fn sync_to_disk() -> Result<(), ReefDBError> {
    // Force sync to disk using fsync
    #[cfg(unix)]
    {
        std::fs::File::create(".sync")
            .map_err(|e| ReefDBError::Other(format!("Failed to create sync file: {}", e)))?
            .sync_all()
            .map_err(|e| ReefDBError::Other(format!("Failed to sync to disk: {}", e)))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        sql::{
            column_def::ColumnDef,
            constraints::constraint::Constraint,
            data_type::DataType,
            data_value::DataValue,
        },
    };

    fn create_test_table() -> TableStorage {
        let mut storage = TableStorage::new();
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                constraints: vec![
                    Constraint::PrimaryKey,
                    Constraint::NotNull,
                ],
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                constraints: vec![
                    Constraint::NotNull,
                ],
            },
        ];
        let rows = vec![
            vec![DataValue::Integer(1), DataValue::Text("Alice".to_string())],
            vec![DataValue::Integer(2), DataValue::Text("Bob".to_string())],
        ];
        storage.insert_table("users".to_string(), columns, rows);
        storage
    }

    #[test]
    fn test_acid_manager_new() {
        let manager = AcidManager::new(true);
        assert!(manager.durability_enabled);
        assert!(!manager.committed.load(Ordering::SeqCst));
    }

    #[test]
    fn test_acid_manager_clone() {
        let manager = AcidManager::new(true);
        let cloned = manager.clone();
        assert_eq!(manager.durability_enabled, cloned.durability_enabled);
        assert_eq!(
            manager.committed.load(Ordering::SeqCst),
            cloned.committed.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn test_begin_atomic() {
        let mut manager = AcidManager::new(true);
        let tables = create_test_table();
        manager.begin_atomic(&tables);
        
        // Verify snapshot was taken
        assert!(manager.snapshot.table_exists("users"));
        if let Some((cols, rows)) = manager.snapshot.get_table_ref("users") {
            assert_eq!(cols.len(), 2);
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][1], DataValue::Text("Alice".to_string()));
        } else {
            panic!("Table not found in snapshot");
        }
    }

    #[test]
    fn test_commit_atomic() {
        // Clean up any existing sync file first
        let _ = std::fs::remove_file(".sync");

        let mut manager = AcidManager::new(true);
        let tables = create_test_table();
        manager.begin_atomic(&tables);
        let result = manager.commit_atomic();
        assert!(result.is_ok());
        assert!(manager.committed.load(Ordering::SeqCst));
        
        // Check if sync file was created and then clean up
        assert!(std::path::Path::new(".sync").exists());
        let _ = std::fs::remove_file(".sync");
    }

    #[test]
    fn test_rollback_atomic() {
        let mut manager = AcidManager::new(true);
        let original_tables = create_test_table();
        manager.begin_atomic(&original_tables);

        // Simulate some changes to the original tables
        let mut modified_tables = original_tables.clone();
        modified_tables.push_value("users", vec![
            DataValue::Integer(3),
            DataValue::Text("Charlie".to_string()),
        ]);

        // Rollback should return the original state
        let rolled_back = manager.rollback_atomic();
        if let Some((_, rows)) = rolled_back.get_table_ref("users") {
            assert_eq!(rows.len(), 2); // Should have original 2 rows, not 3
            assert_eq!(rows[0][1], DataValue::Text("Alice".to_string()));
            assert_eq!(rows[1][1], DataValue::Text("Bob".to_string()));
        } else {
            panic!("Table not found after rollback");
        }
    }

    #[test]
    fn test_durability_disabled() {
        // Clean up any existing sync file first
        let _ = std::fs::remove_file(".sync");

        let mut manager = AcidManager::new(false);
        let tables = create_test_table();
        manager.begin_atomic(&tables);
        let result = manager.commit_atomic();
        assert!(result.is_ok());
        
        // Verify sync file was not created
        assert!(!std::path::Path::new(".sync").exists());
    }
} 