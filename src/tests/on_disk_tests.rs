#[cfg(test)]
mod tests {
    use crate::sql::{data_value::DataValue, column_def::ColumnDef, data_type::DataType};
    use crate::storage::{disk::OnDiskStorage, Storage};
    use crate::indexes::{IndexType, IndexManager, verification::IndexVerification};
    use crate::indexes::gin::GinIndex;
    use crate::fts::tokenizers::default::DefaultTokenizer;
    use tempfile::tempdir;

    #[test]
    fn test_disk_storage_with_indexes() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db_path_str = db_path.to_str().unwrap().to_string();
        
        // Create initial storage
        let mut storage = OnDiskStorage::new(db_path_str.clone());
        
        // Create a table with a text column
        let columns = vec![
            ColumnDef::new("id", DataType::Integer, vec![]),
            ColumnDef::new("content", DataType::Text, vec![]),
        ];
        
        let rows = vec![
            vec![
                DataValue::Integer(1),
                DataValue::Text("hello world".to_string()),
            ],
            vec![
                DataValue::Integer(2),
                DataValue::Text("testing gin".to_string()),
            ],
        ];
        
        // Insert table
        storage.insert_table("test_table".to_string(), columns, rows);
        storage.save();
        
        // Create GIN index
        let mut gin = GinIndex::<DefaultTokenizer>::new();
        
        // Add some tokens directly
        gin.add_raw_token(b"hello", 1);
        gin.add_raw_token(b"world", 1);
        gin.add_raw_token(b"testing", 2);
        gin.add_raw_token(b"gin", 2);
        
        storage.create_index("test_table", "content", IndexType::GIN(gin)).unwrap();
        
        // Verify index consistency
        let result = storage.verify_index_consistency("test_table", "content").unwrap();
        assert!(result.is_consistent, "Initial index should be consistent");
        assert!(result.issues.is_empty(), "Initial index should have no issues");
        
        // Drop the storage instance to ensure it's written to disk
        drop(storage);
        
        // Create new storage instance from the same file
        let storage = OnDiskStorage::new(db_path_str);
        
        // Verify the table exists and has correct data
        assert!(storage.table_exists("test_table"));
        
        // Verify index still exists and is consistent
        let result = storage.verify_index_consistency("test_table", "content").unwrap();
        assert!(result.is_consistent, "Loaded index should be consistent");
        assert!(result.issues.is_empty(), "Loaded index should have no issues");
    }
} 