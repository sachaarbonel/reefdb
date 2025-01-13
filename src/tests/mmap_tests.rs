use crate::{
    sql::{
        data_type::DataType,
        data_value::DataValue,
        column_def::ColumnDef,
        constraints::constraint::Constraint,
    },
    storage::mmap::MmapStorage,
    storage::Storage,
    indexes::{IndexManager, IndexType, btree::BTreeIndex},
    error::ReefDBError,
};
use tempfile::NamedTempFile;

#[test]
fn test_mmap_basic_operations() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path().to_string_lossy().to_string();
    
    // Test table creation and data insertion
    {
        let mut storage = MmapStorage::new(file_path.clone());
        
        // Create table schema
        let columns = vec![
            ColumnDef::new("id", DataType::Integer, vec![Constraint::PrimaryKey]),
            ColumnDef::new("name", DataType::Text, vec![]),
            ColumnDef::new("age", DataType::Integer, vec![]),
        ];
        
        // Insert initial data
        let rows = vec![
            vec![
                DataValue::Integer(1),
                DataValue::Text("John".to_string()),
                DataValue::Integer(20),
            ],
            vec![
                DataValue::Integer(2),
                DataValue::Text("Jane".to_string()),
                DataValue::Integer(25),
            ],
        ];
        
        storage.insert_table("users".to_string(), columns, rows);
        assert!(storage.table_exists("users"));
    }

    // Test data persistence and retrieval
    {
        let mut storage = MmapStorage::new(file_path.clone());
        assert!(storage.table_exists("users"));
        
        let (schema, rows) = storage.get_table("users").unwrap();
        assert_eq!(schema.len(), 3);
        assert_eq!(rows.len(), 2);
        
        // Verify schema
        assert_eq!(schema[0].name, "id");
        assert_eq!(schema[1].name, "name");
        assert_eq!(schema[2].name, "age");
        
        // Verify data
        assert_eq!(rows[0][0], DataValue::Integer(1));
        assert_eq!(rows[0][1], DataValue::Text("John".to_string()));
        assert_eq!(rows[0][2], DataValue::Integer(20));
    }

    // Test data modification
    {
        let mut storage = MmapStorage::new(file_path.clone());
        
        // Test push_value
        let new_row = vec![
            DataValue::Integer(3),
            DataValue::Text("Bob".to_string()),
            DataValue::Integer(30),
        ];
        let result = storage.push_value("users", new_row);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3); // Should be the new length
        
        // Test update_table
        let updates = vec![
            ("age".to_string(), DataValue::Integer(21)),
        ];
        let where_clause = Some(("name".to_string(), DataValue::Text("John".to_string())));
        println!("Before update - Getting table data:");
        let (_, rows_before) = storage.get_table("users").unwrap();
        for (i, row) in rows_before.iter().enumerate() {
            println!("Row {}: {:?}", i, row);
        }
        
        let updated = storage.update_table("users", updates, where_clause);
        println!("Number of rows updated: {}", updated);
        
        println!("After update - Getting table data:");
        let (_, rows_after) = storage.get_table("users").unwrap();
        for (i, row) in rows_after.iter().enumerate() {
            println!("Row {}: {:?}", i, row);
        }
        
        assert_eq!(updated, 1);
    }

    // Test data persistence after modifications
    {
        let mut storage = MmapStorage::new(file_path.clone());
        let (_, rows) = storage.get_table("users").unwrap();
        
        // Verify the update worked
        let john_row = rows.iter().find(|row| row[1] == DataValue::Text("John".to_string())).unwrap();
        assert_eq!(john_row[2], DataValue::Integer(21));
        
        // Verify the new row was added
        let bob_row = rows.iter().find(|row| row[1] == DataValue::Text("Bob".to_string())).unwrap();
        assert_eq!(bob_row[2], DataValue::Integer(30));
    }

    // Test schema modifications
    {
        let mut storage = MmapStorage::new(file_path.clone());
        
        // Test add_column
        let new_column = ColumnDef::new("email", DataType::Text, vec![]);
        let result = storage.add_column("users", new_column);
        assert!(result.is_ok());
        
        // Test rename_column
        let result = storage.rename_column("users", "email", "contact_email");
        assert!(result.is_ok());
        
        // Test drop_column
        let result = storage.drop_column("users", "contact_email");
        assert!(result.is_ok());
    }

    // Test index operations
    {
        let mut storage = MmapStorage::new(file_path.clone());
        
        // Create an index
        let btree = BTreeIndex::new();
        let result = storage.create_index("users", "age", IndexType::BTree(btree));
        assert!(result.is_ok());
        
        // Verify index exists
        let index = storage.get_index("users", "age");
        assert!(index.is_ok());
        
        // Drop index
        storage.drop_index("users", "age");
    }

    // Test delete operations
    {
        let mut storage = MmapStorage::new(file_path.clone());
        
        // Test delete with where clause
        let where_clause = Some(("name".to_string(), DataValue::Text("Bob".to_string())));
        let deleted = storage.delete_table("users", where_clause);
        assert_eq!(deleted, 1);
        
        // Verify deletion
        let (_, rows) = storage.get_table("users").unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row[1] != DataValue::Text("Bob".to_string())));
    }

    // Test table removal
    {
        let mut storage = MmapStorage::new(file_path.clone());
        
        // Test remove_table
        let removed = storage.remove_table("users");
        assert!(removed);
        
        // Verify table is gone
        assert!(!storage.table_exists("users"));
    }
}

#[test]
fn test_mmap_error_handling() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path().to_string_lossy().to_string();
    let mut storage = MmapStorage::new(file_path);

    // Test operations on non-existent table
    assert!(matches!(
        storage.push_value("nonexistent", vec![DataValue::Integer(1)]),
        Err(ReefDBError::TableNotFound(_))
    ));

    assert!(matches!(
        storage.add_column("nonexistent", ColumnDef::new("test", DataType::Integer, vec![])),
        Err(ReefDBError::TableNotFound(_))
    ));

    assert!(matches!(
        storage.drop_column("nonexistent", "test"),
        Err(ReefDBError::TableNotFound(_))
    ));

    // Test operations on non-existent column
    let columns = vec![ColumnDef::new("id", DataType::Integer, vec![])];
    storage.insert_table("test".to_string(), columns, vec![]);

    assert!(matches!(
        storage.drop_column("test", "nonexistent"),
        Err(ReefDBError::ColumnNotFound(_))
    ));

    assert!(matches!(
        storage.rename_column("test", "nonexistent", "new_name"),
        Err(ReefDBError::ColumnNotFound(_))
    ));
}

#[test]
fn test_mmap_concurrent_access() {
    use std::thread;
    use std::sync::Arc;
    use std::sync::Mutex;

    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path().to_string_lossy().to_string();
    
    // Initialize storage with a table
    {
        let mut storage = MmapStorage::new(file_path.clone());
        let columns = vec![ColumnDef::new("counter", DataType::Integer, vec![])];
        storage.insert_table("counters".to_string(), columns, vec![]);
    }

    // Create shared storage
    let storage = Arc::new(Mutex::new(MmapStorage::new(file_path)));
    let mut handles = vec![];

    // Spawn multiple threads to insert data
    for i in 0..10 {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            let mut storage = storage_clone.lock().unwrap();
            storage.push_value(
                "counters",
                vec![DataValue::Integer(i)],
            ).unwrap();
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify results
    let mut storage = Arc::try_unwrap(storage).unwrap().into_inner().unwrap();
    let (_, rows) = storage.get_table("counters").unwrap();
    assert_eq!(rows.len(), 10);
} 