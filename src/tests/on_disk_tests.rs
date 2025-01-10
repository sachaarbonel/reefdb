#[cfg(test)]
mod tests {
    use std::fs;
    use crate::{OnDiskReefDB, error::ReefDBError, result::ReefDBResult};
    use crate::sql::{statements::Statement, data_value::DataValue};
    use crate::transaction::IsolationLevel;

    type Result<T> = std::result::Result<T, ReefDBError>;

    #[test]
    fn test_on_disk_basic_workflow() -> Result<()> {
        // Setup: Create temporary file paths
        let kv_path = "test_kv.db";
        let index_path = "test_index.bin";

        // Cleanup any existing files
        let _ = fs::remove_file(kv_path);
        let _ = fs::remove_file(index_path);

        let mut db = OnDiskReefDB::create_on_disk(kv_path.to_string(), index_path.to_string())?;

        // Create test table and insert data
        db.query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")?;
        db.query("INSERT INTO users VALUES (1, 'Alice')")?;
        db.query("INSERT INTO users VALUES (2, 'Bob')")?;

        // Verify data was inserted correctly
        let result = db.query("SELECT name FROM users")?;
        match result {
            ReefDBResult::Select(rows) => {
                assert_eq!(rows.len(), 2);
                // Check that both names are present, without assuming order
                let names: Vec<String> = rows.iter()
                    .map(|(_, values)| match &values[0] {
                        DataValue::Text(name) => name.clone(),
                        _ => panic!("Expected text value"),
                    })
                    .collect();
                assert!(names.contains(&"Alice".to_string()));
                assert!(names.contains(&"Bob".to_string()));
            },
            _ => panic!("Expected Select result"),
        }

        // Cleanup
        let _ = fs::remove_file(kv_path);
        let _ = fs::remove_file(index_path);

        Ok(())
    }

    #[test]
    fn test_on_disk_persistence() -> Result<()> {
        // Setup: Create temporary file paths
        let kv_path = "persist_test_kv.db";
        let index_path = "persist_test_index.bin";

        // Cleanup any existing files
        let _ = fs::remove_file(kv_path);
        let _ = fs::remove_file(index_path);

        // First session: Create database and insert data
        {
            let mut db = OnDiskReefDB::create_on_disk(kv_path.to_string(), index_path.to_string())?;
            db.query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")?;
            db.query("INSERT INTO users VALUES (1, 'Alice')")?;
            db.query("INSERT INTO users VALUES (2, 'Bob')")?;
            // Database is dropped here, which should persist the data
        }

        // Second session: Open the database and verify data persisted
        {
            let mut db = OnDiskReefDB::create_on_disk(kv_path.to_string(), index_path.to_string())?;
            let result = db.query("SELECT name FROM users")?;
            match result {
                ReefDBResult::Select(rows) => {
                    assert_eq!(rows.len(), 2);
                    // Check that both names are present, without assuming order
                    let names: Vec<String> = rows.iter()
                        .map(|(_, values)| match &values[0] {
                            DataValue::Text(name) => name.clone(),
                            _ => panic!("Expected text value"),
                        })
                        .collect();
                    assert!(names.contains(&"Alice".to_string()));
                    assert!(names.contains(&"Bob".to_string()));
                },
                _ => panic!("Expected Select result"),
            }
        }

        // Cleanup
        let _ = fs::remove_file(kv_path);
        let _ = fs::remove_file(index_path);

        Ok(())
    }
} 