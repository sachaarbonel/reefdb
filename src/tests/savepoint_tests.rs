#[cfg(test)]
mod tests {
    use crate::{
        InMemoryReefDB,
        sql::{
            statements::{Statement, create::CreateStatement, insert::InsertStatement},
            column_def::ColumnDef,
            data_type::DataType,
            data_value::DataValue,
            constraints::constraint::Constraint,
        },
        error::ReefDBError,
        result::ReefDBResult,
        transaction::IsolationLevel,
    };

    fn setup_test_table(db: &mut InMemoryReefDB, transaction_id: u64) -> Result<(), ReefDBError> {
        // Create test table
        let columns = vec![
            ColumnDef::new("id", DataType::Integer, vec![Constraint::PrimaryKey]),
            ColumnDef::new("name", DataType::Text, vec![]),
        ];
        db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::Create(CreateStatement::Table("users".to_string(), columns)))?;
        Ok(())
    }

    #[test]
    fn test_savepoint_basic_operations() -> Result<(), ReefDBError> {
        let mut db = InMemoryReefDB::create_in_memory()?;
        
        // Begin transaction
        let transaction_id = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;

        // Create test table
        setup_test_table(&mut db, transaction_id)?;

        // Insert initial data
        let insert_stmt = Statement::Insert(InsertStatement::IntoTable(
            "users".to_string(),
            vec![DataValue::Integer(1), DataValue::Text("Alice".to_string())],
        ));
        db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, insert_stmt)?;

        // Create a savepoint
        let (_, savepoint_stmt) = Statement::parse("SAVEPOINT sp1").unwrap();
        db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, savepoint_stmt)?;

        // Insert more data
        let insert_stmt2 = Statement::Insert(InsertStatement::IntoTable(
            "users".to_string(),
            vec![DataValue::Integer(2), DataValue::Text("Bob".to_string())],
        ));
        db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, insert_stmt2)?;

        // Rollback to savepoint
        let (_, rollback_stmt) = Statement::parse("ROLLBACK TO SAVEPOINT sp1").unwrap();
        db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, rollback_stmt)?;

        // Verify only Alice exists
        let (_, select_stmt) = Statement::parse("SELECT * FROM users").unwrap();
        let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, select_stmt)?;
        
        if let ReefDBResult::Select(rows) = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][1], DataValue::Text("Alice".to_string()));
        } else {
            panic!("Expected Select result");
        }

        // Commit transaction
        db.transaction_manager.as_mut().unwrap().commit_transaction(transaction_id)?;

        Ok(())
    }

    #[test]
    fn test_savepoint_without_transaction() -> Result<(), ReefDBError> {
        let mut db = InMemoryReefDB::create_in_memory()?;

        // Try to create a savepoint without an active transaction
        let (_, savepoint_stmt) = Statement::parse("SAVEPOINT sp1").unwrap();
        let result = db.execute_statement(savepoint_stmt);
        assert!(matches!(result, Err(ReefDBError::TransactionNotActive)));

        Ok(())
    }

    #[test]
    fn test_savepoint_multiple_levels() -> Result<(), ReefDBError> {
        let mut db = InMemoryReefDB::create_in_memory()?;
        
        // Begin transaction
        let transaction_id = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;

        // Create test table
        setup_test_table(&mut db, transaction_id)?;

        // Insert initial data
        let insert_stmt = Statement::Insert(InsertStatement::IntoTable(
            "users".to_string(),
            vec![DataValue::Integer(1), DataValue::Text("Alice".to_string())],
        ));
        db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, insert_stmt)?;

        // Create first savepoint
        let (_, sp1_stmt) = Statement::parse("SAVEPOINT sp1").unwrap();
        db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, sp1_stmt)?;

        // Insert second record
        let insert_stmt2 = Statement::Insert(InsertStatement::IntoTable(
            "users".to_string(),
            vec![DataValue::Integer(2), DataValue::Text("Bob".to_string())],
        ));
        db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, insert_stmt2)?;

        // Create second savepoint
        let (_, sp2_stmt) = Statement::parse("SAVEPOINT sp2").unwrap();
        db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, sp2_stmt)?;

        // Insert third record
        let insert_stmt3 = Statement::Insert(InsertStatement::IntoTable(
            "users".to_string(),
            vec![DataValue::Integer(3), DataValue::Text("Charlie".to_string())],
        ));
        db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, insert_stmt3)?;

        // Rollback to first savepoint
        let (_, rollback_stmt) = Statement::parse("ROLLBACK TO SAVEPOINT sp1").unwrap();
        db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, rollback_stmt)?;

        // Verify only Alice exists
        let (_, select_stmt) = Statement::parse("SELECT * FROM users").unwrap();
        let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, select_stmt)?;
        
        if let ReefDBResult::Select(rows) = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][1], DataValue::Text("Alice".to_string()));
        } else {
            panic!("Expected Select result");
        }

        // Commit transaction
        db.transaction_manager.as_mut().unwrap().commit_transaction(transaction_id)?;

        Ok(())
    }

    #[test]
    fn test_savepoint_error_cases() -> Result<(), ReefDBError> {
        let mut db = InMemoryReefDB::create_in_memory()?;

        // Begin transaction
        let transaction_id = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;

        // Create a savepoint
        let (_, sp1_stmt) = Statement::parse("SAVEPOINT sp1").unwrap();
        db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, sp1_stmt)?;

        // Try to create a duplicate savepoint
        let (_, sp1_stmt_dup) = Statement::parse("SAVEPOINT sp1").unwrap();
        let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, sp1_stmt_dup);
        assert!(matches!(result, Err(ReefDBError::Other(_))));

        // Try to rollback to non-existent savepoint
        let (_, rollback_stmt) = Statement::parse("ROLLBACK TO SAVEPOINT nonexistent").unwrap();
        let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, rollback_stmt);
        assert!(matches!(result, Err(ReefDBError::SavepointNotFound(_))));

        // Commit transaction
        db.transaction_manager.as_mut().unwrap().commit_transaction(transaction_id)?;

        Ok(())
    }

    #[test]
    fn test_savepoint_with_delete() -> Result<(), ReefDBError> {
        let mut db = InMemoryReefDB::create_in_memory()?;
        
        // Begin transaction
        let transaction_id = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;

        // Create test table
        setup_test_table(&mut db, transaction_id)?;

        // Insert initial data
        let insert_stmt = Statement::Insert(InsertStatement::IntoTable(
            "users".to_string(),
            vec![DataValue::Integer(1), DataValue::Text("Alice".to_string())],
        ));
        db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, insert_stmt)?;

        // Create savepoint
        let (_, sp1_stmt) = Statement::parse("SAVEPOINT sp1").unwrap();
        db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, sp1_stmt)?;

        // Delete the record
        let (_, delete_stmt) = Statement::parse("DELETE FROM users WHERE id = 1").unwrap();
        db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, delete_stmt)?;

        // Verify record is deleted
        let (_, select_stmt) = Statement::parse("SELECT * FROM users").unwrap();
        let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, select_stmt)?;
        
        if let ReefDBResult::Select(rows) = result {
            assert_eq!(rows.len(), 0);
        } else {
            panic!("Expected Select result");
        }

        // Rollback to savepoint
        let (_, rollback_stmt) = Statement::parse("ROLLBACK TO SAVEPOINT sp1").unwrap();
        db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, rollback_stmt)?;

        // Verify record is restored
        let (_, select_stmt) = Statement::parse("SELECT * FROM users").unwrap();
        let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, select_stmt)?;
        
        if let ReefDBResult::Select(rows) = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][1], DataValue::Text("Alice".to_string()));
        } else {
            panic!("Expected Select result");
        }

        // Commit transaction
        db.transaction_manager.as_mut().unwrap().commit_transaction(transaction_id)?;

        Ok(())
    }
} 