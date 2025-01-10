use super::*;
use crate::error::ReefDBError;
use crate::result::ReefDBResult;
use crate::InMemoryReefDB;
use crate::transaction::IsolationLevel;

#[test]
fn test_index_operations() -> Result<(), ReefDBError> {
    let mut db = InMemoryReefDB::create_in_memory()?;
    
    // Begin a transaction
    let transaction_id = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;
    
    // Test 1: Create a table
    let columns = vec![
        ColumnDef::new("id", DataType::Integer, vec![]),
        ColumnDef::new("name", DataType::Text, vec![]),
        ColumnDef::new("age", DataType::Integer, vec![]),
    ];
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::Create(CreateStatement::Table("users".to_string(), columns)))?;
    assert_eq!(result, ReefDBResult::CreateTable);

    // Test 2: Create an index on the age column
    let create_index_stmt = CreateIndexStatement {
        table_name: "users".to_string(),
        column_name: "age".to_string(),
    };
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::CreateIndex(create_index_stmt))?;
    assert_eq!(result, ReefDBResult::CreateIndex);

    // Test 3: Insert some data
    let values1 = vec![
        DataValue::Integer(1),
        DataValue::Text("Alice".to_string()),
        DataValue::Integer(25),
    ];
    let values2 = vec![
        DataValue::Integer(2),
        DataValue::Text("Bob".to_string()),
        DataValue::Integer(30),
    ];
    db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::Insert(InsertStatement::IntoTable("users".to_string(), values1)))?;
    db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::Insert(InsertStatement::IntoTable("users".to_string(), values2)))?;

    // Test 4: Drop the index
    let drop_index_stmt = DropIndexStatement {
        table_name: "users".to_string(),
        column_name: "age".to_string(),
    };
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::DropIndex(drop_index_stmt))?;
    assert_eq!(result, ReefDBResult::DropIndex);

    // Test 5: Try to create index on non-existent table (should fail)
    let create_index_stmt = CreateIndexStatement {
        table_name: "nonexistent".to_string(),
        column_name: "age".to_string(),
    };
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::CreateIndex(create_index_stmt));
    assert!(matches!(result, Err(ReefDBError::TableNotFound(_))));

    // Test 6: Try to create index on non-existent column (should fail)
    let create_index_stmt = CreateIndexStatement {
        table_name: "users".to_string(),
        column_name: "nonexistent".to_string(),
    };
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::CreateIndex(create_index_stmt));
    assert!(matches!(result, Err(ReefDBError::ColumnNotFound(_))));

    // Test 7: Try to drop non-existent index (should fail)
    let drop_index_stmt = DropIndexStatement {
        table_name: "users".to_string(),
        column_name: "nonexistent".to_string(),
    };
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::DropIndex(drop_index_stmt));
    assert!(matches!(result, Err(ReefDBError::ColumnNotFound(_))));

    // Commit the transaction
    db.transaction_manager.as_mut().unwrap().commit_transaction(transaction_id)?;

    Ok(())
} 