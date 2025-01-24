use super::*;
use crate::error::ReefDBError;
use crate::result::ReefDBResult;
use crate::sql::table_reference::TableReference;
use crate::sql::column::ColumnType;
use crate::InMemoryReefDB;
use crate::transaction::IsolationLevel;
use crate::sql::statements::drop::DropStatement;

#[test]
fn test_drop_table() {
    let mut db = InMemoryReefDB::create_in_memory().unwrap();
    
    // Create initial table
    let stmt = Statement::Create(CreateStatement::Table(
        "users".to_string(),
        vec![
            ColumnDef::new("id", DataType::Integer, vec![Constraint::PrimaryKey]),
            ColumnDef::new("name", DataType::Text, vec![]),
        ],
    ));
    db.execute_statement(stmt).unwrap();

    // Insert some data
    let stmt = Statement::Insert(InsertStatement::IntoTable(
        "users".to_string(),
        vec![DataValue::Integer(1), DataValue::Text("John".to_string())],
    ));
    db.execute_statement(stmt).unwrap();

    // Drop the table
    let stmt = Statement::Drop(DropStatement {
        table_name: "users".to_string(),
    });
    db.execute_statement(stmt).unwrap();

    // Verify the table is gone by trying to select from it
    let stmt = Statement::Select(SelectStatement::FromTable(
        TableReference {
            name: "users".to_string(),
            alias: None,
        },
        vec![Column { name: "*".to_string(), table: None, column_type: ColumnType::Wildcard }],
        None,
        vec![],
        vec![],
    ));
    assert!(db.execute_statement(stmt).is_err());
}

#[test]
fn test_drop_nonexistent_table() {
    let mut db = InMemoryReefDB::create_in_memory().unwrap();
    
    // Try to drop a non-existent table
    let stmt = Statement::Drop(DropStatement {
        table_name: "nonexistent".to_string(),
    });
    assert!(db.execute_statement(stmt).is_err());
}

#[test]
fn test_operations_after_drop() {
    let mut db = InMemoryReefDB::create_in_memory().unwrap();
    
    // Create initial table
    let stmt = Statement::Create(CreateStatement::Table(
        "users".to_string(),
        vec![
            ColumnDef::new("id", DataType::Integer, vec![Constraint::PrimaryKey]),
            ColumnDef::new("name", DataType::Text, vec![]),
        ],
    ));
    db.execute_statement(stmt).unwrap();

    // Drop the table
    let stmt = Statement::Drop(DropStatement {
        table_name: "users".to_string(),
    });
    db.execute_statement(stmt).unwrap();

    // Try to insert into dropped table
    let stmt = Statement::Insert(InsertStatement::IntoTable(
        "users".to_string(),
        vec![DataValue::Integer(1), DataValue::Text("John".to_string())],
    ));
    assert!(db.execute_statement(stmt).is_err());

    // Try to update dropped table
    let stmt = Statement::Update(UpdateStatement::UpdateTable(
        "users".to_string(),
        vec![("name".to_string(), DataValue::Text("Jane".to_string()))],
        None,
    ));
    assert!(db.execute_statement(stmt).is_err());

    // Try to alter dropped table
    let stmt = Statement::Alter(AlterStatement {
        table_name: "users".to_string(),
        alter_type: AlterType::AddColumn(ColumnDef::new("age", DataType::Integer, vec![])),
    });
    assert!(db.execute_statement(stmt).is_err());

    // Verify we can create a new table with the same name
    let stmt = Statement::Create(CreateStatement::Table(
        "users".to_string(),
        vec![
            ColumnDef::new("id", DataType::Integer, vec![Constraint::PrimaryKey]),
            ColumnDef::new("name", DataType::Text, vec![]),
        ],
    ));
    assert!(db.execute_statement(stmt).is_ok());
} 