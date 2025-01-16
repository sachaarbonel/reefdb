use super::*;
use crate::error::ReefDBError;
use crate::result::ReefDBResult;
use crate::sql::clauses::join_clause::TableReference;
use crate::InMemoryReefDB;
use crate::transaction::IsolationLevel;
use crate::sql::statements::alter::{AlterStatement, AlterType};

#[test]
fn test_add_column() {
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

    // Add a new column
    let stmt = Statement::Alter(AlterStatement {
        table_name: "users".to_string(),
        alter_type: AlterType::AddColumn(ColumnDef::new("age", DataType::Integer, vec![])),
    });
    db.execute_statement(stmt).unwrap();

    // Verify the new column exists with default value
    let stmt = Statement::Select(SelectStatement::FromTable(
        TableReference {
            name: "users".to_string(),
            alias: None,
        },
        vec![Column { name: "age".to_string(), table: None }],
        None,
        vec![],
    ));
    if let ReefDBResult::Select(rows) = db.execute_statement(stmt).unwrap() {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0], DataValue::Integer(0)); // Default value for new integer column
    } else {
        panic!("Expected Select result");
    }
}

#[test]
fn test_drop_column() {
    let mut db = InMemoryReefDB::create_in_memory().unwrap();
    
    // Create initial table
    let stmt = Statement::Create(CreateStatement::Table(
        "users".to_string(),
        vec![
            ColumnDef::new("id", DataType::Integer, vec![Constraint::PrimaryKey]),
            ColumnDef::new("name", DataType::Text, vec![]),
            ColumnDef::new("age", DataType::Integer, vec![]),
        ],
    ));
    db.execute_statement(stmt).unwrap();

    // Insert some data
    let stmt = Statement::Insert(InsertStatement::IntoTable(
        "users".to_string(),
        vec![
            DataValue::Integer(1),
            DataValue::Text("John".to_string()),
            DataValue::Integer(25),
        ],
    ));
    db.execute_statement(stmt).unwrap();

    // Drop the age column
    let stmt = Statement::Alter(AlterStatement {
        table_name: "users".to_string(),
        alter_type: AlterType::DropColumn("age".to_string()),
    });
    db.execute_statement(stmt).unwrap();

    // Verify the column is gone
    let stmt = Statement::Select(SelectStatement::FromTable(
        TableReference {
            name: "users".to_string(),
            alias: None,
        },
        vec![Column { name: "*".to_string(), table: None }],
        None,
        vec![],
    ));
    if let ReefDBResult::Select(rows) = db.execute_statement(stmt).unwrap() {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.len(), 2); // Only id and name columns should remain
        assert_eq!(rows[0].1[0], DataValue::Integer(1));
        assert_eq!(rows[0].1[1], DataValue::Text("John".to_string()));
    } else {
        panic!("Expected Select result");
    }
}

#[test]
fn test_rename_column() {
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

    // Rename the name column to username
    let stmt = Statement::Alter(AlterStatement {
        table_name: "users".to_string(),
        alter_type: AlterType::RenameColumn("name".to_string(), "username".to_string()),
    });
    db.execute_statement(stmt).unwrap();

    // Verify the column was renamed and data preserved
    let stmt = Statement::Select(SelectStatement::FromTable(
        TableReference {
            name: "users".to_string(),
            alias: None,
        },
        vec![Column { name: "username".to_string(), table: None }],
        None,
        vec![],
    ));
    if let ReefDBResult::Select(rows) = db.execute_statement(stmt).unwrap() {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0], DataValue::Text("John".to_string()));
    } else {
        panic!("Expected Select result");
    }
}

#[test]
fn test_alter_errors() {
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

    // Test adding duplicate column
    let stmt = Statement::Alter(AlterStatement {
        table_name: "users".to_string(),
        alter_type: AlterType::AddColumn(ColumnDef::new("name", DataType::Text, vec![])),
    });
    assert!(db.execute_statement(stmt).is_err());

    // Test dropping non-existent column
    let stmt = Statement::Alter(AlterStatement {
        table_name: "users".to_string(),
        alter_type: AlterType::DropColumn("age".to_string()),
    });
    assert!(db.execute_statement(stmt).is_err());

    // Test renaming to existing column name
    let stmt = Statement::Alter(AlterStatement {
        table_name: "users".to_string(),
        alter_type: AlterType::RenameColumn("name".to_string(), "id".to_string()),
    });
    assert!(db.execute_statement(stmt).is_err());

    // Test altering non-existent table
    let stmt = Statement::Alter(AlterStatement {
        table_name: "nonexistent".to_string(),
        alter_type: AlterType::AddColumn(ColumnDef::new("test", DataType::Text, vec![])),
    });
    assert!(db.execute_statement(stmt).is_err());
} 