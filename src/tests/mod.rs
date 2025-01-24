pub mod create_tests;
pub mod select_tests;
pub mod insert_tests;
pub mod update_tests;
pub mod delete_tests;
pub mod alter_tests;
pub mod drop_tests;
pub mod index_tests;
pub mod savepoint_tests;
pub mod search_tests;
pub mod on_disk_tests;
pub mod mvcc_integration_tests;
pub mod join_integration_tests;
pub mod fts_tests;
pub mod mmap_tests;
pub mod data_types;
use crate::sql::{
    column_def::ColumnDef,
    data_type::DataType,
    data_value::DataValue,
    statements::{
        Statement,
        create::CreateStatement,
        select::SelectStatement,
        insert::InsertStatement,
        update::UpdateStatement,
        delete::DeleteStatement,
        alter::{AlterStatement, AlterType},
        drop::DropStatement,
        create_index::CreateIndexStatement,
        drop_index::DropIndexStatement,
    },
    column::Column,
    clauses::wheres::where_type::{WhereType, WhereClause},
    constraints::constraint::Constraint,
    operators::op::Op,
    table_reference::TableReference,
    column::ColumnType,
};

#[cfg(test)]
mod tests {
    use crate::{
        error::ReefDBError,
        sql::{
            data_type::DataType,
            data_value::DataValue,
            statements::{Statement, create::CreateStatement, insert::InsertStatement},
            column::{Column, ColumnType},
            table_reference::TableReference,
        },
        result::ReefDBResult,
        InMemoryReefDB,
        transaction::IsolationLevel,
    };

    #[test]
    fn test_autocommit() -> Result<(), ReefDBError> {
        let mut db = InMemoryReefDB::create_in_memory()?;
        
        // Verify autocommit is enabled by default
        assert!(db.is_autocommit());
        assert_eq!(db.get_autocommit_isolation_level(), IsolationLevel::ReadCommitted);
        
        // Create a table
        let create_stmt = Statement::Create(CreateStatement::Table(
            "users".to_string(),
            vec![
                crate::sql::column_def::ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![],
                },
                crate::sql::column_def::ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    constraints: vec![],
                },
            ],
        ));
        db.execute_statement(create_stmt)?;
        
        // Insert data
        let insert_stmt = Statement::Insert(InsertStatement::IntoTable(
            "users".to_string(),
            vec![
                DataValue::Integer(1),
                DataValue::Text("Alice".to_string()),
            ],
        ));
        db.execute_statement(insert_stmt)?;
        
        // Verify data persists without explicit commit
        let select_stmt = Statement::Select(crate::sql::statements::select::SelectStatement::FromTable(
            TableReference { 
                name: "users".to_string(),
                alias: None,
            },
            vec![
                Column {
                    table: None,
                    name: "*".to_string(),
                    column_type: ColumnType::Wildcard,
                },
            ],
            None,
            vec![],
            vec![],
        ));
        let result = db.execute_statement(select_stmt)?;
        
        if let ReefDBResult::Select(rows) = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][1], DataValue::Text("Alice".to_string()));
        } else {
            panic!("Expected Select result");
        }
        
        Ok(())
    }

    #[test]
    fn test_autocommit_rollback() -> Result<(), ReefDBError> {
        let mut db = InMemoryReefDB::create_in_memory()?;
        
        // Create table
        let create_stmt = Statement::Create(CreateStatement::Table(
            "users".to_string(),
            vec![
                crate::sql::column_def::ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![],
                },
                crate::sql::column_def::ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    constraints: vec![],
                },
            ],
        ));
        db.execute_statement(create_stmt)?;
        
        // Try to insert invalid data (text where integer is expected)
        let invalid_stmt = Statement::Insert(InsertStatement::IntoTable(
            "users".to_string(),
            vec![
                DataValue::Text("invalid".to_string()),  // Should be Integer
                DataValue::Text("Bob".to_string()),
            ],
        ));
        let result = db.execute_statement(invalid_stmt);
        assert!(result.is_err());
        
        // Verify no data was persisted
        let select_stmt = Statement::Select(crate::sql::statements::select::SelectStatement::FromTable(
            TableReference { 
                name: "users".to_string(),
                alias: None,
            },
            vec![
                Column {
                    table: None,
                    name: "*".to_string(),
                    column_type: ColumnType::Wildcard,
                },
            ],
            None,
            vec![],
            vec![],
        ));
        let result = db.execute_statement(select_stmt)?;
        
        if let ReefDBResult::Select(rows) = result {
            assert_eq!(rows.len(), 0);
        } else {
            panic!("Expected Select result");
        }
        
        Ok(())
    }

    #[test]
    fn test_autocommit_disabled() -> Result<(), ReefDBError> {
        let mut db = InMemoryReefDB::create_in_memory()?;
        
        // Disable autocommit
        db.set_autocommit(false);
        assert!(!db.is_autocommit());
        
        // Create table
        let create_stmt = Statement::Create(CreateStatement::Table(
            "users".to_string(),
            vec![
                crate::sql::column_def::ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![],
                },
                crate::sql::column_def::ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    constraints: vec![],
                },
            ],
        ));
        db.execute_statement(create_stmt)?;
        
        // Begin transaction
        db.execute_statement(Statement::BeginTransaction)?;
        
        // Insert data
        let insert_stmt = Statement::Insert(InsertStatement::IntoTable(
            "users".to_string(),
            vec![
                DataValue::Integer(1),
                DataValue::Text("Alice".to_string()),
            ],
        ));
        db.execute_statement(insert_stmt)?;
        
        // Create select statement that we'll reuse
        let select_stmt = Statement::Select(crate::sql::statements::select::SelectStatement::FromTable(
            TableReference { 
                name: "users".to_string(),
                alias: None,
            },
            vec![
                Column {
                    table: None,
                    name: "*".to_string(),
                    column_type: ColumnType::Wildcard,
                },
            ],
            None,
            vec![],
            vec![],
        ));
        
        // Verify data is not visible before commit
        let result = db.execute_statement(select_stmt.clone())?;
        
        if let ReefDBResult::Select(rows) = result {
            assert_eq!(rows.len(), 1); // Data is visible within transaction
        } else {
            panic!("Expected Select result");
        }
        
        // Commit transaction
        db.execute_statement(Statement::Commit)?;
        
        // Verify data persists after commit
        let result = db.execute_statement(select_stmt)?;
        
        if let ReefDBResult::Select(rows) = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][1], DataValue::Text("Alice".to_string()));
        } else {
            panic!("Expected Select result");
        }
        
        Ok(())
    }
} 