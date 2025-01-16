use crate::{
    error::ReefDBError, result::ReefDBResult, sql::{
        clauses::{
            full_text_search::{ Language, QueryType, TSQuery}, join_clause::TableReference, wheres::where_type::WhereType, FTSClause
        },
        column::Column,
        column_def::ColumnDef,
        constraints::{
            constraint::Constraint,
            foreignkey::ForeignKeyConstraint,
        },
        data_type::DataType,
        data_value::DataValue,
        statements::{create::CreateStatement, insert::InsertStatement, select::SelectStatement, Statement},
    }, transaction::IsolationLevel, InMemoryReefDB
};

#[test]
fn test_create_statement() -> Result<(), ReefDBError> {
    let mut db = InMemoryReefDB::create_in_memory()?;
    
    // Begin a transaction
    let transaction_id = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;
    
    // Test 1: Create a basic table with different data types
    let columns = vec![
        ColumnDef::new("id", DataType::Integer, vec![]),
        ColumnDef::new("name", DataType::Text, vec![]),
        ColumnDef::new("active", DataType::Integer, vec![]),  // Used as boolean
    ];
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::Create(CreateStatement::Table("users".to_string(), columns)))?;
    assert_eq!(result, ReefDBResult::CreateTable);

    // Test 2: Verify table exists and has correct schema
    let select_stmt = SelectStatement::FromTable(
        TableReference {
            name: "users".to_string(),
            alias: None,
        },
        vec![Column { name: "*".to_string(), table: None }],
        None,
        vec![],
    );
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::Select(select_stmt));
    assert!(result.is_ok()); // Table should exist and be queryable

    // Test 3: Create table with all constraint types
    let columns = vec![
        ColumnDef::new("id", DataType::Integer, vec![Constraint::PrimaryKey]),
        ColumnDef::new("username", DataType::Text, vec![Constraint::NotNull, Constraint::Unique]),
        ColumnDef::new("email", DataType::Text, vec![Constraint::Unique]),
        ColumnDef::new("age", DataType::Integer, vec![]),
        ColumnDef::new("department_id", DataType::Integer, vec![
            Constraint::ForeignKey(ForeignKeyConstraint {
                table_name: "departments".to_string(),
                column_name: "id".to_string(),
            })
        ]),
    ];
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::Create(CreateStatement::Table("employees".to_string(), columns)))?;
    assert_eq!(result, ReefDBResult::CreateTable);

    // Test 4: Create table with full-text search column
    let columns = vec![
        ColumnDef::new("id", DataType::Integer, vec![Constraint::PrimaryKey]),
        ColumnDef::new("title", DataType::Text, vec![]),
        ColumnDef::new("content", DataType::TSVector, vec![]),  // Full-text search column
    ];
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::Create(CreateStatement::Table("articles".to_string(), columns)))?;
    assert_eq!(result, ReefDBResult::CreateTable);

    // Test 5: Attempt to create table that already exists (should fail)
    let columns = vec![
        ColumnDef::new("id", DataType::Integer, vec![]),
        ColumnDef::new("name", DataType::Text, vec![]),
    ];
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::Create(CreateStatement::Table("users".to_string(), columns)));
    assert!(matches!(result, Err(ReefDBError::Other(_))));

    // Test 6: Create table with empty column list (should fail)
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::Create(CreateStatement::Table("empty".to_string(), vec![])));
    assert!(matches!(result, Err(ReefDBError::Other(_))));

    // Test 7: Insert data to verify constraints
    // Test PRIMARY KEY constraint
    let values = vec![
        DataValue::Integer(1),
        DataValue::Text("john_doe".to_string()),
        DataValue::Text("john@example.com".to_string()),
        DataValue::Integer(30),
        DataValue::Integer(1),
    ];
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::Insert(InsertStatement::IntoTable("employees".to_string(), values.clone())))?;
    assert_eq!(result, ReefDBResult::Insert(1));

    // Test UNIQUE constraint (should fail with duplicate username)
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::Insert(InsertStatement::IntoTable("employees".to_string(), values)));
    assert!(matches!(result, Err(ReefDBError::Other(_))));

    // Test NOT NULL constraint (should fail)
    let values = vec![
        DataValue::Integer(2),
        DataValue::Text("".to_string()),  // Empty string for NOT NULL column
        DataValue::Text("jane@example.com".to_string()),
        DataValue::Integer(25),
        DataValue::Integer(1),
    ];
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::Insert(InsertStatement::IntoTable("employees".to_string(), values)));
    assert!(matches!(result, Err(ReefDBError::Other(_))));

    // Test 8: Verify FTS functionality
    let values = vec![
        DataValue::Integer(1),
        DataValue::Text("Rust Programming".to_string()),
        DataValue::Text("Learn Rust programming language basics".to_string()),
    ];
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::Insert(InsertStatement::IntoTable("articles".to_string(), values)))?;
    assert_eq!(result, ReefDBResult::Insert(1));

    // Test FTS search
    let column = Column { name: "content".to_string(), table: None };
    let query = TSQuery::new("Rust".to_string())
        .with_type(QueryType::Plain)
        .with_language(Language::English);

    let where_clause = WhereType::FTS(FTSClause::new(column, query.text)
        .with_language(Language::English)
        .with_query_type(QueryType::Plain));

    let select_stmt = SelectStatement::FromTable(
        TableReference {
            name: "articles".to_string(),
            alias: None,
        },
        vec![Column { name: "*".to_string(), table: None }],
        Some(where_clause),
        vec![],
    );
    let result = db.transaction_manager.as_mut().unwrap().execute_statement(transaction_id, Statement::Select(select_stmt))?;
    if let ReefDBResult::Select(rows) = result {
        assert_eq!(rows.len(), 1); // Should find one matching article
    } else {
        panic!("Expected Select result");
    }

    // Commit the transaction
    db.transaction_manager.as_mut().unwrap().commit_transaction(transaction_id)?;

    Ok(())
} 