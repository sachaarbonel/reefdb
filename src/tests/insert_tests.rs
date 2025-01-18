use super::*;
use crate::error::ReefDBError;
use crate::result::ReefDBResult;
use crate::sql::table_reference::TableReference;
use crate::sql::column::ColumnType;
use crate::InMemoryReefDB;

#[test]
fn test_insert_statement() -> Result<(), ReefDBError> {
    let mut db = InMemoryReefDB::create_in_memory()?;
    
    // Test 1: Create a table with different column types and constraints
    let columns = vec![
        ColumnDef::new("id", DataType::Integer, vec![Constraint::PrimaryKey]),
        ColumnDef::new("name", DataType::Text, vec![Constraint::NotNull]),
        ColumnDef::new("age", DataType::Integer, vec![]),
        ColumnDef::new("email", DataType::Text, vec![Constraint::Unique]),
    ];
    db.execute_statement(Statement::Create(CreateStatement::Table("users".to_string(), columns)))?;

    // Test 2: Basic insert with all columns
    let values = vec![
        DataValue::Integer(1),
        DataValue::Text("Alice".to_string()),
        DataValue::Integer(25),
        DataValue::Text("alice@example.com".to_string()),
    ];
    let result = db.execute_statement(Statement::Insert(InsertStatement::IntoTable("users".to_string(), values)))?;
    assert_eq!(result, ReefDBResult::Insert(1)); // Should return rowid 1

    // Test 3: Verify the inserted row
    let select_stmt = SelectStatement::FromTable(
        TableReference {
            name: "users".to_string(),
            alias: None,
        },
        vec![Column { name: "*".to_string(), table: None ,column_type: ColumnType::Wildcard}],
        None,
        vec![],
    );
    let result = db.execute_statement(Statement::Select(select_stmt))?;
    if let ReefDBResult::Select(rows) = result {
        assert_eq!(rows.len(), 1);
        let values = &rows[0];
        assert_eq!(values[0], DataValue::Integer(1));
        assert_eq!(values[1], DataValue::Text("Alice".to_string()));
        assert_eq!(values[2], DataValue::Integer(25));
        assert_eq!(values[3], DataValue::Text("alice@example.com".to_string()));
    } else {
        panic!("Expected Select result");
    }

    // Test 4: Insert with wrong number of values (should fail)
    let values = vec![
        DataValue::Integer(2),
        DataValue::Text("Bob".to_string()),
        DataValue::Integer(30),
    ];
    let result = db.execute_statement(Statement::Insert(InsertStatement::IntoTable("users".to_string(), values)));
    assert!(matches!(result, Err(ReefDBError::Other(_))));

    // Test 5: Insert with type mismatch (should fail)
    let values = vec![
        DataValue::Text("not an integer".to_string()),  // Wrong type for id
        DataValue::Text("Charlie".to_string()),
        DataValue::Integer(35),
        DataValue::Text("charlie@example.com".to_string()),
    ];
    let result = db.execute_statement(Statement::Insert(InsertStatement::IntoTable("users".to_string(), values)));
    assert!(matches!(result, Err(ReefDBError::Other(_))));

    // Test 6: Insert into non-existent table (should fail)
    let values = vec![DataValue::Integer(1)];
    let result = db.execute_statement(Statement::Insert(InsertStatement::IntoTable("nonexistent".to_string(), values)));
    assert!(matches!(result, Err(ReefDBError::TableNotFound(_))));

    // Test 7: Multiple successful inserts
    let values2 = vec![
        DataValue::Integer(2),
        DataValue::Text("Bob".to_string()),
        DataValue::Integer(30),
        DataValue::Text("bob@example.com".to_string()),
    ];
    let values3 = vec![
        DataValue::Integer(3),
        DataValue::Text("Charlie".to_string()),
        DataValue::Integer(35),
        DataValue::Text("charlie@example.com".to_string()),
    ];

    let result = db.execute_statement(Statement::Insert(InsertStatement::IntoTable("users".to_string(), values2)))?;
    assert_eq!(result, ReefDBResult::Insert(2)); // Should return rowid 2
    let result = db.execute_statement(Statement::Insert(InsertStatement::IntoTable("users".to_string(), values3)))?;
    assert_eq!(result, ReefDBResult::Insert(3)); // Should return rowid 3

    // Test 8: Verify all inserted rows
    let select_stmt = SelectStatement::FromTable(
        TableReference {
            name: "users".to_string(),
            alias: None,
        },
        vec![Column { name: "*".to_string(), table: None, column_type: ColumnType::Wildcard }],
        None,
        vec![],
    );
    let result = db.execute_statement(Statement::Select(select_stmt))?;
    if let ReefDBResult::Select(rows) = result {
        assert_eq!(rows.len(), 3);
        let values: Vec<_> = rows.rows.into_iter().map(|(_, v)| v).collect();
        
        // First row (Alice)
        assert_eq!(values[0][0], DataValue::Integer(1));
        assert_eq!(values[0][1], DataValue::Text("Alice".to_string()));
        assert_eq!(values[0][2], DataValue::Integer(25));
        assert_eq!(values[0][3], DataValue::Text("alice@example.com".to_string()));
        
        // Second row (Bob)
        assert_eq!(values[1][0], DataValue::Integer(2));
        assert_eq!(values[1][1], DataValue::Text("Bob".to_string()));
        assert_eq!(values[1][2], DataValue::Integer(30));
        assert_eq!(values[1][3], DataValue::Text("bob@example.com".to_string()));
        
        // Third row (Charlie)
        assert_eq!(values[2][0], DataValue::Integer(3));
        assert_eq!(values[2][1], DataValue::Text("Charlie".to_string()));
        assert_eq!(values[2][2], DataValue::Integer(35));
        assert_eq!(values[2][3], DataValue::Text("charlie@example.com".to_string()));
    } else {
        panic!("Expected Select result");
    }

    Ok(())
} 