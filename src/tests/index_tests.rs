use crate::{
    error::ReefDBError,
    result::ReefDBResult,
    InMemoryReefDB,
    sql::{
        statements::{
            create_index::{CreateIndexStatement, IndexType},
            Statement,
        },
        column_def::ColumnDef,
        data_type::DataType,
        data_value::DataValue,
        statements::{create::CreateStatement, insert::InsertStatement},
        constraints::constraint::Constraint,
    },
};

#[test]
fn test_index_operations() -> Result<(), ReefDBError> {
    let mut db = InMemoryReefDB::create_in_memory()?;

    // Test 1: Create a table
    let columns = vec![
        ColumnDef::new("id", DataType::Integer, vec![Constraint::PrimaryKey]),
        ColumnDef::new("name", DataType::Text, vec![]),
        ColumnDef::new("age", DataType::Integer, vec![]),
    ];
    let result = db.execute_statement(Statement::Create(CreateStatement::Table("users".to_string(), columns)))?;
    assert_eq!(result, ReefDBResult::CreateTable);

    // Test 2: Create an index on the age column
    let create_index_stmt = CreateIndexStatement {
        table_name: "users".to_string(),
        column_name: "age".to_string(),
        index_type: IndexType::BTree,
    };
    let result = db.execute_statement(Statement::CreateIndex(create_index_stmt))?;
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
    db.execute_statement(Statement::Insert(InsertStatement::IntoTable("users".to_string(), values1)))?;
    db.execute_statement(Statement::Insert(InsertStatement::IntoTable("users".to_string(), values2)))?;

    Ok(())
} 