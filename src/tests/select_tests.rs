use crate::{InMemoryReefDB, error::ReefDBError, result::ReefDBResult};
use crate::sql::{
    clauses::wheres::where_type::{WhereType, WhereClause},
    data_value::DataValue,
    operators::op::Op,
    data_type::DataType,
};

type Result<T> = std::result::Result<T, ReefDBError>;

#[test]
fn test_select_with_where() -> Result<()> {
    let mut db = InMemoryReefDB::create_in_memory()?;

    // Create and populate table
    db.query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")?;
    db.query("INSERT INTO users VALUES (1, 'Alice')")?;
    db.query("INSERT INTO users VALUES (2, 'Bob')")?;

    // Test SELECT with WHERE clause
    let where_clause = WhereType::Regular(WhereClause::new(
        "id".to_string(),
        Op::Equal,
        DataValue::Integer(1),
        None
    ));

    if let ReefDBResult::Select(results) = db.query("SELECT * FROM users WHERE id = 1")? {
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], DataValue::Integer(1));
        assert_eq!(results[0][1], DataValue::Text("Alice".to_string()));

        // Verify column information
        assert_eq!(results.columns.len(), 2);
        assert_eq!(results.columns[0].name, "id");
        assert_eq!(results.columns[0].data_type, DataType::Integer);
        assert_eq!(results.columns[0].table, Some("users".to_string()));
        assert_eq!(results.columns[1].name, "name");
        assert_eq!(results.columns[1].data_type, DataType::Text);
        assert_eq!(results.columns[1].table, Some("users".to_string()));
    } else {
        panic!("Expected Select result");
    }

    Ok(())
}

#[test]
fn test_select_all() -> Result<()> {
    let mut db = InMemoryReefDB::create_in_memory()?;

    // Create and populate table
    db.query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")?;
    db.query("INSERT INTO users VALUES (1, 'Alice')")?;
    db.query("INSERT INTO users VALUES (2, 'Bob')")?;

    // Test SELECT *
    if let ReefDBResult::Select(results) = db.query("SELECT * FROM users")? {
        assert_eq!(results.len(), 2);
        assert_eq!(results[0][0], DataValue::Integer(1));
        assert_eq!(results[0][1], DataValue::Text("Alice".to_string()));
        assert_eq!(results[1][0], DataValue::Integer(2));
        assert_eq!(results[1][1], DataValue::Text("Bob".to_string()));

        // Verify column information
        assert_eq!(results.columns.len(), 2);
        assert_eq!(results.columns[0].name, "id");
        assert_eq!(results.columns[0].data_type, DataType::Integer);
        assert_eq!(results.columns[0].table, Some("users".to_string()));
        assert_eq!(results.columns[1].name, "name");
        assert_eq!(results.columns[1].data_type, DataType::Text);
        assert_eq!(results.columns[1].table, Some("users".to_string()));
    } else {
        panic!("Expected Select result");
    }

    Ok(())
} 