use crate::{InMemoryReefDB, error::ReefDBError, result::ReefDBResult};
use crate::sql::{
    clauses::wheres::where_type::{WhereType, WhereClause},
    data_value::DataValue,
    operators::op::Op,
    statements::{Statement, delete::DeleteStatement},
};

type Result<T> = std::result::Result<T, ReefDBError>;

#[test]
fn test_delete_basic() -> Result<()> {
    let mut db = InMemoryReefDB::create_in_memory()?;

    // Create and populate table
    db.query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")?;
    db.query("INSERT INTO users VALUES (1, 'Alice')")?;
    db.query("INSERT INTO users VALUES (2, 'Bob')")?;

    // Test DELETE
    let where_clause = WhereType::Regular(WhereClause::new(
        "id".to_string(),
        Op::Equal,
        DataValue::Integer(1),
        None,
    ));

    db.query("DELETE FROM users WHERE id = 1")?;

    // Verify deletion
    if let ReefDBResult::Select(rows) = db.query("SELECT * FROM users")? {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], DataValue::Integer(2));
        assert_eq!(rows[0][1], DataValue::Text("Bob".to_string()));
    } else {
        panic!("Expected Select result");
    }

    Ok(())
}

#[test]
fn test_delete_multiple_rows() -> Result<()> {
    let mut db = InMemoryReefDB::create_in_memory()?;

    // Create and populate table
    db.query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, status TEXT)")?;
    db.query("INSERT INTO users VALUES (1, 'Alice', 'inactive')")?;
    db.query("INSERT INTO users VALUES (2, 'Bob', 'inactive')")?;
    db.query("INSERT INTO users VALUES (3, 'Charlie', 'active')")?;

    // Test DELETE multiple rows
    let where_clause = WhereType::Regular(WhereClause::new(
        "status".to_string(),
        Op::Equal,
        DataValue::Text("inactive".to_string()),
        None,
    ));

    db.query("DELETE FROM users WHERE status = 'inactive'")?;

    // Verify deletion
    if let ReefDBResult::Select(rows) = db.query("SELECT * FROM users")? {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], DataValue::Integer(3));
        assert_eq!(rows[0][1], DataValue::Text("Charlie".to_string()));
        assert_eq!(rows[0][2], DataValue::Text("active".to_string()));
    } else {
        panic!("Expected Select result");
    }

    Ok(())
}

#[test]
fn parse_delete_with_where_test() {
    let res = Statement::parse("DELETE FROM users WHERE id = 1");
    let where_clause = WhereType::Regular(WhereClause::new(
        "id".to_string(),
        Op::Equal,
        DataValue::Integer(1),
        None,
    ));
    assert_eq!(
        res,
        Ok((
            "",
            Statement::Delete(DeleteStatement::FromTable(
                "users".to_string(),
                Some(where_clause),
            ))
        ))
    );
}

#[test]
fn parse_delete_with_where_text_test() {
    let res = Statement::parse("DELETE FROM users WHERE status = 'inactive'");
    let where_clause = WhereType::Regular(WhereClause::new(
        "status".to_string(),
        Op::Equal,
        DataValue::Text("inactive".to_string()),
        None,
    ));
    assert_eq!(
        res,
        Ok((
            "",
            Statement::Delete(DeleteStatement::FromTable(
                "users".to_string(),
                Some(where_clause),
            ))
        ))
    );
} 