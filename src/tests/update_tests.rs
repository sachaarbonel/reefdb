use crate::{InMemoryReefDB, error::ReefDBError, result::ReefDBResult};
use crate::sql::{
    clauses::wheres::where_type::{WhereType, WhereClause},
    data_value::DataValue,
    operators::op::Op,
    statements::{Statement, update::UpdateStatement},
};

type Result<T> = std::result::Result<T, ReefDBError>;

#[test]
fn test_update_basic() -> Result<()> {
    let mut db = InMemoryReefDB::create_in_memory()?;

    // Create and populate table
    db.query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")?;
    db.query("INSERT INTO users VALUES (1, 'Alice')")?;
    db.query("INSERT INTO users VALUES (2, 'Bob')")?;

    // Test UPDATE
    let where_clause = WhereType::Regular(WhereClause::new(
        "id".to_string(),
        Op::Equal,
        DataValue::Integer(1),
        None,
    ));

    db.query("UPDATE users SET name = 'Alice Updated' WHERE id = 1")?;

    // Verify update
    if let ReefDBResult::Select(rows) = db.query("SELECT * FROM users WHERE id = 1")? {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], DataValue::Integer(1));
        assert_eq!(rows[0][1], DataValue::Text("Alice Updated".to_string()));
    } else {
        panic!("Expected Select result");
    }

    Ok(())
}

#[test]
fn test_update_multiple_rows() -> Result<()> {
    let mut db = InMemoryReefDB::create_in_memory()?;

    // Create and populate table
    db.query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, status TEXT)")?;
    db.query("INSERT INTO users VALUES (1, 'Alice', 'active')")?;
    db.query("INSERT INTO users VALUES (2, 'Bob', 'active')")?;
    db.query("INSERT INTO users VALUES (3, 'Charlie', 'active')")?;

    // Test UPDATE multiple rows
    let where_clause = WhereType::Regular(WhereClause::new(
        "status".to_string(),
        Op::Equal,
        DataValue::Text("active".to_string()),
        None,
    ));

    db.query("UPDATE users SET status = 'inactive' WHERE status = 'active'")?;

    // Verify all rows were updated
    if let ReefDBResult::Select(rows) = db.query("SELECT * FROM users")? {
        assert_eq!(rows.len(), 3);
        for row in rows.rows {
            assert_eq!(row.1[2], DataValue::Text("inactive".to_string()));
        }
    } else {
        panic!("Expected Select result");
    }

    Ok(())
}

#[test]
fn parse_update_with_where_test() {
    let res = Statement::parse("UPDATE users SET name = 'John' WHERE id = 1");
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
            Statement::Update(UpdateStatement::UpdateTable(
                "users".to_string(),
                vec![("name".to_string(), DataValue::Text("John".to_string()))],
                Some(where_clause),
            ))
        ))
    );
}

#[test]
fn parse_update_multiple_columns_test() {
    let res = Statement::parse(
        "UPDATE users SET name = 'John', age = 30, status = 'active' WHERE status = 'active'",
    );
    let where_clause = WhereType::Regular(WhereClause::new(
        "status".to_string(),
        Op::Equal,
        DataValue::Text("active".to_string()),
        None,
    ));
    assert_eq!(
        res,
        Ok((
            "",
            Statement::Update(UpdateStatement::UpdateTable(
                "users".to_string(),
                vec![
                    ("name".to_string(), DataValue::Text("John".to_string())),
                    ("age".to_string(), DataValue::Integer(30)),
                    ("status".to_string(), DataValue::Text("active".to_string())),
                ],
                Some(where_clause),
            ))
        ))
    );
} 