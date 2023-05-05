use crate::{
    error::ToyDBError,
    indexes::fts::search::Search,
    result::ToyDBResult,
    sql::{data_value::DataValue, statements::Statement},
    storage::Storage,
    InMemoryToyDB, ToyDB,
};

#[derive(Clone)]
pub struct Transaction<S: Storage + Clone, FTS: Search + Clone> {
    toy_db: ToyDB<S, FTS>,
}

impl<S: Storage + Clone, FTS: Search + Clone> Transaction<S, FTS> {
    pub fn execute_statement(&mut self, stmt: Statement) -> Result<ToyDBResult, ToyDBError> {
        self.toy_db.execute_statement(stmt)
    }

    pub fn commit(&mut self, toy_db: &mut ToyDB<S, FTS>) {
        toy_db.tables = self.toy_db.tables.clone();
        toy_db.inverted_index = self.toy_db.inverted_index.clone();
    }

    pub fn rollback(&mut self, toy_db: &mut ToyDB<S, FTS>) {
        self.toy_db.tables = toy_db.tables.clone();
        self.toy_db.inverted_index = toy_db.inverted_index.clone();
    }
}

// Add this method to the ToyDB struct
impl<S: Storage + Clone, FTS: Search + Clone> ToyDB<S, FTS> {
    pub fn begin_transaction(&self) -> Transaction<S, FTS> {
        Transaction {
            toy_db: self.clone(),
        }
    }
}

#[test]
fn test_transactions() {
    let mut db = InMemoryToyDB::new((), ());

    // Create a table and insert a row outside of a transaction
    let (_, create_stmt) = Statement::parse("CREATE TABLE users (name TEXT, age INTEGER)").unwrap();
    db.execute_statement(create_stmt);
    let (_, insert_stmt) = Statement::parse("INSERT INTO users VALUES ('alice', 30)").unwrap();
    db.execute_statement(insert_stmt);

    // Start a transaction and insert two rows
    let mut transaction = db.begin_transaction();
    let (_, insert_stmt2) = Statement::parse("INSERT INTO users VALUES ('jane', 25)").unwrap();
    transaction
        .execute_statement(insert_stmt2)
        .unwrap();
    let (_, insert_stmt3) = Statement::parse("INSERT INTO users VALUES ('john', 27)").unwrap();
    transaction
        .execute_statement(insert_stmt3)
        .unwrap();

    let (_, select_stmt) = Statement::parse("SELECT name, age FROM users").unwrap();
    // Execute a SELECT statement before committing the transaction
    let select_result_before_commit = transaction
        .execute_statement(select_stmt)
        .unwrap();

    // Check if the SELECT result contains changes from the transaction
    assert_eq!(
        select_result_before_commit,
        ToyDBResult::Select(vec![
            (
                0,
                vec![DataValue::Text("alice".to_string()), DataValue::Integer(30)]
            ),
            (
                1,
                vec![DataValue::Text("jane".to_string()), DataValue::Integer(25)]
            ),
            (
                2,
                vec![DataValue::Text("john".to_string()), DataValue::Integer(27)]
            ),
        ])
    );

    // Commit the transaction
    transaction.commit(&mut db);

let (_, select_stmt2) = Statement::parse("SELECT name, age FROM users").unwrap();
    // Execute a SELECT statement after committing the transaction
    let select_result_after_commit = db
        .execute_statement(select_stmt2)
        .unwrap();

    // Check if the SELECT result contains changes from the committed transaction
    assert_eq!(select_result_after_commit, select_result_before_commit);

    // Start a new transaction and insert a new row
    let mut transaction2 = db.begin_transaction();
    let (_, insert_stmt4) = Statement::parse("INSERT INTO users VALUES ('emma', 18)").unwrap();
    transaction2
        .execute_statement(insert_stmt4)
        .unwrap();
    let (_, select_stmt3) = Statement::parse("SELECT name, age FROM users").unwrap();
    let select_result_before_rollback = transaction2
        .execute_statement(select_stmt3)
        .unwrap();

    assert_eq!(
        select_result_before_rollback,
        ToyDBResult::Select(vec![
            (
                0,
                vec![DataValue::Text("alice".to_string()), DataValue::Integer(30)]
            ),
            (
                1,
                vec![DataValue::Text("jane".to_string()), DataValue::Integer(25)]
            ),
            (
                2,
                vec![DataValue::Text("john".to_string()), DataValue::Integer(27)]
            ),
            (
                3,
                vec![DataValue::Text("emma".to_string()), DataValue::Integer(18)]
            ),
        ])
    );

    // Rollback the transaction
    transaction2.rollback(&mut db);
    let (_, select_stmt4) = Statement::parse("SELECT name, age FROM users").unwrap();
    // Check if the rollback has discarded the changes made in the transaction
    let select_result_after_rollback = db
        .execute_statement(select_stmt4)
        .unwrap();
    assert_eq!(select_result_after_rollback, select_result_after_commit);
}
