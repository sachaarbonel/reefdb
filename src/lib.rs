mod storage;
use nom::IResult;
mod sql;

use sql::{
    clauses::join_clause::{JoinClause, JoinType},
    data_value::DataValue,
    statements::{
        create::CreateStatement, delete::DeleteStatement, insert::InsertStatement,
        select::SelectStatement, update::UpdateStatement, Statement,
    },
};

use storage::Storage;

struct ToyDB<S: Storage> {
    tables: S,
}

#[derive(PartialEq, Debug)]
pub enum ToyDBResult {
    Select(Vec<Vec<DataValue>>),
    Insert(usize),
    CreateTable,
    Update(usize),
    Delete(usize),
}

#[derive(Debug, PartialEq)]
pub enum ToyDBError {
    TableNotFound(String),
    ColumnNotFound(String),
    ParseError(String),
}

impl<S: Storage> ToyDB<S> {
    pub fn new(args: S::NewArgs) -> Self {
        ToyDB {
            tables: S::new(args),
        }
    }

    fn execute_statement(&mut self, stmt: Statement) -> Result<ToyDBResult, ToyDBError> {
        match stmt {
            Statement::Delete(DeleteStatement::FromTable(table_name, where_clause)) => {
                if let Some((schema, table)) = self.tables.get_table(&table_name) {
                    let mut deleted_rows = 0;
                    for i in (0..table.len()).rev() {
                        if let Some(where_col) = &where_clause {
                            if let Some(col_index) = schema
                                .iter()
                                .position(|column_def| &column_def.name == &where_col.col_name)
                            {
                                if table[i][col_index] == where_col.value {
                                    table.remove(i);
                                    deleted_rows += 1;
                                }
                            }
                        } else {
                            table.remove(i);
                            deleted_rows += 1;
                        }
                    }
                    Ok(ToyDBResult::Delete(deleted_rows))
                } else {
                    Err(ToyDBError::TableNotFound(table_name))
                }
            }
            Statement::Create(CreateStatement::Table(table_name, cols)) => {
                self.tables
                    .insert_table(table_name, cols.clone(), Vec::new());
                Ok(ToyDBResult::CreateTable)
            }
            Statement::Insert(InsertStatement::IntoTable(table_name, values)) => {
                self.tables.push_value(&table_name, values);
                Ok(ToyDBResult::Insert(1))
                // if let Some((columns, table)) = self.tables.get_table(&table_name) {
                //     table.push(values);
                //     self.tables.save();
                //     Ok(ToyDBResult::Insert(table.len()))
                // } else {
                //     eprintln!("Table not found: {}", table_name);
                //     Ok(ToyDBResult::Insert(0))
                // }
            }
            Statement::Select(SelectStatement::FromTable(table_name, columns, where_clause, _)) => {
                // println!("where_clause: {:#?}", where_clause);
                if let Some((schema, table)) = self.tables.get_table(&table_name) {
                    let column_indexes: Vec<_> = columns
                        .iter()
                        .map(|column_name| {
                            schema
                                .iter()
                                .position(|column_def| &column_def.name == &column_name.name)
                                .unwrap()
                        })
                        .collect();
                    // println!("column_indexes: {:?}", column_indexes);

                    let mut result = Vec::new();

                    for row in table {
                        let selected_columns: Vec<_> = row
                            .iter()
                            .enumerate()
                            .filter_map(|(i, value)| {
                                if column_indexes.contains(&i) {
                                    Some(value.clone())
                                } else {
                                    None
                                }
                            })
                            .collect();
                        // println!("row: {:?}", row);
                        if let Some(where_col) = &where_clause {
                            // println!("where_col: {:?}", where_col);
                            if let Some(col_index) = schema
                                .iter()
                                .position(|column_def| &column_def.name == &where_col.col_name)
                            {
                                if row[col_index] == where_col.value {
                                    result.push(selected_columns);
                                }
                            } else {
                                eprintln!("Column not found: {}", where_col.col_name);
                            }
                        } else {
                            result.push(selected_columns);
                        }
                    }

                    Ok(ToyDBResult::Select(result))
                } else {
                    Err(ToyDBError::TableNotFound(table_name))
                }
            }
            Statement::Update(UpdateStatement::UpdateTable(table_name, updates, where_clause)) => {
                //destructuring where_clause into Option<(std::string::String, DataValue)>
                let where_col = where_clause.map(|where_col| (where_col.col_name, where_col.value));
                let affected_rows = self.tables.update_table(&table_name, updates, where_col);
                Ok(ToyDBResult::Update(affected_rows))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::storage::{disk::OnDiskStorage, memory::InMemoryStorage};

    use super::*;

    #[test]
    fn test_database_on_disk() {
        let kv_path = "kv.db";

        let mut db: ToyDB<OnDiskStorage> = ToyDB::new(kv_path.to_string());

        let statements = vec![
            "CREATE TABLE users (name TEXT, age INTEGER)",
            "INSERT INTO users VALUES ('alice', 30)",
            "INSERT INTO users VALUES ('bob', 28)",
            "UPDATE users SET age = 31 WHERE name = 'bob'",
            "SELECT name, age FROM users",
            "SELECT name FROM users",
            "SELECT name FROM users WHERE age = 30",
        ];
        let mut results = Vec::new();
        for statement in statements {
            match Statement::parse(statement) {
                Ok((_, stmt)) => {
                    results.push(db.execute_statement(stmt));
                }
                Err(err) => eprintln!("Failed to parse statement: {}", err),
            }
        }

        let expected_results = vec![
            Ok(ToyDBResult::CreateTable),
            Ok(ToyDBResult::Insert(1)),
            Ok(ToyDBResult::Insert(1)),
            Ok(ToyDBResult::Update(2)),
            Ok(ToyDBResult::Select(vec![
                vec![DataValue::Text("alice".to_string()), DataValue::Integer(30)],
                vec![DataValue::Text("bob".to_string()), DataValue::Integer(31)],
            ])),
            Ok(ToyDBResult::Select(vec![
                vec![DataValue::Text("alice".to_string())],
                vec![DataValue::Text("bob".to_string())],
            ])),
            Ok(ToyDBResult::Select(vec![vec![DataValue::Text(
                "alice".to_string(),
            )]])),
        ];
        assert_eq!(results, expected_results);

        // Check if the users table has been created
        assert!(db.tables.table_exists(&"users".to_string()));

        // Get the users table and check the number of rows
        let (_, users) = db.tables.get_table(&"users".to_string()).unwrap();
        // println!("{:?}", users);
        assert_eq!(users.len(), 2);

        // Check the contents of the users table
        assert_eq!(
            users[0],
            vec![DataValue::Text("alice".to_string()), DataValue::Integer(30)]
        );
        assert_eq!(
            users[1],
            vec![DataValue::Text("bob".to_string()), DataValue::Integer(31)]
        );

        // Cleanup
        fs::remove_file(kv_path).unwrap();
    }

    //test delete
    #[test]
    fn test_delete() {
        let mut db: ToyDB<InMemoryStorage> = ToyDB::new(());
        let statements = vec![
            "CREATE TABLE users (name TEXT, age INTEGER)",
            "INSERT INTO users VALUES ('alice', 30)",
            "INSERT INTO users VALUES ('bob', 28)",
            "DELETE FROM users WHERE name = 'bob'",
            "SELECT name, age FROM users",
            "SELECT name FROM users",
            "SELECT name FROM users WHERE age = 30",
        ];
        let mut results = Vec::new();
        for statement in statements {
            match Statement::parse(statement) {
                Ok((_, stmt)) => {
                    results.push(db.execute_statement(stmt));
                }
                Err(err) => eprintln!("Failed to parse statement: {}", err),
            }
        }
        let expected_results = vec![
            Ok(ToyDBResult::CreateTable),
            Ok(ToyDBResult::Insert(1)),
            Ok(ToyDBResult::Insert(1)),
            Ok(ToyDBResult::Delete(1)),
            Ok(ToyDBResult::Select(vec![vec![
                DataValue::Text("alice".to_string()),
                DataValue::Integer(30),
            ]])),
            Ok(ToyDBResult::Select(vec![vec![DataValue::Text(
                "alice".to_string(),
            )]])),
            Ok(ToyDBResult::Select(vec![vec![DataValue::Text(
                "alice".to_string(),
            )]])),
        ];
        assert_eq!(results, expected_results);
    }

    #[test]
    fn test_database() {
        let mut db: ToyDB<InMemoryStorage> = ToyDB::new(());

        let statements = vec![
            "CREATE TABLE users (name TEXT, age INTEGER)",
            "INSERT INTO users VALUES ('alice', 30)",
            "INSERT INTO users VALUES ('bob', 28)",
            "UPDATE users SET age = 31 WHERE name = 'bob'",
            "SELECT name, age FROM users",
            "SELECT name FROM users",
            "SELECT name FROM users WHERE age = 30",
        ];
        let mut results = Vec::new();
        for statement in statements {
            match Statement::parse(statement) {
                Ok((_, stmt)) => {
                    results.push(db.execute_statement(stmt));
                }
                Err(err) => eprintln!("Failed to parse statement: {}", err),
            }
        }

        let expected_results = vec![
            Ok(ToyDBResult::CreateTable),
            Ok(ToyDBResult::Insert(1)),
            Ok(ToyDBResult::Insert(1)),
            Ok(ToyDBResult::Update(2)), // Updated 1 row
            Ok(ToyDBResult::Select(vec![
                vec![DataValue::Text("alice".to_string()), DataValue::Integer(30)],
                vec![DataValue::Text("bob".to_string()), DataValue::Integer(31)],
            ])),
            Ok(ToyDBResult::Select(vec![
                vec![DataValue::Text("alice".to_string())],
                vec![DataValue::Text("bob".to_string())],
            ])),
            Ok(ToyDBResult::Select(vec![vec![DataValue::Text(
                "alice".to_string(),
            )]])),
        ];
        assert_eq!(results, expected_results);

        // Check if the users table has been created
        assert!(db.tables.table_exists(&"users".to_string()));

        // Get the users table and check the number of rows
        let (_, users) = db.tables.get_table(&"users".to_string()).unwrap();
        // println!("{:?}", users);
        assert_eq!(users.len(), 2);

        // Check the contents of the users table
        assert_eq!(
            users[0],
            vec![DataValue::Text("alice".to_string()), DataValue::Integer(30)]
        );
        assert_eq!(
            users[1],
            vec![DataValue::Text("bob".to_string()), DataValue::Integer(31)]
        );
    }
}
