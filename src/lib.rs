mod storage;

use indexes::fts::{
    default::{DefaultSearchIdx, OnDiskSearchIdx},
    search::Search,
};
use nom::IResult;
mod indexes;
mod sql;

use sql::{
    clauses::wheres::where_type::WhereType,
    data_type::DataType,
    data_value::DataValue,
    statements::{
        create::CreateStatement, delete::DeleteStatement, insert::InsertStatement,
        select::SelectStatement, update::UpdateStatement, Statement,
    },
};

use storage::{disk::OnDiskStorage, memory::InMemoryStorage, Storage};

pub type InMemoryToyDB = ToyDB<InMemoryStorage, DefaultSearchIdx>;

pub type OnDiskToyDB = ToyDB<OnDiskStorage, OnDiskSearchIdx>;

pub struct ToyDB<S: Storage, FTS: Search> {
    tables: S,
    inverted_index: FTS,
}

#[derive(PartialEq, Debug)]
pub enum ToyDBResult {
    Select(Vec<(usize, Vec<DataValue>)>),
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

impl<S: Storage, FTS: Search> ToyDB<S, FTS> {
    pub fn new(args: S::NewArgs, args2: FTS::NewArgs) -> Self {
        ToyDB {
            tables: S::new(args),
            inverted_index: FTS::new(args2),
        }
    }

    fn execute_statement(&mut self, stmt: Statement) -> Result<ToyDBResult, ToyDBError> {
        match stmt {
            Statement::Delete(DeleteStatement::FromTable(table_name, where_type)) => {
                if let Some((schema, table)) = self.tables.get_table(&table_name) {
                    let mut deleted_rows = 0;
                    for i in (0..table.len()).rev() {
                        if let Some(where_type) = &where_type {
                            match where_type {
                                WhereType::Regular(where_clause) => {
                                    if let Some(col_index) = schema.iter().position(|column_def| {
                                        &column_def.name == &where_clause.col_name
                                    }) {
                                        if table[i][col_index] == where_clause.value {
                                            table.remove(i);
                                            deleted_rows += 1;
                                        }
                                    }
                                }
                                WhereType::FTS(_) => unimplemented!(),
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
                    .insert_table(table_name.clone(), cols.clone(), Vec::new());

                // Add columns with DataType::FTSText to the InvertedIndex
                for column_def in cols.iter() {
                    if column_def.data_type == DataType::FTSText {
                        self.inverted_index
                            .add_column(&table_name, &column_def.name);
                    }
                }

                Ok(ToyDBResult::CreateTable)
            }
            Statement::Insert(InsertStatement::IntoTable(table_name, values)) => {
                let row_id = self.tables.push_value(&table_name, values.clone());
                if let Some((schema, _)) = self.tables.get_table(&table_name) {
                    for (i, value) in values.iter().enumerate() {
                        if schema[i].data_type == DataType::FTSText {
                            if let DataValue::Text(ref text) = value {
                                self.inverted_index.add_document(
                                    &table_name,
                                    &schema[i].name,
                                    row_id,
                                    text,
                                );
                            }
                        }
                    }
                }
                Ok(ToyDBResult::Insert(1))
            }
            Statement::Select(SelectStatement::FromTable(table_name, columns, where_type, _)) => {
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

                    let mut result = Vec::new();

                    if let Some(where_type) = where_type {
                        match where_type {
                            WhereType::FTS(fts_where) => {
                                let row_ids = self.inverted_index.search(
                                    &table_name,
                                    &fts_where.col.name,
                                    &fts_where.query,
                                );
                                for (rowid, row) in table.iter().enumerate() {
                                    if row_ids.contains(&rowid) {
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
                                        result.push((rowid, selected_columns));
                                    }
                                }
                            }
                            WhereType::Regular(where_clause) => {
                                for (rowid, row) in table.iter().enumerate() {
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
                                    if let Some(col_index) = schema.iter().position(|column_def| {
                                        &column_def.name == &where_clause.col_name
                                    }) {
                                        if row[col_index] == where_clause.value {
                                            result.push((rowid, selected_columns));
                                        }
                                    } else {
                                        eprintln!("Column not found: {}", where_clause.col_name);
                                    }
                                }
                            }
                        }
                    } else {
                        for (rowid, row) in table.iter().enumerate() {
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
                            result.push((rowid, selected_columns));
                        }
                    }

                    Ok(ToyDBResult::Select(result))
                } else {
                    Err(ToyDBError::TableNotFound(table_name))
                }
            }
            Statement::Update(UpdateStatement::UpdateTable(table_name, updates, where_clause)) => {
                match where_clause {
                    Some(WhereType::Regular(where_clause)) => {
                        let where_col = (where_clause.col_name, where_clause.value);
                        let affected_rows =
                            self.tables
                                .update_table(&table_name, updates.clone(), Some(where_col));

                        // Update FTSText columns in the InvertedIndex
                        let fts_columns = self.tables.get_fts_columns(&table_name);
                        for (column_name, _) in &updates {
                            if fts_columns.contains(&column_name) {
                                let (_, rows) = self.tables.get_table_ref(&table_name).unwrap();
                                for (rowid, row) in rows.iter().enumerate() {
                                    let schema = self.tables.get_schema_ref(&table_name).unwrap();
                                    let column_index = schema
                                        .iter()
                                        .position(|col| col.name == *column_name)
                                        .unwrap();
                                    if let DataValue::Text(ref text) = row[column_index] {
                                        self.inverted_index.update_document(
                                            &table_name,
                                            &column_name,
                                            rowid,
                                            text,
                                        );
                                    }
                                }
                            }
                        }

                        Ok(ToyDBResult::Update(affected_rows))
                    }
                    Some(WhereType::FTS(_)) => {
                        unimplemented!()
                    }
                    None => {
                        let affected_rows = self.tables.update_table(&table_name, updates, None);
                        Ok(ToyDBResult::Update(affected_rows))
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn test_fts_text_search() {
        let mut db = InMemoryToyDB::new((), ());

        let statements = vec![
            "CREATE TABLE books (title TEXT, author TEXT, description FTS_TEXT)",
            "INSERT INTO books VALUES ('Book 1', 'Author 1', 'A book about the history of computer science.')",
            "INSERT INTO books VALUES ('Book 2', 'Author 2', 'A book about modern programming languages.')",
            "INSERT INTO books VALUES ('Book 3', 'Author 3', 'A book about the future of artificial intelligence.')",
            "SELECT title, author FROM books WHERE description MATCH 'computer science'",
            "SELECT title, author FROM books WHERE description MATCH 'artificial intelligence'",
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
            Ok(ToyDBResult::Insert(1)),
            Ok(ToyDBResult::Select(vec![(
                0,
                vec![
                    DataValue::Text("Book 1".to_string()),
                    DataValue::Text("Author 1".to_string()),
                ],
            )])),
            Ok(ToyDBResult::Select(vec![(
                2,
                vec![
                    DataValue::Text("Book 3".to_string()),
                    DataValue::Text("Author 3".to_string()),
                ],
            )])),
        ];

        assert_eq!(results, expected_results);
    }

    #[test]
    fn test_database_on_disk() {
        let kv_path = "kv.db";
        let index = "index.bin";

        let mut db = OnDiskToyDB::new(kv_path.to_string(), index.to_string());

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
                (
                    0,
                    vec![DataValue::Text("alice".to_string()), DataValue::Integer(30)],
                ),
                (
                    1,
                    vec![DataValue::Text("bob".to_string()), DataValue::Integer(31)],
                ),
            ])),
            Ok(ToyDBResult::Select(vec![
                (0, vec![DataValue::Text("alice".to_string())]),
                (1, vec![DataValue::Text("bob".to_string())]),
            ])),
            Ok(ToyDBResult::Select(vec![(
                0,
                vec![DataValue::Text("alice".to_string())],
            )])),
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

    #[test]
    fn test_delete() {
        let mut db = InMemoryToyDB::new((), ());
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
            Ok(ToyDBResult::Select(vec![(
                0,
                vec![DataValue::Text("alice".to_string()), DataValue::Integer(30)],
            )])),
            Ok(ToyDBResult::Select(vec![(
                0,
                vec![DataValue::Text("alice".to_string())],
            )])),
            Ok(ToyDBResult::Select(vec![(
                0,
                vec![DataValue::Text("alice".to_string())],
            )])),
        ];
        assert_eq!(results, expected_results);
    }

    #[test]
    fn test_database() {
        let mut db = InMemoryToyDB::new((), ());

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
                (
                    0,
                    vec![DataValue::Text("alice".to_string()), DataValue::Integer(30)],
                ),
                (
                    1,
                    vec![DataValue::Text("bob".to_string()), DataValue::Integer(31)],
                ),
            ])),
            Ok(ToyDBResult::Select(vec![
                (0, vec![DataValue::Text("alice".to_string())]),
                (1, vec![DataValue::Text("bob".to_string())]),
            ])),
            Ok(ToyDBResult::Select(vec![(
                0,
                vec![DataValue::Text("alice".to_string())],
            )])),
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
