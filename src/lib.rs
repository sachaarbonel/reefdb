mod storage;

use error::ReefDBError;
use indexes::fts::{
    default::{DefaultSearchIdx, OnDiskSearchIdx},
    search::Search,
};
use nom::IResult;
mod indexes;
mod sql;
mod transaction;
use result::ReefDBResult;
use sql::{
    clauses::{join_clause::JoinType, wheres::where_type::WhereType},
    data_type::DataType,
    data_value::DataValue,
    statements::{
        create::CreateStatement, delete::DeleteStatement, insert::InsertStatement,
        select::SelectStatement, update::UpdateStatement, alter::{AlterStatement, AlterType},
        drop::DropStatement, Statement,
    },
};

mod error;
mod result;

use storage::{disk::OnDiskStorage, memory::InMemoryStorage, Storage};

pub type InMemoryReefDB = ReefDB<InMemoryStorage, DefaultSearchIdx>;

impl InMemoryReefDB {
    pub fn new() -> Self {
        ReefDB::init((), ())
    }
}

pub type OnDiskReefDB = ReefDB<OnDiskStorage, OnDiskSearchIdx>;

impl OnDiskReefDB {
    pub fn new(db_path: String, index_path: String) -> Self {
        ReefDB::init(db_path, index_path)
    }
}

//clone
#[derive(Clone)]
pub struct ReefDB<S: Storage, FTS: Search> {
    tables: S,
    inverted_index: FTS,
}

impl<S: Storage, FTS: Search> ReefDB<S, FTS> {
    fn init(args: S::NewArgs, args2: FTS::NewArgs) -> Self {
        ReefDB {
            tables: S::new(args),
            inverted_index: FTS::new(args2),
        }
    }

    pub fn query(&mut self, query: &str) -> Result<ReefDBResult, ReefDBError> {
        match Statement::parse(query) {
            Ok((_, stmt)) => self.execute_statement(stmt),
            Err(err) => {
                eprintln!("Failed to parse statement: {}", err);
                Err(ReefDBError::Other(err.to_string()))
            }
        }
    }

    fn execute_statement(&mut self, stmt: Statement) -> Result<ReefDBResult, ReefDBError> {
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
                                            //also remove from inverted index if it's a fts column
                                            if schema[col_index].data_type == DataType::FTSText {
                                                self.inverted_index.remove_document(
                                                    &table_name,
                                                    &where_clause.col_name,
                                                    i,
                                                );
                                            }
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
                    Ok(ReefDBResult::Delete(deleted_rows))
                } else {
                    Err(ReefDBError::TableNotFound(table_name))
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

                Ok(ReefDBResult::CreateTable)
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
                Ok(ReefDBResult::Insert(1))
            }
            Statement::Select(SelectStatement::FromTable(
                table_name,
                columns,
                where_type,
                joins,
            )) => {
                if let Some((schema, table)) = self.tables.get_table_ref(&table_name) {
                    let mut result = Vec::<(usize, Vec<DataValue>)>::new();

                    // If there are no join clauses, perform a regular select operation
                    if joins.is_empty() {
                        // if there is a where clause, filter the result
                        if let Some(where_type) = where_type {
                            let column_indexes: Vec<_> = columns
                                .iter()
                                .map(|column_name| {
                                    schema
                                        .iter()
                                        .position(|column_def| {
                                            &column_def.name == &column_name.name
                                        })
                                        .unwrap()
                                })
                                .collect();
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
                                        if let Some(col_index) =
                                            schema.iter().position(|column_def| {
                                                &column_def.name == &where_clause.col_name
                                            })
                                        {
                                            if row[col_index] == where_clause.value {
                                                result.push((rowid, selected_columns));
                                            }
                                        } else {
                                            eprintln!(
                                                "Column not found: {}",
                                                where_clause.col_name
                                            );
                                        }
                                    }
                                }
                            }
                        } else {
                            for (rowid, row) in table.iter().enumerate() {
                                let column_indexes: Vec<_> = columns
                                    .iter()
                                    .map(|column_name| {
                                        schema
                                            .iter()
                                            .position(|column_def| {
                                                &column_def.name == &column_name.name
                                            })
                                            .unwrap()
                                    })
                                    .collect();

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
                    } else {
                        println!("Joining tables");
                        // Iterate over join clauses
                        for join in joins {
                            let join_type: JoinType = join.join_type;
                            let join_table_name = join.table_name;
                            let left_col = join.on.0;
                            let right_col = join.on.1;

                            if join_type == JoinType::Inner {
                                println!("Inner join");
                                if let Some((join_schema, join_table)) =
                                    self.tables.get_table_ref(&join_table_name)
                                {
                                    let join_schema = join_schema.clone();
                                    println!(
                                        "Join schema: {:?} joined table name {:?}",
                                        join_schema, join_table_name
                                    );

                                    println!("normal schema: {:?}", schema);

                                    let join_table = join_table.clone();
                                    println!("Join table: {:?}", join_table);
                                    let left_col_index = schema
                                        .iter()
                                        .position(|col| col.name == left_col.column_name)
                                        .unwrap();
                                    println!(
                                        "Left col index: {:?} left_col.column_name {:?}",
                                        left_col_index, left_col.column_name
                                    );
                                    let right_col_index = join_schema
                                        .iter()
                                        .position(|col| col.name == right_col.column_name)
                                        .unwrap();
                                    println!(
                                        "Right col index: {:?} right_col.column_name  {:?}",
                                        right_col_index, right_col.column_name
                                    );
                                    for (rowid, row) in table.iter().enumerate() {
                                        for join_row in join_table.iter() {
                                            println!("Join row: {:?}", join_row);
                                            println!("row {:?}", row);

                                            if row[left_col_index] == join_row[right_col_index] {
                                                let mut selected_columns = vec![];

                                                for column_name in &columns {
                                                    if let Some(index) = schema
                                                        .iter()
                                                        .position(|column_def| &column_def.name == &column_name.name)
                                                    {
                                                        println!(
                                                            "index {:?} schema.len() {:?}",
                                                            index, schema.len()
                                                        );
                                                        selected_columns.push(row[index].clone());
                                                    } else if let Some(join_col_index) = join_schema
                                                        .iter()
                                                        .position(|col| {
                                                            println!(
                                                                "col.name {:?} column_name.name {:?}",
                                                                col.name, column_name.name
                                                            );
                                                            &col.name == &column_name.name
                                                        }) {
                                                        println!("idx {:?}", join_col_index);
                                                        selected_columns.push(join_row[join_col_index].clone());
                                                        println!("selected_columns {:?}", selected_columns);
                                                    } else {
                                                        panic!("Invalid column name.");
                                                    }
                                                }
                                                result.push((rowid, selected_columns));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    Ok(ReefDBResult::Select(result))
                } else {
                    Err(ReefDBError::TableNotFound(table_name))
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

                        Ok(ReefDBResult::Update(affected_rows))
                    }
                    Some(WhereType::FTS(_)) => {
                        unimplemented!()
                    }
                    None => {
                        let affected_rows = self.tables.update_table(&table_name, updates, None);
                        Ok(ReefDBResult::Update(affected_rows))
                    }
                }
            }
            Statement::Alter(AlterStatement { table_name, alter_type }) => {
                if let Some((schema, rows)) = self.tables.get_table(&table_name) {
                    match alter_type {
                        AlterType::AddColumn(column_def) => {
                            schema.push(column_def.clone());
                            for row in rows.iter_mut() {
                                row.push(DataValue::Text("NULL".to_string()));
                            }
                            if column_def.data_type == DataType::FTSText {
                                self.inverted_index.add_column(&table_name, &column_def.name);
                            }
                            Ok(ReefDBResult::AlterTable)
                        }
                        AlterType::DropColumn(column_name) => {
                            if let Some(col_idx) = schema.iter().position(|col| col.name == column_name) {
                                schema.remove(col_idx);
                                for row in rows.iter_mut() {
                                    row.remove(col_idx);
                                }
                                Ok(ReefDBResult::AlterTable)
                            } else {
                                Err(ReefDBError::ColumnNotFound(column_name))
                            }
                        }
                        AlterType::RenameColumn(old_name, new_name) => {
                            if let Some(col) = schema.iter_mut().find(|col| col.name == old_name) {
                                col.name = new_name;
                                Ok(ReefDBResult::AlterTable)
                            } else {
                                Err(ReefDBError::ColumnNotFound(old_name))
                            }
                        }
                    }
                } else {
                    Err(ReefDBError::TableNotFound(table_name))
                }
            }
            Statement::Drop(DropStatement { table_name }) => {
                if self.tables.table_exists(&table_name) {
                    // Get FTS columns before removing the table
                    let fts_columns = self.tables.get_fts_columns(&table_name);
                    
                    // Remove the table
                    self.tables.remove_table(&table_name);
                    
                    // Clean up FTS indexes for the table
                    for column in fts_columns {
                        // Note: We might want to add a method to remove all FTS entries for a table
                        // For now, we'll just remove document entries
                        self.inverted_index.remove_document(&table_name, &column, 0);
                    }
                    
                    Ok(ReefDBResult::DropTable)
                } else {
                    Err(ReefDBError::TableNotFound(table_name))
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
    fn test_inner_join() {
        let mut db = InMemoryReefDB::new();

        let queries = vec![
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT, user_id INTEGER FOREIGN KEY (id) REFERENCES users)",
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        "INSERT INTO posts VALUES (1, 'Post 1', 1)",
        "INSERT INTO posts VALUES (2, 'Post 2', 2)",
        "SELECT users.name, posts.title FROM users INNER JOIN posts ON users.id = posts.user_id",
    ];

        let mut results = Vec::new();
        for query in queries {
            results.push(db.query(query));
        }

        let expected_results = vec![
            Ok(ReefDBResult::CreateTable),
            Ok(ReefDBResult::CreateTable),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Select(vec![
                (
                    0,
                    vec![
                        DataValue::Text("Alice".to_string()),
                        DataValue::Text("Post 1".to_string()),
                    ],
                ),
                (
                    1,
                    vec![
                        DataValue::Text("Bob".to_string()),
                        DataValue::Text("Post 2".to_string()),
                    ],
                ),
            ])),
        ];
        assert_eq!(results, expected_results);
    }

    #[test]
    fn test_fts_text_search() {
        let mut db = InMemoryReefDB::new();

        let queries = vec![
            "CREATE TABLE books (title TEXT, author TEXT, description FTS_TEXT)",
            "INSERT INTO books VALUES ('Book 1', 'Author 1', 'A book about the history of computer science.')",
            "INSERT INTO books VALUES ('Book 2', 'Author 2', 'A book about modern programming languages.')",
            "INSERT INTO books VALUES ('Book 3', 'Author 3', 'A book about the future of artificial intelligence.')",
            "SELECT title, author FROM books WHERE description MATCH 'computer science'",
            "SELECT title, author FROM books WHERE description MATCH 'artificial intelligence'",
        ];

        let mut results = Vec::new();
        for query in queries {
            results.push(db.query(query));
        }

        let expected_results = vec![
            Ok(ReefDBResult::CreateTable),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Select(vec![(
                0,
                vec![
                    DataValue::Text("Book 1".to_string()),
                    DataValue::Text("Author 1".to_string()),
                ],
            )])),
            Ok(ReefDBResult::Select(vec![(
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

        let mut db = OnDiskReefDB::new(kv_path.to_string(), index.to_string());

        let queries = vec![
            "CREATE TABLE users (name TEXT, age INTEGER)",
            "INSERT INTO users VALUES ('alice', 30)",
            "INSERT INTO users VALUES ('bob', 28)",
            "UPDATE users SET age = 31 WHERE name = 'bob'",
            "SELECT name, age FROM users",
            "SELECT name FROM users",
            "SELECT name FROM users WHERE age = 30",
        ];
        let mut results = Vec::new();
        for query in queries {
            results.push(db.query(query));
        }

        let expected_results = vec![
            Ok(ReefDBResult::CreateTable),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Update(2)),
            Ok(ReefDBResult::Select(vec![
                (
                    0,
                    vec![DataValue::Text("alice".to_string()), DataValue::Integer(30)],
                ),
                (
                    1,
                    vec![DataValue::Text("bob".to_string()), DataValue::Integer(31)],
                ),
            ])),
            Ok(ReefDBResult::Select(vec![
                (0, vec![DataValue::Text("alice".to_string())]),
                (1, vec![DataValue::Text("bob".to_string())]),
            ])),
            Ok(ReefDBResult::Select(vec![(
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
        let mut db = InMemoryReefDB::new();
        let queries = vec![
            "CREATE TABLE users (name TEXT, age INTEGER)",
            "INSERT INTO users VALUES ('alice', 30)",
            "INSERT INTO users VALUES ('bob', 28)",
            "DELETE FROM users WHERE name = 'bob'",
            "SELECT name, age FROM users",
            "SELECT name FROM users",
            "SELECT name FROM users WHERE age = 30",
        ];
        let mut results = Vec::new();
        for query in queries {
            results.push(db.query(query));
        }

        let expected_results = vec![
            Ok(ReefDBResult::CreateTable),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Delete(1)),
            Ok(ReefDBResult::Select(vec![(
                0,
                vec![DataValue::Text("alice".to_string()), DataValue::Integer(30)],
            )])),
            Ok(ReefDBResult::Select(vec![(
                0,
                vec![DataValue::Text("alice".to_string())],
            )])),
            Ok(ReefDBResult::Select(vec![(
                0,
                vec![DataValue::Text("alice".to_string())],
            )])),
        ];
        assert_eq!(results, expected_results);
    }

    #[test]
    fn test_database() {
        let mut db = InMemoryReefDB::new();

        let queries = vec![
            "CREATE TABLE users (name TEXT, age INTEGER)",
            "INSERT INTO users VALUES ('alice', 30)",
            "INSERT INTO users VALUES ('bob', 28)",
            "UPDATE users SET age = 31 WHERE name = 'bob'",
            "SELECT name, age FROM users",
            "SELECT name FROM users",
            "SELECT name FROM users WHERE age = 30",
        ];
        let mut results = Vec::new();
        for query in queries {
            results.push(db.query(query));
        }

        let expected_results = vec![
            Ok(ReefDBResult::CreateTable),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Update(2)), // Updated 1 row
            Ok(ReefDBResult::Select(vec![
                (
                    0,
                    vec![DataValue::Text("alice".to_string()), DataValue::Integer(30)],
                ),
                (
                    1,
                    vec![DataValue::Text("bob".to_string()), DataValue::Integer(31)],
                ),
            ])),
            Ok(ReefDBResult::Select(vec![
                (0, vec![DataValue::Text("alice".to_string())]),
                (1, vec![DataValue::Text("bob".to_string())]),
            ])),
            Ok(ReefDBResult::Select(vec![(
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

    #[test]
    fn test_alter_and_drop() {
        let mut db = InMemoryReefDB::new();

        let queries = vec![
            "CREATE TABLE users (name TEXT, age INTEGER)",
            "INSERT INTO users VALUES ('alice', 30)",
            "ALTER TABLE users ADD COLUMN email TEXT",
            "SELECT name, age, email FROM users",
            "ALTER TABLE users RENAME COLUMN email TO contact",
            "SELECT name, age, contact FROM users",
            "ALTER TABLE users DROP COLUMN contact",
            "SELECT name, age FROM users",
            "DROP TABLE users",
            "CREATE TABLE users (name TEXT)",  // Should work as table was dropped
        ];

        let mut results = Vec::new();
        for query in queries {
            results.push(db.query(query));
        }

        let expected_results = vec![
            Ok(ReefDBResult::CreateTable),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::AlterTable),
            Ok(ReefDBResult::Select(vec![(
                0,
                vec![
                    DataValue::Text("alice".to_string()),
                    DataValue::Integer(30),
                    DataValue::Text("NULL".to_string()),
                ],
            )])),
            Ok(ReefDBResult::AlterTable),
            Ok(ReefDBResult::Select(vec![(
                0,
                vec![
                    DataValue::Text("alice".to_string()),
                    DataValue::Integer(30),
                    DataValue::Text("NULL".to_string()),
                ],
            )])),
            Ok(ReefDBResult::AlterTable),
            Ok(ReefDBResult::Select(vec![(
                0,
                vec![
                    DataValue::Text("alice".to_string()),
                    DataValue::Integer(30),
                ],
            )])),
            Ok(ReefDBResult::DropTable),
            Ok(ReefDBResult::CreateTable),
        ];

        assert_eq!(results, expected_results);
    }
}
