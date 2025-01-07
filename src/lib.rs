use error::ReefDBError;
use indexes::{
    fts::{
        memory::InvertedIndex,
        tokenizers::default::DefaultTokenizer,
        disk::OnDiskInvertedIndex,
        search::Search,
    },
    IndexType,
    btree::BTreeIndex,
    DefaultIndexManager,
    IndexManager,
    disk::OnDiskIndexManager,
};
use result::ReefDBResult;
use sql::{
    clauses::wheres::where_type::WhereType,
    data_value::DataValue,
    statements::{
        create::CreateStatement,
        delete::DeleteStatement,
        insert::InsertStatement,
        select::SelectStatement,
        update::UpdateStatement,
        alter::AlterStatement,
        drop::DropStatement,
        Statement,
    },
};

mod error;
mod result;
mod indexes;
mod sql;
mod storage;
mod transaction;

use storage::{disk::OnDiskStorage, memory::InMemoryStorage, Storage};

pub type DefaultSearchIdx = InvertedIndex<DefaultTokenizer>;
pub type OnDiskSearchIdx = OnDiskInvertedIndex<DefaultTokenizer>;

pub type InMemoryReefDB = ReefDB<InMemoryStorage, DefaultSearchIdx>;

impl InMemoryReefDB {
    pub fn new() -> Self {
        ReefDB::init((), (), DefaultIndexManager::new())
    }
}

pub type OnDiskReefDB = ReefDB<OnDiskStorage, OnDiskSearchIdx, OnDiskIndexManager<String>>;

impl OnDiskReefDB {
    pub fn new(db_path: String, index_path: String) -> Self {
        ReefDB {
            tables: OnDiskStorage::new(db_path),
            inverted_index: OnDiskSearchIdx::new(index_path.clone()),
            index_manager: OnDiskIndexManager::new(index_path),
        }
    }
}

#[derive(Clone)]
pub struct ReefDB<S: Storage, FTS: Search, IDX: IndexManager<String> = DefaultIndexManager<String>> {
    tables: S,
    inverted_index: FTS,
    index_manager: IDX,
}

impl<S: Storage, FTS: Search, IDX: IndexManager<String>> ReefDB<S, FTS, IDX> {
    fn init(args: S::NewArgs, args2: FTS::NewArgs, idx: IDX) -> Self {
        ReefDB {
            tables: S::new(args),
            inverted_index: FTS::new(args2),
            index_manager: idx,
        }
    }

    pub fn query(&mut self, query: &str) -> Result<ReefDBResult, ReefDBError> {
        let (_, stmt) = Statement::parse(query).unwrap();
        self.execute_statement(stmt)
    }

    pub fn create_table(&mut self, stmt: CreateStatement) -> Result<ReefDBResult, ReefDBError> {
        match stmt {
            CreateStatement::Table(table_name, columns) => {
                // Add FTS columns to the inverted index
                for column in &columns {
                    if column.data_type == sql::data_type::DataType::FTSText {
                        self.inverted_index.add_column(&table_name, &column.name);
                    }
                }
                self.tables.insert_table(table_name, columns, vec![]);
                Ok(ReefDBResult::CreateTable)
            }
        }
    }

    pub fn insert(&mut self, stmt: InsertStatement) -> Result<ReefDBResult, ReefDBError> {
        match stmt {
            InsertStatement::IntoTable(table_name, values) => {
                let row_id = self.tables.push_value(&table_name, values.clone());
                
                // Add FTS values to the inverted index
                if let Some((schema, _)) = self.tables.get_table(&table_name) {
                    for (i, column) in schema.iter().enumerate() {
                        if column.data_type == sql::data_type::DataType::FTSText {
                            if let DataValue::Text(text) = &values[i] {
                                self.inverted_index.add_document(&table_name, &column.name, row_id - 1, text);
                            }
                        }
                    }
                }
                
                Ok(ReefDBResult::Insert(row_id))
            }
        }
    }

    pub fn select(&mut self, stmt: SelectStatement) -> Result<ReefDBResult, ReefDBError> {
        match stmt {
            SelectStatement::FromTable(table_name, columns, where_clause, joins) => {
                // Get all the data we need upfront to avoid multiple mutable borrows
                let main_table = if let Some((schema, data)) = self.tables.get_table(&table_name) {
                    Some((schema.clone(), data.clone()))
                } else {
                    return Err(ReefDBError::TableNotFound(table_name));
                };

                let mut result = Vec::new();
                let (schema, data) = main_table.unwrap();

                if joins.is_empty() {
                    // Handle where clause if present
                    let filtered_rows: Vec<_> = if let Some(where_type) = where_clause {
                        match where_type {
                            WhereType::Regular(where_clause) => {
                                let col_idx = schema.iter().position(|c| c.name == where_clause.col_name).unwrap();
                                data.iter().enumerate()
                                    .filter(|(_, row)| row[col_idx] == where_clause.value)
                                    .collect()
                            }
                            WhereType::FTS(fts_clause) => {
                                let matching_rows = self.inverted_index.search(
                                    &table_name,
                                    &fts_clause.col.name,
                                    &fts_clause.query,
                                );
                                data.iter().enumerate()
                                    .filter(|(i, _)| matching_rows.contains(i))
                                    .collect()
                            }
                        }
                    } else {
                        data.iter().enumerate().collect()
                    };

                    // Select requested columns
                    for (i, row) in filtered_rows {
                        let mut selected_values = Vec::new();
                        for col in &columns {
                            if let Some(col_idx) = schema.iter().position(|c| c.name == col.name) {
                                selected_values.push(row[col_idx].clone());
                            }
                        }
                        result.push((i, selected_values));
                    }
                } else {
                    // Handle joins
                    for join in &joins {
                        let join_table = if let Some((schema, data)) = self.tables.get_table(&join.table_name) {
                            (schema.clone(), data.clone())
                        } else {
                            continue;
                        };

                        let (join_schema, join_data) = join_table;
                        
                        for (i, row) in data.iter().enumerate() {
                            // Find the join column indices
                            let left_col_idx = schema.iter().position(|c| c.name == join.on.0.column_name).unwrap();
                            let right_col_idx = join_schema.iter().position(|c| c.name == join.on.1.column_name).unwrap();

                            // Find matching rows in the joined table
                            for join_row in join_data.iter() {
                                if row[left_col_idx] == join_row[right_col_idx] {
                                    let mut selected_values = Vec::new();
                                    
                                    // Select requested columns from both tables
                                    for col in &columns {
                                        if let Some(table) = &col.table {
                                            if table == &table_name {
                                                if let Some(idx) = schema.iter().position(|c| c.name == col.name) {
                                                    selected_values.push(row[idx].clone());
                                                }
                                            } else if table == &join.table_name {
                                                if let Some(idx) = join_schema.iter().position(|c| c.name == col.name) {
                                                    selected_values.push(join_row[idx].clone());
                                                }
                                            }
                                        }
                                    }
                                    result.push((i, selected_values));
                                }
                            }
                        }
                    }
                }
                Ok(ReefDBResult::Select(result))
            }
        }
    }

    pub fn update(&mut self, stmt: UpdateStatement) -> Result<ReefDBResult, ReefDBError> {
        match stmt {
            UpdateStatement::UpdateTable(table_name, updates, where_clause) => {
                let rows_affected = self.tables.update_table(&table_name, updates, where_clause.map(|w| match w {
                    WhereType::Regular(clause) => (clause.col_name, clause.value),
                    WhereType::FTS(_) => unimplemented!("FTS where clauses are not supported for updates"),
                }));
                Ok(ReefDBResult::Update(rows_affected))
            }
        }
    }

    pub fn delete(&mut self, stmt: DeleteStatement) -> Result<ReefDBResult, ReefDBError> {
        match stmt {
            DeleteStatement::FromTable(table_name, where_clause) => {
                let rows_affected = self.tables.delete_table(&table_name, where_clause.map(|w| match w {
                    WhereType::Regular(clause) => (clause.col_name, clause.value),
                    WhereType::FTS(_) => unimplemented!("FTS where clauses are not supported for deletes"),
                }));
                Ok(ReefDBResult::Delete(rows_affected))
            }
        }
    }

    pub fn alter_table(&mut self, stmt: AlterStatement) -> Result<ReefDBResult, ReefDBError> {
        let table_name = stmt.table_name.clone();
        if let Some((schema, rows)) = self.tables.get_table(&table_name) {
            match stmt.alter_type {
                sql::statements::alter::AlterType::AddColumn(column_def) => {
                    schema.push(column_def.clone());
                    for row in rows.iter_mut() {
                        row.push(DataValue::Text("NULL".to_string()));
                    }
                    if column_def.data_type == sql::data_type::DataType::FTSText {
                        self.inverted_index.add_column(&table_name, &column_def.name);
                    }
                }
                sql::statements::alter::AlterType::DropColumn(column_name) => {
                    if let Some(col_idx) = schema.iter().position(|c| c.name == column_name) {
                        schema.remove(col_idx);
                        for row in rows.iter_mut() {
                            row.remove(col_idx);
                        }
                    } else {
                        return Err(ReefDBError::ColumnNotFound(column_name));
                    }
                }
                sql::statements::alter::AlterType::RenameColumn(old_name, new_name) => {
                    if let Some(col) = schema.iter_mut().find(|c| c.name == old_name) {
                        col.name = new_name;
                    } else {
                        return Err(ReefDBError::ColumnNotFound(old_name));
                    }
                }
            }
            Ok(ReefDBResult::AlterTable)
        } else {
            Err(ReefDBError::TableNotFound(table_name))
        }
    }

    pub fn drop_table(&mut self, stmt: DropStatement) -> Result<ReefDBResult, ReefDBError> {
        let table_name = stmt.table_name.clone();
        if self.tables.remove_table(&table_name) {
            Ok(ReefDBResult::DropTable)
        } else {
            Err(ReefDBError::TableNotFound(table_name))
        }
    }

    pub fn execute_statement(&mut self, stmt: Statement) -> Result<ReefDBResult, ReefDBError> {
        match stmt {
            Statement::Create(create_stmt) => self.create_table(create_stmt),
            Statement::Insert(insert_stmt) => self.insert(insert_stmt),
            Statement::Select(select_stmt) => self.select(select_stmt),
            Statement::Update(update_stmt) => self.update(update_stmt),
            Statement::Delete(delete_stmt) => self.delete(delete_stmt),
            Statement::Alter(alter_stmt) => self.alter_table(alter_stmt),
            Statement::Drop(drop_stmt) => self.drop_table(drop_stmt),
            Statement::CreateIndex(create_idx_stmt) => {
                self.index_manager.create_index(
                    &create_idx_stmt.table_name,
                    &create_idx_stmt.column_name,
                    IndexType::BTree(BTreeIndex::new()),
                );
                Ok(ReefDBResult::CreateIndex)
            }
            Statement::DropIndex(drop_idx_stmt) => {
                self.index_manager.drop_index(
                    &drop_idx_stmt.table_name,
                    &drop_idx_stmt.column_name,
                );
                Ok(ReefDBResult::DropIndex)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use super::*;
    use crate::sql::data_value::DataValue;

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
            let (_, stmt) = Statement::parse(query).unwrap();
            results.push(db.execute_statement(stmt));
        }

        let expected_results = vec![
            Ok(ReefDBResult::CreateTable),
            Ok(ReefDBResult::CreateTable),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Insert(2)),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Insert(2)),
            Ok(ReefDBResult::Select(vec![
                (0, vec![
                    DataValue::Text("Alice".to_string()),
                    DataValue::Text("Post 1".to_string()),
                ]),
                (1, vec![
                    DataValue::Text("Bob".to_string()),
                    DataValue::Text("Post 2".to_string()),
                ]),
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
            let (_, stmt) = Statement::parse(query).unwrap();
            results.push(db.execute_statement(stmt));
        }

        let expected_results = vec![
            Ok(ReefDBResult::CreateTable),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Insert(2)),
            Ok(ReefDBResult::Insert(3)),
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
            let (_, stmt) = Statement::parse(query).unwrap();
            results.push(db.execute_statement(stmt));
        }

        let expected_results = vec![
            Ok(ReefDBResult::CreateTable),
            Ok(ReefDBResult::Insert(0)),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Update(2)),
            Ok(ReefDBResult::Select(vec![
                (0, vec![DataValue::Text("alice".to_string()), DataValue::Integer(30)]),
                (1, vec![DataValue::Text("bob".to_string()), DataValue::Integer(31)]),
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
            let (_, stmt) = Statement::parse(query).unwrap();
            results.push(db.execute_statement(stmt));
        }

        let expected_results = vec![
            Ok(ReefDBResult::CreateTable),
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Insert(2)),
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
            let (_, stmt) = Statement::parse(query).unwrap();
            results.push(db.execute_statement(stmt));
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
            Ok(ReefDBResult::Insert(2)),
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
