use error::ReefDBError;
use indexes::fts::memory::InvertedIndex;
use indexes::fts::tokenizers::default::DefaultTokenizer;
use indexes::fts::disk::OnDiskInvertedIndex;
use indexes::fts::search::Search;
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
    table::Table,
    column_def::ColumnDef,
};

mod error;
mod result;
mod indexes;
mod sql;
mod storage;
pub mod transaction;
pub mod transaction_manager;
pub mod wal;
pub mod acid;
pub mod mvcc;
pub mod deadlock;

use storage::{disk::OnDiskStorage, memory::InMemoryStorage, Storage};
use std::path::PathBuf;
use transaction_manager::TransactionManager;
use wal::WriteAheadLog;
use transaction::IsolationLevel;
use std::collections::HashMap;

#[derive(Clone)]
struct TableStorage {
    tables: HashMap<String, Table>,
}

impl TableStorage {
    fn new() -> Self {
        TableStorage {
            tables: HashMap::new(),
        }
    }
}

impl Storage for TableStorage {
    type NewArgs = ();

    fn new(_args: Self::NewArgs) -> Self {
        Self::new()
    }

    fn insert_table(&mut self, table_name: String, columns: Vec<ColumnDef>, rows: Vec<Vec<DataValue>>) {
        let mut table = Table::new(columns);
        for row in rows {
            table.insert_row(row);
        }
        self.tables.insert(table_name, table);
    }

    fn get_table(&mut self, table_name: &str) -> Option<&mut (Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        self.tables.get_mut(table_name).map(|table| &mut table.data)
    }

    fn table_exists(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }

    fn push_value(&mut self, table_name: &str, row: Vec<DataValue>) -> usize {
        if let Some(table) = self.tables.get_mut(table_name) {
            table.insert_row(row)
        } else {
            0
        }
    }

    fn update_table(&mut self, table_name: &str, updates: Vec<(String, DataValue)>, where_clause: Option<(String, DataValue)>) -> usize {
        if let Some(table) = self.tables.get_mut(table_name) {
            let mut count = 0;
            for row in &mut table.data.1 {
                let mut should_update = true;
                if let Some((col_name, value)) = &where_clause {
                    if let Some(col_idx) = table.data.0.iter().position(|c| &c.name == col_name) {
                        if row[col_idx] != *value {
                            should_update = false;
                        }
                    }
                }
                if should_update {
                    for (col_name, new_value) in &updates {
                        if let Some(col_idx) = table.data.0.iter().position(|c| &c.name == col_name) {
                            row[col_idx] = new_value.clone();
                        }
                    }
                    count += 1;
                }
            }
            count
        } else {
            0
        }
    }

    fn delete_table(&mut self, table_name: &str, where_clause: Option<(String, DataValue)>) -> usize {
        if let Some(table) = self.tables.get_mut(table_name) {
            let initial_len = table.data.1.len();
            if let Some((col_name, value)) = where_clause {
                if let Some(col_idx) = table.data.0.iter().position(|c| c.name == col_name) {
                    table.data.1.retain(|row| row[col_idx] != value);
                }
            } else {
                table.data.1.clear();
            }
            initial_len - table.data.1.len()
        } else {
            0
        }
    }

    fn get_table_ref(&self, table_name: &str) -> Option<&(Vec<ColumnDef>, Vec<Vec<DataValue>>)> {
        self.tables.get(table_name).map(|table| &table.data)
    }

    fn remove_table(&mut self, table_name: &str) -> bool {
        self.tables.remove(table_name).is_some()
    }
}

pub type DefaultSearchIdx = InvertedIndex<DefaultTokenizer>;
pub type OnDiskSearchIdx = OnDiskInvertedIndex<DefaultTokenizer>;

pub type InMemoryReefDB = ReefDB<InMemoryStorage, DefaultSearchIdx>;

impl InMemoryReefDB {
    pub fn create_in_memory() -> Self {
        ReefDB::create(InMemoryStorage::new(()), ())
    }
}

pub type OnDiskReefDB = ReefDB<OnDiskStorage, OnDiskSearchIdx>;

impl OnDiskReefDB {
    pub fn create_on_disk(db_path: String, _index_path: String) -> Result<Self, ReefDBError> {
        let storage = OnDiskStorage::new(db_path.clone());
        let data_dir = PathBuf::from(db_path).parent().unwrap().to_path_buf();
        ReefDB::with_transaction_support(storage, data_dir)
    }
}

#[derive(Clone)]
pub struct ReefDB<S: Storage + Clone, FTS: Search + Clone>
where
    FTS::NewArgs: Clone,
{
    pub(crate) tables: TableStorage,
    pub(crate) inverted_index: HashMap<String, FTS>,
    storage: S,
    transaction_manager: Option<TransactionManager<S, FTS>>,
    data_dir: Option<PathBuf>,
}

impl<S: Storage + Clone, FTS: Search + Clone> ReefDB<S, FTS>
where
    FTS::NewArgs: Clone,
{
    pub fn with_transaction_support(storage: S, data_dir: PathBuf) -> Result<Self, ReefDBError> {
        let wal_path = data_dir.join("reef.wal");
        let wal = WriteAheadLog::new(wal_path)
            .map_err(|e| ReefDBError::Other(format!("Failed to create WAL: {}", e)))?;
        
        let mut db = ReefDB {
            tables: TableStorage::new(),
            inverted_index: HashMap::new(),
            storage,
            transaction_manager: None,
            data_dir: Some(data_dir),
        };
        
        let tm = TransactionManager::create(db.clone(), wal);
        db.transaction_manager = Some(tm);
        
        Ok(db)
    }

    pub fn create(storage: S, _fts_args: FTS::NewArgs) -> Self {
        ReefDB {
            tables: TableStorage::new(),
            inverted_index: HashMap::new(),
            storage,
            transaction_manager: None,
            data_dir: None,
        }
    }

    pub fn begin_transaction(&mut self, isolation_level: IsolationLevel) -> Result<u64, ReefDBError> {
        if let Some(tm) = &mut self.transaction_manager {
            tm.begin_transaction(isolation_level)
        } else {
            Err(ReefDBError::Other("Transaction support not enabled".to_string()))
        }
    }

    pub fn commit_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        if let Some(tm) = &mut self.transaction_manager {
            tm.commit_transaction(transaction_id)
        } else {
            Err(ReefDBError::Other("Transaction support not enabled".to_string()))
        }
    }

    pub fn rollback_transaction(&mut self, transaction_id: u64) -> Result<(), ReefDBError> {
        if let Some(tm) = &mut self.transaction_manager {
            tm.rollback_transaction(transaction_id)
        } else {
            Err(ReefDBError::Other("Transaction support not enabled".to_string()))
        }
    }

    pub fn query(&mut self, query: &str) -> Result<ReefDBResult, ReefDBError> {
        let (_, stmt) = Statement::parse(query).unwrap();
        self.execute_statement(stmt)
    }

    pub fn create_table(&mut self, stmt: CreateStatement) -> Result<ReefDBResult, ReefDBError> {
        match stmt {
            CreateStatement::Table(table_name, columns) => {
                // Initialize FTS index for the table if it has any FTS columns
                let has_fts_columns = columns.iter().any(|col| col.data_type == sql::data_type::DataType::FTSText);
                if has_fts_columns {
                    let new_index = FTS::new(FTS::NewArgs::default());
                    self.inverted_index.insert(table_name.clone(), new_index);
                }

                // Add FTS columns to the inverted index
                for column in &columns {
                    if column.data_type == sql::data_type::DataType::FTSText {
                        if let Some(index) = self.inverted_index.get_mut(&table_name) {
                            index.add_column(&table_name, &column.name);
                        }
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
                                if let Some(index) = self.inverted_index.get_mut(&table_name) {
                                    index.add_document(&table_name, &column.name, row_id, text);
                                }
                            }
                        }
                    }
                }
                
                Ok(ReefDBResult::Insert(row_id + 1))
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
                                let matching_rows = self.search_fts(&table_name, &fts_clause.col.name, &fts_clause.query);
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
                        self.add_fts_column(&table_name, &column_def.name);
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
            Statement::CreateIndex(_) => Ok(ReefDBResult::CreateIndex),
            Statement::DropIndex(_) => Ok(ReefDBResult::DropIndex),
        }
    }

    fn search_fts(&self, table_name: &str, column_name: &str, query: &str) -> Vec<usize> {
        if let Some(index) = self.inverted_index.get(table_name) {
            index.search(table_name, column_name, query).into_iter().collect()
        } else {
            Vec::new()
        }
    }

    fn add_to_fts(&mut self, table_name: &str, column_name: &str, row_id: usize, text: &str) {
        if let Some(index) = self.inverted_index.get_mut(table_name) {
            index.add_document(table_name, column_name, row_id, text);
        }
    }

    fn add_fts_column(&mut self, table_name: &str, column_name: &str) {
        if !self.inverted_index.contains_key(table_name) {
            let new_index = FTS::new(FTS::NewArgs::default());
            self.inverted_index.insert(table_name.to_string(), new_index);
        }
        if let Some(index) = self.inverted_index.get_mut(table_name) {
            index.add_column(table_name, column_name);
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
        let mut db = InMemoryReefDB::create_in_memory();

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
        let mut db = InMemoryReefDB::create_in_memory();

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
        use std::fs::{create_dir_all, File};
        use std::path::Path;

        // Create test directory
        let test_dir = Path::new("test_db");
        create_dir_all(test_dir).unwrap();

        let kv_path = test_dir.join("kv.db");
        let index = test_dir.join("index.bin");

        // Create empty files
        File::create(&kv_path).unwrap();
        File::create(&index).unwrap();

        let mut db = OnDiskReefDB::create_on_disk(kv_path.to_string_lossy().to_string(), index.to_string_lossy().to_string()).unwrap();

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
            Ok(ReefDBResult::Insert(1)),
            Ok(ReefDBResult::Insert(2)),
            Ok(ReefDBResult::Update(1)), // Updated 1 row (where name = 'bob')
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

        // Cleanup
        std::fs::remove_dir_all(test_dir).unwrap();
    }

    #[test]
    fn test_delete() {
        let mut db = InMemoryReefDB::create_in_memory();
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
        let mut db = InMemoryReefDB::create_in_memory();

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
        let mut db = InMemoryReefDB::create_in_memory();

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
            Ok(ReefDBResult::Update(1)), // Updated 1 row (where name = 'bob')
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
