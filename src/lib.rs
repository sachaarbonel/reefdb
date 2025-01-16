use crate::sql::table_reference::TableReference;
use crate::sql::data_type::DataType;
use crate::sql::{
    statements::{
        Statement,
        create::CreateStatement,
        insert::InsertStatement,
        select::SelectStatement,
        update::UpdateStatement,
        delete::DeleteStatement,
        alter::{AlterStatement, AlterType},
        drop::DropStatement,
        create_index::CreateIndexStatement,
        drop_index::DropIndexStatement,
    },
    column_def::ColumnDef,
    clauses::{
        wheres::where_type::WhereType,
        join_clause::JoinClause,
    },
    data_value::DataValue,
    column::Column,
};
use crate::result::ReefDBResult;
use crate::error::ReefDBError;
use crate::transaction::IsolationLevel;
use crate::transaction_manager::TransactionManager;
use crate::wal::WriteAheadLog;
use crate::mvcc::MVCCManager;
use crate::storage::{Storage, TableStorage};
use crate::indexes::{index_manager::IndexManager, btree::BTreeIndex, index_manager::IndexType};
use crate::fts::search::Search;
use std::any::Any;
use std::sync::{Arc, Mutex};
use std::path::PathBuf;

pub mod storage;
pub mod transaction;
pub mod transaction_manager;
pub mod wal;
pub mod error;
pub mod acid;
pub mod result;
pub mod mvcc;
pub mod deadlock;
pub mod sql;
pub mod indexes;
pub mod savepoint;
pub mod locks;
pub mod key_format;
pub mod fts;
pub mod functions;
#[cfg(test)]
pub mod tests;

pub type InMemoryReefDB = ReefDB<storage::memory::InMemoryStorage, fts::default::DefaultSearchIdx>;
pub type OnDiskReefDB = ReefDB<storage::disk::OnDiskStorage, fts::default::DefaultSearchIdx>;
pub type MmapReefDB = ReefDB<storage::mmap::MmapStorage, fts::default::DefaultSearchIdx>;

impl InMemoryReefDB {
    pub fn create_in_memory() -> Result<Self, ReefDBError> {
        let mut db = ReefDB {
            tables: TableStorage::new(),
            inverted_index: fts::default::DefaultSearchIdx::new(),
            storage: storage::memory::InMemoryStorage::new(),
            transaction_manager: None,
            data_dir: None,
            autocommit: true,
            autocommit_isolation_level: IsolationLevel::ReadCommitted,
            mvcc_manager: Arc::new(Mutex::new(MVCCManager::new())),
            current_transaction_id: None,
        };
        db.transaction_manager = Some(TransactionManager::create(
            db.clone(),
            WriteAheadLog::new_in_memory()?,
        ));
        Ok(db)
    }
}

impl OnDiskReefDB {
    pub fn create_on_disk(kv_path: String, _index_path: String) -> Result<Self, ReefDBError> {
        let mut db = ReefDB::<storage::disk::OnDiskStorage, fts::default::DefaultSearchIdx>::create_with_args(
            storage::disk::OnDiskStorage::new(kv_path.clone()),
            Default::default(),
        );
        db.transaction_manager = Some(TransactionManager::create(
            db.clone(),
            WriteAheadLog::new(PathBuf::from(kv_path + ".wal"))?,
        ));
        Ok(db)
    }
}

#[derive(Clone)]
pub struct ReefDB<S: Storage + IndexManager + Clone + Any, FTS: Search + Clone>
where
    FTS::NewArgs: Clone + Default,
{
    pub(crate) tables: TableStorage,
    pub(crate) inverted_index: FTS,
    pub(crate) storage: S,
    pub(crate) transaction_manager: Option<TransactionManager<S, FTS>>,
    pub(crate) data_dir: Option<PathBuf>,
    pub(crate) autocommit: bool,
    pub(crate) autocommit_isolation_level: IsolationLevel,
    pub(crate) mvcc_manager: Arc<Mutex<MVCCManager>>,
    pub(crate) current_transaction_id: Option<u64>,
}

impl<S: Storage + IndexManager + Clone + Any, FTS: Search + Clone> ReefDB<S, FTS>
where
    FTS::NewArgs: Clone + Default,
{
    fn create_with_args(storage: S, fts_args: FTS::NewArgs) -> Self {
        let mut db = ReefDB {
            tables: TableStorage::new(),
            inverted_index: FTS::new(fts_args),
            storage,
            transaction_manager: None,
            data_dir: None,
            autocommit: true,
            autocommit_isolation_level: IsolationLevel::ReadCommitted,
            mvcc_manager: Arc::new(Mutex::new(MVCCManager::new())),
            current_transaction_id: None,
        };

        let transaction_manager = Some(TransactionManager::create(
            db.clone(),
            WriteAheadLog::new_in_memory().unwrap(),
        ));
        db.transaction_manager = transaction_manager;
        db
    }


    fn verify_table_exists(&self, table_name: &str) -> Result<(), ReefDBError> {
        if !self.storage.table_exists(table_name) {
            return Err(ReefDBError::TableNotFound(table_name.to_string()));
        }
        Ok(())
    }

    fn get_table_schema(&self, table_name: &str) -> Result<&(Vec<ColumnDef>, Vec<Vec<DataValue>>), ReefDBError> {
        self.storage.get_table_ref(table_name)
            .ok_or_else(|| ReefDBError::TableNotFound(table_name.to_string()))
    }

    fn handle_create(&mut self, name: String, columns: Vec<ColumnDef>) -> Result<ReefDBResult, ReefDBError> {
        if columns.is_empty() {
            return Err(ReefDBError::Other("Cannot create table with empty column list".to_string()));
        }
        
        // Check if table exists in either storage or tables
        if self.storage.table_exists(&name) || self.tables.table_exists(&name) {
            return Err(ReefDBError::Other(format!("Table {} already exists", name)));
        }
        
        // Create table in both storage and tables
        self.storage.insert_table(name.clone(), columns.clone(), vec![]);
        self.tables.insert_table(name.clone(), columns.clone(), vec![]);

        // Register FTS columns with the inverted index
        for column in columns.iter() {
            if column.data_type == DataType::TSVector {
                self.inverted_index.add_column(&name, &column.name);
            }
        }

        // Ensure the table was created successfully
        if !self.storage.table_exists(&name) || !self.tables.table_exists(&name) {
            return Err(ReefDBError::Other("Failed to create table".to_string()));
        }

        Ok(ReefDBResult::CreateTable)
    }

    fn handle_insert(&mut self, table_name: String, values: Vec<DataValue>) -> Result<ReefDBResult, ReefDBError> {
        // First, collect all the information we need
        let schema = {
            let (schema, _) = self.get_table_schema(&table_name)?;
            schema.clone()
        };

        // Validate number of values matches number of columns
        if values.len() != schema.len() {
            return Err(ReefDBError::Other(format!(
                "Number of values ({}) does not match number of columns ({})",
                values.len(),
                schema.len()
            )));
        }

        // Validate value types match column types
        for (value, column) in values.iter().zip(schema.iter()) {
            if !value.matches_type(&column.data_type) {
                return Err(ReefDBError::Other(format!(
                    "Value type mismatch for column {}: expected {:?}, got {:?}",
                    column.name,
                    column.data_type,
                    value
                )));
            }
        }

        // Insert the values into both storage and tables
        let row_id = self.storage.push_value(&table_name, values.clone())?;
        self.tables.push_value(&table_name, values.clone())?;

        // Update FTS index for any FTS columns
        for (i, col) in schema.iter().enumerate() {
            if col.data_type == DataType::TSVector {
                if let DataValue::Text(text) = &values[i] {
                    self.inverted_index.add_document(&table_name, &col.name, row_id, text);
                }
            }
        }

        Ok(ReefDBResult::Insert(row_id))
    }

    fn handle_select(
        &self,
        table_ref: TableReference,
        columns: Vec<Column>,
        where_clause: Option<WhereType>,
        joins: Vec<JoinClause>,
    ) -> Result<ReefDBResult, ReefDBError> {
        self.verify_table_exists(&table_ref.name)?;
        let (schema, data) = self.get_table_schema(&table_ref.name)?;
        
        let mut result = Vec::new();
        if joins.is_empty() {
            self.handle_simple_select(&table_ref.name, schema, data, &columns, where_clause, &mut result)?;
        } else {
            self.handle_join_select(&table_ref.name, schema, data, &columns, where_clause, &joins, &mut result)?;
        }
        
        Ok(ReefDBResult::Select(result))
    }

    fn handle_simple_select(
        &self,
        table_name: &str,
        schema: &Vec<ColumnDef>,
        data: &Vec<Vec<DataValue>>,
        columns: &[Column],
        where_clause: Option<WhereType>,
        result: &mut Vec<(usize, Vec<DataValue>)>,
    ) -> Result<(), ReefDBError> {
        for (i, row) in data.iter().enumerate() {
            let include_row = if let Some(where_clause) = &where_clause {
                self.evaluate_where_clause(where_clause, row, &[], schema, &[], table_name)?
            } else {
                true
            };

            if include_row {
                let mut selected_values = Vec::new();
                for col in columns {
                    if col.name == "*" {
                        selected_values.extend(row.iter().cloned());
                    } else {
                        let col_idx = schema.iter()
                            .position(|c| c.name == col.name)
                            .ok_or_else(|| ReefDBError::ColumnNotFound(col.name.clone()))?;
                        selected_values.push(row[col_idx].clone());
                    }
                }
                result.push((i, selected_values));
            }
        }
        Ok(())
    }

    fn handle_join_select(
        &self,
        table_name: &str,
        schema: &Vec<ColumnDef>,
        data: &Vec<Vec<DataValue>>,
        columns: &[Column],
        where_clause: Option<WhereType>,
        joins: &[JoinClause],
        result: &mut Vec<(usize, Vec<DataValue>)>,
    ) -> Result<(), ReefDBError> {
        for join in joins {
            if let Some((join_schema, join_data)) = self.storage.get_table_ref(&join.table_ref.name) {
                let left_col_idx = schema.iter()
                    .position(|c| c.name == join.on.0.column_name)
                    .ok_or_else(|| ReefDBError::ColumnNotFound(join.on.0.column_name.clone()))?;
                let right_col_idx = join_schema.iter()
                    .position(|c| c.name == join.on.1.column_name)
                    .ok_or_else(|| ReefDBError::ColumnNotFound(join.on.1.column_name.clone()))?;

                for (i, row) in data.iter().enumerate() {
                    for join_row in join_data.iter() {
                        if row[left_col_idx] == join_row[right_col_idx] {
                            let include_row = if let Some(where_clause) = &where_clause {
                                self.evaluate_where_clause(where_clause, row, join_row, schema, join_schema, table_name)?
                            } else {
                                true
                            };

                            if include_row {
                                let mut selected_values = Vec::new();
                                for col in columns {
                                    if col.name == "*" {
                                        selected_values.extend(row.iter().cloned());
                                        selected_values.extend(join_row.iter().cloned());
                                    } else {
                                        let value = if let Some(table) = &col.table {
                                            if table == &join.table_ref.name {
                                                if let Some(idx) = join_schema.iter().position(|c| c.name == col.name) {
                                                    join_row[idx].clone()
                                                } else {
                                                    continue;
                                                }
                                            } else {
                                                if let Some(idx) = schema.iter().position(|c| c.name == col.name) {
                                                    row[idx].clone()
                                                } else {
                                                    continue;
                                                }
                                            }
                                        } else {
                                            if let Some(idx) = schema.iter().position(|c| c.name == col.name) {
                                                row[idx].clone()
                                            } else if let Some(idx) = join_schema.iter().position(|c| c.name == col.name) {
                                                join_row[idx].clone()
                                            } else {
                                                continue;
                                            }
                                        };
                                        selected_values.push(value);
                                    }
                                }
                                result.push((i, selected_values));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn evaluate_where_clause(
        &self,
        where_clause: &WhereType,
        row: &[DataValue],
        join_row: &[DataValue],
        schema: &[ColumnDef],
        join_schema: &[ColumnDef],
        main_table: &str,
    ) -> Result<bool, ReefDBError> {
        match where_clause {
            WhereType::Regular(clause) => {
                let (col_idx, row_to_check) = if let Some(table) = &clause.table {
                    if table == main_table {
                        let idx = schema.iter()
                            .position(|c| c.name == clause.col_name)
                            .ok_or_else(|| ReefDBError::ColumnNotFound(format!("{}.{}", table, clause.col_name)))?;
                        (idx, row)
                    } else {
                        let idx = join_schema.iter()
                            .position(|c| c.name == clause.col_name)
                            .ok_or_else(|| ReefDBError::ColumnNotFound(format!("{}.{}", table, clause.col_name)))?;
                        (idx, join_row)
                    }
                } else {
                    // If no table is specified, try both schemas in order
                    if let Some(idx) = schema.iter().position(|c| c.name == clause.col_name) {
                        (idx, row)
                    } else if let Some(idx) = join_schema.iter().position(|c| c.name == clause.col_name) {
                        (idx, join_row)
                    } else {
                        return Err(ReefDBError::ColumnNotFound(clause.col_name.clone()));
                    }
                };

                Ok(clause.operator.evaluate(&row_to_check[col_idx], &clause.value))
            }
            WhereType::FTS(clause) => {
                let table_name = if let Some(table) = &clause.column.table {
                    table
                } else {
                    main_table
                };
                let col_name = &clause.column.name;
                let query = &clause.query.text;
                
                // Get the row ID from the current row
                let row_id = match row.first()
                    .ok_or_else(|| ReefDBError::Other("Row is empty".to_string()))? {
                    DataValue::Integer(id) => *id,
                    _ => return Err(ReefDBError::Other("First column is not an integer".to_string())),
                };
                
                // Search the inverted index
                let results = self.inverted_index.search(table_name, col_name, query);
                
                Ok(results.contains(&(row_id as usize)))
            }
            WhereType::And(left, right) => {
                let left_result = self.evaluate_where_clause(left, row, join_row, schema, join_schema, main_table)?;
                let right_result = self.evaluate_where_clause(right, row, join_row, schema, join_schema, main_table)?;
                Ok(left_result && right_result)
            }
            WhereType::Or(left, right) => {
                let left_result = self.evaluate_where_clause(left, row, join_row, schema, join_schema, main_table)?;
                let right_result = self.evaluate_where_clause(right, row, join_row, schema, join_schema, main_table)?;
                Ok(left_result || right_result)
            }
        }
    }

    fn handle_update(
        &mut self,
        table_name: String,
        updates: Vec<(String, DataValue)>,
        where_clause: Option<WhereType>,
    ) -> Result<ReefDBResult, ReefDBError> {
        self.verify_table_exists(&table_name)?;
        let (schema, _) = self.get_table_schema(&table_name)?;

        // Validate update columns exist and value types match
        for (col_name, value) in &updates {
            let column = schema.iter()
                .find(|c| &c.name == col_name)
                .ok_or_else(|| ReefDBError::ColumnNotFound(col_name.clone()))?;

            if !value.matches_type(&column.data_type) {
                return Err(ReefDBError::Other(format!(
                    "Value type mismatch for column {}: expected {:?}, got {:?}",
                    col_name,
                    column.data_type,
                    value
                )));
            }
        }

        // Validate where clause column exists if present
        if let Some(where_clause) = &where_clause {
            self.validate_where_clause(where_clause, &schema)?;
        }

        // Convert WhereType to simple where clause for storage layer
        let storage_where = where_clause.and_then(|w| match w {
            WhereType::Regular(clause) => Some((clause.col_name, clause.value)),
            WhereType::FTS(_) => None, // FTS not supported for updates
            WhereType::And(_, _) => None, // Complex conditions not supported for updates
            WhereType::Or(_, _) => None, // Complex conditions not supported for updates
        });

        let updated_count = self.storage.update_table(&table_name, updates, storage_where);
        Ok(ReefDBResult::Update(updated_count))
    }

    fn validate_where_clause(&self, where_clause: &WhereType, schema: &[ColumnDef]) -> Result<(), ReefDBError> {
        match where_clause {
            WhereType::Regular(clause) => {
                if !schema.iter().any(|c| c.name == clause.col_name) {
                    return Err(ReefDBError::ColumnNotFound(clause.col_name.clone()));
                }
            }
            WhereType::FTS(clause) => {
                if !schema.iter().any(|c| c.name == clause.column.name) {
                    return Err(ReefDBError::ColumnNotFound(clause.column.name.clone()));
                }
            }
            WhereType::And(left, right) => {
                self.validate_where_clause(left, schema)?;
                self.validate_where_clause(right, schema)?;
            }
            WhereType::Or(left, right) => {
                self.validate_where_clause(left, schema)?;
                self.validate_where_clause(right, schema)?;
            }
        }
        Ok(())
    }

    fn handle_delete(
        &mut self,
        table_name: String,
        where_clause: Option<WhereType>,
    ) -> Result<ReefDBResult, ReefDBError> {
        self.verify_table_exists(&table_name)?;
        let (schema, _) = self.get_table_schema(&table_name)?;

        // Validate where clause column exists if present
        if let Some(where_clause) = &where_clause {
            self.validate_where_clause(where_clause, &schema)?;
        }

        // Convert WhereType to simple where clause for storage layer
        let storage_where = where_clause.and_then(|w| match w {
            WhereType::Regular(clause) => Some((clause.col_name, clause.value)),
            WhereType::FTS(_) => None, // FTS not supported for deletes
            WhereType::And(_, _) => None, // Complex conditions not supported for deletes
            WhereType::Or(_, _) => None, // Complex conditions not supported for deletes
        });

        let deleted_count = self.storage.delete_table(&table_name, storage_where);
        Ok(ReefDBResult::Delete(deleted_count))
    }

    fn handle_alter(&mut self, table_name: String, alter_type: AlterType) -> Result<ReefDBResult, ReefDBError> {
        self.verify_table_exists(&table_name)?;
        let (schema, _) = self.get_table_schema(&table_name)?;

        match alter_type {
            AlterType::AddColumn(column_def) => {
                // Verify column doesn't already exist
                if schema.iter().any(|c| c.name == column_def.name) {
                    return Err(ReefDBError::Other(
                        format!("Column {} already exists in table {}", column_def.name, table_name)
                    ));
                }

                self.storage.add_column(&table_name, column_def)?;
            },
            AlterType::DropColumn(column_name) => {
                self.storage.drop_column(&table_name, &column_name)?;
            },
            AlterType::RenameColumn(old_name, new_name) => {
                // Verify new name doesn't already exist
                if schema.iter().any(|c| c.name == new_name) {
                    return Err(ReefDBError::Other(
                        format!("Column {} already exists in table {}", new_name, table_name)
                    ));
                }

                self.storage.rename_column(&table_name, &old_name, &new_name)?;
            }
        }

        Ok(ReefDBResult::AlterTable)
    }

    fn handle_drop(&mut self, table_name: String) -> Result<ReefDBResult, ReefDBError> {
        self.verify_table_exists(&table_name)?;
        self.storage.drop_table(&table_name);
        self.tables.drop_table(&table_name);
        Ok(ReefDBResult::DropTable)
    }

    fn handle_create_index(&mut self, stmt: CreateIndexStatement) -> Result<ReefDBResult, ReefDBError> {
        self.verify_table_exists(&stmt.table_name)?;
        let (schema, _) = self.get_table_schema(&stmt.table_name)?;

        // Verify column exists
        if !schema.iter().any(|c| c.name == stmt.column_name) {
            return Err(ReefDBError::ColumnNotFound(stmt.column_name));
        }

        // Create B-Tree index
        let btree = BTreeIndex::new();
        self.storage.create_index(&stmt.table_name, &stmt.column_name, IndexType::BTree(btree));

        Ok(ReefDBResult::CreateIndex)
    }

    fn handle_drop_index(&mut self, stmt: DropIndexStatement) -> Result<ReefDBResult, ReefDBError> {
        self.verify_table_exists(&stmt.table_name)?;
        let (schema, _) = self.get_table_schema(&stmt.table_name)?;

        // Verify column exists
        if !schema.iter().any(|c| c.name == stmt.column_name) {
            return Err(ReefDBError::ColumnNotFound(stmt.column_name));
        }

        // Drop the index
        self.storage.drop_index(&stmt.table_name, &stmt.column_name);

        Ok(ReefDBResult::DropIndex)
    }

    fn handle_savepoint(&mut self, name: String) -> Result<ReefDBResult, ReefDBError> {
        if let Some(tx_id) = self.current_transaction_id {
            if let Some(tm) = &mut self.transaction_manager {
                tm.create_savepoint(tx_id, name)?;
                Ok(ReefDBResult::Savepoint)
            } else {
                Err(ReefDBError::Other("Transaction manager not initialized".to_string()))
            }
        } else {
            Err(ReefDBError::TransactionNotActive)
        }
    }

    fn handle_rollback_to_savepoint(&mut self, name: String) -> Result<ReefDBResult, ReefDBError> {
        if let Some(tx_id) = self.current_transaction_id {
            if let Some(tm) = &mut self.transaction_manager {
                let restored_state = tm.rollback_to_savepoint(tx_id, &name)?;
                
                // First clear both states
                self.tables = TableStorage::new();
                self.storage.clear();
                
                // Then restore from the savepoint state
                for (table_name, (columns, rows)) in restored_state.tables.iter() {
                    // Create the table in both storage and tables
                    self.storage.insert_table(table_name.clone(), columns.clone(), rows.clone());
                    self.tables.insert_table(table_name.clone(), columns.clone(), rows.clone());
                }
                
                Ok(ReefDBResult::RollbackToSavepoint)
            } else {
                Err(ReefDBError::Other("Transaction manager not initialized".to_string()))
            }
        } else {
            Err(ReefDBError::TransactionNotActive)
        }
    }

    fn handle_release_savepoint(&mut self, name: String) -> Result<ReefDBResult, ReefDBError> {
        if let Some(tx_id) = self.current_transaction_id {
            if let Some(tm) = &mut self.transaction_manager {
                tm.release_savepoint(tx_id, &name)?;
                Ok(ReefDBResult::ReleaseSavepoint)
            } else {
                Err(ReefDBError::Other("Transaction manager not initialized".to_string()))
            }
        } else {
            Err(ReefDBError::TransactionNotActive)
        }
    }

    fn handle_begin_transaction(&mut self) -> Result<ReefDBResult, ReefDBError> {
        if let Some(tm) = &mut self.transaction_manager {
            let tx_id = tm.begin_transaction(IsolationLevel::Serializable)?;
            self.current_transaction_id = Some(tx_id);
            Ok(ReefDBResult::BeginTransaction)
        } else {
            Err(ReefDBError::Other("Transaction manager not initialized".to_string()))
        }
    }

    fn handle_commit(&mut self) -> Result<ReefDBResult, ReefDBError> {
        if let Some(tx_id) = self.current_transaction_id {
            if let Some(tm) = &mut self.transaction_manager {
                tm.commit_transaction(tx_id)?;
                self.current_transaction_id = None;
                Ok(ReefDBResult::Commit)
            } else {
                Err(ReefDBError::Other("Transaction manager not initialized".to_string()))
            }
        } else {
            Err(ReefDBError::TransactionNotActive)
        }
    }

    pub fn execute_statement(&mut self, stmt: Statement) -> Result<ReefDBResult, ReefDBError> {
        match stmt {
            Statement::Create(CreateStatement::Table(name, columns)) => {
                self.handle_create(name, columns)
            },
            Statement::Select(SelectStatement::FromTable(table_name, columns, where_clause, joins)) => {
                self.handle_select(table_name, columns, where_clause, joins)
            },
            Statement::Insert(InsertStatement::IntoTable(table_name, values)) => {
                self.handle_insert(table_name, values)
            },
            Statement::Update(UpdateStatement::UpdateTable(table_name, updates, where_clause)) => {
                self.handle_update(table_name, updates, where_clause)
            },
            Statement::Delete(DeleteStatement::FromTable(table_name, where_clause)) => {
                self.handle_delete(table_name, where_clause)
            },
            Statement::Alter(AlterStatement { table_name, alter_type }) => {
                self.handle_alter(table_name, alter_type)
            },
            Statement::Drop(DropStatement { table_name }) => {
                self.handle_drop(table_name)
            },
            Statement::CreateIndex(stmt) => {
                self.handle_create_index(stmt)
            },
            Statement::DropIndex(stmt) => {
                self.handle_drop_index(stmt)
            },
            Statement::Savepoint(sp_stmt) => {
                self.handle_savepoint(sp_stmt.name)
            },
            Statement::RollbackToSavepoint(name) => {
                self.handle_rollback_to_savepoint(name)
            },
            Statement::ReleaseSavepoint(name) => {
                self.handle_release_savepoint(name)
            },
            Statement::BeginTransaction => {
                self.handle_begin_transaction()
            },
            Statement::Commit => {
                self.handle_commit()
            },
        }
    }

    pub fn query(&mut self, sql: &str) -> Result<ReefDBResult, ReefDBError> {
        use crate::sql::parser::Parser;
        let stmt = Parser::parse_sql(sql)?;
        self.execute_statement(stmt)
    }
}
