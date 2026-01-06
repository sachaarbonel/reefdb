use std::any::Any;

use log::debug;

use crate::error::ReefDBError;
use crate::fts::search::Search;
use crate::indexes::index_manager::IndexManager;
use crate::key_format::KeyFormat;
use crate::result::{ColumnInfo, QueryResult, ReefDBResult};
use crate::sql::clauses::wheres::where_type::WhereType;
use crate::sql::data_value::DataValue;
use crate::sql::statements::{
    create::CreateStatement,
    delete::DeleteStatement,
    insert::InsertStatement,
    select::SelectStatement,
    update::UpdateStatement,
    Statement,
};
use crate::storage::Storage;
use crate::transaction::{IsolationLevel, TransactionState};

use super::{TransactionGuard, TransactionManager};

impl<S: Storage + IndexManager + Clone + Any, FTS: Search + Clone> TransactionManager<S, FTS>
where
    FTS::NewArgs: Clone,
{
    pub fn execute_statement(&mut self, transaction_id: u64, stmt: Statement) -> Result<ReefDBResult, ReefDBError> {
        match stmt {
            Statement::Create(create_stmt) => {
                let transaction = self.get_transaction(transaction_id)?;
                transaction.execute_statement(Statement::Create(create_stmt))
            }
            Statement::Insert(insert_stmt) => {
                let transaction = self.get_transaction(transaction_id)?;
                transaction.execute_statement(Statement::Insert(insert_stmt))
            }
            Statement::Update(UpdateStatement::UpdateTable(table_name, updates, where_clause)) => {
                // First get the transaction guard
                let mut guard = self.get_transaction_guard(transaction_id)?;

                // Handle serializable mode if needed
                if guard.isolation_level == IsolationLevel::Serializable {
                    let snapshot = guard.transaction.acid_manager.get_committed_snapshot();
                    let mut final_state = snapshot.clone();
                    final_state.restore_from(&guard.transaction.reef_db.tables);
                    guard.transaction.reef_db.tables.restore_from(&final_state);
                }

                // Get table data
                let table_data = guard.transaction.reef_db.storage.get_table_ref(&table_name)
                    .ok_or_else(|| ReefDBError::TableNotFound(table_name.clone()))?;
                let (schema, rows) = table_data.clone(); // Clone to avoid lifetime issues

                // Drop the guard before getting the MVCC manager
                drop(guard);

                // Now get the MVCC manager
                let mut mvcc_manager = self.mvcc_manager.lock()
                    .map_err(|_| ReefDBError::Other("Failed to acquire MVCC manager lock".to_string()))?;

                let mut updated_count = 0;

                // Process each row
                for row in rows {
                    // Get the ID from the first column (primary key)
                    let id = match &row[0] {
                        DataValue::Integer(n) => n.to_string(),
                        _ => continue,
                    };
                    let key = KeyFormat::row(&table_name, 0, &id);

                    // Check where clause
                    let should_update = if let Some(ref where_clause) = where_clause {
                        Self::evaluate_where_clause(
                            where_clause,
                            &row,
                            &schema,
                            &table_name,
                        )
                    } else {
                        true
                    };

                    if should_update {
                        // Create a new version with the updated values
                        let mut new_data = row.clone();
                        for (col_name, new_value) in &updates {
                            if let Some(col_idx) = schema.iter().position(|c| c.name == *col_name) {
                                new_data[col_idx] = new_value.clone();
                            }
                        }

                        // Write the new version using MVCC
                        mvcc_manager.write(transaction_id, key, new_data)?;
                        updated_count += 1;
                    }
                }

                Ok(ReefDBResult::Update(updated_count))
            }
            Statement::Delete(delete_stmt) => {
                let transaction = self.get_transaction(transaction_id)?;
                transaction.execute_statement(Statement::Delete(delete_stmt))
            }
            Statement::Drop(drop_stmt) => {
                let transaction = self.get_transaction(transaction_id)?;
                transaction.execute_statement(Statement::Drop(drop_stmt))
            }
            Statement::Select(SelectStatement::FromTable(table_ref, columns, where_clause, joins, order_by)) => {
                // First get the transaction guard and storage data
                let guard = self.get_transaction_guard(transaction_id)?;

                // Handle serializable mode if needed
                if guard.isolation_level == IsolationLevel::Serializable {
                    let snapshot = guard.transaction.acid_manager.get_committed_snapshot();
                    guard.transaction.reef_db.tables.restore_from(&snapshot);
                }

                // Get table data and clone what we need
                let table_data = guard.transaction.reef_db.storage.get_table_ref(&table_ref.name)
                    .ok_or_else(|| ReefDBError::TableNotFound(table_ref.name.clone()))?;
                let schema = table_data.0.to_vec();
                let rows = table_data.1.to_vec();
                let current_isolation_level = guard.isolation_level.clone();

                // Get all joined table data upfront
                let mut joined_tables = Vec::new();
                let mut joined_schemas = Vec::new();
                for join in joins.iter() {
                    let joined_table = guard.transaction.reef_db.storage.get_table_ref(&join.table_ref.name)
                        .ok_or_else(|| ReefDBError::TableNotFound(join.table_ref.name.clone()))?;
                    joined_schemas.push((join.table_ref.name.as_str(), joined_table.0.as_slice()));
                    joined_tables.push((join.clone(), (joined_table.0.to_vec(), joined_table.1.to_vec())));
                }

                // Create column info for all tables
                let column_info = if joins.is_empty() {
                    ColumnInfo::from_schema_and_columns(&schema, &columns, &table_ref.name)?
                } else {
                    ColumnInfo::from_joined_schemas(&schema, &table_ref.name, &joined_schemas, &columns)?
                };

                // Get the MVCC manager
                let mut mvcc_manager = self.mvcc_manager.lock()
                    .map_err(|_| ReefDBError::Other("Failed to acquire MVCC manager lock".to_string()))?;

                let mut results = Vec::new();

                // Process each row
                for (i, row) in rows.iter().enumerate() {
                    // Get the ID from the first column (primary key)
                    let id = match &row[0] {
                        DataValue::Integer(n) => n.to_string(),
                        _ => continue,
                    };
                    let key = KeyFormat::row(&table_ref.name, 0, &id);

                    // Read MVCC data - use read_committed to ensure we see committed changes
                    let data = if current_isolation_level == IsolationLevel::ReadCommitted {
                        match mvcc_manager.read_committed(transaction_id, &key)? {
                            Some(data) => data,
                            None => {
                                // If no committed version exists, check for uncommitted changes
                                match mvcc_manager.read_uncommitted(&key)? {
                                    Some(_) => row.clone(), // If there are uncommitted changes, use original row
                                    None => row.clone()     // If no changes at all, use original row
                                }
                            }
                        }
                    } else {
                        match mvcc_manager.read_committed(transaction_id, &key)? {
                            Some(data) => data,
                            None => row.clone()
                        }
                    };

                    // Handle joins if present
                    let mut matched_rows = vec![(data.clone(), schema.clone())];

                    for (join, (joined_schema, joined_rows)) in &joined_tables {
                        let mut new_matched_rows = Vec::new();

                        for (curr_row, curr_schema) in matched_rows {
                            for joined_row in joined_rows {
                                let should_join = Self::evaluate_join_condition(
                                    &join.on,
                                    &curr_row,
                                    &curr_schema,
                                    joined_row,
                                    joined_schema,
                                    &table_ref.name,
                                    &join.table_ref.name,
                                );

                                if should_join {
                                    let mut combined_row = curr_row.clone();
                                    combined_row.extend(joined_row.clone());

                                    let mut combined_schema = curr_schema.clone();
                                    combined_schema.extend(joined_schema.clone());

                                    // Check where clause on the complete joined data
                                    let should_include = if let Some(ref where_clause) = where_clause {
                                        let mut result = true;
                                        match where_clause {
                                            WhereType::Regular(clause) => {
                                                // Find the column in the schema
                                                let col_idx = if let Some(ref clause_table) = clause.table {
                                                    // If table is specified, find the correct schema section
                                                    let (schema_start, schema_len) = if clause_table == &table_ref.name {
                                                        (0, schema.len())
                                                    } else {
                                                        let mut start = schema.len();
                                                        let mut len = 0;
                                                        for (join_info, (join_schema, _)) in &joined_tables {
                                                            if &join_info.table_ref.name == clause_table {
                                                                len = join_schema.len();
                                                                break;
                                                            }
                                                            start += join_schema.len();
                                                        }
                                                        (start, len)
                                                    };

                                                    // Add safety check for schema boundaries
                                                    if schema_start >= combined_schema.len() {
                                                        None
                                                    } else {
                                                        let end = std::cmp::min(schema_start + schema_len, combined_schema.len());
                                                        combined_schema[schema_start..end]
                                                            .iter()
                                                            .position(|c| c.name == clause.col_name)
                                                            .map(|pos| schema_start + pos)
                                                    }
                                                } else {
                                                    // If no table specified, look in all columns
                                                    combined_schema.iter().position(|c| c.name == clause.col_name)
                                                };

                                                if let Some(idx) = col_idx {
                                                    result = clause.operator.evaluate(&combined_row[idx], &clause.value);
                                                } else {
                                                    result = false;
                                                }
                                            }
                                            WhereType::And(left, right) => {
                                                result = Self::evaluate_where_clause(left, &combined_row, &combined_schema, &table_ref.name) &&
                                                        Self::evaluate_where_clause(right, &combined_row, &combined_schema, &table_ref.name);
                                            }
                                            WhereType::Or(left, right) => {
                                                result = Self::evaluate_where_clause(left, &combined_row, &combined_schema, &table_ref.name) ||
                                                        Self::evaluate_where_clause(right, &combined_row, &combined_schema, &table_ref.name);
                                            }
                                            WhereType::FTS(_) => {
                                                result = false;
                                            }
                                        }
                                        result
                                    } else {
                                        true
                                    };

                                    if should_include {
                                        new_matched_rows.push((combined_row, combined_schema));
                                    }
                                }
                            }
                        }
                        matched_rows = new_matched_rows;
                    }

                    // Process each matched row
                    for (joined_data, _) in matched_rows {
                        results.push((i, joined_data));
                    }
                }

                // Sort results if order by clauses are present
                results = self.sort_results(results, &order_by, &schema, &table_ref.name, &joined_tables);

                // Project columns after sorting
                let projected_results = self.project_results(results, &columns, &schema, &table_ref.name, &joined_tables);

                Ok(ReefDBResult::Select(QueryResult::with_columns(projected_results, column_info)))
            }
            Statement::CreateIndex(create_index_stmt) => {
                let transaction = self.get_transaction(transaction_id)?;
                transaction.execute_statement(Statement::CreateIndex(create_index_stmt))
            }
            Statement::DropIndex(drop_index_stmt) => {
                let transaction = self.get_transaction(transaction_id)?;
                transaction.execute_statement(Statement::DropIndex(drop_index_stmt))
            }
            Statement::Alter(alter_stmt) => {
                let transaction = self.get_transaction(transaction_id)?;
                transaction.execute_statement(Statement::Alter(alter_stmt))
            }
            Statement::Savepoint(savepoint_stmt) => {
                self.create_savepoint(transaction_id, savepoint_stmt.name)?;
                Ok(ReefDBResult::Savepoint)
            }
            Statement::RollbackToSavepoint(name) => {
                self.rollback_to_savepoint(transaction_id, &name)?;
                Ok(ReefDBResult::RollbackToSavepoint)
            }
            Statement::ReleaseSavepoint(name) => {
                self.release_savepoint(transaction_id, &name)?;
                Ok(ReefDBResult::ReleaseSavepoint)
            }
            _ => {
                let transaction = self.get_transaction(transaction_id)?;
                transaction.execute_statement(stmt)
            }
        }
    }

    pub fn execute_statement_committed(&mut self, stmt: Statement) -> Result<ReefDBResult, ReefDBError> {
        let reef_db = self.reef_db.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire database lock".to_string()))?;

        match stmt {
            Statement::Select(SelectStatement::FromTable(table_ref, columns, where_clause, _joins, order_by)) => {
                let mvcc_manager = self.mvcc_manager.lock()
                    .map_err(|_| ReefDBError::Other("Failed to acquire MVCC manager lock".to_string()))?;

                // Get the table data
                let (schema, rows) = reef_db.storage.get_table_ref(&table_ref.name)
                    .ok_or_else(|| ReefDBError::TableNotFound(table_ref.name.clone()))?;

                debug!("MVCC Debug - Table {} has {} rows in storage", table_ref.name, rows.len());

                let mut results: Vec<(usize, Vec<DataValue>)> = Vec::new();
                for (i, row) in rows.iter().enumerate() {
                    // Get the ID from the first column (primary key)
                    let id = match &row[0] {
                        DataValue::Integer(n) => n.to_string(),
                        _ => continue, // Skip non-integer IDs
                    };
                    let key = KeyFormat::row(&table_ref.name, 0, &id);
                    debug!("MVCC Debug - Checking visibility for key: {}", key);
                    if let Ok(Some(data)) = mvcc_manager.read_committed(0, &key) {
                        debug!("MVCC Debug - Found visible version for key: {} with data: {:?}", key, data);

                        // First check if the row matches the where clause
                        let should_include = if let Some(ref where_clause) = where_clause {
                            debug!("MVCC Debug - Evaluating where clause: {:?}", where_clause);
                            debug!("MVCC Debug - Row data: {:?}", data);
                            debug!("MVCC Debug - Schema: {:?}", schema);
                            reef_db.evaluate_where_clause(
                                where_clause,
                                &data,  // Use the full row data for where clause evaluation
                                &[],    // No join row for simple select
                                schema,
                                &[],    // No join schema for simple select
                                &table_ref.name,
                            ).unwrap_or(false)
                        } else {
                            true
                        };

                        debug!("MVCC Debug - Row should be included: {}", should_include);

                        if should_include {
                            // If the row matches, then select the requested columns
                            let row_data = if columns.iter().any(|c| c.name != "*") {
                                let mut selected_data = Vec::new();
                                for col in &columns {
                                    if col.name == "*" {
                                        // Include all columns
                                        selected_data = data.clone();
                                        break;
                                    }
                                    if let Some(idx) = schema.iter().position(|c| c.name == col.name) {
                                        selected_data.push(data[idx].clone());
                                    }
                                }
                                selected_data
                            } else {
                                // If no specific columns or only * is specified, include all columns
                                data.clone()
                            };

                            debug!("MVCC Debug - Including row in results: {:?}", row_data);
                            results.push((i, row_data));
                        }
                    }
                }

                // Sort results if order by clauses are present
                results = self.sort_results(results, &order_by, schema, &table_ref.name, &[]);

                debug!("MVCC Debug - Final results count: {}", results.len());
                let column_infos = ColumnInfo::from_schema_and_columns(&schema, &columns, &table_ref.name)?;
                Ok(ReefDBResult::Select(QueryResult::with_columns(results, column_infos)))
            },
            _ => Err(ReefDBError::Other("Only SELECT statements are supported in read committed mode".to_string())),
        }
    }

    fn try_execute_with_retry(&mut self, transaction_id: u64, stmt: Statement, max_retries: u32) -> Result<ReefDBResult, ReefDBError> {
        if !self.mvcc_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire MVCC manager lock".to_string()))?
            .is_active(transaction_id)
        {
            return Err(ReefDBError::TransactionNotActive);
        }

        let mut retries = 0;
        loop {
            match self.execute_statement_internal(transaction_id, stmt.clone()) {
                Ok(result) => return Ok(result),
                Err(ReefDBError::Deadlock) if retries < max_retries => {
                    // On deadlock, wait briefly with exponential backoff and retry
                    std::thread::sleep(std::time::Duration::from_millis(10 * (1 << retries)));
                    retries += 1;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn execute_statement_internal(&mut self, transaction_id: u64, stmt: Statement) -> Result<ReefDBResult, ReefDBError> {
        // Check transaction state first
        let transaction = self.active_transactions.get(&transaction_id)
            .ok_or_else(|| ReefDBError::TransactionNotFound(transaction_id))?;

        if transaction.get_state() != &TransactionState::Active {
            return Err(ReefDBError::TransactionNotActive);
        }

        let isolation_level = transaction.get_isolation_level().clone();
        drop(transaction);

        // First acquire any needed locks based on the statement type
        match &stmt {
            Statement::Insert(InsertStatement::IntoTable(table_name, _)) => {
                self.acquire_lock(transaction_id, table_name, crate::locks::LockType::Exclusive)?;
            }
            Statement::Update(UpdateStatement::UpdateTable(table_name, _, _)) => {
                self.acquire_lock(transaction_id, table_name, crate::locks::LockType::Exclusive)?;
            }
            Statement::Delete(DeleteStatement::FromTable(table_name, _)) => {
                self.acquire_lock(transaction_id, table_name, crate::locks::LockType::Exclusive)?;
            }
            Statement::Create(CreateStatement::Table(table_name, _)) => {
                self.acquire_lock(transaction_id, table_name, crate::locks::LockType::Exclusive)?;
            }
            Statement::Select(SelectStatement::FromTable(table_ref, _, _, _,_)) => {
                // For serializable isolation, we need shared locks to prevent phantom reads
                // But with MVCC, we don't need to acquire locks for reads since each transaction
                // sees its own snapshot of the data
                if isolation_level == IsolationLevel::Serializable && !self.mvcc_manager.lock()
                    .map_err(|_| ReefDBError::Other("Failed to acquire MVCC manager lock".to_string()))?
                    .is_active(transaction_id) {
                    self.acquire_lock(transaction_id, &table_ref.name, crate::locks::LockType::Shared)?;
                }
            }
            _ => {}
        }

        // Get transaction again for execution
        let transaction = self.active_transactions.get_mut(&transaction_id)
            .ok_or_else(|| ReefDBError::TransactionNotFound(transaction_id))?;

        // For serializable mode, ensure we're using the correct snapshot
        // from the start of the transaction for all operations
        if isolation_level == IsolationLevel::Serializable {
            // Get our snapshot from the start of the transaction
            let snapshot = transaction.acid_manager.get_committed_snapshot();

            // For SELECT statements, we want to see the snapshot from when the transaction started
            match &stmt {
                Statement::Select(SelectStatement::FromTable(_, _, _, _,_)) => {
                    transaction.reef_db.tables.restore_from(&snapshot);
                }
                _ => {
                    // For other statements, we want to see our own changes plus the snapshot
                    let mut final_state = snapshot.clone();
                    final_state.restore_from(&transaction.reef_db.tables);
                    transaction.reef_db.tables.restore_from(&final_state);
                }
            }
        }

        transaction.execute_statement(stmt)
    }

    fn get_transaction_guard(&mut self, transaction_id: u64) -> Result<TransactionGuard<S, FTS>, ReefDBError> {
        let transaction = self.get_transaction_mut(transaction_id)?;
        let isolation_level = transaction.get_isolation_level();
        Ok(TransactionGuard {
            transaction,
            isolation_level,
        })
    }
}
