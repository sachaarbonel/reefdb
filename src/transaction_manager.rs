use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::fts::search::Search;

use crate::{
    deadlock::DeadlockDetector,
    error::ReefDBError,
    indexes::{
        index_manager::IndexManager,
       
    },
    key_format::KeyFormat,
    locks::LockManager,
    locks::LockType,
    mvcc::MVCCManager,
    result::ReefDBResult,
    savepoint::SavepointManager,
    sql::{
        clauses::{
            join_clause::JoinClause,
            wheres::where_type::WhereType,
        },
        column::Column,
        column_def::ColumnDef,
        column_value_pair::ColumnValuePair,
        data_value::DataValue,
        statements::{
            alter::AlterStatement,
            create::CreateStatement,
            create_index::CreateIndexStatement,
            delete::DeleteStatement,
            drop::DropStatement,
            drop_index::DropIndexStatement,
            insert::InsertStatement,
            select::SelectStatement,
            update::UpdateStatement,
            Statement,
        },
    },
    storage::{
        memory::InMemoryStorage,
        Storage,
        TableStorage,
    },
    transaction::{
        Transaction,
        IsolationLevel,
        TransactionState,
    },
    wal::{WriteAheadLog, WALEntry, WALOperation},
    ReefDB,
};

#[derive(Clone)]
pub struct TransactionManager<S: Storage + IndexManager + Clone + Any, FTS: Search + Clone>
where
    FTS::NewArgs: Clone,
{
    active_transactions: HashMap<u64, Transaction<S, FTS>>,
    lock_manager: Arc<Mutex<LockManager>>,
    wal: Arc<Mutex<WriteAheadLog>>,
    reef_db: Arc<Mutex<ReefDB<S, FTS>>>,
    mvcc_manager: Arc<Mutex<MVCCManager>>,
    deadlock_detector: Arc<Mutex<DeadlockDetector>>,
    savepoint_manager: Arc<Mutex<SavepointManager>>,
}

// Helper structs
struct TransactionGuard<'a, S, FTS>
where
    S: Storage + IndexManager + Clone + Any,
    FTS: Search + Clone,
    FTS::NewArgs: Clone,
{
    transaction: &'a mut Transaction<S, FTS>,
    isolation_level: IsolationLevel,
}

impl<S: Storage + IndexManager + Clone + Any, FTS: Search + Clone> TransactionManager<S, FTS>
where
    FTS::NewArgs: Clone,
{
    pub fn create(reef_db: ReefDB<S, FTS>, wal: WriteAheadLog) -> Self {
        TransactionManager {
            active_transactions: HashMap::new(),
            lock_manager: Arc::new(Mutex::new(LockManager::new())),
            wal: Arc::new(Mutex::new(wal)),
            reef_db: Arc::new(Mutex::new(reef_db.clone())),
            mvcc_manager: reef_db.mvcc_manager.clone(),
            deadlock_detector: Arc::new(Mutex::new(DeadlockDetector::new())),
            savepoint_manager: Arc::new(Mutex::new(SavepointManager::new())),
        }
    }

    pub fn begin_transaction(&mut self, isolation_level: IsolationLevel) -> Result<u64, ReefDBError> {
        let reef_db = self.reef_db.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire database lock".to_string()))?;
        
        let transaction = Transaction::create((*reef_db).clone(), isolation_level);
        let id = transaction.get_id();
        
        // Initialize MVCC timestamp for the transaction
        self.mvcc_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire MVCC manager lock".to_string()))?
            .begin_transaction(id);
        
        self.active_transactions.insert(id, transaction);
        Ok(id)
    }

    pub fn commit_transaction(&mut self, id: u64) -> Result<(), ReefDBError> {
        let mut transaction = self.active_transactions.remove(&id)
            .ok_or_else(|| ReefDBError::Other("Transaction not found".to_string()))?;
        
        if transaction.get_state() != &TransactionState::Active {
            return Err(ReefDBError::Other("Transaction is not active".to_string()));
        }

        // Get the final transaction state before commit
        let final_state = transaction.get_table_state();

        // Write to WAL before committing
        let wal_entry = WALEntry {
            transaction_id: id,
            timestamp: std::time::SystemTime::now(),
            operation: WALOperation::Commit,
            table_name: String::new(),
            data: vec![],
        };

        self.wal.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire WAL lock".to_string()))?
            .append_entry(wal_entry)?;

        // Commit MVCC changes first
        let commit_result = self.mvcc_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire MVCC manager lock".to_string()))?
            .commit(id);

        if let Err(e) = commit_result {
            // If MVCC commit fails, rollback the transaction
            self.rollback_transaction(id)?;
            return Err(e);
        }

        // Only update the database state after MVCC commit succeeds
        let mut reef_db = self.reef_db.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire database lock".to_string()))?;
        
        // Update database state with final transaction state
        reef_db.tables.restore_from(&final_state);
        
        // Commit the transaction
        transaction.commit(&mut reef_db)?;

        // Release locks and remove from deadlock detector
        self.lock_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire lock manager".to_string()))?
            .release_transaction_locks(id);
        
        self.deadlock_detector.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire deadlock detector".to_string()))?
            .remove_transaction(id);

        Ok(())
    }

    pub fn rollback_transaction(&mut self, id: u64) -> Result<(), ReefDBError> {
        let mut transaction = self.active_transactions.remove(&id)
            .ok_or_else(|| ReefDBError::Other("Transaction not found".to_string()))?;

        let mut reef_db = self.reef_db.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire database lock".to_string()))?;
        
        transaction.rollback(&mut reef_db)?;

        // Rollback MVCC changes
        self.mvcc_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire MVCC manager lock".to_string()))?
            .rollback(id);

        // Release locks and remove from deadlock detector
        self.lock_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire lock manager".to_string()))?
            .release_transaction_locks(id);
        
        self.deadlock_detector.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire deadlock detector".to_string()))?
            .remove_transaction(id);

        // Clear savepoints for this transaction
        let mut savepoint_manager = self.savepoint_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire savepoint manager lock".to_string()))?;
        savepoint_manager.clear_transaction_savepoints(id);

        Ok(())
    }

    pub fn acquire_lock(&self, transaction_id: u64, table_name: &str, lock_type: LockType) -> Result<(), ReefDBError> {
        let mut lock_manager = self.lock_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire lock manager".to_string()))?;
        
        // Check for deadlocks before acquiring lock
        let mut deadlock_detector = self.deadlock_detector.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire deadlock detector".to_string()))?;
        
        // Get current lock holders for this table
        let lock_holders = lock_manager.get_lock_holders(table_name);
        
        // If there are existing locks and we don't already have a lock, add wait-for edges
        if !lock_holders.is_empty() && !lock_manager.has_lock(transaction_id, table_name) {
            for holder_id in lock_holders {
                if holder_id != transaction_id {
                    deadlock_detector.add_wait(transaction_id, holder_id, table_name.to_string());
                    
                    // Check for deadlocks
                    let active_txs: Vec<&Transaction<S, FTS>> = self.active_transactions.values().collect();
                    if let Some(victim_tx) = deadlock_detector.detect_deadlock(&active_txs) {
                        if victim_tx == transaction_id {
                            // Remove the wait edge since we're aborting
                            deadlock_detector.remove_transaction(transaction_id);
                            return Err(ReefDBError::Deadlock);
                        }
                    }
                }
            }
        }
        
        // Try to acquire the lock
        match lock_manager.acquire_lock(transaction_id, table_name, lock_type) {
            Ok(()) => {
                // Successfully acquired lock, remove any wait edges
                deadlock_detector.remove_transaction(transaction_id);
                Ok(())
            }
            Err(e) => {
                // Failed to acquire lock, remove any wait edges
                deadlock_detector.remove_transaction(transaction_id);
                Err(e)
            }
        }
    }

    pub fn create_savepoint(&mut self, transaction_id: u64, name: String) -> Result<(), ReefDBError> {
        let transaction = self.active_transactions.get(&transaction_id)
            .ok_or_else(|| ReefDBError::TransactionNotFound(transaction_id))?;
        
        if transaction.get_state() != &TransactionState::Active {
            return Err(ReefDBError::TransactionNotActive);
        }
        
        // Get the transaction's current state
        let table_state = transaction.get_table_state();
        
        // Create the savepoint with this state
        self.savepoint_manager.lock()
            .map_err(|_| ReefDBError::LockAcquisitionFailed("Failed to acquire savepoint manager lock".to_string()))?
            .create_savepoint(transaction_id, name, table_state)?;
        
        Ok(())
    }

    pub fn rollback_to_savepoint(&mut self, transaction_id: u64, name: &str) -> Result<TableStorage, ReefDBError> {
        let transaction = self.active_transactions.get_mut(&transaction_id)
            .ok_or_else(|| ReefDBError::TransactionNotFound(transaction_id))?;
        
        if transaction.get_state() != &TransactionState::Active {
            return Err(ReefDBError::TransactionNotActive);
        }
        
        // Get the savepoint state
        let restored_state = self.savepoint_manager.lock()
            .map_err(|_| ReefDBError::LockAcquisitionFailed("Failed to acquire savepoint manager lock".to_string()))?
            .rollback_to_savepoint(transaction_id, name)?;
        
        // Update transaction's state
        transaction.restore_table_state(&restored_state);
        
        // Update database state
        let mut reef_db = self.reef_db.lock()
            .map_err(|_| ReefDBError::LockAcquisitionFailed("Failed to acquire database lock".to_string()))?;
        reef_db.tables.restore_from(&restored_state);
        
        // Update storage state
        for (table_name, (columns, rows)) in restored_state.tables.iter() {
            reef_db.storage.insert_table(table_name.clone(), columns.clone(), rows.clone());
        }
        
        // Write WAL entry for rollback
        let wal_entry = WALEntry {
            transaction_id,
            timestamp: std::time::SystemTime::now(),
            operation: WALOperation::Rollback,
            table_name: String::new(),
            data: vec![],
        };
        
        self.wal.lock()
            .map_err(|_| ReefDBError::LockAcquisitionFailed("Failed to acquire WAL lock".to_string()))?
            .append_entry(wal_entry)?;
        
        Ok(restored_state)
    }

    pub fn release_savepoint(&mut self, transaction_id: u64, name: &str) -> Result<(), ReefDBError> {
        let transaction = self.active_transactions.get(&transaction_id)
            .ok_or_else(|| ReefDBError::TransactionNotFound(transaction_id))?;
        
        if transaction.get_state() != &TransactionState::Active {
            return Err(ReefDBError::TransactionNotActive);
        }
        
        let mut savepoint_manager = self.savepoint_manager.lock()
            .map_err(|_| ReefDBError::LockAcquisitionFailed("Failed to acquire savepoint manager lock".to_string()))?;
        
        savepoint_manager.release_savepoint(transaction_id, name)
    }

    fn get_transaction_guard(&mut self, transaction_id: u64) -> Result<TransactionGuard<S, FTS>, ReefDBError> {
        let transaction = self.get_transaction_mut(transaction_id)?;
        let isolation_level = transaction.get_isolation_level();
        Ok(TransactionGuard {
            transaction,
            isolation_level,
        })
    }

    fn evaluate_where_clause(
        where_clause: &WhereType,
        row_data: &[DataValue],
        schema: &[ColumnDef],
        table_name: &str,
    ) -> bool {
        match where_clause {
            WhereType::Regular(clause) => {
                // Find the column in the schema
                let col_idx = if let Some(ref clause_table) = clause.table {
                    // If table is specified, only look in that table's columns
                    if clause_table == table_name {
                        schema.iter().position(|c| c.name == clause.col_name)
                    } else {
                        // If the table doesn't match, we might be looking at joined data
                        // In this case, we need to look through all columns
                        schema.iter().position(|c| c.name == clause.col_name)
                    }
                } else {
                    // If no table specified, look in all columns
                    schema.iter().position(|c| c.name == clause.col_name)
                };
                
                if let Some(idx) = col_idx {
                    clause.operator.evaluate(&row_data[idx], &clause.value)
                } else {
                    false
                }
            },
            WhereType::FTS(_) => {
                // FTS search is handled separately by the FTS index
                false
            },
            WhereType::And(left, right) => {
                Self::evaluate_where_clause(left, row_data, schema, table_name) &&
                Self::evaluate_where_clause(right, row_data, schema, table_name)
            },
            WhereType::Or(left, right) => {
                Self::evaluate_where_clause(left, row_data, schema, table_name) ||
                Self::evaluate_where_clause(right, row_data, schema, table_name)
            },
        }
    }

    fn evaluate_join_condition(
        condition: &(ColumnValuePair, ColumnValuePair),
        left_data: &[DataValue],
        left_schema: &[ColumnDef],
        right_data: &[DataValue],
        right_schema: &[ColumnDef],
        left_table: &str,
        right_table: &str,
    ) -> bool {
        let (left_pair, right_pair) = condition;
        
        // Get values from both tables
        let left_value = if left_pair.table_name.is_empty() || left_pair.table_name == left_table {
            if let Some(idx) = left_schema.iter().position(|c| c.name == left_pair.column_name) {
                Some(&left_data[idx])
            } else {
                None
            }
        } else if left_pair.table_name == right_table {
            if let Some(idx) = right_schema.iter().position(|c| c.name == left_pair.column_name) {
                Some(&right_data[idx])
            } else {
                None
            }
        } else {
            None
        };

        let right_value = if right_pair.table_name.is_empty() || right_pair.table_name == left_table {
            if let Some(idx) = left_schema.iter().position(|c| c.name == right_pair.column_name) {
                Some(&left_data[idx])
            } else {
                None
            }
        } else if right_pair.table_name == right_table {
            if let Some(idx) = right_schema.iter().position(|c| c.name == right_pair.column_name) {
                Some(&right_data[idx])
            } else {
                None
            }
        } else {
            None
        };

        // Compare the values if both were found
        if let (Some(left_val), Some(right_val)) = (left_value, right_value) {
            left_val == right_val
        } else {
            false
        }
    }

    pub fn execute_statement(&mut self, transaction_id: u64, stmt: Statement) -> Result<ReefDBResult, ReefDBError> {
        match stmt {
            Statement::Select(SelectStatement::FromTable(table_ref, columns, where_clause, joins)) => {
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
                for join in joins.iter() {
                    let joined_table = guard.transaction.reef_db.storage.get_table_ref(&join.table_ref.name)
                        .ok_or_else(|| ReefDBError::TableNotFound(join.table_ref.name.clone()))?;
                    joined_tables.push((join, (joined_table.0.to_vec(), joined_table.1.to_vec())));
                }

                // Drop the guard before getting the MVCC manager
                drop(guard);

                // Now get the MVCC manager
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
                                                    
                                                    if schema_start + schema_len <= combined_schema.len() {
                                                        combined_schema[schema_start..schema_start + schema_len]
                                                            .iter()
                                                            .position(|c| c.name == clause.col_name)
                                                            .map(|pos| schema_start + pos)
                                                    } else {
                                                        None
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
                                            },
                                            WhereType::And(left, right) => {
                                                result = Self::evaluate_where_clause(left, &combined_row, &combined_schema, &table_ref.name) &&
                                                        Self::evaluate_where_clause(right, &combined_row, &combined_schema, &table_ref.name);
                                            },
                                            WhereType::Or(left, right) => {
                                                result = Self::evaluate_where_clause(left, &combined_row, &combined_schema, &table_ref.name) ||
                                                        Self::evaluate_where_clause(right, &combined_row, &combined_schema, &table_ref.name);
                                            },
                                            WhereType::FTS(_) => {
                                                result = false;
                                            },
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
                    for (joined_data, joined_schema) in matched_rows {
                        // Project only the requested columns
                        let mut projected = Vec::new();
                        if columns.iter().any(|c| c.name == "*") {
                            projected = joined_data;
                        } else {
                            for col in &columns {
                                let col_value = if let Some(table) = &col.table {
                                    // Find column in specific table's schema
                                    let (schema_start, schema_len) = if table == &table_ref.name {
                                        (0, schema.len())
                                    } else {
                                        let mut start = schema.len();
                                        let mut len = 0;
                                        for (join, (join_schema, _)) in &joined_tables {
                                            if &join.table_ref.name == table {
                                                len = join_schema.len();
                                                break;
                                            }
                                            start += join_schema.len();
                                        }
                                        (start, len)
                                    };
                                    
                                    if schema_start + schema_len <= joined_schema.len() {
                                        if let Some(idx) = joined_schema[schema_start..schema_start + schema_len]
                                            .iter()
                                            .position(|c| c.name == col.name)
                                        {
                                            Some(joined_data[schema_start + idx].clone())
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    // Try to find column in any table
                                    if let Some(idx) = joined_schema.iter().position(|c| c.name == col.name) {
                                        Some(joined_data[idx].clone())
                                    } else {
                                        None
                                    }
                                };
                                
                                if let Some(value) = col_value {
                                    projected.push(value);
                                }
                            }
                        }
                        results.push((i, projected));
                    }
                }

                Ok(ReefDBResult::Select(results))
            },
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
            },
            _ => {
                let transaction = self.get_transaction(transaction_id)?;
                transaction.execute_statement(stmt)
            },
        }
    }

    pub fn execute_statement_committed(&mut self, stmt: Statement) -> Result<ReefDBResult, ReefDBError> {
        let reef_db = self.reef_db.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire database lock".to_string()))?;

        match stmt {
            Statement::Select(SelectStatement::FromTable(table_ref, columns, where_clause, _joins)) => {
                let mvcc_manager = self.mvcc_manager.lock()
                    .map_err(|_| ReefDBError::Other("Failed to acquire MVCC manager lock".to_string()))?;

                // Get the table data
                let (schema, rows) = reef_db.storage.get_table_ref(&table_ref.name)
                    .ok_or_else(|| ReefDBError::TableNotFound(table_ref.name.clone()))?;

                println!("MVCC Debug - Table {} has {} rows in storage", table_ref.name, rows.len());

                let mut results: Vec<(usize, Vec<DataValue>)> = Vec::new();
                for (i, row) in rows.iter().enumerate() {
                    // Get the ID from the first column (primary key)
                    let id = match &row[0] {
                        DataValue::Integer(n) => n.to_string(),
                        _ => continue, // Skip non-integer IDs
                    };
                    let key = KeyFormat::row(&table_ref.name, 0, &id);
                    println!("MVCC Debug - Checking visibility for key: {}", key);
                    if let Ok(Some(data)) = mvcc_manager.read_committed(0, &key) {
                        println!("MVCC Debug - Found visible version for key: {} with data: {:?}", key, data);
                        
                        // First check if the row matches the where clause
                        let should_include = if let Some(ref where_clause) = where_clause {
                            println!("MVCC Debug - Evaluating where clause: {:?}", where_clause);
                            println!("MVCC Debug - Row data: {:?}", data);
                            println!("MVCC Debug - Schema: {:?}", schema);
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

                        println!("MVCC Debug - Row should be included: {}", should_include);

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

                            println!("MVCC Debug - Including row in results: {:?}", row_data);
                            results.push((i, row_data));
                        }
                    }
                }

                println!("MVCC Debug - Final results count: {}", results.len());
                Ok(ReefDBResult::Select(results))
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
                self.acquire_lock(transaction_id, table_name, LockType::Exclusive)?;
            }
            Statement::Update(UpdateStatement::UpdateTable(table_name, _, _)) => {
                self.acquire_lock(transaction_id, table_name, LockType::Exclusive)?;
            }
            Statement::Delete(DeleteStatement::FromTable(table_name, _)) => {
                self.acquire_lock(transaction_id, table_name, LockType::Exclusive)?;
            }
            Statement::Create(CreateStatement::Table(table_name, _)) => {
                self.acquire_lock(transaction_id, table_name, LockType::Exclusive)?;
            }
            Statement::Select(SelectStatement::FromTable(table_ref, _, _, _)) => {
                // For serializable isolation, we need shared locks to prevent phantom reads
                // But with MVCC, we don't need to acquire locks for reads since each transaction
                // sees its own snapshot of the data
                if isolation_level == IsolationLevel::Serializable && !self.mvcc_manager.lock()
                    .map_err(|_| ReefDBError::Other("Failed to acquire MVCC manager lock".to_string()))?
                    .is_active(transaction_id) {
                    self.acquire_lock(transaction_id, &table_ref.name, LockType::Shared)?;
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
                Statement::Select(SelectStatement::FromTable(_, _, _, _)) => {
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

    pub fn get_transaction_state(&self, transaction_id: u64) -> Result<TableStorage, ReefDBError> {
        let transaction = self.active_transactions.get(&transaction_id)
            .ok_or_else(|| ReefDBError::Other("Transaction not found".to_string()))?;
        
        Ok(transaction.get_table_state())
    }

    pub fn update_database_state(&mut self, state: TableStorage) {
        // Update the database state
        if let Ok(mut reef_db) = self.reef_db.lock() {
            reef_db.tables.restore_from(&state);
            
            // Get the updated state to propagate to transactions
            let updated_state = reef_db.tables.clone();
            drop(reef_db); // Release the lock before updating transactions
            
            // Update all active transactions to see the new state
            for tx in self.active_transactions.values_mut() {
                if tx.get_state() == &TransactionState::Active {
                    tx.reef_db.tables.restore_from(&updated_state);
                    tx.acid_manager.begin_atomic(&updated_state);
                }
            }
        }
    }

    fn get_transaction(&mut self, transaction_id: u64) -> Result<&mut Transaction<S, FTS>, ReefDBError> {
        self.active_transactions
            .get_mut(&transaction_id)
            .ok_or_else(|| ReefDBError::Other("Transaction not found".to_string()))
    }

    // Helper methods for MVCC operations
    fn read_mvcc_data(&self, key: &str) -> Result<Option<Vec<DataValue>>, ReefDBError> {
        let mvcc_manager = self.mvcc_manager.lock().unwrap();
        // Use a special system transaction ID (0) for direct reads
        mvcc_manager.read_committed(0, key)
    }

    fn write_mvcc_data(&self, transaction_id: u64, key: String, data: Vec<DataValue>) -> Result<(), ReefDBError> {
        let mut mvcc_manager = self.mvcc_manager.lock()
            .map_err(|_| ReefDBError::Other("Failed to acquire MVCC manager lock".to_string()))?;
        mvcc_manager.write(transaction_id, key, data)
    }

    // Helper method to get a mutable transaction reference
    fn get_transaction_mut(&mut self, transaction_id: u64) -> Result<&mut Transaction<S, FTS>, ReefDBError> {
        self.active_transactions
            .get_mut(&transaction_id)
            .ok_or_else(|| ReefDBError::Other("Transaction not found".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::InMemoryReefDB;

    #[test]
    fn test_transaction_manager() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal = WriteAheadLog::new(wal_path).unwrap();
        
        let db = InMemoryReefDB::create_in_memory().unwrap();
        let mut tm = TransactionManager::create(db, wal);
        
        // Begin transaction
        let tx_id = tm.begin_transaction(IsolationLevel::Serializable).unwrap();
        
        // Acquire lock
        tm.acquire_lock(tx_id, "users", LockType::Exclusive).unwrap();
        
        // Try to acquire conflicting lock (should fail)
        let tx_id2 = tm.begin_transaction(IsolationLevel::Serializable).unwrap();
        assert!(tm.acquire_lock(tx_id2, "users", LockType::Shared).is_err());
        
        // Commit first transaction
        tm.commit_transaction(tx_id).unwrap();
        
        // Now second transaction should be able to acquire lock
        assert!(tm.acquire_lock(tx_id2, "users", LockType::Shared).is_ok());
    }
} 