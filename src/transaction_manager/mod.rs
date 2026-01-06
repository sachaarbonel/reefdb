mod execution;
mod joins;
mod locks;
mod savepoints;
mod wal;

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::deadlock::DeadlockDetector;
use crate::error::ReefDBError;
use crate::fts::search::Search;
use crate::indexes::index_manager::IndexManager;
use crate::locks::LockManager;
use crate::mvcc::MVCCManager;
use crate::storage::{Storage, TableStorage};
use crate::transaction::{IsolationLevel, Transaction, TransactionState};
use crate::wal::WriteAheadLog;
use crate::ReefDB;

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
        self.append_commit_entry(id)?;

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

        Ok(())
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
    use crate::sql::data_type::DataType;
    use crate::sql::statements::{Statement, create::CreateStatement, insert::InsertStatement, select::SelectStatement};
    use crate::sql::column::Column;
    use crate::sql::clauses::order_by::{OrderByClause, OrderDirection};
    use crate::sql::column_def::ColumnDef;
    use crate::sql::constraints::constraint::Constraint;
    use crate::sql::table_reference::TableReference;
    use crate::result::ReefDBResult;
    use crate::sql::data_value::DataValue;

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
        tm.acquire_lock(tx_id, "users", crate::locks::LockType::Exclusive).unwrap();

        // Try to acquire conflicting lock (should fail)
        let tx_id2 = tm.begin_transaction(IsolationLevel::Serializable).unwrap();
        assert!(tm.acquire_lock(tx_id2, "users", crate::locks::LockType::Shared).is_err());

        // Commit first transaction
        tm.commit_transaction(tx_id).unwrap();

        // Now second transaction should be able to acquire lock
        assert!(tm.acquire_lock(tx_id2, "users", crate::locks::LockType::Shared).is_ok());
    }

    #[test]
    fn test_order_by() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal = WriteAheadLog::new(wal_path).unwrap();

        let db = InMemoryReefDB::create_in_memory().unwrap();
        let mut tm = TransactionManager::create(db, wal);

        // Begin transaction
        let tx_id = tm.begin_transaction(IsolationLevel::Serializable).unwrap();

        // Create users table
        let create_stmt = Statement::Create(CreateStatement::Table(
            "users".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![Constraint::PrimaryKey, Constraint::NotNull, Constraint::Unique],
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    constraints: vec![Constraint::NotNull],
                },
                ColumnDef {
                    name: "age".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![Constraint::NotNull],
                },
            ],
        ));
        tm.execute_statement(tx_id, create_stmt).unwrap();

        // Insert test data
        let insert_stmt1 = Statement::Insert(InsertStatement::IntoTable(
            "users".to_string(),
            vec![
                DataValue::Integer(1),
                DataValue::Text("Alice".to_string()),
                DataValue::Integer(25),
            ],
        ));
        tm.execute_statement(tx_id, insert_stmt1).unwrap();

        let insert_stmt2 = Statement::Insert(InsertStatement::IntoTable(
            "users".to_string(),
            vec![
                DataValue::Integer(2),
                DataValue::Text("Bob".to_string()),
                DataValue::Integer(30),
            ],
        ));
        tm.execute_statement(tx_id, insert_stmt2).unwrap();

        let insert_stmt3 = Statement::Insert(InsertStatement::IntoTable(
            "users".to_string(),
            vec![
                DataValue::Integer(3),
                DataValue::Text("Charlie".to_string()),
                DataValue::Integer(20),
            ],
        ));
        tm.execute_statement(tx_id, insert_stmt3).unwrap();

        // Test ORDER BY age DESC
        let select_stmt = Statement::Select(SelectStatement::FromTable(
            TableReference {
                name: "users".to_string(),
                alias: None,
            },
            vec![
                Column {
                    table: None,
                    name: "name".to_string(),
                    column_type: crate::sql::column::ColumnType::Regular("name".to_string()),
                },
                Column {
                    table: None,
                    name: "age".to_string(),
                    column_type: crate::sql::column::ColumnType::Regular("age".to_string()),
                },
            ],
            None,
            vec![],
            vec![OrderByClause {
                column: Column {
                    table: None,
                    name: "age".to_string(),
                    column_type: crate::sql::column::ColumnType::Regular("age".to_string()),
                },
                direction: OrderDirection::Desc,
            }],
        ));

        let result = tm.execute_statement(tx_id, select_stmt).unwrap();

        if let ReefDBResult::Select(query_result) = result {
            let rows = query_result.rows;
            assert_eq!(rows.len(), 3);
            // Check order: Bob (30), Alice (25), Charlie (20)
            assert_eq!(rows[0].1[0], DataValue::Text("Bob".to_string()));
            assert_eq!(rows[0].1[1], DataValue::Integer(30));
            assert_eq!(rows[1].1[0], DataValue::Text("Alice".to_string()));
            assert_eq!(rows[1].1[1], DataValue::Integer(25));
            assert_eq!(rows[2].1[0], DataValue::Text("Charlie".to_string()));
            assert_eq!(rows[2].1[1], DataValue::Integer(20));
        } else {
            panic!("Expected Select result");
        }

        // Test multiple ORDER BY: age ASC, name DESC
        let select_stmt = Statement::Select(SelectStatement::FromTable(
            TableReference {
                name: "users".to_string(),
                alias: None,
            },
            vec![
                Column {
                    table: None,
                    name: "name".to_string(),
                    column_type: crate::sql::column::ColumnType::Regular("name".to_string()),
                },
                Column {
                    table: None,
                    name: "age".to_string(),
                    column_type: crate::sql::column::ColumnType::Regular("age".to_string()),
                },
            ],
            None,
            vec![],
            vec![
                OrderByClause {
                    column: Column {
                        table: None,
                        name: "age".to_string(),
                        column_type: crate::sql::column::ColumnType::Regular("age".to_string()),
                    },
                    direction: OrderDirection::Asc,
                },
                OrderByClause {
                    column: Column {
                        table: None,
                        name: "name".to_string(),
                        column_type: crate::sql::column::ColumnType::Regular("name".to_string()),
                    },
                    direction: OrderDirection::Desc,
                },
            ],
        ));

        let result = tm.execute_statement(tx_id, select_stmt).unwrap();

        if let ReefDBResult::Select(query_result) = result {
            let rows = query_result.rows;
            assert_eq!(rows.len(), 3);
            // Check order: Charlie (20), Alice (25), Bob (30)
            assert_eq!(rows[0].1[0], DataValue::Text("Charlie".to_string()));
            assert_eq!(rows[0].1[1], DataValue::Integer(20));
            assert_eq!(rows[1].1[0], DataValue::Text("Alice".to_string()));
            assert_eq!(rows[1].1[1], DataValue::Integer(25));
            assert_eq!(rows[2].1[0], DataValue::Text("Bob".to_string()));
            assert_eq!(rows[2].1[1], DataValue::Integer(30));
        } else {
            panic!("Expected Select result");
        }

        tm.commit_transaction(tx_id).unwrap();
    }

    #[test]
    fn test_integration() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal = WriteAheadLog::new(wal_path).unwrap();

        let db = InMemoryReefDB::create_in_memory().unwrap();
        let mut tm = TransactionManager::create(db, wal);

        // Begin transaction
        let tx_id = tm.begin_transaction(IsolationLevel::Serializable).unwrap();

        // Create users table
        let create_stmt = Statement::Create(CreateStatement::Table(
            "users".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![Constraint::PrimaryKey, Constraint::NotNull, Constraint::Unique],
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    constraints: vec![Constraint::NotNull],
                },
                ColumnDef {
                    name: "age".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![Constraint::NotNull],
                },
            ],
        ));
        tm.execute_statement(tx_id, create_stmt).unwrap();

        // Insert test data
        let insert_stmt1 = Statement::Insert(InsertStatement::IntoTable(
            "users".to_string(),
            vec![
                DataValue::Integer(1),
                DataValue::Text("Alice".to_string()),
                DataValue::Integer(25),
            ],
        ));
        tm.execute_statement(tx_id, insert_stmt1).unwrap();

        let insert_stmt2 = Statement::Insert(InsertStatement::IntoTable(
            "users".to_string(),
            vec![
                DataValue::Integer(2),
                DataValue::Text("Bob".to_string()),
                DataValue::Integer(30),
            ],
        ));
        tm.execute_statement(tx_id, insert_stmt2).unwrap();

        let insert_stmt3 = Statement::Insert(InsertStatement::IntoTable(
            "users".to_string(),
            vec![
                DataValue::Integer(3),
                DataValue::Text("Charlie".to_string()),
                DataValue::Integer(20),
            ],
        ));
        tm.execute_statement(tx_id, insert_stmt3).unwrap();

        // Create orders table
        let create_orders_stmt = Statement::Create(CreateStatement::Table(
            "orders".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![Constraint::PrimaryKey, Constraint::NotNull, Constraint::Unique],
                },
                ColumnDef {
                    name: "user_id".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![Constraint::NotNull],
                },
                ColumnDef {
                    name: "amount".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![Constraint::NotNull],
                },
            ],
        ));
        tm.execute_statement(tx_id, create_orders_stmt).unwrap();

        // Insert test data into orders
        let insert_order1 = Statement::Insert(InsertStatement::IntoTable(
            "orders".to_string(),
            vec![
                DataValue::Integer(1),
                DataValue::Integer(1), // Alice
                DataValue::Integer(25),
            ],
        ));
        tm.execute_statement(tx_id, insert_order1).unwrap();

        let insert_order2 = Statement::Insert(InsertStatement::IntoTable(
            "orders".to_string(),
            vec![
                DataValue::Integer(2),
                DataValue::Integer(2), // Bob
                DataValue::Integer(30),
            ],
        ));
        tm.execute_statement(tx_id, insert_order2).unwrap();

        let insert_order3 = Statement::Insert(InsertStatement::IntoTable(
            "orders".to_string(),
            vec![
                DataValue::Integer(3),
                DataValue::Integer(3), // Charlie
                DataValue::Integer(20),
            ],
        ));
        tm.execute_statement(tx_id, insert_order3).unwrap();

        // Test 1: Simple select, order by age DESC
        let select_stmt = Statement::Select(SelectStatement::FromTable(
            TableReference {
                name: "users".to_string(),
                alias: None,
            },
            vec![
                Column {
                    table: None,
                    name: "name".to_string(),
                    column_type: crate::sql::column::ColumnType::Regular("name".to_string()),
                },
                Column {
                    table: None,
                    name: "age".to_string(),
                    column_type: crate::sql::column::ColumnType::Regular("age".to_string()),
                },
            ],
            None,
            vec![],
            vec![OrderByClause {
                column: Column {
                    table: None,
                    name: "age".to_string(),
                    column_type: crate::sql::column::ColumnType::Regular("age".to_string()),
                },
                direction: OrderDirection::Desc,
            }],
        ));

        let result = tm.execute_statement(tx_id, select_stmt).unwrap();

        if let ReefDBResult::Select(query_result) = result {
            let rows = query_result.rows;
            assert_eq!(rows.len(), 3);
            // Check order: Bob (30), Alice (25), Charlie (20)
            assert_eq!(rows[0].1[0], DataValue::Text("Bob".to_string()));
            assert_eq!(rows[0].1[1], DataValue::Integer(30));
            assert_eq!(rows[1].1[0], DataValue::Text("Alice".to_string()));
            assert_eq!(rows[1].1[1], DataValue::Integer(25));
            assert_eq!(rows[2].1[0], DataValue::Text("Charlie".to_string()));
            assert_eq!(rows[2].1[1], DataValue::Integer(20));
        } else {
            panic!("Expected Select result");
        }

        // Test 2: Join users and orders, order by amount DESC, name ASC
        let join_clause = crate::sql::clauses::join_clause::JoinClause {
            table_ref: TableReference {
                name: "orders".to_string(),
                alias: None,
            },
            on: (
                crate::sql::column_value_pair::ColumnValuePair {
                    table_name: "users".to_string(),
                    column_name: "id".to_string(),
                },
                crate::sql::column_value_pair::ColumnValuePair {
                    table_name: "orders".to_string(),
                    column_name: "user_id".to_string(),
                },
            ),
            join_type: crate::sql::clauses::join_clause::JoinType::Inner,
        };

        let select_stmt = Statement::Select(SelectStatement::FromTable(
            TableReference {
                name: "users".to_string(),
                alias: None,
            },
            vec![
                Column {
                    table: None,
                    name: "name".to_string(),
                    column_type: crate::sql::column::ColumnType::Regular("name".to_string()),
                },
                Column {
                    table: None,
                    name: "age".to_string(),
                    column_type: crate::sql::column::ColumnType::Regular("age".to_string()),
                },
                Column {
                    table: Some("orders".to_string()),
                    name: "amount".to_string(),
                    column_type: crate::sql::column::ColumnType::Regular("amount".to_string()),
                },
            ],
            None,
            vec![join_clause],
            vec![
                OrderByClause {
                    column: Column {
                        table: Some("orders".to_string()),
                        name: "amount".to_string(),
                        column_type: crate::sql::column::ColumnType::Regular("amount".to_string()),
                    },
                    direction: OrderDirection::Desc,
                },
                OrderByClause {
                    column: Column {
                        table: None,
                        name: "name".to_string(),
                        column_type: crate::sql::column::ColumnType::Regular("name".to_string()),
                    },
                    direction: OrderDirection::Asc,
                },
            ],
        ));

        let result = tm.execute_statement(tx_id, select_stmt).unwrap();

        if let ReefDBResult::Select(query_result) = result {
            let rows = query_result.rows;
            assert_eq!(rows.len(), 3);
            // Check order: Bob (30), Alice (25), Charlie (20)
            assert_eq!(rows[0].1[0], DataValue::Text("Bob".to_string()));
            assert_eq!(rows[0].1[1], DataValue::Integer(30));
            assert_eq!(rows[1].1[0], DataValue::Text("Alice".to_string()));
            assert_eq!(rows[1].1[1], DataValue::Integer(25));
            assert_eq!(rows[2].1[0], DataValue::Text("Charlie".to_string()));
            assert_eq!(rows[2].1[1], DataValue::Integer(20));
        } else {
            panic!("Expected Select result");
        }

        tm.commit_transaction(tx_id).unwrap();
    }
}
