use std::time::SystemTime;
use crate::{
    error::ReefDBError,
    indexes::fts::search::Search,
    storage::Storage,
    ReefDB,
    acid::AcidManager,
    sql::statements::Statement,
    result::ReefDBResult,
    TableStorage,
};

#[derive(Debug, Clone, PartialEq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
    Failed,
}

#[derive(Clone)]
pub struct Transaction<S: Storage + Clone, FTS: Search + Clone>
where
    FTS::NewArgs: Clone,
{
    id: u64,
    state: TransactionState,
    isolation_level: IsolationLevel,
    reef_db: ReefDB<S, FTS>,
    start_timestamp: SystemTime,
    acid_manager: AcidManager,
}

impl<S: Storage + Clone, FTS: Search + Clone> Transaction<S, FTS>
where
    FTS::NewArgs: Clone,
{
    pub fn create(reef_db: ReefDB<S, FTS>, isolation_level: IsolationLevel) -> Self {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let mut acid_manager = AcidManager::new(reef_db.tables.clone());
        acid_manager.begin_atomic(&reef_db.tables);

        Transaction {
            id,
            state: TransactionState::Active,
            isolation_level,
            reef_db: reef_db.clone(),
            start_timestamp: SystemTime::now(),
            acid_manager,
        }
    }

    pub fn commit(&mut self, reef_db: &mut ReefDB<S, FTS>) -> Result<(), ReefDBError> {
        if self.state != TransactionState::Active {
            return Err(ReefDBError::Other("Transaction cannot be committed".to_string()));
        }

        // Commit changes atomically
        self.acid_manager.commit()?;

        // Update the database state using restore_from
        reef_db.tables.restore_from(&self.reef_db.tables);
        reef_db.inverted_index = self.reef_db.inverted_index.clone();
        
        self.state = TransactionState::Committed;
        Ok(())
    }

    pub fn rollback(&mut self, reef_db: &mut ReefDB<S, FTS>) -> Result<(), ReefDBError> {
        if self.state != TransactionState::Active && self.state != TransactionState::Failed {
            return Err(ReefDBError::Other("Transaction cannot be rolled back".to_string()));
        }

        // Rollback to the initial snapshot
        let snapshot = self.acid_manager.rollback_atomic();
        reef_db.tables.restore_from(&snapshot);
        self.reef_db.tables.restore_from(&snapshot);
        
        self.state = TransactionState::RolledBack;
        Ok(())
    }

    pub fn get_state(&self) -> &TransactionState {
        &self.state
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn get_isolation_level(&self) -> &IsolationLevel {
        &self.isolation_level
    }

    pub fn execute_statement(&mut self, stmt: Statement) -> Result<ReefDBResult, ReefDBError> {
        // Execute the statement on the transaction's copy of the database
        let result = self.reef_db.execute_statement(stmt);
        
        // If successful, update the acid manager's snapshot
        if result.is_ok() {
            // Take a snapshot of the current state for ACID compliance
            let current_state = self.reef_db.tables.clone();
            self.acid_manager.begin_atomic(&current_state);
        }
        
        result
    }

    pub(crate) fn restore_table_state(&mut self, snapshot: &TableStorage) {
        // Create a new clean state for the reef_db
        let mut new_reef_db = self.reef_db.clone();
        
        // Replace the tables completely with the snapshot
        new_reef_db.tables = snapshot.clone();
        
        // Update both the transaction's state and the database state
        self.reef_db = new_reef_db.clone();
        
        // Reset the ACID manager with the new state
        self.acid_manager = AcidManager::new(snapshot.clone());
        self.acid_manager.begin_atomic(snapshot);
        
        // Update the database state through the transaction manager
        if let Some(tm) = &mut self.reef_db.transaction_manager {
            let _ = tm.update_database_state(snapshot.clone());
        }
    }

    pub(crate) fn get_table_state(&self) -> TableStorage {
        self.reef_db.tables.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryReefDB;
    use crate::sql::statements::Statement;

    #[test]
    fn test_transactions() {
        let mut db = InMemoryReefDB::create_in_memory();

        // Create a table and insert a row outside of a transaction
        let (_, create_stmt) = Statement::parse("CREATE TABLE users (name TEXT, age INTEGER)").unwrap();
        db.execute_statement(create_stmt).unwrap();
        let (_, insert_stmt) = Statement::parse("INSERT INTO users VALUES ('alice', 30)").unwrap();
        db.execute_statement(insert_stmt).unwrap();

        // Start a transaction and insert two rows
        let mut transaction = Transaction::create(db.clone(), IsolationLevel::Serializable);
        let (_, insert_stmt2) = Statement::parse("INSERT INTO users VALUES ('jane', 25)").unwrap();
        transaction.execute_statement(insert_stmt2).unwrap();
        let (_, insert_stmt3) = Statement::parse("INSERT INTO users VALUES ('john', 27)").unwrap();
        transaction.execute_statement(insert_stmt3).unwrap();

        // Commit the transaction
        transaction.commit(&mut db).unwrap();

        // Start a new transaction and insert a new row
        let mut transaction2 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        let (_, insert_stmt4) = Statement::parse("INSERT INTO users VALUES ('emma', 18)").unwrap();
        transaction2.execute_statement(insert_stmt4).unwrap();

        // Rollback the transaction
        transaction2.rollback(&mut db).unwrap();
    }
}
