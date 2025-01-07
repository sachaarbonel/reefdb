use crate::{
    error::ReefDBError,
    indexes::fts::search::Search,
    result::ReefDBResult,
    sql::statements::Statement,
    storage::Storage,
    ReefDB,
};

use std::time::SystemTime;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
    Failed,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Clone)]
pub struct Transaction<S: Storage + Clone, FTS: Search + Clone>
where
    FTS::NewArgs: Clone,
{
    id: u64,
    state: TransactionState,
    reef_db: ReefDB<S, FTS>,
    start_timestamp: SystemTime,
    isolation_level: IsolationLevel,
}

impl<S: Storage + Clone, FTS: Search + Clone> Transaction<S, FTS>
where
    FTS::NewArgs: Clone,
{
    pub fn create(reef_db: ReefDB<S, FTS>, isolation_level: IsolationLevel) -> Self {
        static TRANSACTION_ID: AtomicU64 = AtomicU64::new(0);
        
        Transaction {
            id: TRANSACTION_ID.fetch_add(1, Ordering::SeqCst),
            state: TransactionState::Active,
            reef_db,
            start_timestamp: SystemTime::now(),
            isolation_level,
        }
    }

    pub fn execute_statement(&mut self, stmt: Statement) -> Result<ReefDBResult, ReefDBError> {
        if self.state != TransactionState::Active {
            return Err(ReefDBError::Other("Transaction is not active".to_string()));
        }
        
        let result = self.reef_db.execute_statement(stmt);
        if result.is_err() {
            self.state = TransactionState::Failed;
        }
        result
    }

    pub fn commit(&mut self, reef_db: &mut ReefDB<S, FTS>) -> Result<(), ReefDBError> {
        if self.state != TransactionState::Active {
            return Err(ReefDBError::Other("Transaction is not active".to_string()));
        }

        reef_db.tables = self.reef_db.tables.clone();
        reef_db.inverted_index = self.reef_db.inverted_index.clone();
        
        self.state = TransactionState::Committed;
        Ok(())
    }

    pub fn rollback(&mut self, reef_db: &mut ReefDB<S, FTS>) -> Result<(), ReefDBError> {
        if self.state != TransactionState::Active && self.state != TransactionState::Failed {
            return Err(ReefDBError::Other("Transaction cannot be rolled back".to_string()));
        }

        self.reef_db.tables = reef_db.tables.clone();
        self.reef_db.inverted_index = reef_db.inverted_index.clone();
        
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryReefDB;

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
