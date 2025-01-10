use std::time::SystemTime;
use crate::error::ReefDBError;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        IsolationLevel::ReadCommitted
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
}

#[derive(Debug, Clone)]
pub struct TransactionStateHandler {
    state: TransactionState,
    start_timestamp: SystemTime,
    transaction_id: u64,
    isolation_level: IsolationLevel,
}

impl TransactionStateHandler {
    pub fn new(transaction_id: u64, isolation_level: IsolationLevel) -> Self {
        Self {
            state: TransactionState::Active,
            start_timestamp: SystemTime::now(),
            transaction_id,
            isolation_level,
        }
    }

    pub fn commit(&mut self) -> Result<(), ReefDBError> {
        if self.state != TransactionState::Active {
            return Err(ReefDBError::TransactionNotActive);
        }
        self.state = TransactionState::Committed;
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), ReefDBError> {
        if self.state != TransactionState::Active {
            return Err(ReefDBError::TransactionNotActive);
        }
        self.state = TransactionState::RolledBack;
        Ok(())
    }

    pub fn get_state(&self) -> &TransactionState {
        &self.state
    }

    pub fn get_start_timestamp(&self) -> SystemTime {
        self.start_timestamp
    }

    pub fn get_id(&self) -> u64 {
        self.transaction_id
    }

    pub fn get_isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_new_transaction() {
        let handler = TransactionStateHandler::new(1, IsolationLevel::ReadCommitted);
        assert_eq!(*handler.get_state(), TransactionState::Active);
        assert_eq!(handler.get_id(), 1);
        assert_eq!(handler.get_isolation_level(), IsolationLevel::ReadCommitted);
    }

    #[test]
    fn test_commit_transaction() {
        let mut handler = TransactionStateHandler::new(1, IsolationLevel::ReadCommitted);
        
        // Test successful commit
        assert!(handler.commit().is_ok());
        assert_eq!(*handler.get_state(), TransactionState::Committed);
        
        // Test commit on already committed transaction
        assert!(handler.commit().is_err());
    }

    #[test]
    fn test_rollback_transaction() {
        let mut handler = TransactionStateHandler::new(1, IsolationLevel::ReadCommitted);
        
        // Test successful rollback
        assert!(handler.rollback().is_ok());
        assert_eq!(*handler.get_state(), TransactionState::RolledBack);
        
        // Test rollback on already rolled back transaction
        assert!(handler.rollback().is_err());
    }

    #[test]
    fn test_isolation_level_default() {
        assert_eq!(IsolationLevel::default(), IsolationLevel::ReadCommitted);
    }

    #[test]
    fn test_transaction_timestamps() {
        let handler1 = TransactionStateHandler::new(1, IsolationLevel::ReadCommitted);
        thread::sleep(Duration::from_millis(10));
        let handler2 = TransactionStateHandler::new(2, IsolationLevel::ReadCommitted);
        
        assert!(handler2.get_start_timestamp() > handler1.get_start_timestamp());
    }

    #[test]
    fn test_transaction_state_transitions() {
        let mut handler = TransactionStateHandler::new(1, IsolationLevel::ReadCommitted);
        
        // Initial state
        assert_eq!(*handler.get_state(), TransactionState::Active);
        
        // After commit
        handler.commit().unwrap();
        assert_eq!(*handler.get_state(), TransactionState::Committed);
        
        // Cannot rollback after commit
        assert!(handler.rollback().is_err());
        assert_eq!(*handler.get_state(), TransactionState::Committed);
    }
}