use std::collections::HashMap;
use crate::error::ReefDBError;
use super::types::LockType;

#[derive(Debug)]
pub struct LockManager {
    pub(crate) table_locks: HashMap<String, Vec<(u64, LockType)>>,
}

impl LockManager {
    pub fn new() -> Self {
        LockManager {
            table_locks: HashMap::new(),
        }
    }

    pub fn acquire_lock(&mut self, transaction_id: u64, table_name: &str, lock_type: LockType) -> Result<(), ReefDBError> {
        let locks = self.table_locks.entry(table_name.to_string()).or_insert_with(Vec::new);
        
        // Check if this transaction already has a lock on the table
        let existing_lock = locks.iter().find(|(id, _)| *id == transaction_id);
        
        if let Some((_, existing_lock_type)) = existing_lock {
            // If requesting the same lock type, return success
            if *existing_lock_type == lock_type {
                return Ok(());
            }
            
            // For lock upgrades (shared -> exclusive), we need to check for conflicts
            if *existing_lock_type == LockType::Shared && lock_type == LockType::Exclusive {
                // Check if any other transaction holds a shared lock
                if locks.iter().any(|(id, lt)| *id != transaction_id && *lt == LockType::Shared) {
                    return Err(ReefDBError::LockConflict(format!(
                        "Lock conflict: Transaction {} cannot upgrade to {:?} lock on table {} due to existing shared locks",
                        transaction_id, lock_type, table_name
                    )));
                }
                // Remove the shared lock and add the exclusive lock
                locks.retain(|(id, _)| *id != transaction_id);
                locks.push((transaction_id, lock_type));
                return Ok(());
            }
            
            // For lock downgrades (exclusive -> shared), just add the shared lock
            if *existing_lock_type == LockType::Exclusive && lock_type == LockType::Shared {
                locks.push((transaction_id, lock_type));
                return Ok(());
            }
        }
        
        // Check for conflicts with other transactions
        for (existing_id, existing_lock) in locks.iter() {
            if *existing_id != transaction_id {
                match (existing_lock, &lock_type) {
                    // Shared locks are compatible with each other
                    (LockType::Shared, LockType::Shared) => continue,
                    // All other combinations are incompatible
                    _ => {
                        return Err(ReefDBError::LockConflict(format!(
                            "Lock conflict: Transaction {} cannot acquire {:?} lock on table {} held by transaction {}",
                            transaction_id, lock_type, table_name, existing_id
                        )));
                    }
                }
            }
        }
        
        // Add the lock to the table's lock list
        locks.push((transaction_id, lock_type));
     
        Ok(())
    }

    pub fn release_transaction_locks(&mut self, transaction_id: u64) {
        for locks in self.table_locks.values_mut() {
            locks.retain(|(id, _)| *id != transaction_id);
        }
        // Clean up empty lock lists
        self.table_locks.retain(|_, locks| !locks.is_empty());
    }

    pub fn get_lock_holders(&self, table_name: &str) -> Vec<u64> {
        self.table_locks
            .get(table_name)
            .map(|locks| locks.iter().map(|(id, _)| *id).collect())
            .unwrap_or_default()
    }

    pub fn has_lock(&self, transaction_id: u64, table_name: &str) -> bool {
        self.table_locks
            .get(table_name)
            .map(|locks| locks.iter().any(|(id, _)| *id == transaction_id))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_manager() {
        let mut manager = LockManager::new();

        // Test acquiring shared locks
        assert!(manager.acquire_lock(1, "users", LockType::Shared).is_ok());
        assert!(manager.acquire_lock(2, "users", LockType::Shared).is_ok());

        // Test acquiring exclusive lock fails when shared locks exist
        assert!(matches!(
            manager.acquire_lock(3, "users", LockType::Exclusive),
            Err(ReefDBError::LockConflict(_))
        ));

        // Test releasing locks
        manager.release_transaction_locks(1);
        manager.release_transaction_locks(2);

        // Test acquiring exclusive lock succeeds after releases
        assert!(manager.acquire_lock(3, "users", LockType::Exclusive).is_ok());

        // Test acquiring shared lock fails when exclusive lock exists
        assert!(matches!(
            manager.acquire_lock(4, "users", LockType::Shared),
            Err(ReefDBError::LockConflict(_))
        ));

        // Test different tables don't conflict
        assert!(manager.acquire_lock(4, "posts", LockType::Exclusive).is_ok());
    }

    #[test]
    fn test_lock_holders() {
        let mut manager = LockManager::new();
        
        // Add some locks
        manager.acquire_lock(1, "users", LockType::Shared).unwrap();
        manager.acquire_lock(2, "users", LockType::Shared).unwrap();
        
        // Test get_lock_holders
        let holders = manager.get_lock_holders("users");
        assert_eq!(holders.len(), 2);
        assert!(holders.contains(&1));
        assert!(holders.contains(&2));
        
        // Test has_lock
        assert!(manager.has_lock(1, "users"));
        assert!(manager.has_lock(2, "users"));
    }

    #[test]
    fn test_same_transaction_locks() {
        let mut manager = LockManager::new();
        
        // Test acquiring both shared and exclusive locks for the same transaction
        assert!(manager.acquire_lock(1, "users", LockType::Shared).is_ok());
        assert!(manager.acquire_lock(1, "users", LockType::Exclusive).is_ok());
        
        // Test other transactions still can't acquire locks
        assert!(matches!(
            manager.acquire_lock(2, "users", LockType::Shared),
            Err(ReefDBError::LockConflict(_))
        ));
    }

    #[test]
    fn test_mixed_locks_same_transaction() {
        let mut manager = LockManager::new();
        
        // Test acquiring exclusive lock first
        assert!(manager.acquire_lock(1, "users", LockType::Exclusive).is_ok());
        
        // Test acquiring shared lock after exclusive for same transaction
        assert!(manager.acquire_lock(1, "users", LockType::Shared).is_ok());
        
        // Verify other transactions still can't acquire any locks
        assert!(matches!(
            manager.acquire_lock(2, "users", LockType::Shared),
            Err(ReefDBError::LockConflict(_))
        ));
        assert!(matches!(
            manager.acquire_lock(2, "users", LockType::Exclusive),
            Err(ReefDBError::LockConflict(_))
        ));
    }
} 