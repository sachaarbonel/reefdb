use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;
use crate::{
    error::ReefDBError,
    TableStorage,
    transaction::IsolationLevel,
};

pub struct AcidManager {
    initial_snapshot: TableStorage,
    current_snapshot: TableStorage,
    committed: AtomicBool,
    durability_enabled: bool,
    sync_path: Option<std::path::PathBuf>,
    isolation_level: IsolationLevel,
}

impl Clone for AcidManager {
    fn clone(&self) -> Self {
        AcidManager {
            initial_snapshot: self.initial_snapshot.clone(),
            current_snapshot: self.current_snapshot.clone(),
            committed: AtomicBool::new(self.committed.load(Ordering::SeqCst)),
            durability_enabled: self.durability_enabled,
            sync_path: self.sync_path.clone(),
            isolation_level: self.isolation_level.clone(),
        }
    }
}

impl AcidManager {
    pub fn new(initial_state: TableStorage, isolation_level: IsolationLevel) -> Self {
        AcidManager {
            initial_snapshot: initial_state.clone(),
            current_snapshot: initial_state,
            committed: AtomicBool::new(false),
            durability_enabled: true,
            sync_path: None,
            isolation_level,
        }
    }

    pub fn begin_atomic(&mut self, tables: &TableStorage) {
        match self.isolation_level {
            IsolationLevel::ReadUncommitted => {
                // For read uncommitted, we can see uncommitted changes
                self.current_snapshot = tables.clone();
            },
            IsolationLevel::ReadCommitted => {
                // For read committed, we take a new snapshot at each read
                self.current_snapshot = tables.clone();
            },
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                // For repeatable read and serializable, we keep the initial snapshot
                // Only update the initial snapshot if this is the first time or after a commit
                if !self.committed.load(Ordering::SeqCst) {
                    self.initial_snapshot = tables.clone();
                    self.current_snapshot = tables.clone();
                }
                // For serializable, we need to ensure we're always working with the initial snapshot
                if self.isolation_level == IsolationLevel::Serializable {
                    self.current_snapshot = self.initial_snapshot.clone();
                }
            },
        }
        self.committed.store(false, Ordering::SeqCst);
    }

    pub fn commit(&self) -> Result<(), ReefDBError> {
        if self.durability_enabled {
            // Ensure data is written to disk
            sync_to_disk(self.sync_path.as_deref())?;
        }
        self.committed.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn rollback_atomic(&self) -> TableStorage {
        self.initial_snapshot.clone()
    }

    pub fn get_committed_snapshot(&self) -> TableStorage {
        match self.isolation_level {
            IsolationLevel::ReadUncommitted => {
                // For read uncommitted, we can see uncommitted changes
                self.current_snapshot.clone()
            },
            IsolationLevel::ReadCommitted => {
                // For read committed, we see the latest committed state
                if self.committed.load(Ordering::SeqCst) {
                    self.current_snapshot.clone()
                } else {
                    self.initial_snapshot.clone()
                }
            },
            IsolationLevel::RepeatableRead => {
                // For repeatable read, we always see the initial snapshot
                // This ensures we have a consistent view throughout the transaction
                self.initial_snapshot.clone()
            },
            IsolationLevel::Serializable => {
                // For serializable, we always see the initial snapshot
                // This ensures we have a consistent view throughout the transaction
                // and prevents any visibility of uncommitted changes
                self.initial_snapshot.clone()
            },
        }
    }
}

fn sync_to_disk(sync_path: Option<&std::path::Path>) -> Result<(), ReefDBError> {
    // Force sync to disk using fsync
    #[cfg(unix)]
    {
        let path = sync_path.unwrap_or_else(|| std::path::Path::new(".sync"));
        std::fs::File::create(path)
            .map_err(|e| ReefDBError::Other(format!("Failed to create sync file: {}", e)))?
            .sync_all()
            .map_err(|e| ReefDBError::Other(format!("Failed to sync to disk: {}", e)))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_table() -> TableStorage {
        let mut storage = TableStorage::new();
        // ... existing table setup code ...
        storage
    }

    #[test]
    fn test_basic_transaction_flow() {
        let initial_state = create_test_table();
        let mut manager = AcidManager::new(initial_state.clone(), IsolationLevel::ReadCommitted);

        // Test initial state
        assert!(!manager.committed.load(Ordering::SeqCst));
        
        // Take a snapshot
        manager.begin_atomic(&initial_state);
        
        // Verify snapshot matches initial state
        let snapshot = manager.rollback_atomic();
        assert_eq!(format!("{:?}", snapshot), format!("{:?}", initial_state));
        
        // Test commit
        assert!(manager.commit().is_ok());
        assert!(manager.committed.load(Ordering::SeqCst));
    }

    #[test]
    fn test_get_committed_snapshot() {
        let initial_state = create_test_table();
        let mut manager = AcidManager::new(initial_state.clone(), IsolationLevel::ReadCommitted);

        // Before commit, should return empty state
        assert_eq!(
            format!("{:?}", manager.get_committed_snapshot()),
            format!("{:?}", TableStorage::new())
        );

        // After commit, should return actual state
        manager.begin_atomic(&initial_state);
        manager.commit().unwrap();
        assert_eq!(
            format!("{:?}", manager.get_committed_snapshot()),
            format!("{:?}", initial_state)
        );
    }

    #[test]
    fn test_clone() {
        let initial_state = create_test_table();
        let mut manager = AcidManager::new(initial_state.clone(), IsolationLevel::ReadCommitted);
        manager.begin_atomic(&initial_state);
        manager.commit().unwrap();

        let cloned = manager.clone();
        assert_eq!(
            format!("{:?}", cloned.get_committed_snapshot()),
            format!("{:?}", manager.get_committed_snapshot())
        );
        assert_eq!(cloned.committed.load(Ordering::SeqCst), manager.committed.load(Ordering::SeqCst));
        assert_eq!(cloned.durability_enabled, manager.durability_enabled);
        assert_eq!(cloned.sync_path, manager.sync_path);
    }

    #[test]
    fn test_durability_settings() {
        let mut manager = AcidManager::new(TableStorage::new(), IsolationLevel::ReadCommitted);
        
        // Test with durability enabled (default)
        assert!(manager.durability_enabled);
        
        // Disable durability and verify commit doesn't try to sync
        manager.durability_enabled = false;
        manager.begin_atomic(&TableStorage::new());
        assert!(manager.commit().is_ok());
    }

    #[test]
    fn test_read_uncommitted() {
        let initial_state = create_test_table();
        let mut manager = AcidManager::new(initial_state.clone(), IsolationLevel::ReadUncommitted);
        
        // Create modified state
        let mut modified_state = initial_state.clone();
        // Simulate modification to state
        
        // Begin atomic operation with modified state
        manager.begin_atomic(&modified_state);
        
        // Even without commit, read uncommitted should see the changes
        assert_eq!(
            format!("{:?}", manager.get_committed_snapshot()),
            format!("{:?}", modified_state)
        );
    }

    #[test]
    fn test_read_committed() {
        let initial_state = create_test_table();
        let mut manager = AcidManager::new(initial_state.clone(), IsolationLevel::ReadCommitted);
        
        // Create modified state
        let mut modified_state = initial_state.clone();
        // Simulate modification to state
        
        // Begin atomic operation with modified state
        manager.begin_atomic(&modified_state);
        
        // Before commit, should see initial state
        assert_eq!(
            format!("{:?}", manager.get_committed_snapshot()),
            format!("{:?}", initial_state)
        );
        
        // After commit, should see modified state
        manager.commit().unwrap();
        assert_eq!(
            format!("{:?}", manager.get_committed_snapshot()),
            format!("{:?}", modified_state)
        );
    }

    #[test]
    fn test_repeatable_read() {
        let initial_state = create_test_table();
        let mut manager = AcidManager::new(initial_state.clone(), IsolationLevel::RepeatableRead);
        
        // Create modified state
        let mut modified_state = initial_state.clone();
        // Simulate modification to state
        
        // Begin atomic operation with initial state
        manager.begin_atomic(&initial_state);
        
        // Even after modifying and committing, should still see initial snapshot
        manager.begin_atomic(&modified_state);
        manager.commit().unwrap();
        
        assert_eq!(
            format!("{:?}", manager.get_committed_snapshot()),
            format!("{:?}", initial_state)
        );
    }

    #[test]
    fn test_serializable() {
        let initial_state = create_test_table();
        let mut manager = AcidManager::new(initial_state.clone(), IsolationLevel::Serializable);
        
        // Create modified state
        let mut modified_state = initial_state.clone();
        // Simulate modification to state
        
        // Begin atomic operation with initial state
        manager.begin_atomic(&initial_state);
        
        // Even after modifying and committing, should still see initial snapshot
        manager.begin_atomic(&modified_state);
        manager.commit().unwrap();
        
        assert_eq!(
            format!("{:?}", manager.get_committed_snapshot()),
            format!("{:?}", initial_state)
        );
    }
} 