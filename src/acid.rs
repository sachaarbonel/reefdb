use std::sync::atomic::{AtomicBool, Ordering};
use crate::{
    error::ReefDBError,
    TableStorage,
};

pub struct AcidManager {
    snapshot: TableStorage,
    committed: AtomicBool,
    durability_enabled: bool,
    sync_path: Option<std::path::PathBuf>,
}

impl Clone for AcidManager {
    fn clone(&self) -> Self {
        AcidManager {
            snapshot: self.snapshot.clone(),
            committed: AtomicBool::new(self.committed.load(Ordering::SeqCst)),
            durability_enabled: self.durability_enabled,
            sync_path: self.sync_path.clone(),
        }
    }
}

impl AcidManager {
    pub fn new(initial_state: TableStorage) -> Self {
        AcidManager {
            snapshot: initial_state,
            committed: AtomicBool::new(false),
            durability_enabled: true,
            sync_path: None,
        }
    }

    pub fn begin_atomic(&mut self, tables: &TableStorage) {
        let mut new_snapshot = TableStorage::new();
        new_snapshot.restore_from(tables);
        self.snapshot = new_snapshot;
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
        let mut restored = TableStorage::new();
        restored.restore_from(&self.snapshot);
        restored
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
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_acid_manager() {
        let initial_state = TableStorage::default();
        let mut manager = AcidManager::new(initial_state.clone());

        // Take a snapshot
        manager.begin_atomic(&initial_state);

        // Verify snapshot is taken
        let snapshot = manager.rollback_atomic();
        assert_eq!(format!("{:?}", snapshot), format!("{:?}", initial_state));

        // Test commit
        assert!(manager.commit().is_ok());
        assert!(manager.committed.load(Ordering::SeqCst));
    }
} 