use crate::error::ReefDBError;
use crate::storage::{Storage, TableStorage};
use crate::fts::search::Search;
use crate::ReefDB;

#[derive(Clone, Debug)]
pub struct SnapshotMeta {
    pub last_applied_command: crate::state_machine::CommandId,
}

#[derive(Clone, Debug)]
pub struct SnapshotData {
    pub tables: TableStorage,
}

pub trait SnapshotProvider {
    fn snapshot(&self) -> Result<(SnapshotMeta, SnapshotData), ReefDBError>;
    fn restore(&mut self, meta: SnapshotMeta, data: SnapshotData) -> Result<(), ReefDBError>;
}

impl<S, FTS> SnapshotProvider for ReefDB<S, FTS>
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + 'static,
    FTS: Search + Clone,
    FTS::NewArgs: Clone + Default,
{
    fn snapshot(&self) -> Result<(SnapshotMeta, SnapshotData), ReefDBError> {
        let meta = SnapshotMeta {
            last_applied_command: self.next_command_id.saturating_sub(1),
        };
        let data = SnapshotData {
            tables: self.tables.clone(),
        };
        Ok((meta, data))
    }

    fn restore(&mut self, meta: SnapshotMeta, data: SnapshotData) -> Result<(), ReefDBError> {
        // Replace in-memory tables and storage from snapshot
        self.tables = TableStorage::new();
        self.tables.restore_from(&data.tables);
        self.storage.restore_from(&data.tables);

        // Reset idempotency tracking to the snapshot boundary
        self.applied_commands.clear();
        self.next_command_id = meta.last_applied_command.saturating_add(1);
        Ok(())
    }
}


