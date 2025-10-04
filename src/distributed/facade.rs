use crate::distributed::raft_node::SingleNodeRaft;
use crate::error::ReefDBError;
use crate::snapshot::{SnapshotData, SnapshotMeta};
use crate::state_machine::{CommandBatch, ReplicatedCommand};
use crate::storage::Storage;
use crate::fts::search::Search;

/// A high-level facade simplifying the distributed API for application developers.
/// Hides batches and raft internals; exposes propose commands and query helpers.
pub struct DistributedReef<S, FTS>
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + 'static,
    FTS: Search + Clone,
    FTS::NewArgs: Clone + Default,
{
    raft: SingleNodeRaft<S, FTS>,
}

impl<S, FTS> DistributedReef<S, FTS>
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + 'static,
    FTS: Search + Clone,
    FTS::NewArgs: Clone + Default,
{
    pub fn from_raft(raft: SingleNodeRaft<S, FTS>) -> Self {
        Self { raft }
    }

    pub fn into_inner(self) -> SingleNodeRaft<S, FTS> { self.raft }

    pub fn query(&mut self, sql: &str) -> Result<crate::result::ReefDBResult, ReefDBError> {
        self.raft.reef_mut().query(sql)
    }

    pub fn create_table(&mut self, name: &str, columns: Vec<crate::sql::column_def::ColumnDef>) -> Result<(), ReefDBError> {
        self.propose_commands(vec![ReplicatedCommand::CreateTable { name: name.to_string(), columns }])
    }

    pub fn drop_table(&mut self, name: &str) -> Result<(), ReefDBError> {
        self.propose_commands(vec![ReplicatedCommand::DropTable { name: name.to_string() }])
    }

    pub fn insert_row(&mut self, table: &str, values: Vec<crate::sql::data_value::DataValue>) -> Result<(), ReefDBError> {
        self.propose_commands(vec![ReplicatedCommand::InsertRow { table: table.to_string(), values }])
    }

    pub fn update_rows(
        &mut self,
        table: &str,
        updates: Vec<(String, crate::sql::data_value::DataValue)>,
        where_clause: Option<(String, crate::sql::data_value::DataValue)>,
    ) -> Result<(), ReefDBError> {
        self.propose_commands(vec![ReplicatedCommand::UpdateRows {
            table: table.to_string(),
            updates,
            where_clause,
        }])
    }

    pub fn delete_rows(
        &mut self,
        table: &str,
        where_clause: Option<(String, crate::sql::data_value::DataValue)>,
    ) -> Result<(), ReefDBError> {
        self.propose_commands(vec![ReplicatedCommand::DeleteRows { table: table.to_string(), where_clause }])
    }

    pub fn snapshot(&self) -> Result<(SnapshotMeta, SnapshotData), ReefDBError> { self.raft.snapshot() }
    pub fn restore(&mut self, meta: SnapshotMeta, data: SnapshotData) -> Result<(), ReefDBError> { self.raft.restore(meta, data) }

    fn propose_commands(&mut self, commands: Vec<ReplicatedCommand>) -> Result<(), ReefDBError> {
        // Use the engine's next_command_id to keep idempotency aligned with the state machine
        let id = self.raft.reef_mut().next_command_id();
        let batch = CommandBatch { id, commands };
        self.raft.propose(batch)
    }

    pub fn leadership_info(&self) -> crate::distributed::types::LeadershipInfo { self.raft.leadership_info() }
}


