use crate::error::ReefDBError;
use crate::snapshot::{SnapshotData, SnapshotMeta, SnapshotProvider};
use crate::state_machine::{CommandBatch};
use crate::storage::Storage;
use crate::fts::search::Search;
use crate::ReefDB;

use crate::distributed::types::{LeadershipInfo, NodeRole};

/// A minimal single-node Raft-like wrapper that provides the shape needed to integrate a real Raft implementation.
///
/// In single-node mode, `propose` immediately applies the batch to the local state machine and returns the results.
pub struct SingleNodeRaft<S, FTS>
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + 'static,
    FTS: Search + Clone,
    FTS::NewArgs: Clone + Default,
{
    reef: ReefDB<S, FTS>,
    term: u64,
    commit_index: u64,
    apply_index: u64,
}

impl<S, FTS> SingleNodeRaft<S, FTS>
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + 'static,
    FTS: Search + Clone,
    FTS::NewArgs: Clone + Default,
{
    pub fn new(reef: ReefDB<S, FTS>) -> Self {
        Self { reef, term: 1, commit_index: 0, apply_index: 0 }
    }

    /// Serialize a `CommandBatch` into bytes to be used as a raft log entry payload.
    pub fn encode_entry(batch: &CommandBatch) -> Result<Vec<u8>, ReefDBError> {
        bincode::serialize(batch).map_err(|e| ReefDBError::Other(format!("encode raft entry: {e}")))
    }

    /// Decode a raft log entry payload back into a `CommandBatch`.
    pub fn decode_entry(bytes: &[u8]) -> Result<CommandBatch, ReefDBError> {
        bincode::deserialize(bytes).map_err(|e| ReefDBError::Other(format!("decode raft entry: {e}")))
    }

    /// Propose a batch. In single-node mode this is applied immediately.
    pub fn propose(&mut self, batch: CommandBatch) -> Result<(), ReefDBError> {
        let encoded = Self::encode_entry(&batch)?;
        self.on_commit(&encoded)
    }

    /// Apply a committed entry into the state machine.
    pub fn on_commit(&mut self, bytes: &[u8]) -> Result<(), ReefDBError> {
        let batch: CommandBatch = Self::decode_entry(bytes)?;
        let _outcomes = self.reef.apply_batch(batch)?;
        self.commit_index = self.commit_index.saturating_add(1);
        self.apply_index = self.apply_index.saturating_add(1);
        Ok(())
    }

    /// Take a snapshot via the state's `SnapshotProvider` implementation.
    pub fn snapshot(&self) -> Result<(SnapshotMeta, SnapshotData), ReefDBError> {
        <ReefDB<S, FTS> as SnapshotProvider>::snapshot(&self.reef)
    }

    /// Restore a snapshot.
    pub fn restore(&mut self, meta: SnapshotMeta, data: SnapshotData) -> Result<(), ReefDBError> {
        <ReefDB<S, FTS> as SnapshotProvider>::restore(&mut self.reef, meta, data)
    }

    pub fn leadership_info(&self) -> LeadershipInfo {
        LeadershipInfo {
            role: NodeRole::Leader,
            term: self.term,
            commit_index: self.commit_index,
            apply_index: self.apply_index,
        }
    }

    pub fn reef_mut(&mut self) -> &mut ReefDB<S, FTS> { &mut self.reef }
    pub fn reef(&self) -> &ReefDB<S, FTS> { &self.reef }
}


