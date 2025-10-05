use crate::error::ReefDBError;
use crate::snapshot::{SnapshotData, SnapshotMeta, SnapshotProvider};
use crate::state_machine::{CommandBatch};
use crate::storage::Storage;
use crate::fts::search::Search;
use crate::ReefDB;

use crate::distributed::types::{LeadershipInfo, NodeRole};
#[cfg(feature = "raft-tikv")]
use crate::distributed::network::transport::RaftTransport;
#[cfg(feature = "raft-tikv")]
use std::sync::Arc;
#[cfg(feature = "raft-tikv")]
use crate::distributed::config::NodeConfig;

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

    /// Serialize a snapshot pair.
    pub fn encode_snapshot(meta: &SnapshotMeta, data: &SnapshotData) -> Result<Vec<u8>, ReefDBError> {
        bincode::serialize(&(meta, data)).map_err(|e| ReefDBError::Other(format!("encode snapshot: {e}")))
    }

    /// Decode a snapshot pair.
    pub fn decode_snapshot(bytes: &[u8]) -> Result<(SnapshotMeta, SnapshotData), ReefDBError> {
        bincode::deserialize(bytes).map_err(|e| ReefDBError::Other(format!("decode snapshot: {e}")))
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


/// Thin adapter to present a future multi-node Raft node with the same API
/// while delegating to the single-node implementation for now.
pub struct RaftNode<S, FTS>
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + 'static,
    FTS: Search + Clone,
    FTS::NewArgs: Clone + Default,
{
    inner: SingleNodeRaft<S, FTS>,
}

impl<S, FTS> RaftNode<S, FTS>
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + 'static,
    FTS: Search + Clone,
    FTS::NewArgs: Clone + Default,
{
    pub fn from_single(inner: SingleNodeRaft<S, FTS>) -> Self { Self { inner } }
    pub fn propose(&mut self, batch: CommandBatch) -> Result<(), ReefDBError> { self.inner.propose(batch) }
    pub fn snapshot(&self) -> Result<(SnapshotMeta, SnapshotData), ReefDBError> { self.inner.snapshot() }
    pub fn restore(&mut self, meta: SnapshotMeta, data: SnapshotData) -> Result<(), ReefDBError> { self.inner.restore(meta, data) }
    pub fn leadership_info(&self) -> LeadershipInfo { self.inner.leadership_info() }
    pub fn reef_mut(&mut self) -> &mut ReefDB<S, FTS> { self.inner.reef_mut() }
    pub fn reef(&self) -> &ReefDB<S, FTS> { self.inner.reef() }
}

// Minimal real Raft scaffolding using TiKV raft in-memory storage.
// This intentionally does not implement persistence or networking yet.
#[allow(dead_code)]
#[cfg(feature = "raft-tikv")]
pub struct RealRaftNode<S, FTS>
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + 'static,
    FTS: Search + Clone,
    FTS::NewArgs: Clone + Default,
{
    pub id: u64,
    pub raw: raft::RawNode<raft::storage::MemStorage>,
    pub raft_apply_index: u64,
    pub reef: ReefDB<S, FTS>,
    pub peers: Vec<u64>,
    pub transport: Option<Arc<dyn RaftTransport>>, 
}

#[cfg(feature = "raft-tikv")]
impl<S, FTS> RealRaftNode<S, FTS>
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + 'static,
    FTS: Search + Clone,
    FTS::NewArgs: Clone + Default,
{
    pub fn new(id: u64, reef: ReefDB<S, FTS>) -> Self {
        use raft::{Config, storage::MemStorage, RawNode};
        let mut cfg = Config::new(id);
        cfg.id = id;
        cfg.validate().expect("invalid raft config");
        let storage = MemStorage::new_with_conf_state((vec![id], vec![]));
        let raw = RawNode::new(&cfg, storage, &raft::default_logger()).expect("rawnode");
        Self { id, raw, raft_apply_index: 0, reef, peers: vec![id], transport: None }
    }

    pub fn new_with_peers(id: u64, reef: ReefDB<S, FTS>, peers: Vec<u64>) -> Self {
        use raft::{Config, storage::MemStorage, RawNode};
        let mut cfg = Config::new(id);
        cfg.id = id;
        cfg.validate().expect("invalid raft config");
        let storage = MemStorage::new_with_conf_state((peers.clone(), vec![]));
        let raw = RawNode::new(&cfg, storage, &raft::default_logger()).expect("rawnode");
        Self { id, raw, raft_apply_index: 0, reef, peers, transport: None }
    }

    pub fn new_with_config(node_cfg: &NodeConfig, reef: ReefDB<S, FTS>) -> Self {
        use raft::{Config, storage::MemStorage, RawNode};
        let mut cfg = Config::new(node_cfg.node_id);
        cfg.id = node_cfg.node_id;
        if let Some(e) = node_cfg.election_tick { cfg.election_tick = e as usize; }
        if let Some(h) = node_cfg.heartbeat_tick { cfg.heartbeat_tick = h as usize; }
        cfg.validate().expect("invalid raft config");
        let peers: Vec<u64> = node_cfg.peers.iter().map(|p| p.node_id).collect();
        let storage = MemStorage::new_with_conf_state((peers.clone(), vec![]));
        let raw = RawNode::new(&cfg, storage, &raft::default_logger()).expect("rawnode");
        Self { id: node_cfg.node_id, raw, raft_apply_index: 0, reef, peers, transport: None }
    }

    pub fn start(&mut self) {
        let _ = self.raw.campaign();
    }

    pub fn with_transport(mut self, transport: Arc<dyn RaftTransport>, peers: Vec<u64>) -> Self {
        self.transport = Some(transport);
        self.peers = peers;
        self
    }

    pub fn propose(&mut self, batch: CommandBatch) -> Result<(), ReefDBError> {
        let data = SingleNodeRaft::<S, FTS>::encode_entry(&batch)?;
        self.raw.propose(vec![], data).map_err(|e| ReefDBError::Other(e.to_string()))?;
        Ok(())
    }

    pub fn on_step(&mut self, msg: raft::prelude::Message) -> Result<(), ReefDBError> {
        self.raw.step(msg).map_err(|e| ReefDBError::Other(e.to_string()))
    }

    pub fn tick(&mut self) { self.raw.tick(); }

    pub fn on_ready(&mut self) -> Result<(), ReefDBError> {
        if !self.raw.has_ready() { return Ok(()); }
        let mut ready = self.raw.ready();
        if !ready.messages().is_empty() {
            if let Some(tx) = &self.transport {
                for m in ready.take_messages() {
                    let _ = tx.send(m.to, &m);
                }
            }
        }
        if !ready.entries().is_empty() {
            // Persist entries to raft log here
        }
        if let Some(hs) = ready.hs() {
            // Persist hard state
            let _ = hs;
        }
        if !ready.committed_entries().is_empty() {
            for ent in ready.committed_entries().iter() {
                if !ent.data.is_empty() {
                    let _ = self.apply_entry(&ent.data)?;
                    self.raft_apply_index = ent.index;
                }
            }
        }
        self.raw.advance(ready);
        Ok(())
    }

    fn apply_entry(&mut self, data: &[u8]) -> Result<(), ReefDBError> {
        let batch = SingleNodeRaft::<S, FTS>::decode_entry(data)?;
        let _ = self.reef.apply_batch(batch)?;
        Ok(())
    }

    pub fn leadership_info(&self) -> LeadershipInfo {
        LeadershipInfo {
            role: if self.raw.raft.state == raft::StateRole::Leader { NodeRole::Leader } else { NodeRole::Follower },
            term: self.raw.raft.term,
            commit_index: self.raw.raft.raft_log.committed,
            apply_index: self.raft_apply_index,
        }
    }
}

/// Spawn a background loop to drive Raft ticks and readiness processing.
/// The caller controls the tick cadence via `interval` to align with configured heartbeat/election ticks.
#[cfg(feature = "raft-tikv")]
pub fn spawn_raft_background<S, FTS>(
    node: Arc<tokio::sync::Mutex<RealRaftNode<S, FTS>>>,
    interval: std::time::Duration,
) -> tokio::task::JoinHandle<()>
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + 'static,
    FTS: Search + Clone,
    FTS::NewArgs: Clone + Default,
{
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let mut guard = node.lock().await;
            guard.tick();
            let _ = guard.on_ready();
        }
    })
}

