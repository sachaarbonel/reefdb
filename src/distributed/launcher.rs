use std::net::SocketAddr;
use std::sync::Arc;

use crate::distributed::config::NodeConfig;
use crate::distributed::facade::DistributedReef;
use crate::distributed::network::server::{serve as serve_grpc, RpcState};
use crate::distributed::raft_node::SingleNodeRaft;
use crate::fts::search::Search;
use crate::storage::Storage;
use crate::ReefDB;

#[cfg(feature = "raft-tikv")]
use crate::distributed::raft_node::{RealRaftNode, spawn_raft_background};
#[cfg(feature = "raft-tikv")]
use crate::distributed::network::transport::GrpcTransport;

/// Launch a distributed Reef node, starting gRPC and (optionally) the Raft background loop.
/// Returns the spawned tasks' JoinHandles so the caller can await or supervise them.
pub async fn launch_node<S, FTS>(
    cfg: NodeConfig,
    db: ReefDB<S, FTS>,
) -> Result<(
    Arc<tokio::sync::Mutex<DistributedReef<S, FTS>>>,
    tokio::task::JoinHandle<()>,
    Option<tokio::task::JoinHandle<()>>,
), Box<dyn std::error::Error + Send + Sync>>
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + Send + Sync + 'static,
    FTS: Search + Clone + Send + Sync + 'static,
    FTS::NewArgs: Clone + Default,
{
    let raft = SingleNodeRaft::new(db.clone());
    let reef = DistributedReef::from_raft(raft);
    let app_state = Arc::new(tokio::sync::Mutex::new(reef));

    let mut peer_map = std::collections::HashMap::new();
    for p in &cfg.peers { peer_map.insert(p.node_id, p.addr.clone()); }

    #[cfg(feature = "raft-tikv")]
    let (rpc_state, raft_node_handle) = {
        let transport = std::sync::Arc::new(GrpcTransport::new(peer_map.clone()));
        let real = RealRaftNode::new_with_config(&cfg, db.clone())
            .with_transport(transport, cfg.peers.iter().map(|p| p.node_id).collect());
        let raft_node = Arc::new(tokio::sync::Mutex::new(real));
        {
            let mut guard = raft_node.blocking_lock();
            guard.start();
        }
        (Arc::new(RpcState { reef: app_state.clone(), raft_node: Some(raft_node.clone()), peers: peer_map }), Some(raft_node))
    };

    #[cfg(not(feature = "raft-tikv"))]
    let rpc_state = Arc::new(RpcState { reef: app_state.clone(), peers: peer_map });

    let rpc_addr: SocketAddr = cfg.rpc_addr.parse()?;
    let grpc_task = tokio::spawn(async move {
        let _ = serve_grpc(rpc_state, rpc_addr).await;
    });

    #[cfg(feature = "raft-tikv")]
    let raft_loop = {
        let tick_ms = cfg.raft_tick_ms.unwrap_or(100);
        let node = raft_node_handle.expect("raft node handle");
        Some(spawn_raft_background(node, std::time::Duration::from_millis(tick_ms)))
    };

    #[cfg(not(feature = "raft-tikv"))]
    let raft_loop = None;

    Ok((app_state, grpc_task, raft_loop))
}


