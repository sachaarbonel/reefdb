use std::net::SocketAddr;
use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};
use tokio_stream::StreamExt;

use crate::distributed::facade::DistributedReef;
use crate::distributed::network::pb::reefdb::raft::{raft_server::{Raft, RaftServer}, RaftMessage};
use crate::distributed::network::pb::reefdb::raft::LeadershipInfo as PbLeadershipInfo;
use crate::distributed::network::pb::reefdb::sql::{sql_server::{Sql, SqlServer}, SqlRequest, SqlResponse};
#[cfg(feature = "raft-tikv")]
use crate::distributed::network::client::SqlClient;
use crate::fts::search::Search;
use crate::storage::Storage;
use crate::distributed::raft_node::SingleNodeRaft;
#[cfg(feature = "raft-tikv")]
use crate::distributed::raft_node::RealRaftNode;
#[cfg(feature = "raft-tikv")]
use protobuf::Message as PbMessage;
#[cfg(feature = "raft-tikv")]
use raft::prelude::Message as RaftWireMessage;

#[derive(Clone)]
pub struct RpcState<S, FTS>
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + 'static,
    FTS: Search + Clone,
    FTS::NewArgs: Clone + Default,
{
    pub reef: Arc<Mutex<DistributedReef<S, FTS>>>,
    #[cfg(feature = "raft-tikv")]
    pub raft_node: Option<Arc<Mutex<RealRaftNode<S, FTS>>>>,
    pub peers: HashMap<u64, String>,
}

#[derive(Clone)]
pub struct RaftSvc<S, FTS>(Arc<RpcState<S, FTS>>)
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + 'static,
    FTS: Search + Clone,
    FTS::NewArgs: Clone + Default;

#[tonic::async_trait]
impl<S, FTS> Raft for RaftSvc<S, FTS>
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + Send + Sync + 'static,
    FTS: Search + Clone + Send + Sync + 'static,
    FTS::NewArgs: Clone + Default,
{
    async fn step(&self, request: Request<RaftMessage>) -> Result<Response<RaftMessage>, Status> {
        let bytes = request.into_inner().data;
        #[cfg(feature = "raft-tikv")]
        if let Some(node) = &self.0.raft_node {
            let msg = protobuf::Message::parse_from_bytes::<raft::prelude::Message>(&bytes)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let mut guard = node.lock().await;
            guard.on_step(msg).map_err(|e| Status::internal(e.to_string()))?;
            guard.on_ready().map_err(|e| Status::internal(e.to_string()))?;
        }
        Ok(Response::new(RaftMessage { data: vec![] }))
    }

    async fn install_snapshot(
        &self,
        request: Request<tonic::Streaming<RaftMessage>>,
    ) -> Result<Response<RaftMessage>, Status> {
        let mut stream = request.into_inner();
        let mut buffer: Vec<u8> = Vec::new();
        while let Some(chunk) = stream.next().await {
            let msg = chunk?;
            buffer.extend_from_slice(&msg.data);
        }
        let (meta, data) = SingleNodeRaft::<S, FTS>::decode_snapshot(&buffer)
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut guard = self.0.reef.lock().await;
        guard.restore(meta, data).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(RaftMessage { data: vec![] }))
    }

    async fn get_info(&self, _req: Request<RaftMessage>) -> Result<Response<PbLeadershipInfo>, Status> {
        #[cfg(feature = "raft-tikv")]
        if let Some(node) = &self.0.raft_node {
            let guard = node.lock().await;
            let role = if guard.raw.raft.state == raft::StateRole::Leader { "Leader" } else { "Follower" };
            let info = PbLeadershipInfo { role: role.to_string(), term: guard.raw.raft.term, commit_index: guard.raw.raft.raft_log.committed, apply_index: guard.raft_apply_index };
            return Ok(Response::new(info));
        }
        Ok(Response::new(PbLeadershipInfo { role: "Leader".to_string(), term: 1, commit_index: 0, apply_index: 0 }))
    }
}

#[derive(Clone)]
pub struct SqlSvc<S, FTS>(Arc<RpcState<S, FTS>>)
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + 'static,
    FTS: Search + Clone,
    FTS::NewArgs: Clone + Default;

#[tonic::async_trait]
impl<S, FTS> Sql for SqlSvc<S, FTS>
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + Send + Sync + 'static,
    FTS: Search + Clone + Send + Sync + 'static,
    FTS::NewArgs: Clone + Default,
{
    async fn execute(&self, request: Request<SqlRequest>) -> Result<Response<SqlResponse>, Status> {
        let sql = request.into_inner().sql;
        #[cfg(feature = "raft-tikv")]
        if let Some(node) = &self.0.raft_node {
            let node_guard = node.lock().await;
            let is_leader = node_guard.raw.raft.state == raft::StateRole::Leader;
            let leader_id = node_guard.raw.raft.leader_id;
            drop(node_guard);
            if !is_leader {
                if leader_id == 0 { return Ok(Response::new(SqlResponse { ok: false, result: "no leader".to_string() })); }
                if let Some(addr) = self.0.peers.get(&leader_id) {
                    let mut client = SqlClient::connect(addr).await.map_err(|e| Status::unavailable(e.to_string()))?;
                    let resp = client.execute(&sql).await.map_err(|e| Status::unavailable(e.to_string()))?;
                    return Ok(Response::new(resp));
                } else {
                    return Ok(Response::new(SqlResponse { ok: false, result: format!("leader {} addr unknown", leader_id) }));
                }
            }
        }
        let mut guard = self.0.reef.lock().await;
        let res = guard.query(&sql);
        match res {
            Ok(r) => Ok(Response::new(SqlResponse { ok: true, result: format!("{:?}", r) })),
            Err(e) => Ok(Response::new(SqlResponse { ok: false, result: e.to_string() })),
        }
    }
}

pub async fn serve<S, FTS>(
    state: Arc<RpcState<S, FTS>>,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: Storage + crate::indexes::index_manager::IndexManager + Clone + Send + Sync + 'static,
    FTS: Search + Clone + Send + Sync + 'static,
    FTS::NewArgs: Clone + Default,
{
    let raft = RaftSvc(state.clone());
    let sql = SqlSvc(state.clone());

    Server::builder()
        .add_service(RaftServer::new(raft))
        .add_service(SqlServer::new(sql))
        .serve(addr)
        .await?;
    Ok(())
}


