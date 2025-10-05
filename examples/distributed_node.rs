use std::net::SocketAddr;
use std::sync::Arc;

use axum::{routing::{get, post}, Router, extract::State, Json};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use reefdb::distributed::config::NodeConfig;
use reefdb::distributed::facade::DistributedReef;
use reefdb::distributed::network::server::{serve as serve_grpc, RpcState};
use reefdb::distributed::raft_node::SingleNodeRaft;
use reefdb::OnDiskReefDB;
#[cfg(feature = "raft-tikv")]
use reefdb::distributed::raft_node::RealRaftNode;
#[cfg(feature = "raft-tikv")]
use reefdb::distributed::network::transport::GrpcTransport;

#[derive(Clone)]
struct AppState {
    reef: Arc<tokio::sync::Mutex<DistributedReef<reefdb::storage::disk::OnDiskStorage, reefdb::fts::default::DefaultSearchIdx>>>,
}

#[derive(Deserialize)]
struct SqlPayload { sql: String }

#[derive(Serialize)]
struct ExecResponse { ok: bool, result: String }

#[tokio::main]
async fn main() {
    let cfg_path = std::env::args().nth(1).unwrap_or_else(|| "node.yaml".to_string());
    let cfg = NodeConfig::from_file(&cfg_path).expect("failed to load config");

    let db: OnDiskReefDB = OnDiskReefDB::create_on_disk(cfg.data_dir.clone(), String::from(""))
        .expect("failed to open on-disk db");

    // Initialize the single-node facade for SQL
    let raft = SingleNodeRaft::new(db.clone());
    let reef = DistributedReef::from_raft(raft);
    let app_state = AppState { reef: Arc::new(tokio::sync::Mutex::new(reef)) };

    // Start gRPC server
    // Build peer map for server (used for leader forwarding)
    let mut peer_map = std::collections::HashMap::new();
    for p in &cfg.peers { peer_map.insert(p.node_id, p.addr.clone()); }
    #[cfg(feature = "raft-tikv")]
    let rpc_state = {
        let transport = std::sync::Arc::new(GrpcTransport::new(peer_map.clone()));
        let peers: Vec<u64> = cfg.peers.iter().map(|p| p.node_id).collect();
        let real = RealRaftNode::new_with_peers(cfg.node_id, db).with_transport(transport, peers);
        let raft_node = Arc::new(tokio::sync::Mutex::new(real));
        Arc::new(RpcState { reef: app_state.reef.clone(), raft_node: Some(raft_node.clone()), peers: peer_map })
    };
    #[cfg(not(feature = "raft-tikv"))]
    let rpc_state = Arc::new(RpcState { reef: app_state.reef.clone(), peers: peer_map });
    let rpc_addr: SocketAddr = cfg.rpc_addr.parse().expect("invalid rpc_addr");
    let grpc_task = tokio::spawn(async move {
        serve_grpc(rpc_state, rpc_addr).await.expect("grpc server failed")
    });

    #[cfg(feature = "raft-tikv")]
    let raft_loop = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            interval.tick().await;
            // The raft loop is managed inside the server's node; no-op here or extend as needed
        }
    });

    // Start HTTP gateway
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/query", post(handle_query))
        .with_state(app_state);

    let http_addr: SocketAddr = cfg.http_addr.parse().expect("invalid http_addr");
    let listener = TcpListener::bind(http_addr).await.expect("bind http_addr");
    println!("HTTP listening on {} | gRPC on {}", http_addr, cfg.rpc_addr);
    let http_task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    #[cfg(feature = "raft-tikv")]
    let _ = tokio::join!(grpc_task, http_task, raft_loop);
    #[cfg(not(feature = "raft-tikv"))]
    let _ = tokio::join!(grpc_task, http_task);
}

async fn handle_query(State(state): State<AppState>, Json(payload): Json<SqlPayload>) -> Json<ExecResponse> {
    let mut guard = state.reef.lock().await;
    let res = guard.query(&payload.sql);
    match res {
        Ok(r) => Json(ExecResponse { ok: true, result: format!("{:?}", r) }),
        Err(e) => Json(ExecResponse { ok: false, result: e.to_string() }),
    }
}


