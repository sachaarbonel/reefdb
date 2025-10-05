use std::net::SocketAddr;
use std::sync::Arc;

use axum::{routing::{get, post}, Router, extract::State, Json};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use reefdb::distributed::config::NodeConfig;
use reefdb::distributed::launcher::launch_node;
use reefdb::distributed::facade::DistributedReef;
use reefdb::OnDiskReefDB;

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
    let cfg = NodeConfig::from_file(&cfg_path).expect("failed to load config").apply_defaults();
    cfg.validate().expect("invalid config");

    let db: OnDiskReefDB = OnDiskReefDB::create_on_disk(cfg.data_dir.clone(), String::from(""))
        .expect("failed to open on-disk db");
    let (reef_handle, grpc_task, _raft_loop_opt) = launch_node(cfg.clone(), db.clone()).await.expect("launch failed");
    let app_state = AppState { reef: reef_handle.clone() };

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
    let _ = {
        let raft_handle = _raft_loop_opt.expect("raft loop handle");
        tokio::join!(grpc_task, http_task, raft_handle)
    };
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


