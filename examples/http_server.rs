use axum::{routing::{get, post}, Router, extract::State, Json};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;

use reefdb::distributed::facade::DistributedReef;
use reefdb::distributed::raft_node::SingleNodeRaft;
use reefdb::{OnDiskReefDB};

#[derive(Clone)]
struct AppState {
    reef: Arc<std::sync::Mutex<DistributedReef<reefdb::storage::disk::OnDiskStorage, reefdb::fts::default::DefaultSearchIdx>>>,
}

#[derive(Deserialize)]
struct SqlPayload { sql: String }

#[derive(Serialize)]
struct ExecResponse { ok: bool, result: String }

#[tokio::main]
async fn main() {
    // Simple single-node HTTP server using on-disk storage
    let data_dir = std::env::var("REEFDB_DATA").unwrap_or_else(|_| "./reef_kv.db".to_string());
    let db: OnDiskReefDB = OnDiskReefDB::create_on_disk(data_dir, String::from(""))
        .expect("failed to open on-disk db");

    let raft = SingleNodeRaft::new(db);
    let reef = DistributedReef::from_raft(raft);
    let state = AppState { reef: Arc::new(std::sync::Mutex::new(reef)) };

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/query", post(handle_query))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let listener = TcpListener::bind(addr).await.expect("bind 0.0.0.0:8080");
    println!("Listening on {}", addr);
    axum::serve(listener, app).await.unwrap();
}

async fn handle_query(State(state): State<AppState>, Json(payload): Json<SqlPayload>) -> Json<ExecResponse> {
    let mut guard = state.reef.lock().unwrap();
    let res = guard.query(&payload.sql);
    match res {
        Ok(r) => Json(ExecResponse { ok: true, result: format!("{:?}", r) }),
        Err(e) => Json(ExecResponse { ok: false, result: e.to_string() }),
    }
}


