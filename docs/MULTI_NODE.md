### ReefDB Multi-Node Deployment Guide

This guide explains how to evolve the current single-node distributed layer into a production-grade multi-node setup. It builds directly on ReefDB’s state machine and snapshot abstractions, and the thin distributed scaffolding already in-tree.

Key building blocks already present:
- `state_machine.rs`: `ReplicatedCommand`, `CommandBatch`, `ReefDB::apply(id, cmd)`, `ReefDB::apply_batch(batch)`
- `snapshot.rs`: `SnapshotProvider::{snapshot, restore}`, `SnapshotMeta { last_applied_command }`, `SnapshotData { tables }`
- `distributed/raft_node.rs`: single-node apply bridge, bincode codec for `CommandBatch`
- `distributed/facade.rs`: `DistributedReef` API hiding batches and raft internals

The steps below introduce a real Raft core, network RPCs, persistence for Raft metadata/log, and a clean boot/run lifecycle for a multi-node cluster.

---

### 1) Dependencies

Add the following crates (pin versions per your Cargo registry policy):
- Raft core: `raft` (TiKV Raft)
- RPC: `tonic` (gRPC) and `prost` (protobuf)
- Runtime: `tokio` (already used in the HTTP example)

Serialization: continue using `bincode` for `CommandBatch` payloads (already enabled). Keep `TableStorage`, `SnapshotMeta`, `SnapshotData`, and `CommandBatch` serde-enabled (already done).

---

### 2) Module Layout

Create a dedicated distributed subsystem:
- `src/distributed/`
  - `raft_node.rs`: replace the current `SingleNodeRaft` with a real `RaftNode` integrating the Raft core
  - `network/{server.rs, client.rs}`: gRPC services/clients for Raft traffic and an optional client gateway
  - `proto/raft.proto`: Raft RPC message envelopes
  - `facade.rs`: remains as the primary end-user entrypoint (`DistributedReef`)

Keep the public surface minimal: propose/apply, snapshot/restore hooks, and leadership info.

---

### 3) Raft State Machine Mapping

- Log entry payload: `CommandBatch` serialized with `bincode`
- Apply-on-commit: on commit, decode and call `ReefDB::apply_batch(batch)`
- Idempotency: guaranteed by `CommandId` and `applied_commands` in ReefDB; snapshot meta carries `last_applied_command`

Pseudo-code (bridged by the Raft apply thread):
```rust
fn on_raft_commit(bytes: &[u8], reef: &mut ReefDB<_, _>) -> Result<(), ReefDBError> {
    let batch: CommandBatch = bincode::deserialize(bytes)
        .map_err(|e| ReefDBError::Other(format!("decode raft entry: {e}")))?;
    let _ = reef.apply_batch(batch)?;
    Ok(())
}
```

---

### 4) Network RPCs (gRPC via tonic)

Define the Raft traffic and a thin client gateway:

`proto/raft.proto` (sketch):
```proto
syntax = "proto3";
package reefdb.raft;

message RaftMessage { bytes data = 1; }

service Raft {
  rpc Step(RaftMessage) returns (RaftMessage);        // generic raft message pass-through
  rpc InstallSnapshot(stream RaftMessage) returns (RaftMessage); // streaming if needed
}

// Optional: client gateway for SQL (can be separate service)
package reefdb.sql;
message SqlRequest { string sql = 1; }
message SqlResponse { bool ok = 1; string result = 2; }
service Sql {
  rpc Execute(SqlRequest) returns (SqlResponse);
}
```

Server responsibilities:
- `Raft::Step`: deliver messages to the local Raft core
- `Raft::InstallSnapshot`: handle Raft snapshot install; bridge to `SnapshotProvider::restore`
- `Sql::Execute` (optional): on follower, redirect/forward to leader; on leader, build command(s), propose, wait for commit+apply, return result

---

### 5) Persistence for Raft

Persist under a dedicated raft directory per node (e.g., `raft/` next to your KV file):
- Hard state (term, vote)
- Raft log (entries contain `bincode(CommandBatch)`)
- Snapshot files (serialized `(SnapshotMeta, SnapshotData)`)

Durability policy:
- Prefer Raft log as the single authoritative WAL; `apply` is the only mutation path

---

### 6) Node Configuration

Use a simple YAML file for each node:
```yaml
node_id: 1
data_dir: ./data/node1/reef_kv.db
raft_dir: ./data/node1/raft
rpc_addr: 127.0.0.1:50051
http_addr: 127.0.0.1:8081
peers:
  - { node_id: 1, addr: 127.0.0.1:50051 }
  - { node_id: 2, addr: 127.0.0.1:50052 }
  - { node_id: 3, addr: 127.0.0.1:50053 }
```

Recommended env vars (alternatively): `REEFDB_DATA`, `REEFDB_RAFT_DIR`, `REEFDB_RPC_ADDR`, `REEFDB_HTTP_ADDR`.

---

### 7) Boot Sequence

On process start:
1. Initialize storage (`ReefDB`), then the Raft node with the node_id and persisted state
2. If a snapshot exists, `SnapshotProvider::restore(meta, data)`
3. Replay committed Raft log entries after the snapshot via the apply bridge
4. Start the Raft RPC server and join the cluster (self or via bootstrap API)

This keeps in-memory state aligned with the last applied index and `next_command_id`.

---

### 8) Write and Read Paths

Write (client → leader → raft → apply):
1. Client sends SQL to the gateway on the leader (or follower forwards to leader)
2. Leader translates SQL → `Vec<ReplicatedCommand>` → `CommandBatch`
3. Serialize with `bincode` and propose to Raft
4. On commit, leader’s apply thread calls `ReefDB::apply_batch`; reply to client

Reads:
- Linearizable: route to leader, optionally use Raft ReadIndex to ensure state is caught up before serving from memory
- Stale (optional): serve from followers with last-applied visibility

---

### 9) Snapshots and Compaction

Hook Raft’s snapshot request/install to `SnapshotProvider`:
- On request: `ReefDB::snapshot()` → serialize and stream
- On install: decode and `ReefDB::restore(meta, data)`; then compact the log up to snapshot index

Snapshot should include all durable data to rebuild in-memory tables and indexes (`storage.restore_from(&tables)`).

---

### 10) Cluster Membership

Expose admin APIs to:
- Bootstrap a new cluster (initial configuration)
- Add/remove nodes via Raft configuration changes
- Observe role/term/commit-index/apply-index metrics

---

### 11) Local 3-Node Example

For quick testing (one process per terminal):
1. Prepare 3 config files as in section 6 (node_id 1..3, unique dirs and ports)
2. Start each node process:
   - Start Raft RPC server (tonic) and HTTP gateway (axum) per config
3. Bootstrap: on node 1, call an admin endpoint to initialize the cluster with [1,2,3]
4. Send SQL to the HTTP gateway of the leader (or any node if followers forward)

Note: The current `examples/http_server.rs` is single-node. For multi-node, move SQL execution behind the Raft-backed gateway and add leader forwarding.

---

### 12) Failure and Recovery

On crash/restart:
1. Load snapshot if present via `SnapshotProvider::restore`
2. Replay committed log entries after the snapshot via the apply bridge
3. Rejoin cluster and continue

---

### 13) Security and Observability

- Secure RPC with mTLS when running in untrusted networks
- Export metrics: role, term, commit/apply index, log length, snapshot stats
- Log slow apply and IO operations

---

### 14) Testing Plan

- Unit: encode/decode `CommandBatch`, idempotent `apply`/`apply_batch` on replay
- Snapshot: round-trip preserves `last_applied_command` and table contents
- Integration (single-node): propose→commit→apply; crash/recover; snapshot+compaction
- Integration (multi-node): leader election, quorum commits, follower catch-up, snapshot install, failover, linearizable reads, membership changes

---

### 15) Migration from Single-Node

1. Launch a single-node Raft cluster
2. Install a snapshot from current `ReefDB::snapshot()`
3. Switch clients to the leader endpoint
4. Add followers via configuration changes

---

### 16) Implementation Notes

- Keep the authoritative mutation path strictly inside `ReefDB::apply*`
- Do not introduce parallel WALs per index; Raft log is the WAL
- Use `DistributedReef` as the client-facing API; keep Raft and batches hidden


