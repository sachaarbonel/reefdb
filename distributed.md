### Drop-in Raft Integration for ReefDB

This document specifies the minimal, production-grade changes required to integrate a Raft consensus layer on top of ReefDB’s refactored state machine and snapshot APIs.

The plan assumes the following abstractions now exist:
- `state_machine.rs`
  - `ReplicatedCommand`, `CommandBatch { id, commands }`
  - `ReefDB::apply(id, cmd)` and `ReefDB::apply_batch(batch)` (idempotent via `applied_commands`)
  - Monotonic `CommandId` and `next_command_id()`
- `snapshot.rs`
  - `SnapshotProvider` with `snapshot()` and `restore()`
  - `SnapshotMeta { last_applied_command }` and `SnapshotData { tables }`

With these in place, Raft integration is a thin orchestration layer around: (1) log replication of `CommandBatch`, (2) apply-on-commit into `ReefDB`, and (3) snapshot/restore wiring.

---

### 1) Dependencies

Add a Raft implementation and an RPC transport. Two common combinations:
- Raft core: `tikv/raft` crate
- RPC: `tonic` (gRPC over HTTP/2) with `prost` for protobuf, or any equivalent RPC transport

Also add async runtime (`tokio`) and a durable key-value or file-backed storage for Raft metadata/log (e.g., simple file I/O or `sled`), plus `bincode` (already used) to serialize `CommandBatch` into Raft log entries.

Note: Do not change `ReefDB`’s command encoding. Reuse `CommandBatch` as the Raft log entry payload to keep a single, deterministic source of truth.

---

### 2) New Modules and Files

Create a small distributed subsystem under `src/distributed/`:
- `src/distributed/mod.rs`: module wiring
- `src/distributed/raft_node.rs`: Raft node wrapper and state machine apply bridge
- `src/distributed/network/{server.rs,client.rs}`: RPC server/client for Raft messages and a thin client gateway for SQL requests (optional initial cut)
- `src/distributed/proto/`:
  - Raft RPC definitions if using protobuf/tonic, or Rust structs if you choose a custom transport

Keep the surface minimal: the Raft node exposes propose/apply APIs and role/leadership info.

---

### 3) Raft State Machine Mapping

- Log entry payload: `CommandBatch` serialized with `bincode`.
- Apply-on-commit: on Raft commit, deserialize `CommandBatch` and call `ReefDB::apply_batch(batch)`.
- Idempotency: ensured by `CommandId` and `applied_commands` map in `ReefDB`. Persist idempotency boundary via snapshot metadata (`last_applied_command`).

Pseudo-code in the Raft apply thread:
```rust
fn on_raft_commit(bytes: &[u8], reef: &mut ReefDB<_, _>) -> Result<(), ReefDBError> {
    let batch: CommandBatch = bincode::deserialize(bytes)
        .map_err(|e| ReefDBError::Other(format!("decode raft entry: {e}")))?;
    reef.apply_batch(batch)?;
    // fsync underlying storage if required by your durability policy
    Ok(())
}
```

---

### 4) Write Path (Client → Leader → Raft → Apply)

1. Build a `CommandBatch` on the leader from incoming statement(s):
   - Assign a batch id: either use `ReefDB::next_command_id()` (reserved for leadership context) or a leader-local monotonic generator converted to `CommandId`.
   - Include one or more `ReplicatedCommand`s.
2. Serialize with `bincode` and propose to Raft.
3. Wait for Raft commit (quorum). On commit, the leader’s apply thread invokes `ReefDB::apply_batch`.
4. Only after apply succeeds (and storage flush if configured) return success to the client.

Notes:
- Batch sizing is a policy decision; start with one logical statement per batch for simplicity.
- All local mutation paths must go through apply-on-commit. Avoid any side effects outside `apply`/`apply_batch`.

---

### 5) Read Path and Consistency

Support at least two modes:
- Linearizable reads: route reads to leader; optionally use Raft ReadIndex to avoid proposing no-ops.
- Stale reads (optional): allow follower reads with last-applied visibility guarantees; document staleness.

Implementation hooks:
- Add a thin query frontend that detects the leader role and forwards or rejects writes on followers.
- For linearizable reads, perform a ReadIndex round before serving from in-memory state to ensure the node’s apply index has caught up to the leader’s commit index.

---

### 6) Snapshots and Log Compaction

Wire `SnapshotProvider` into the Raft library’s snapshot/restore hooks:
- On snapshot request: call `ReefDB::snapshot()` to obtain `(SnapshotMeta, SnapshotData)`, serialize, and hand to Raft.
- On install snapshot: deserialize, then `ReefDB::restore(meta, data)`; this resets `applied_commands` and advances `next_command_id` to `last_applied_command + 1`.
- After snapshot install, Raft can compact the log up to the snapshot index.

Ensure the snapshot includes all durable state required to rebuild in-memory indexes/tables. The current provider clones `tables` and relies on `storage.restore_from(&tables)` to rebuild storage state.

---

### 7) Raft Persistence

Persist the following in a dedicated directory (e.g., `raft/` next to your data file):
- Hard state (current term, voted_for)
- Raft log entries (serialized `CommandBatch`)
- Snapshot files (serialized `(SnapshotMeta, SnapshotData)`)

Durability policy:
- Either (A) Raft log is the authoritative WAL and you do not keep a parallel SQL WAL, or (B) keep a local WAL as an extra layer and write it during apply.
- Prefer (A) for simplicity: the Raft log is your only WAL; `apply` is the single mutation path.

---

### 8) Boot and Recovery Sequence

On node start:
1. Initialize storage (`ReefDB`), then initialize Raft.
2. If a snapshot exists, install it via `SnapshotProvider::restore`.
3. Replay any committed Raft log entries after the snapshot by invoking `on_raft_commit` for each.
4. Start RPC server and participate in the cluster.

This ensures the in-memory state and `next_command_id` align with the Raft-applied index.

---

### 9) Cluster Membership

Provide admin APIs to:
- Bootstrap a new cluster with an initial configuration.
- Add/remove nodes via Raft configuration change entries.
- Expose role/term/commit-index/apply-index metrics for observability.

---

### 10) Client Frontend (Optional First Cut)

Expose a basic SQL endpoint that:
- For writes: forwards to the leader (or returns a leader hint on followers).
- For reads: supports linearizable (leader or ReadIndex) and optional follower-stale reads.

Keep this thin; correctness lives in Raft + `ReefDB::apply*`.

---

### 11) Testing Strategy

- Unit: encode/decode `CommandBatch`, idempotent `apply`/`apply_batch` with repeated entries.
- Snapshot: snapshot/restore round-trip preserves `last_applied_command` and table contents.
- Integration (single-node): propose, commit, apply; crash and recover from log; snapshot + compaction.
- Integration (multi-node): leader election, quorum commits, follower catch-up, snapshot install, failover, and linearizable reads.

---

### 12) Minimal Code Touch Points

- Build/propose: leader-side function to translate parsed SQL → `Vec<ReplicatedCommand>` → `CommandBatch` → Raft propose.
- Apply bridge: Raft commit callback → `ReefDB::apply_batch(batch)`.
- Snapshot hooks: Raft snapshot request/install → `SnapshotProvider::{snapshot, restore}`.
- Remove or bypass any legacy write paths and per-index WALs; the authoritative mutation path is `apply`.

No changes are required inside `apply` implementations beyond normal bug fixes; all distribution logic resides beside the database (Raft node, RPC, and bootstrap wiring).

---

### 13) Operational Notes

- Configure timeouts and election settings appropriate for your deployment.
- Expose metrics (role, term, commit/apply index, log length, snapshot stats).
- Secure RPCs (mTLS) if running in untrusted networks.

---

### 14) Migration Guidance

If migrating an existing single-node data file:
1. Start a single-node Raft cluster, install a snapshot from the current `ReefDB::snapshot()`.
2. Switch client writes to the leader endpoint.
3. Add followers via configuration changes.

This avoids dual-writer scenarios and ensures a clean cutover to Raft-backed durability.


