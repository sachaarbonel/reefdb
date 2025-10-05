### ReefDB Distributed Raft – Remaining Work

This document tracks the outstanding tasks to deliver a production-grade, multi-node Raft integration as outlined in `docs/MULTI_NODE.md` and implemented partially under `src/distributed/`.

### Current state

- Network layer: tonic gRPC services for Raft (`Step`, `InstallSnapshot`, `GetInfo`) and a thin SQL gateway with leader-forwarding.
- Snapshot bridge: stream `InstallSnapshot`, decode, and call `SnapshotProvider::restore`.
- Command encoding: `CommandBatch` bincode helpers for log payloads and snapshots.
- Facade: `DistributedReef` builds batches from high-level APIs and uses state machine idempotency.
- Optional TiKV Raft scaffolding: behind feature `raft-tikv` (requires `protoc`). Default builds do not depend on TiKV Raft.

### Gaps to close

1) Raft core wiring (feature `raft-tikv`)
- Implement persistent raft storage instead of `MemStorage`:
  - Hard state (term, vote), log entries directory, snapshot manifest/files.
  - fsync policy for log appends and snapshots.
- Drive Raft lifecycle:
  - Background tick loop (heartbeat, election) and `on_ready` processing.
  - Outbound message send via gRPC transport with retries/backoff.
- Message encoding/decoding:
  - Ensure `raft::prelude::Message` is encoded/decoded consistently on the wire.
  - Backpressure and max message size limits on `Step`.

2) Apply-on-commit path
- Ensure all committed entries are decoded to `CommandBatch` and applied via `ReefDB::apply_batch`.
- Durability policy choice and enforcement:
  - Prefer “Raft log is the only WAL”; verify no parallel WAL writes outside apply.
  - If a secondary WAL is kept, perform it strictly within apply after commit.

3) Snapshotting and compaction
- Snapshot creation:
  - Hook Raft snapshot request to `SnapshotProvider::snapshot()`; stream to followers.
- Install snapshot:
  - Completed at RPC surface; add raft-side install integration and compaction up to snapshot index.
- Trigger and cadence:
  - Policies for when to snapshot and compact (log size, time-based, or entry count).

4) Read paths
- Linearizable reads:
  - Implement Raft ReadIndex on leader before serving from memory.
  - Return leader hint when follower receives linearizable read without forwarding.
- Optional follower-stale reads with last-applied visibility; document staleness bounds.

5) Cluster membership & bootstrap
- Bootstrap API to create initial cluster (IDs, peers).
- Configuration changes (add/remove nodes) via Raft conf change entries.
- Persistent node identity and peer list in config directory.

6) Transport & topology
- gRPC transport robustness:
  - Connection pooling, TLS/mTLS support, exponential backoff.
  - Batching of Raft messages where beneficial; max in-flight control.
- Leader forwarding lifecycle:
  - Fast path for redirects; cache leader ID with short TTL.

7) Observability & ops
- Metrics: role, term, commit/apply index, log length, snapshot stats, transport retry counters.
- Structured logging around apply latency, IO latency, elections.
- Admin RPCs: health, metrics scrape, force snapshot/compact, leader transfer.

8) Testing & validation
- Unit tests: encode/decode `CommandBatch`, idempotent `apply` replay, snapshot round-trip.
- Single-node integration: propose→commit→apply, crash/recover from log, snapshot + compaction.
- Multi-node integration: leader election, quorum commit, follower catch-up, snapshot install, failover.
- Consistency tests: linearizable reads (ReadIndex), Jepsen-style sequences (where applicable).

9) Configuration & UX
- Node YAML schema finalization, env overrides, and validation.
- Sensible defaults for election/heartbeat timeouts, snapshot thresholds, directories.
- Example `distributed_node` readme and scripts to launch 3 local nodes.

10) Security
- gRPC TLS/mTLS support and key rotation guidance for untrusted networks.

### Acceptance criteria

- A 3-node cluster can elect a leader, accept SQL writes through the leader, replicate, apply, and survive leader failover without data loss.
- Snapshots reduce log size; nodes can crash and recover from the raft state (snapshot + log replay) without manual intervention.
- Linearizable read mode available; follower-stale optional and documented.
- Admin endpoints expose role/term/indexes; metrics available for scraping.

### Feature flags and build

- `raft-core`: default distributed scaffolding without TiKV Raft; builds without `protoc`.
- `raft-tikv`: enables TiKV Raft integration (requires a working `protoc` in PATH).

### Implementation plan by module (actionable)

- src/distributed/raft_node.rs
  - `raft-core`:
    - Keep `SingleNodeRaft` as-is for local-only behavior. Ensure `encode_entry`/`decode_entry` and snapshot helpers remain the single source used by both modes.
  - `raft-tikv`:
    - Expand `RealRaftNode` lifecycle:
      - Add background tick loop (`tokio::time::interval`) and drive `on_ready()` continuously. Expose `start_background()` that spawns both.
      - Persist Ready data: hard state, entries, snapshot (see storage tasks). Update `raft_apply_index` after apply.
      - Implement `apply_entry()` using `SingleNodeRaft::decode_entry` then `ReefDB::apply_batch`.
      - Wire outbound messages to transport (`transport.send`) with basic error handling and metrics.
    - Add bootstrap helper: create node with provided peers and initial conf state. Expose `new_with_config(NodeConfig)`.
    - Gate all `protobuf`/`raft` usage behind `#[cfg(feature = "raft-tikv")]` (already present for most code paths).

- src/distributed/network/transport.rs (feature `raft-tikv`)
  - Replace raw `tokio::spawn` send with:
    - Connection pooling keyed by node id with bounded LRU and per-connection `tonic::transport::Channel` health checks.
    - Exponential backoff (jitter) on send failure; cap retries; record counters.
    - Configurable max in-flight messages per peer; drop/queue with backpressure.
    - Optional TLS/mTLS via `tonic::transport::ClientTlsConfig` sourced from `NodeConfig`.
  - Enforce max message size on client, align with server limits.

- src/distributed/network/server.rs
  - Raft RPC:
    - `Step`: enforce max message size and backpressure. Decode using `protobuf::Message::parse_from_bytes::<raft::prelude::Message>` (already done). After `on_step`, always call `on_ready()`; return an ack or leader hint.
    - `InstallSnapshot`: stream-accumulate chunks, then call `SnapshotProvider::restore` (already implemented via `SingleNodeRaft::decode_snapshot` → `DistributedReef::restore`). Add metrics and IO duration logging.
    - `GetInfo`: return role/term/indexes. When `raft-tikv` disabled, return static leader info for single-node.
  - SQL RPC:
    - Leader forwarding: keep current fast-path; add short-TTL leader cache; include leader address in error on unknown leader.
    - Add linearizable reads mode: on leader, perform ReadIndex before executing. On follower, return leader hint unless explicitly allowed stale reads.
  - Server builder: set request/response size limits; optional TLS.

- src/distributed/facade.rs
  - Keep facade as the single high-level entrypoint. It already builds `CommandBatch` with state-machine idempotency via `next_command_id()` and delegates to raft `propose`.
  - Add read helpers that select between linearizable and follower-stale modes based on a runtime flag.

- src/distributed/config.rs
  - Extend `NodeConfig` with:
    - Timeouts: election_ms, heartbeat_ms; snapshot/compaction thresholds.
    - Transport: TLS files (ca, cert, key), max_in_flight_per_peer, max_grpc_msg_bytes.
    - Read policy: linearizable default on/off; follower_stale allowed.
  - Add `validate()` and `apply_defaults()`.
  - Persist identity and peer list under `raft_dir` (e.g., `node.yaml`) at bootstrap. Load on restart.

- Persistent Raft storage (new module)
  - Add `src/distributed/raft_storage.rs` (feature `raft-tikv`) implementing a durable store for:
    - HardState (term, vote, commit) with fsync on update.
    - Log entries in append-only segments under `raft_dir/log/` with bounded segment size and fsync policy.
    - Snapshot manifest and chunked files under `raft_dir/snap/`; atomic install.
    - Minimal compaction: delete log entries up to snapshot index after successful install.
  - Wire `RealRaftNode::on_ready()` to persist via this storage and to apply committed entries after durable write.

- src/distributed/proto/*.proto and build
  - Keep `raft.proto` and `sql.proto` as-is for now. Enforce sizes via tonic server/client settings instead of changing protos.
  - Ensure `build.rs` compiles protos at build time into `network::pb` (present in tree). Document `protoc` requirement when enabling `raft-tikv`.

### Policy decisions to finalize (with verification hooks)

- Raft log as sole WAL vs. dual-WAL:
  - Preferred: raft log as the only durability primitive for replicated writes; apply writes state-machine-side without separate WAL.
  - Unconfirmed: whether modules under `wal/` are currently used outside apply paths. Audit all write paths; if used, constrain to apply-on-commit only.
- Snapshot cadence: choose entry-count or size-based triggers; expose in config; instrument to tune.
- Linearizable reads default: on for production; allow opt-out.

### Short-term next steps (milestones)

1. Drive Raft lifecycle (raft-tikv):
   - Tick/on_ready loops; outbound send via current `GrpcTransport`.
   - Minimal persistence stubs that no-op but plumb interfaces, then replace with real storage.
2. Apply-on-commit end-to-end:
   - Commit entry → decode `CommandBatch` → `ReefDB::apply_batch` → update `apply_index`.
   - Smoke test with 1 node: propose, commit, apply.
3. Snapshot install bridge end-to-end:
   - Use existing `InstallSnapshot` RPC; add raft-side apply and compaction to snapshot index.
4. Leader forwarding and read paths:
   - Keep existing forwarding; add error messages with leader hint; add linearizable read path behind a flag.
5. Bootstrap and config:
   - Load peers from YAML; initialize conf state; write `node.yaml` in `raft_dir`.

