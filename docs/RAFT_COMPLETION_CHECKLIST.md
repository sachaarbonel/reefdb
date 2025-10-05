### ReefDB Raft Completion Checklist

This document lists the concrete, code-anchored work items required to finish the multi-node Raft integration. It complements `docs/DISTRIBUTED_RAFT_TODO.md` by focusing on missing implementation pieces, file locations, and verification steps.

Scope: Completing the implementation when feature `raft-tikv` is enabled. Default builds (`raft-core`) remain single-node and do not require `protoc`.

### Current code anchors (verified)

- `src/distributed/raft_node.rs`
  - `SingleNodeRaft` provides `encode_entry`/`decode_entry`, snapshot encode/decode, and apply-on-commit in single-node mode.
  - `RealRaftNode` exists with `MemStorage`, `propose`, `on_step`, `tick`, `on_ready` skeleton, and `leadership_info`.
  - `spawn_raft_background(...)` drives `tick` and `on_ready` periodically.
- `src/distributed/network/server.rs`
  - gRPC `Step` decodes `raft::prelude::Message` and calls `on_step`/`on_ready`.
  - `InstallSnapshot` streams bytes and calls `DistributedReef::restore` via `SingleNodeRaft::decode_snapshot`.
  - `GetInfo` returns role/term/indexes; falls back to single-node values without `raft-tikv`.
- `src/distributed/network/transport.rs` (feature-gated)
  - `GrpcTransport` with naïve async send and a simple client cache.
- `src/distributed/facade.rs`
  - `DistributedReef` builds `CommandBatch` with state-machine idempotency (`next_command_id`) and delegates to raft `propose`.
- `src/distributed/config.rs`
  - `NodeConfig` includes dirs, RPC addresses, peers, and defaults for `raft_tick_ms`, `election_tick`, `heartbeat_tick`, `max_grpc_msg_bytes` with `validate()`/`apply_defaults()`.
- `src/distributed/proto/*.proto`
  - Raft: `Step`, `InstallSnapshot`, `GetInfo`. SQL: `Execute`.

### Missing implementation (must-do)

1) Durable Raft storage (replace in-memory `MemStorage`)
- Add `src/distributed/raft_storage.rs` (feature `raft-tikv`) to persist:
  - HardState (term, vote, commit) with fsync.
  - Log entries: append-only segments under `<raft_dir>/log/` with segment rotation and fsync policy.
  - Snapshot: manifest and chunked files under `<raft_dir>/snap/` with atomic install.
- In `RealRaftNode::on_ready()` (`src/distributed/raft_node.rs`), replace placeholders:
  - Persist `ready.entries()` to log before exposing commit.
  - Persist `ready.hs()` to stable storage.
  - Handle `ready.snapshot()` apply/install.
  - After durable commit, apply `committed_entries` to the state machine and advance apply index.

2) Apply-on-commit durability policy
- Enforce: replication WAL is the only WAL for replicated writes. Do not write any separate WAL outside the apply path.
- Audit `wal/` module usage to ensure writes happen strictly during `apply_entry()` in `RealRaftNode`.

3) Snapshotting and compaction (raft-side)
- Hook Raft snapshot creation to `SnapshotProvider::snapshot()` and implement streaming to followers via gRPC.
- On `InstallSnapshot`, after RPC-level restore, integrate with Raft to compact log up to snapshot index; delete old segments.
- Add policies (size/entries/time) to trigger snapshot and log compaction; expose in `NodeConfig` and honor defaults.

4) Transport robustness and backpressure
- `src/distributed/network/transport.rs`:
  - Add connection pooling with bounded LRU; health checks for `tonic::transport::Channel`.
  - Exponential backoff with jitter and capped retries; counters for failures.
  - Max in-flight messages per peer; apply backpressure or drop with metrics.
  - Optional TLS/mTLS via `ClientTlsConfig` sourced from `NodeConfig`.
- `src/distributed/network/server.rs`:
  - Enforce max request/response message sizes.
  - Backpressure on `Step`; reject when overloaded; count and log.

5) Read paths
- Leader linearizable reads:
  - Implement ReadIndex on leader before serving from memory; expose via facade helper (e.g., `execute_linearizable`).
- Follower stale reads (optional):
  - Allow reads up to `last_applied`; document staleness bounds; guard behind a config flag.
- On follower, return leader hint when linearizable read requested.

6) Cluster membership and bootstrap
- Bootstrap initial cluster from YAML: write persistent identity and peer list under `raft_dir`.
- Implement conf change entries to add/remove nodes; update persistent peer list.
- Provide a simple Admin RPC for bootstrap and safe conf changes.

7) Observability & admin
- Metrics: role, term, commit/apply index, log length, snapshot stats, transport retry counters.
- Structured logging around apply latency, IO latency, elections, and snapshotting.
- Admin RPCs: health, metrics scrape, force snapshot/compact, leader transfer.

8) Configuration & UX
- Extend `NodeConfig`:
  - Snapshot/compaction thresholds; read policy; TLS paths; transport in-flight limits; gRPC size limits.
  - `validate()` and `apply_defaults()` for the new fields.
- Provide a `run_from_config(path)` launcher to hide setup in examples.

9) Security
- TLS/mTLS for gRPC (server and client), reload/rotation guidance.
- Document operational guidance for untrusted networks.

### Nice-to-have (post-MVP)

- Batch Raft messages where beneficial.
- Leader forwarding cache with short TTL and error paths with leader address hints.
- Persistent write-ahead in raft storage with group commit tuning.

### Verification plan

- Unit tests
  - `SingleNodeRaft::encode_entry/decode_entry` round-trips.
  - Snapshot encode/decode round-trips.
  - Idempotent `apply_entry` replays.
- Single-node integration
  - Propose → commit → apply; crash/recover from raft state (snapshot + log replay).
  - Snapshot + compaction reduces log size.
- Multi-node integration
  - Leader election; quorum commit; follower catch-up; snapshot install; failover.
- Consistency
  - Linearizable reads via ReadIndex.

### Acceptance

- Three nodes can elect a leader, accept SQL writes through the leader, replicate, apply, and survive leader failover without data loss.
- Nodes crash and recover from raft state without manual intervention. Snapshots reduce log size.
- Linearizable read mode available; follower-stale optional and documented.
- Admin endpoints expose role/term/indexes; metrics available for scraping.


