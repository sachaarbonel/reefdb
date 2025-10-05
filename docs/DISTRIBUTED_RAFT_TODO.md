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


