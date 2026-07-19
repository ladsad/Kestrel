# Changelog

All notable changes to this project are documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project does not yet follow semantic versioning tags since it's pre-implementation — versioning starts at `v0.1.0` when Phase 1 ships.

## [0.1.0] - 2026-07-17
### Added
- RESP2 protocol parser/serializer
- In-memory string, hash, list, set, sorted-set commands
- Single-node concurrent connection handling

### Benchmarked
- Single-node throughput: ~247k ops/sec with 50 concurrent connections (see docs/BENCHMARKS.md)

## [0.2.0] - 2026-07-17
### Added
- Append-Only File (AOF) durability with configurable fsync policies (`always`, `everysec`, `no`).
- Write-ahead logging integration intercepting mutable state commands.
- Periodic store snapshotting (RDB equivalent) via Go `encoding/gob`.
- Startup recovery logic implementing snapshot loading followed by AOF replay.
- Atomic AOF file rotation without blocking incoming read commands.

### Benchmarked
- AOF replay time: ~367k ops in ~318ms.

## [0.3.0] - 2026-07-18
### Added
- Leader-follower streaming replication.
- Follower node initialization via `--replicaof` flag.
- Atomic state synchronization via initial snapshot (`SYNC` command) followed by asynchronous, non-blocking command stream.
- `INFO replication` command to query leader offset.

### Benchmarked
- 3-node cluster throughput: ~168k ops/sec (writes on leader, reads on follower).

## [0.4.0] - 2026-07-19
### Added
- Integrated `hashicorp/raft` for robust consensus, leader election, and distributed log replication.
- Dynamic cluster configuration via the new `RAFTJOIN` custom command.
- Integrated `raft-boltdb` for durable log and stable configuration storage.
- Automated snapshotting managed internally by the Raft FSM, replacing the custom AOF rotation logic.
- Dedicated `cmd/failover-bench` to test and measure leader election failover time.

### Benchmarked
- Measured leader election failover time and write resumption at ~1.5s (dictated by 1000ms election timeout).

## [0.5.0] - 2026-07-19
### Added
- Consistent hash ring (`pkg/sharding/hashring.go`) using `hash/fnv` with configurable virtual nodes for cluster key distribution.
- Modified core server Raft loop to return a precise `-MOVED <leader_address>` redirect error instead of a generic "not leader" error on write commands.
- Standalone stateless router proxy (`cmd/kestrel-proxy`) that transparently intercepts client TCP traffic, hashes keys, routes traffic to the correct shard, and automatically catches `-MOVED` redirects to find new leaders natively.
- Dedicated `cmd/sharding-test` benchmarking integration to test dynamic multi-shard distribution.

## [Unreleased]

### Planned
- Phase 6: Live Terminal Dashboard (TUI) and Observability
- Head-to-head performance comparison benchmark against Redis

---

_Entries below will be added as each phase ships, e.g.:_

```
## [0.1.0] - YYYY-MM-DD
### Added
- RESP2 protocol parser/serializer
- In-memory string, hash, list, set, sorted-set commands
- Single-node concurrent connection handling

### Benchmarked
- Single-node throughput: N ops/sec at p99 < N ms (see docs/BENCHMARKS.md)
```
