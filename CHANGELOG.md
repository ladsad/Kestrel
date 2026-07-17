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

## [Unreleased]

### Planned
- Phase 3: Replication (Leader-follower streaming)

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
