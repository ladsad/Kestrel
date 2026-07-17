# Changelog

All notable changes to this project are documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project does not yet follow semantic versioning tags since it's pre-implementation — versioning starts at `v0.1.0` when Phase 1 ships.

## [Unreleased]

### Added
- Initial project documentation: design doc, protocol reference, roadmap, testing strategy, benchmark plan, contributing guide.

### Planned
- Phase 1: RESP2 server with core commands (see [`docs/ROADMAP.md`](docs/ROADMAP.md))

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
