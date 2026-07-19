# Kestrel

A distributed, fault-tolerant key-value store built from scratch in Go — RESP2-compatible wire protocol, write-ahead durability, leader-follower replication, and Raft-based automatic failover.

> **Status:** Pre-implementation. This repo currently contains the design documentation; implementation follows the milestones in [`docs/ROADMAP.md`](docs/ROADMAP.md). This README will be updated with real benchmark numbers and a usage demo as each phase lands.

## Why this exists

Most "build your own Redis" projects stop at a single-node command interpreter. Kestrel goes further: it's an exercise in the primitives that real distributed data stores are built from — concurrent state management, durability guarantees, replication, and consensus — with every claim backed by a reproducible benchmark rather than an architecture diagram alone.

## Planned Features

- [x] Phase 1: Single-Node Server (In-memory data structures, RESP2 protocol)
- [x] Phase 2: Durability (AOF + Snapshotting)
- [x] Phase 3: Replication (Leader-follower streaming)
- [x] Phase 4: Consensus & Failover (Raft-based)
- [ ] Phase 5: Sharding (Consistent hashing)
- [ ] Phase 6: Observability & Live Dashboard
- [ ] Comparative Benchmark vs. Redis

## Live Dashboard

<!-- TODO: Add GIF/screenshot of the TUI during a simulated leader failure here once implemented -->
![Live Dashboard Placeholder](https://via.placeholder.com/800x400.png?text=Live+Dashboard+Demo+-+Coming+Soon)

## Quick Start

```bash
# once Phase 1 lands:
git clone https://github.com/ladsad/kestrel.git
cd kestrel
go run ./cmd/kestrel --port 6380

# in another terminal
redis-cli -p 6380
> SET foo bar
> GET foo

# once Phase 6 lands, run the live cluster dashboard:
go run ./cmd/kestrel-dashboard --peers localhost:6380,localhost:6381,localhost:6382
```

## Documentation

| Doc | Contents |
|---|---|
| [`docs/DESIGN.md`](docs/DESIGN.md) | Full design doc — goals, architecture, phase-by-phase detailed design |
| [`docs/PROTOCOL.md`](docs/PROTOCOL.md) | Wire protocol and command reference |
| [`docs/ROADMAP.md`](docs/ROADMAP.md) | Milestones, phases, and exit criteria |
| [`docs/TESTING.md`](docs/TESTING.md) | Test strategy across layers |
| [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) | Benchmark methodology and results, including a head-to-head comparison against Redis |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Dev setup and contribution guidelines |
| [`CHANGELOG.md`](CHANGELOG.md) | Notable changes per release |

## Non-Goals

Kestrel does not aim for full Redis command-set parity (no Lua scripting, no pub/sub), multi-datacenter replication, or a client library ecosystem. See [`docs/DESIGN.md`](docs/DESIGN.md#4-non-goals) for the full scoping rationale.

## License

MIT — see [`LICENSE`](LICENSE).
