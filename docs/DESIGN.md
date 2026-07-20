# Kestrel: A Distributed, Fault-Tolerant Key-Value Store

| | |
|---|---|
| **Author** | Shaurya Kumar |
| **Status** | Implementation Complete |
| **Last Updated** | 2026-07-17 |
| **Target Language** | Go 1.22+ |
| **Repo** | `github.com/ladsad/kestrel` (proposed) |

---

## 1. Objective

Build a distributed key-value store from first principles — no database engine, no consensus library used as a black box without understanding it, no framework hiding the networking layer — that supports the RESP2 protocol, durable persistence, leader-follower replication, Raft-based consensus for automatic failover, and (stretch) consistent-hash sharding across multiple Raft groups.

The goal is not to reproduce Redis. The goal is to demonstrate, with working code and measured numbers, that I can reason about and implement the primitives that real distributed systems are built from: concurrent state management, durability guarantees, replication, consensus, and the failure modes that show up only under concurrency and partial failure.

## 2. Background & Motivation

My existing project work (Confoundr, Pitwall, Mustard Archives) demonstrates strength in data engineering: ETL pipelines, batch/stream processing, orchestration. It does not demonstrate systems-level software engineering: networking, concurrency control, consensus, or storage engine internals — skills that are directly evaluated in SDE interview loops (distributed systems design rounds, coding rounds involving concurrency) and that are listed on my resume (C/C++, distributed systems) without a corresponding project as evidence.

This project closes that gap directly and produces defensible, benchmarked claims rather than architecture-diagram-only claims.

## 3. Goals

- **G1.** Single-node in-memory store supporting a real wire protocol (RESP2), interoperable with `redis-cli` for demoability.
- **G2.** Durability: no acknowledged write is lost across a process crash.
- **G3.** Replication: a cluster of N nodes stays consistent under normal operation and under a single node failure.
- **G4.** Automatic failover: cluster continues accepting writes within a bounded time window after leader loss, with no operator intervention.
- **G5.** Every claim is backed by a reproducible benchmark (throughput, latency percentiles, failover time), not an adjective.

## 4. Non-Goals

- Not attempting full Redis command-set parity (no Lua scripting, no pub/sub, no cluster-mode RESP3 features).
- Not optimizing for multi-datacenter / WAN replication — single-region, single-cluster scope only.
- Not building a client library ecosystem — a minimal Go client for testing is sufficient.

## 5. System Overview

```
                         ┌─────────────────────┐       ┌───────────────────────┐
                         │   Client (redis-cli │       │  Live Dashboard (TUI) │
                         │   or Go test client)│       │  (external observer)  │
                         └──────────┬──────────┘       └───────────┬───────────┘
                                    │ RESP2 over TCP               │ Metrics / RPC
                                    ▼                              ▼
                    ┌───────────────────────────────┐
                    │         Kestrel Node            │
                    │  ┌─────────────────────────┐    │
                    │  │   TCP Listener / Conn     │    │
                    │  │   Handler (per-goroutine)  │    │
                    │  └───────────┬───────────────┘    │
                    │              ▼                    │
                    │  ┌─────────────────────────┐    │
                    │  │   Command Dispatcher       │    │
                    │  └───────────┬───────────────┘    │
                    │              ▼                    │
                    │  ┌─────────────────────────┐    │
                    │  │  In-Memory Store (RWMutex- │    │
                    │  │  guarded map + skip lists)  │    │
                    │  └───────────┬───────────────┘    │
                    │              ▼                    │
                    │  ┌─────────────────────────┐    │
                    │  │  AOF Writer + Snapshotter  │    │
                    │  └───────────┬───────────────┘    │
                    │              ▼                    │
                    │  ┌─────────────────────────┐    │
                    │  │  Raft Node (consensus +    │    │
                    │  │  log replication)          │    │
                    │  └───────────┬───────────────┘    │
                    └──────────────┼────────────────────┘
                                   ▼
                     Peer Kestrel Nodes (Raft RPC over
                              gRPC or raw TCP)
```

## 6. Detailed Design by Phase

### Phase 1 — Single-Node Server

**Protocol.** Implement a RESP2 parser/serializer by hand (no library) — simple strings, errors, integers, bulk strings, arrays. This is a deliberately small, well-specified protocol, which makes it a good vehicle for demonstrating clean parser design without scope creep.

**Data structures.**
| Type | Backing structure | Notes |
|---|---|---|
| String | `map[string][]byte` | baseline |
| Hash | `map[string]map[string][]byte` | |
| List | doubly linked list | O(1) push/pop both ends |
| Set | `map[string]map[string]struct{}` | |
| Sorted Set | skip list + hash index | O(log n) insert/range |

**Concurrency model.** One goroutine per client connection. Shared store state guarded by a single `sync.RWMutex` initially; documented as a known bottleneck, with sharded-lock or single-writer-goroutine + channel design considered as a documented alternative (see §8) if benchmarks show contention.

**Commands (v1 set):** `GET SET DEL EXPIRE TTL HSET HGET HDEL LPUSH RPUSH LPOP RPOP ZADD ZRANGE ZSCORE PING`

**Exit criteria:** `redis-cli -p <port>` can connect and run the above commands correctly against a running Kestrel instance.

### Phase 2 — Durability

- **Append-Only File (AOF):** every write command is serialized (RESP-encoded) and appended to a log file before the in-memory mutation is acknowledged to the client (write-ahead ordering).
- **fsync policy:** configurable — `always` (fsync every write, safest/slowest), `everysec` (batched, default), `no` (OS-managed) — documented trade-off, mirrors real production systems' durability knobs.
- **Snapshotting:** periodic full-state dump to a compact binary snapshot file; AOF is truncated/rotated after a successful snapshot to bound replay time on restart.
- **Recovery path:** on startup, load latest snapshot, then replay AOF entries since that snapshot.

**Exit criteria:** kill -9 the process mid-write-burst; on restart, verify (via checksum/count comparison against an independent log of acknowledged writes) zero acknowledged writes are lost, and quantify replay time vs. AOF size.

### Phase 3 — Replication

- Leader-follower model. Leader accepts writes, appends to its local AOF, then streams the same entries to followers over a dedicated replication connection.
- Followers apply entries in order; expose a `replication offset` a client/monitor can query to reason about lag.
- Reads: supported on followers with an explicit staleness acknowledgment (documented as eventual consistency, not linearizable) — this trade-off is stated explicitly rather than glossed over.

**Exit criteria:** 3-node cluster; write to leader; read from follower; measure replication lag under load.

### Phase 4 — Consensus & Failover (Raft)

- Implement Raft leader election, log replication, and commit-index advancement. Use `hashicorp/raft` as the library **but treat this as a "must be able to explain every RPC and state transition," not "black box dependency."** A from-scratch Raft implementation is considered as a follow-up (see §11).
- Election timeout, heartbeat interval, and log-matching are configurable and documented.
- On leader failure (simulated via `kill -9` or network partition via `iptables`/`tc netem`), a follower must be elected and resume accepting writes within a bounded, measured window.

**Exit criteria:** kill the leader in a running 3 or 5-node cluster; measure time-to-new-leader and time-to-writes-resumed; verify no committed entry is lost.

### Phase 5 — Sharding

- Consistent hashing (bounded hash ring) to map keys to shard groups, each shard being its own independent Raft group.
- Thin stateless routing layer in front so a client doesn't need cluster topology awareness.

### Phase 6 — Observability & Live Dashboard

- **Live Terminal Dashboard (TUI):** A command-line dashboard built with Go's `bubbletea` (and `lipgloss` for styling) that visualizes the cluster's consensus and replication state in real time.
  - Renders current leader, term number, and term-change history.
  - Visualizes per-node role (leader/follower/candidate) with live state transitions.
  - Tracks replication lag per follower, updating in real time.
  - Displays ops/sec and p99 latency live.
  - Shows a visual "election in progress" state when triggered.
- **Design Note (TUI Decoupling):** The dashboard reads its data without adding load-bearing coupling to the core server. It connects to the Prometheus `/metrics` endpoint and/or a lightweight internal status RPC on each node (the same surface an external monitor would use), avoiding reaching into internal server state directly.
- Prometheus `/metrics` endpoint: ops/sec by command type, p50/p95/p99 latency histograms, replication lag, Raft term/leader changes, AOF fsync latency.
- Load-testing harness (custom Go client, concurrent connections, configurable read/write mix — a small "YCSB-lite") to produce reproducible throughput/latency numbers.
- Grafana dashboard (optional, reuses Prometheus setup already used in Confoundr — direct resume synergy).

## 7. Testing Strategy

| Layer | Approach |
|---|---|
| Protocol parser | Table-driven unit tests against RESP2 spec edge cases (empty bulk strings, negative array lengths, malformed input) |
| Data structures | Unit tests + property-based tests (e.g. `testing/quick` or `gopter`) for invariants (skip list ordering, list operations) |
| Concurrency | Race detector (`go test -race`) mandatory in CI on every PR |
| Durability | Fault-injection tests: kill process at randomized points during write bursts, verify recovery invariants |
| Replication/Raft | Deterministic simulation where feasible (network partition injection); chaos-style manual test scripts otherwise |
| Performance | Benchmark suite run before/after each phase, results checked into `benchmarks/` as historical record |

## 8. Alternatives Considered

- **Language: C++ instead of Go.** Rejected for this project — C++ would better showcase manual memory management, but the added time cost (custom thread pool, manual synchronization primitives, build tooling) would come at the expense of reaching Phase 4 (consensus), which is the highest-value phase for interview relevance. Documented as a possible future rewrite of Phase 1 only, if time permits.
- **Single global mutex vs. sharded locks vs. single-writer-goroutine.** Starting with the simplest (global `RWMutex`) to establish correctness, then treating lock contention as a measured problem to solve with data (benchmark first, optimize second) rather than premature optimization.
- **Raft library vs. from-scratch consensus.** Library-first to guarantee a working, correct Phase 4 within the project timeline; from-scratch implementation flagged as a stretch/follow-up once the rest of the system is stable.

## 9. Risks & Open Questions

| Risk | Mitigation |
|---|---|
| Raft correctness bugs are notoriously subtle | Lean on `hashicorp/raft`'s tested implementation for the core algorithm initially; add fault-injection tests rather than re-deriving Raft's proof obligations from scratch under time pressure |
| Scope creep across 5–6 phases | Each phase has a hard "exit criteria" (§6) — do not start the next phase until the current one's exit criteria and benchmark are met and committed |
| Benchmarks not reproducible / not credible | All benchmark scripts and raw output committed to the repo; README documents exact hardware/config used |
| Time budget | Phases 1–4 are the "must-ship" scope for the resume bullet; Phase 5 and from-scratch Raft are explicitly stretch |

## 10. Milestones

| Milestone | Deliverable | Status |
|---|---|---|
| M1 | Phase 1 complete, `redis-cli` demo working | Shipped |
| M2 | Phase 2 complete, crash-recovery test passing | Shipped |
| M3 | Phase 3 complete, 3-node replication demo | Shipped |
| M4 | Phase 4 complete, failover benchmark published | Shipped |
| M5 | Phase 5 sharding demo | Shipped |
| M6 | Phase 6 observability and TUI dashboard | Shipped |

## 11. Future Work

- From-scratch Raft implementation (replacing `hashicorp/raft`) as a deep-dive follow-up post-M4.
- RESP3 support / pub-sub.
- Multi-datacenter replication topology.

## 12. Success Metrics

- **Single-node Throughput**: ~12,904 ops/sec at p99 latency of ~11.63 ms.
- **3-Node Cluster Throughput**: ~11,250 ops/sec.
- **Leader Failover Time**: ~1.5s after a simulated leader failure (10 trials).
- **AOF Replay Time**: ~318 ms for 367k writes.
