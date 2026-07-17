# Roadmap

Each phase has a hard **exit criteria** — the next phase does not start until the current one's is met and its benchmark is committed to [`docs/BENCHMARKS.md`](BENCHMARKS.md). This is intentional: it's the guardrail against scope creep across a 5–6 phase project (see [`DESIGN.md §9 Risks`](DESIGN.md#9-risks--open-questions)).

## Phase 1 — Single-Node Server
**Target: Week 2**
- RESP2 parser/serializer (hand-rolled)
- In-memory data structures: strings, hashes, lists, sets, sorted sets
- One goroutine per connection, single `RWMutex` around store state
- v1 command set (see [`PROTOCOL.md`](PROTOCOL.md))
- **Exit criteria:** `redis-cli` connects and runs all v1 commands correctly against a live instance.

## Phase 2 — Durability
**Target: Week 4**
- Append-only file (AOF), write-ahead before ack
- Configurable fsync policy (`always` / `everysec` / `no`)
- Periodic snapshotting + AOF rotation
- Startup recovery: snapshot + AOF replay
- **Exit criteria:** kill -9 mid-write-burst → restart → zero acknowledged writes lost, replay time measured and recorded.

## Phase 3 — Replication
**Target: Week 6**
- Leader-follower streaming replication
- Queryable replication offset / lag
- Follower reads with explicit staleness semantics (documented, not silent)
- **Exit criteria:** 3-node cluster; write to leader, read from follower; replication lag measured under load.

## Phase 4 — Consensus & Failover
**Target: Week 9**
- Raft leader election + log replication (`hashicorp/raft`, with every RPC/state transition understood and explainable)
- Configurable election timeout / heartbeat interval
- Failure injection via `kill -9` and network partition (`tc netem`)
- **Exit criteria:** kill the leader in a live 3–5 node cluster; measure time-to-new-leader and time-to-writes-resumed; verify zero committed-entry loss.

## Phase 5 — Sharding (Stretch)
**Target: Week 12+, only after Phase 4 ships**
- Consistent hashing across independent Raft groups
- Stateless routing layer, no client-side topology awareness required

## Phase 6 — Observability (Ongoing, folded into every phase)
- Prometheus `/metrics`: ops/sec by command, latency histograms, replication lag, Raft term/leader changes
- Custom Go load-testing harness ("YCSB-lite")
- Grafana dashboard (reuses the Prometheus/Grafana pattern from Confoundr)

## Explicitly Deferred (Future Work)
- From-scratch Raft implementation (replacing the library) as a post-M4 deep dive
- RESP3 / pub-sub support
- Multi-datacenter replication

Full rationale for scoping decisions lives in [`DESIGN.md §8 Alternatives Considered`](DESIGN.md#8-alternatives-considered).
