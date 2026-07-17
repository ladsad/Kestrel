# Testing Strategy

Testing approach per layer, and what "done" means for each. The goal is that every phase's exit criteria (see [`ROADMAP.md`](ROADMAP.md)) is backed by an automated test where possible, and a documented manual procedure where automation isn't practical (e.g. real process kills, network partitions).

## Protocol Layer
- **Table-driven unit tests** covering RESP2 spec edge cases: empty bulk strings, negative array lengths (`$-1`, `*-1`), malformed/truncated input, oversized payloads.
- Round-trip tests: encode → decode → assert equality, for every RESP2 type Kestrel emits.

## Data Structures
- Standard unit tests for correctness of each operation (`SET`/`GET`/`DEL`, hash ops, list ops, sorted-set ordering).
- **Property-based tests** (`testing/quick` or `gopter`) for invariants that are easy to get subtly wrong under randomized input — e.g. skip-list ordering always holds after arbitrary insert/delete sequences.

## Concurrency
- `go test -race` is **mandatory** in CI on every PR — no PR merges with the race detector unhappy.
- Concurrent-client stress tests: N goroutines hammering the same keys, asserting no lost updates and no panics.

## Durability (Phase 2+)
- **Fault-injection tests:** spawn the server as a subprocess, drive a write burst, kill it at randomized points (including mid-fsync), restart, and verify recovery invariants against an independently-kept log of acknowledged writes.
- Explicit test for each fsync policy (`always`, `everysec`, `no`) documenting the durability/performance trade-off actually observed, not just claimed.

## Replication & Raft (Phase 3–4)
- Deterministic simulation where feasible: an in-process test harness that can inject network partitions and message delays between simulated nodes.
- Manual chaos scripts for real-process testing: `scripts/chaos/kill-leader.sh`, `scripts/chaos/partition-network.sh` — documented, repeatable, and run before every milestone sign-off.
- Explicit test: kill leader mid-write-stream, verify no committed entry is lost and no uncommitted entry is exposed to reads.

## Performance
- Benchmark suite (see [`BENCHMARKS.md`](BENCHMARKS.md)) run before and after every phase.
- Results are checked into the repo as historical record — regressions are visible in the git history, not just anecdotal.

## CI Pipeline (target)
1. `go vet` + `staticcheck`
2. `go test -race ./...`
3. Benchmark suite (non-blocking, results posted as a PR comment)
4. On `main`: nightly chaos script run against a local multi-node cluster
