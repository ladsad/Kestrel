# Agent Rules for Kestrel

These rules are derived from the project's design and contributing guidelines. The AI agent must adhere to them strictly when generating code, tests, or documentation for this repository.

## 1. Scope Discipline and Phase Progression
- **Strict adherence to phases**: Do not implement features outside the current active phase defined in `docs/ROADMAP.md` (e.g., no pub/sub, no cluster-mode RESP3, no sharding before Phase 5).
- **Exit criteria**: Do not move to the next phase until the current phase's exit criteria are demonstrably met and benchmarks are recorded in `docs/BENCHMARKS.md`.
- **Target functionality**: Kestrel is a learning exercise in distributed systems primitives, not a full Redis clone. Avoid premature optimization or unnecessary command parity.

## 2. Code Style and Conventions
- **Formatting**: Always use standard `gofmt` and `goimports`. No custom formatting.
- **Error Handling**: Prefer explicit error handling over panics (except in `main`). A single malformed request must never crash the server.
- **Commenting**: Comment *why*, not *what*. This is especially critical for concurrency, replication, and Raft subtleties. The code should explain the algorithm.
- **Language**: Use Go 1.22+.

## 3. Concurrency and Safety
- **Race detector**: The race detector (`go test -race ./...`) is non-negotiable. All concurrent code must be race-free.
- **State Management**: Initially (Phase 1), guard shared in-memory store state with a single `sync.RWMutex`. Treat lock contention as a measured problem to solve with data, not premature optimization.
- **Goroutine Model**: Spawn one goroutine per client connection.

## 4. Testing Requirements
- **Protocol Layer**: Write table-driven unit tests covering RESP2 spec edge cases (empty bulk strings, negative lengths, malformed input).
- **Data Structures**: Implement standard unit tests and property-based tests (e.g., `testing/quick`) for complex invariants.
- **Concurrency**: Write concurrent-client stress tests (N goroutines hammering the same keys).

## 5. Documentation and Process
- **Commit Messages**: Use conventional commits (e.g., `feat(protocol): add ZRANGE`, `fix(aof): correct fsync`). **All modified files need to be committed with proper messaging, every time work is done.**
- **Updates**: When adding commands or altering behavior, update `docs/PROTOCOL.md`.
- **Changelog**: Update `CHANGELOG.md` under `[Unreleased]` for notable changes.
