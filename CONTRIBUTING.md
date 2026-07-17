# Contributing to Kestrel

This is primarily a solo learning/portfolio project, but it's built to real-repo standards so it can accept external contributions or be handed off cleanly. If you're another contributor (or future-me six months from now), start here.

## Dev Setup

```bash
git clone https://github.com/ladsad/kestrel.git
cd kestrel
go mod download
go test -race ./...
```

Requires Go 1.22+.

## Before Opening a PR

- [ ] `go vet ./...` and `staticcheck ./...` pass clean
- [ ] `go test -race ./...` passes — the race detector is non-negotiable given how much of this project is concurrency-sensitive
- [ ] New commands/behavior are reflected in [`docs/PROTOCOL.md`](docs/PROTOCOL.md)
- [ ] Anything affecting a phase's exit criteria includes an updated benchmark in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md)
- [ ] `CHANGELOG.md` updated under `[Unreleased]`

## Code Style

- Standard `gofmt` / `goimports` — no custom formatting rules.
- Prefer explicit error handling over panics outside of `main`; a single client's malformed request must never bring down the server.
- Comment *why*, not *what*, for anything implementing a Raft/replication subtlety — the code should be readable as an explanation of the algorithm, not just an implementation of it.

## Scope Discipline

Each phase in [`docs/ROADMAP.md`](docs/ROADMAP.md) has an explicit exit criteria and an explicit non-goals list in [`docs/DESIGN.md`](docs/DESIGN.md). PRs that expand scope beyond the current phase (e.g. adding pub/sub before Phase 4 is done) will be redirected to an issue for later rather than merged — this project's biggest risk is scope creep, not lack of ideas (see [`DESIGN.md §9`](docs/DESIGN.md#9-risks--open-questions)).

## Commit Messages

Conventional-commit style preferred: `feat(protocol): add ZRANGE support`, `fix(aof): correct fsync ordering on rotation`, `test(raft): add leader-kill chaos test`.
