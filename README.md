<div align="center">
  <h1>🦅 Kestrel</h1>
  <p><b>A distributed, fault-tolerant key-value store built from scratch in Go.</b></p>
  
  <!-- OPEN ITEM: Record and embed a live TUI dashboard failover demo GIF here -->

  [![Go Version](https://img.shields.io/github/go-mod/go-version/ladsad/kestrel)](https://go.dev/)
  [![License](https://img.shields.io/github/license/ladsad/kestrel)](LICENSE)
  [![Build Status](https://img.shields.io/github/actions/workflow/status/ladsad/kestrel/ci.yml?branch=main)](https://github.com/ladsad/kestrel/actions)
</div>

---

Kestrel is an exercise in the primitives that real distributed data stores are built from — concurrent state management, durability guarantees, replication, consensus, and sharding. It implements the RESP2 protocol for drop-in interoperability with standard Redis clients (`redis-cli`).

## 📊 Kestrel vs. Redis: At a Glance

The purpose of this comparison is to provide a grounded reference point. Kestrel trades raw in-memory throughput for strict durability. Every write requires consensus replication and a synchronous write-ahead log append to a memory-mapped BoltDB file, whereas default Redis operates entirely in memory with asynchronous replication.

| Metric | Kestrel | Redis |
|---|---|---|
| **Single-node Throughput** | ~12,904 ops/sec | ~34,427 ops/sec |
| **p99 Latency** | ~11.63 ms | ~3.29 ms |
| **Failover Time (10 trials)** | ~1.5s | N/A |
| **3-Node Cluster Throughput** | ~11,250 ops/sec | ~31,890 ops/sec |

*(Tested with 50 concurrent connections for 15s. Full methodology in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md))*

## ✨ Features

- **RESP2 Protocol**: Drop-in compatible with standard Redis clients.
- **Strict Durability**: Backed by BoltDB with a write-ahead log (AOF) and snapshotting for recovery.
- **Raft Consensus**: Uses HashiCorp's Raft implementation for CP (Consistent/Partition Tolerant) automatic failover and leader elections.
- **Sharding Layer**: Consistent hashing via a stateless proxy layer for horizontal scaling.
- **Deep Observability**: Fully instrumented with a Prometheus/Grafana stack and an interactive Bubbletea TUI for real-time cluster monitoring.

## 🏗️ Architecture at a Glance

Client requests (RESP2) route through a stateless proxy to a Kestrel node. Inside the node, one goroutine per connection handles dispatching commands to an in-memory store. Mutations are written to a Write-Ahead Log (AOF) and propagated to peer nodes via Raft consensus before being acknowledged. 

*(For a full breakdown of the cluster topology and state machine, see [`docs/DESIGN.md`](docs/DESIGN.md))*

## 🚀 Quick Start

### 1. Spin up a single node

```bash
git clone https://github.com/ladsad/kestrel.git
cd kestrel

# Bootstrap a single-node Kestrel server
go run ./cmd/kestrel --port 6380 --bootstrap
```

### 2. Connect via `redis-cli`

```bash
# In a new terminal window:
redis-cli -p 6380
> SET kestrel "is flying"
OK
> GET kestrel
"is flying"
```

### 3. Spin up the Observability Stack (Optional)

```bash
cd observability
docker-compose up -d

# Dashboard is available at http://localhost:3000 (admin/admin)
```

### 4. Run the TUI Dashboard

```bash
go run ./cmd/kestrel-dashboard --peers 127.0.0.1:6380
```

---

## 📚 Documentation Index

Our documentation is structured to explain not just *how* Kestrel works, but *why* specific architectural decisions were made.

| Document | Contents |
|---|---|
| [**`DESIGN.md`**](docs/DESIGN.md) | Full architectural design doc (goals, cluster topology, state machine). |
| [**`ROADMAP.md`**](docs/ROADMAP.md) | The 7 chronological development phases of Kestrel. |
| [**`PROTOCOL.md`**](docs/PROTOCOL.md) | The subset of RESP2 commands supported and wire protocol details. |
| [**`OBSERVABILITY.md`**](docs/OBSERVABILITY.md) | Setup and usage instructions for Grafana and the TUI. |
| [**`TESTING.md`**](docs/TESTING.md) | Testing strategy across the network, Raft, and storage layers. |
| [**`BENCHMARKS.md`**](docs/BENCHMARKS.md) | Full methodology and historical performance matrices. |
| [**`CONTRIBUTING.md`**](CONTRIBUTING.md) | Developer setup and contribution guidelines. |
| [**`CHANGELOG.md`**](CHANGELOG.md) | Detailed release notes. |

## 🚫 Non-Goals

Kestrel does **not** aim for full Redis parity. It purposefully avoids features like Lua scripting, Pub/Sub, and multi-datacenter asynchronous replication to maintain a tight scope focused on core distributed systems primitives.

## 📄 License

MIT — see [`LICENSE`](LICENSE).
