<div align="center">
  <h1>🦅 Kestrel</h1>
  <p><b>A distributed, fault-tolerant key-value store built from scratch in Go.</b></p>

---

> **Note:** Most "build your own Redis" projects stop at a single-node command interpreter. Kestrel goes further. It is an exercise in the primitives that real distributed data stores are built from — concurrent state management, durability guarantees, replication, and consensus — with every claim backed by reproducible benchmarks.

## ✨ Key Features

- **RESP2 Protocol**: Drop-in compatible with standard Redis clients (`redis-cli`).
- **Strict Durability**: Backed by BoltDB with a write-ahead log (AOF) and snapshotting for recovery.
- **Raft Consensus**: Uses HashiCorp's Raft implementation for CP (Consistent/Partition Tolerant) automatic failover and leader elections.
- **Sharding Layer**: Consistent hashing via a stateless proxy layer for horizontal scaling.
- **Deep Observability**: Fully instrumented with a Prometheus/Grafana stack and an interactive Bubbletea TUI for real-time cluster monitoring.

---

## ⚡ Performance vs. Redis

Every performance claim in Kestrel is reproducible via the custom `kestrel-bench` utility. Below is a snapshot of our Phase 7 Head-to-Head benchmarking against a default Redis instance using 50 concurrent connections for 15 seconds.

| Metric                               | Kestrel         | Redis           |
| ------------------------------------ | --------------- | --------------- |
| **Single-node Throughput**     | ~12,904 ops/sec | ~34,427 ops/sec |
| **p50 Latency**                | ~2.55 ms        | ~1.41 ms        |
| **3-Node Cluster Throughput**  | ~11,250 ops/sec | ~31,890 ops/sec |
| **Memory Footprint (1M Keys)** | ~808 MB         | ~128 MB         |

**Why the difference?**
Kestrel intentionally trades raw in-memory throughput for strict durability. Every write in Kestrel requires consensus replication and a synchronous write-ahead log append to a memory-mapped BoltDB file. Default Redis operates entirely in memory with asynchronous replication.

*(For a full breakdown of our benchmarking methodology, see [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md))*

---

## 🚀 Quick Start

### 1. Spin up a single node

```bash
git clone https://github.com/ladsad/kestrel.git
cd kestrel

# Bootstrap a single-node Kestrel server on port 6380
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

---

## 📚 Documentation Index

Our documentation is structured to explain not just *how* Kestrel works, but *why* specific architectural decisions were made.

| Document                                               | Contents                                                                |
| ------------------------------------------------------ | ----------------------------------------------------------------------- |
| [**`DESIGN.md`**](docs/DESIGN.md)               | Full architectural design doc (goals, cluster topology, state machine). |
| [**`ROADMAP.md`**](docs/ROADMAP.md)             | The 7 chronological development phases of Kestrel.                      |
| [**`PROTOCOL.md`**](docs/PROTOCOL.md)           | The subset of RESP2 commands supported and wire protocol details.       |
| [**`OBSERVABILITY.md`**](docs/OBSERVABILITY.md) | Setup and usage instructions for Grafana and the TUI.                   |
| [**`TESTING.md`**](docs/TESTING.md)             | Testing strategy across the network, Raft, and storage layers.          |
| [**`BENCHMARKS.md`**](docs/BENCHMARKS.md)       | Full methodology and historical performance matrices.                   |
| [**`CONTRIBUTING.md`**](CONTRIBUTING.md)        | Developer setup and contribution guidelines.                            |
| [**`CHANGELOG.md`**](CHANGELOG.md)              | Detailed release notes.                                                 |

## 🚫 Non-Goals

Kestrel does **not** aim for full Redis parity. It purposefully avoids features like Lua scripting, Pub/Sub, and multi-datacenter asynchronous replication to maintain a tight scope focused on core distributed systems primitives.

## 📄 License

MIT — see [`LICENSE`](LICENSE).
