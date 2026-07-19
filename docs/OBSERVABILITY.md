# Kestrel Observability (Phase 6) 📊

Phase 6 implements a comprehensive observability stack for the Kestrel distributed datastore, enabling real-time monitoring of cluster throughput, consensus state, and operation latencies.

## 1. Live Terminal Dashboard (TUI)

We developed a real-time terminal UI using the `bubbletea` framework. It polls the cluster nodes every second, parsing the `INFO REPLICATION` endpoint to track the Raft consensus status dynamically.

**Features:**
* **Visual States**: The UI highlights the active `Leader` (Green), `Candidate` nodes during elections (Yellow), and `DEAD` nodes if they drop offline (Red).
* **Consensus Tracking**: Tracks the current Raft Term, Last Log Index, and Applied Index for each peer to monitor replication lag.

**Run the Dashboard:**
```bash
go run ./cmd/kestrel-dashboard --peers 127.0.0.1:6380,127.0.0.1:6381,127.0.0.1:6382
```

![TUI Failover Dashboard](assets/tui_dashboard.webp)

## 2. Prometheus & Grafana Stack

Kestrel integrates `prometheus/client_golang` natively to expose a `/metrics` HTTP endpoint. The provided Docker Compose stack instantly spins up a connected Prometheus and Grafana instance.

**Tracked Metrics:**
* `kestrel_commands_total` (Counter): Operation throughput.
* `kestrel_command_duration_seconds` (Histogram): Command latency.
* `kestrel_raft_state` (Gauge): Tracks 1 for Leader, 0 for Follower.
* `kestrel_raft_term` (Gauge): Current cluster epoch.
* `kestrel_raft_last_log_index` / `applied_index` (Gauge): Replication syncing logic.

**Run the Stack:**
```bash
cd observability
docker-compose up -d
```
Then navigate to `http://localhost:3000` (default login: admin/admin) and click on the "Kestrel Cluster Metrics" dashboard under the Dashboards tab.

![Grafana Cluster Metrics Dashboard](assets/grafana_dashboard.webp)
