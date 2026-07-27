# Architectural Comparison: Raft vs. Viewstamped Replication (VSR)

Kestrel was initially built in Phase 4 using the industry-standard `hashicorp/raft` consensus engine. However, in Phase 8, we explored a from-scratch implementation of **Viewstamped Replication (VSR)** to push our understanding of distributed primitives and optimize tail latencies. 

This document explores the structural differences between these two foundational consensus algorithms and the trade-offs we encountered when benchmarking them within Kestrel.

## 1. Core Philosophy

At a high level, both Raft and VSR achieve the same goal: providing a replicated, fault-tolerant state machine. During the "happy path" (normal operation), their latencies are essentially identical, as both require a single leader to sequence incoming operations and synchronously replicate them to a quorum of nodes.

The fundamental divergence lies in **how they elect a leader**.

- **Raft** prioritizes understandability and uses *randomized timers* to break symmetry during elections.
- **VSR** prioritizes determinism and uses a predictable, *round-robin* approach based on the current view number.

## 2. Feature Comparison Matrix

| Feature | Kestrel (Phase 4: Raft) | Kestrel (Phase 8: VSR) | Impact on Kestrel |
| :--- | :--- | :--- | :--- |
| **Leader Election** | Randomized Timers. Nodes wait for a random timeout before starting an election. | Deterministic. The next leader is predetermined by the view number (Round-Robin). | VSR avoids split votes, leading to highly predictable and generally lower p99 latency spikes during node crashes. |
| **Log Up-to-dateness** | Strict. A node cannot become leader unless its log is fully up-to-date. | Flexible (Log Repair). A new leader can repair its log via other replicas before serving requests. | VSR can recover faster if the cluster's storage isn't pristine, increasing availability under heavy chaos. |
| **Failover Latency** | Variable. Usually fast, but unlucky random timers or split votes can compound recovery time. | Consistent. Deterministic progression means the cluster agrees on the next leader instantly. | VSR minimizes the absolute worst-case scenario (e.g., p99.9) for write stalls during a crash. |
| **Implementation Complexity**| Low. Ecosystem support is massive (e.g., `etcd/raft`, `hashicorp/raft`). | Moderate to High. VSR requires more complex state management for log repair and view changes. | Building a production-grade VSR implementation from scratch in Go took significantly more engineering effort than adopting a proven library. |

## 3. The Latency Argument (Why We Built Phase 8)

If we swap Kestrel's consensus engine to Viewstamped Replication, the standard p50 write latency (~2.55ms on our test hardware) remains identical, as the happy-path quorum replication is mathematically equivalent.

However, in our `failover-bench` hard crash simulation, VSR provided a much tighter bound on the **p99 latency**. 
When a Raft leader dies, the cluster waits for a randomized election timeout to expire. If two nodes wake up simultaneously, a split vote occurs, and the cluster must wait for *another* timeout to retry. 
VSR's deterministic failover mechanism (`New Leader = ViewNumber % N`) eliminates these randomized waiting periods. The cluster knows exactly who the next leader should be, drastically reducing the tail latency of a leader failover scenario.

## 4. Conclusion for Data Engineering

While VSR offers appealing theoretical bounds on tail latency during chaos, the trade-off is the sheer engineering complexity of implementing its state transfer and log repair mechanisms correctly. For production deployments prioritizing a proven ecosystem, Raft remains the safer choice. For systems where minimizing p99 write stalls during failovers is the absolute highest priority, VSR's deterministic approach is superior.
