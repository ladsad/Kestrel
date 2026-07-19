package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	CommandsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kestrel_commands_total",
			Help: "Total number of commands processed",
		},
		[]string{"command"},
	)

	CommandDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kestrel_command_duration_seconds",
			Help:    "Latency of commands in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"command"},
	)

	RaftTerm = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "kestrel_raft_term",
			Help: "Current Raft term",
		},
	)

	RaftState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "kestrel_raft_state",
			Help: "Current Raft state (1=Leader, 0=Follower/Candidate)",
		},
	)

	RaftLastLogIndex = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "kestrel_raft_last_log_index",
			Help: "Last log index appended",
		},
	)

	RaftAppliedIndex = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "kestrel_raft_applied_index",
			Help: "Last log index applied to FSM",
		},
	)
)

// RecordCommand tracks the execution time and count of a command
func RecordCommand(cmd string, start time.Time) {
	duration := time.Since(start).Seconds()
	CommandsTotal.WithLabelValues(cmd).Inc()
	CommandDuration.WithLabelValues(cmd).Observe(duration)
}

// UpdateRaftStats updates prometheus gauges with the latest raft stats
func UpdateRaftStats(stats map[string]string) {
	if termStr, ok := stats["term"]; ok {
		if term, err := strconv.ParseFloat(termStr, 64); err == nil {
			RaftTerm.Set(term)
		}
	}
	
	if state, ok := stats["state"]; ok {
		if state == "Leader" {
			RaftState.Set(1)
		} else {
			RaftState.Set(0)
		}
	}
	
	if lastIdxStr, ok := stats["last_log_index"]; ok {
		if idx, err := strconv.ParseFloat(lastIdxStr, 64); err == nil {
			RaftLastLogIndex.Set(idx)
		}
	}
	
	if appliedIdxStr, ok := stats["applied_index"]; ok {
		if idx, err := strconv.ParseFloat(appliedIdxStr, 64); err == nil {
			RaftAppliedIndex.Set(idx)
		}
	}
}
