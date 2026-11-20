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

	ReadsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kestrel_reads_total",
			Help: "Total number of read commands by consistency mode and outcome",
		},
		[]string{"command", "mode", "outcome"},
	)

	ReadDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kestrel_read_duration_seconds",
			Help:    "Latency of read commands in seconds by consistency mode",
			Buckets: []float64{0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		},
		[]string{"command", "mode"},
	)

	ReadVerifyDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kestrel_read_verify_duration_seconds",
			Help:    "Latency of linearizable read verification stages (verify_leader, barrier)",
			Buckets: []float64{0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		},
		[]string{"stage"},
	)

	ReadVerificationFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kestrel_read_verification_failures_total",
			Help: "Total number of strong read rejections by failure reason",
		},
		[]string{"reason"},
	)
)

// RecordCommand tracks the execution time and count of a command
func RecordCommand(cmd string, start time.Time) {
	duration := time.Since(start).Seconds()
	CommandsTotal.WithLabelValues(cmd).Inc()
	CommandDuration.WithLabelValues(cmd).Observe(duration)
}

// RecordRead tracks read command count, outcome, and latency by consistency mode.
func RecordRead(cmd, mode, outcome string, start time.Time) {
	ReadsTotal.WithLabelValues(cmd, mode, outcome).Inc()
	ReadDuration.WithLabelValues(cmd, mode).Observe(time.Since(start).Seconds())
}

// RecordReadVerifyStage tracks the duration of a Raft read-verification stage.
func RecordReadVerifyStage(stage string, start time.Time) {
	ReadVerifyDuration.WithLabelValues(stage).Observe(time.Since(start).Seconds())
}

// RecordReadVerificationFailure increments the counter for failed linearizable read checks.
func RecordReadVerificationFailure(reason string) {
	ReadVerificationFailures.WithLabelValues(reason).Inc()
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
