package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	return m.GetCounter().GetValue()
}

func TestReadConsistencyMetrics(t *testing.T) {
	beforeStrong := counterValue(t, ReadsTotal.WithLabelValues("GET", "strong", "ok"))
	beforeFail := counterValue(t, ReadVerificationFailures.WithLabelValues("not_leader"))

	RecordRead("GET", "strong", "ok", time.Now().Add(-2*time.Millisecond))
	RecordReadVerifyStage("verify_leader", time.Now().Add(-time.Millisecond))
	RecordReadVerifyStage("barrier", time.Now().Add(-time.Millisecond))
	RecordReadVerificationFailure("not_leader")

	if got := counterValue(t, ReadsTotal.WithLabelValues("GET", "strong", "ok")); got != beforeStrong+1 {
		t.Fatalf("expected ReadsTotal to increment to %v, got %v", beforeStrong+1, got)
	}
	if got := counterValue(t, ReadVerificationFailures.WithLabelValues("not_leader")); got != beforeFail+1 {
		t.Fatalf("expected ReadVerificationFailures to increment to %v, got %v", beforeFail+1, got)
	}
}
