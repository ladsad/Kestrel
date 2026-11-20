package main

import (
	"testing"
	"time"
)

func TestIsLinearizableRead(t *testing.T) {
	if !isLinearizableRead(10, 10) {
		t.Fatal("exact committed version should be linearizable")
	}
	if !isLinearizableRead(10, 12) {
		t.Fatal("newer in-flight committed version should be linearizable")
	}
	if isLinearizableRead(10, 9) {
		t.Fatal("older version than committed-at-start must be flagged as stale")
	}
}

func TestComputePercentiles(t *testing.T) {
	if p50, p95, p99 := computePercentiles(nil); p50 != 0 || p95 != 0 || p99 != 0 {
		t.Fatalf("expected zeros for empty input, got %v %v %v", p50, p95, p99)
	}

	samples := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		samples[i] = time.Duration(100-i) * time.Millisecond
	}
	p50, p95, p99 := computePercentiles(samples)
	if p50 != 50*time.Millisecond {
		t.Fatalf("expected p50=50ms, got %v", p50)
	}
	if p95 != 95*time.Millisecond {
		t.Fatalf("expected p95=95ms, got %v", p95)
	}
	if p99 != 99*time.Millisecond {
		t.Fatalf("expected p99=99ms, got %v", p99)
	}
}

func TestParseRoleFromInfo(t *testing.T) {
	info := "role:Leader\r\nterm:2\r\nread_consistency:strong\r\n"
	if got := parseRoleFromInfo(info); got != "Leader" {
		t.Fatalf("expected Leader, got %q", got)
	}
	if got := parseRoleFromInfo("term:1\r\n"); got != "" {
		t.Fatalf("expected empty role when absent, got %q", got)
	}
}
