package consensus

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestPartitionableTransportIsolatesOutboundAndInbound(t *testing.T) {
	tcp1, err := raft.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, io.Discard)
	if err != nil {
		t.Fatalf("NewTCPTransport 1 failed: %v", err)
	}
	tcp2, err := raft.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, io.Discard)
	if err != nil {
		tcp1.Close()
		t.Fatalf("NewTCPTransport 2 failed: %v", err)
	}

	pt1 := NewPartitionableTransport(tcp1)
	pt2 := NewPartitionableTransport(tcp2)
	defer pt1.Close()
	defer pt2.Close()

	addr1 := pt1.LocalAddr()
	addr2 := pt2.LocalAddr()

	// Drain peer2 consumer and reply OK when unpartitioned.
	stopDrain := make(chan struct{})
	defer close(stopDrain)
	go func() {
		for {
			select {
			case <-stopDrain:
				return
			case rpc, ok := <-pt2.Consumer():
				if !ok {
					return
				}
				rpc.Respond(&raft.AppendEntriesResponse{Success: true}, nil)
			}
		}
	}()

	// 1. Unpartitioned outbound RPC succeeds.
	var aeResp raft.AppendEntriesResponse
	if err := pt1.AppendEntries("node2", addr2, &raft.AppendEntriesRequest{}, &aeResp); err != nil {
		t.Fatalf("expected unpartitioned AppendEntries to succeed, got %v", err)
	}

	// Open an AppendPipeline before partitioning to verify existing pipelines are gated too.
	pipe, err := pt1.AppendEntriesPipeline("node2", addr2)
	if err != nil {
		t.Fatalf("AppendEntriesPipeline failed: %v", err)
	}
	defer pipe.Close()

	// 2. Partition pt1: outbound RPCs and existing pipeline must fail immediately.
	pt1.SetPartitioned(true)
	if !pt1.IsPartitioned() {
		t.Fatal("expected pt1.IsPartitioned() == true")
	}

	if err := pt1.AppendEntries("node2", addr2, &raft.AppendEntriesRequest{}, &aeResp); !errors.Is(err, ErrNetworkPartitioned) {
		t.Fatalf("expected ErrNetworkPartitioned on AppendEntries, got %v", err)
	}
	if _, err := pipe.AppendEntries(&raft.AppendEntriesRequest{}, &aeResp); !errors.Is(err, ErrNetworkPartitioned) {
		t.Fatalf("expected ErrNetworkPartitioned on cached pipeline, got %v", err)
	}
	if _, err := pt1.AppendEntriesPipeline("node2", addr2); !errors.Is(err, ErrNetworkPartitioned) {
		t.Fatalf("expected ErrNetworkPartitioned on AppendEntriesPipeline, got %v", err)
	}
	var rvResp raft.RequestVoteResponse
	if err := pt1.RequestVote("node2", addr2, &raft.RequestVoteRequest{}, &rvResp); !errors.Is(err, ErrNetworkPartitioned) {
		t.Fatalf("expected ErrNetworkPartitioned on RequestVote, got %v", err)
	}
	var isResp raft.InstallSnapshotResponse
	if err := pt1.InstallSnapshot("node2", addr2, &raft.InstallSnapshotRequest{}, &isResp, bytes.NewReader(nil)); !errors.Is(err, ErrNetworkPartitioned) {
		t.Fatalf("expected ErrNetworkPartitioned on InstallSnapshot, got %v", err)
	}
	var tnResp raft.TimeoutNowResponse
	if err := pt1.TimeoutNow("node2", addr2, &raft.TimeoutNowRequest{}, &tnResp); !errors.Is(err, ErrNetworkPartitioned) {
		t.Fatalf("expected ErrNetworkPartitioned on TimeoutNow, got %v", err)
	}

	// 3. Inbound RPCs to partitioned pt1 (Consumer and HeartbeatHandler) are rejected.
	var hbCalled bool
	pt1.SetHeartbeatHandler(func(rpc raft.RPC) {
		hbCalled = true
		rpc.Respond(&raft.AppendEntriesResponse{Success: true}, nil)
	})

	err = pt2.AppendEntries("node1", addr1, &raft.AppendEntriesRequest{}, &aeResp)
	if err == nil {
		t.Fatal("expected inbound RPC to partitioned node to fail, got nil")
	}
	if hbCalled {
		t.Fatal("heartbeat handler should not be invoked while partitioned")
	}

	// Ensure nothing leaked onto pt1.Consumer() while partitioned.
	select {
	case rpc := <-pt1.Consumer():
		t.Fatalf("unexpected RPC delivered to partitioned Consumer: %+v", rpc)
	case <-time.After(20 * time.Millisecond):
	}

	// 4. Heal partition and verify RPCs succeed again.
	pt1.SetPartitioned(false)
	if err := pt1.AppendEntries("node2", addr2, &raft.AppendEntriesRequest{}, &aeResp); err != nil {
		t.Fatalf("expected healed outbound AppendEntries to succeed, got %v", err)
	}
}
