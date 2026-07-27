package vsr

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"
)

// MockTransport allows us to simulate network partitions and drops
type MockTransport struct {
	addr    string
	peers   map[string]*MockTransport
	msgChan chan Message
	dropped bool
}

func (m *MockTransport) Send(targetAddr string, msg Message) error {
	if m.dropped {
		return fmt.Errorf("network partitioned")
	}
	target, ok := m.peers[targetAddr]
	if !ok || target.dropped {
		return fmt.Errorf("target unreachable")
	}
	// async delivery
	go func() {
		target.msgChan <- msg
	}()
	return nil
}

func (m *MockTransport) Receive() <-chan Message {
	return m.msgChan
}

func (m *MockTransport) Close() error {
	return nil
}

func TestVSRLeaderElection(t *testing.T) {
	os.Remove("vsr-test-0.log")
	os.Remove("vsr-test-1.log")
	os.Remove("vsr-test-2.log")

	defer os.Remove("vsr-test-0.log")
	defer os.Remove("vsr-test-1.log")
	defer os.Remove("vsr-test-2.log")

	log0, _ := NewLog("vsr-test-0.log")
	log1, _ := NewLog("vsr-test-1.log")
	log2, _ := NewLog("vsr-test-2.log")

	t0 := &MockTransport{addr: "node0", peers: make(map[string]*MockTransport), msgChan: make(chan Message, 100)}
	t1 := &MockTransport{addr: "node1", peers: make(map[string]*MockTransport), msgChan: make(chan Message, 100)}
	t2 := &MockTransport{addr: "node2", peers: make(map[string]*MockTransport), msgChan: make(chan Message, 100)}

	transports := []*MockTransport{t0, t1, t2}
	for _, from := range transports {
		for _, to := range transports {
			from.peers[to.addr] = to
		}
	}

	peers := []string{"node0", "node1", "node2"}

	applyFunc := func(cmd []byte) interface{} { return nil }

	r0 := NewReplica(0, peers, log0, t0, applyFunc)
	r1 := NewReplica(1, peers, log1, t1, applyFunc)
	r2 := NewReplica(2, peers, log2, t2, applyFunc)
	_ = r2 // silence unused warning

	// Node 0 should be leader for view 0
	if !r0.IsLeader() {
		t.Errorf("Expected node 0 to be leader")
	}

	// Submit request to leader
	res := r0.ProcessClientCmd([]byte("SET KEY VAL"))
	if err, ok := res.(error); ok {
		t.Errorf("Failed to process command: %v", err)
	}

	// Verify replication
	time.Sleep(100 * time.Millisecond) // wait for async apply
	
	entry, ok := r1.vsrLog.GetEntry(1)
	if !ok || !bytes.Equal(entry.Command, []byte("SET KEY VAL")) {
		t.Errorf("Replication to node 1 failed")
	}

	// Simulate node 0 crash by dropping its network and triggering view change on node 1
	t0.dropped = true
	r1.triggerViewChange() // Manual trigger to speed up test instead of waiting for timer

	time.Sleep(200 * time.Millisecond)

	// Node 1 should now be leader (view 1)
	if !r1.IsLeader() {
		t.Errorf("Expected node 1 to be leader after view change")
	}
}
