package server

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/ladsad/kestrel/pkg/consensus"
	"github.com/ladsad/kestrel/pkg/metrics"
	"github.com/ladsad/kestrel/pkg/raftfsm"
	"github.com/ladsad/kestrel/pkg/resp"
	"github.com/ladsad/kestrel/pkg/store"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func execRESP(srv *Server, cmd string, args ...string) resp.Value {
	respArgs := make([]resp.Value, len(args))
	for i, a := range args {
		respArgs[i] = resp.NewBulkString([]byte(a))
	}

	var buf bytes.Buffer
	writer := resp.NewWriter(&buf)
	srv.ExecuteCommand(cmd, respArgs, writer)

	val, err := resp.NewReader(&buf).Read()
	if err != nil {
		panic(err)
	}
	return val
}

func TestParseReadConsistency(t *testing.T) {
	cases := []struct {
		in      string
		want    ReadConsistency
		wantErr bool
	}{
		{in: "strong", want: ReadConsistencyStrong},
		{in: "STRONG", want: ReadConsistencyStrong},
		{in: " eventual ", want: ReadConsistencyEventual},
		{in: "", wantErr: true},
		{in: "linearizable", wantErr: true},
	}

	for _, tc := range cases {
		got, err := ParseReadConsistency(tc.in)
		if (err != nil) != tc.wantErr {
			t.Fatalf("ParseReadConsistency(%q) err=%v, wantErr=%v", tc.in, err, tc.wantErr)
		}
		if got != tc.want {
			t.Fatalf("ParseReadConsistency(%q)=%q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestServerConsistencyConfigAndCommand(t *testing.T) {
	srv := New(0, store.New(), nil, "")
	if srv.Consistency() != ReadConsistencyStrong {
		t.Fatalf("expected default strong consistency, got %q", srv.Consistency())
	}

	// Query current mode via RESP
	val := execRESP(srv, "CONSISTENCY")
	if val.Type != resp.TypeBulkString || string(val.Bulk) != "strong" {
		t.Fatalf("expected bulk string 'strong', got %+v", val)
	}

	// Switch to eventual via RESP
	val = execRESP(srv, "CONSISTENCY", "eventual")
	if val.Type != resp.TypeSimpleString || val.Str != "OK" {
		t.Fatalf("expected +OK, got %+v", val)
	}
	if srv.Consistency() != ReadConsistencyEventual {
		t.Fatalf("expected eventual consistency, got %q", srv.Consistency())
	}

	// Verify INFO REPLICATION reports active consistency
	info := execRESP(srv, "INFO", "REPLICATION")
	if !strings.Contains(string(info.Bulk), "read_consistency:eventual\r\n") {
		t.Fatalf("expected INFO REPLICATION to include read_consistency:eventual, got %q", string(info.Bulk))
	}

	// Invalid mode rejected without mutating state
	errVal := execRESP(srv, "CONSISTENCY", "bogus")
	if errVal.Type != resp.TypeError {
		t.Fatalf("expected error for invalid consistency mode, got %+v", errVal)
	}
	if srv.Consistency() != ReadConsistencyEventual {
		t.Fatalf("consistency mutated on error: got %q", srv.Consistency())
	}

	// Wrong arity rejected
	arityErr := execRESP(srv, "CONSISTENCY", "strong", "extra")
	if arityErr.Type != resp.TypeError {
		t.Fatalf("expected arity error, got %+v", arityErr)
	}
}

type testClusterNode struct {
	id   raft.ServerID
	addr raft.ServerAddress
	pt   *consensus.PartitionableTransport
	raft *raft.Raft
	srv  *Server
}

func startTestRaftCluster(t *testing.T, count int) []*testClusterNode {
	t.Helper()

	nodes := make([]*testClusterNode, count)
	cfgServers := make([]raft.Server, count)

	for i := 0; i < count; i++ {
		id := raft.ServerID(fmt.Sprintf("node%d", i+1))
		tcpTrans, err := raft.NewTCPTransport("127.0.0.1:0", nil, 3, time.Second, io.Discard)
		if err != nil {
			t.Fatalf("NewTCPTransport(%s) failed: %v", id, err)
		}
		pt := consensus.NewPartitionableTransport(tcpTrans)
		nodes[i] = &testClusterNode{
			id:   id,
			addr: pt.LocalAddr(),
			pt:   pt,
		}
		cfgServers[i] = raft.Server{ID: id, Address: pt.LocalAddr()}
	}

	for _, n := range nodes {
		cfg := raft.DefaultConfig()
		cfg.LocalID = n.id
		cfg.HeartbeatTimeout = 100 * time.Millisecond
		cfg.ElectionTimeout = 100 * time.Millisecond
		cfg.LeaderLeaseTimeout = 80 * time.Millisecond
		cfg.CommitTimeout = 10 * time.Millisecond
		cfg.LogOutput = io.Discard

		st := store.New()
		srv := New(0, st, nil, ReadConsistencyStrong)
		fsm := raftfsm.NewStoreFSM(st, func(cmd string, args []resp.Value) interface{} {
			return srv.ApplyCommand(cmd, args)
		})

		memStore := raft.NewInmemStore()
		r, err := raft.NewRaft(cfg, fsm, memStore, memStore, raft.NewInmemSnapshotStore(), n.pt)
		if err != nil {
			t.Fatalf("NewRaft(%s) failed: %v", n.id, err)
		}
		n.raft = r
		n.srv = New(0, st, r, ReadConsistencyStrong)
		n.srv.SetPartitionController(n.pt)
		srv.raft = r
	}

	if err := nodes[0].raft.BootstrapCluster(raft.Configuration{Servers: cfgServers}).Error(); err != nil {
		t.Fatalf("BootstrapCluster failed: %v", err)
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.raft.Shutdown().Error()
			_ = n.pt.Close()
		}
	})

	return nodes
}

func waitForLeader(t *testing.T, nodes []*testClusterNode) (*testClusterNode, []*testClusterNode) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var leader *testClusterNode
		var followers []*testClusterNode
		for _, n := range nodes {
			if n.raft.State() == raft.Leader {
				leader = n
			} else {
				followers = append(followers, n)
			}
		}
		if leader != nil && len(followers) == len(nodes)-1 {
			return leader, followers
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for Raft leader election")
	return nil, nil
}

func TestReadCommandsClassification(t *testing.T) {
	readCmds := []string{"GET", "HGET", "HGETALL", "SMEMBERS", "SISMEMBER", "ZRANGE"}
	for _, cmd := range readCmds {
		if !isReadCommand(cmd) {
			t.Fatalf("expected %s to be classified as read command", cmd)
		}
		if isWriteCommand(cmd) {
			t.Fatalf("did not expect %s to be classified as write command", cmd)
		}
	}
	for _, cmd := range []string{"PING", "ECHO", "INFO", "CONSISTENCY"} {
		if isReadCommand(cmd) || isWriteCommand(cmd) {
			t.Fatalf("control command %s should be neither read nor write", cmd)
		}
	}
}

func TestStrongVsEventualReadsOnCluster(t *testing.T) {
	nodes := startTestRaftCluster(t, 3)
	leader, followers := waitForLeader(t, nodes)

	// Write a value through the leader
	if res := execRESP(leader.srv, "SET", "k1", "v1"); res.Type != resp.TypeSimpleString || res.Str != "OK" {
		t.Fatalf("SET failed: %+v", res)
	}
	if res := execRESP(leader.srv, "HSET", "h1", "f1", "hv1"); res.Type != resp.TypeInteger {
		t.Fatalf("HSET failed: %+v", res)
	}

	// Strong reads succeed on leader for all read commands
	if res := execRESP(leader.srv, "GET", "k1"); res.Type != resp.TypeBulkString || string(res.Bulk) != "v1" {
		t.Fatalf("leader strong GET expected v1, got %+v", res)
	}
	if res := execRESP(leader.srv, "HGET", "h1", "f1"); res.Type != resp.TypeBulkString || string(res.Bulk) != "hv1" {
		t.Fatalf("leader strong HGET expected hv1, got %+v", res)
	}

	// Strong reads on follower are rejected with MOVED
	follower := followers[0]
	for _, args := range [][]string{{"GET", "k1"}, {"HGET", "h1", "f1"}, {"HGETALL", "h1"}, {"SMEMBERS", "s1"}, {"SISMEMBER", "s1", "m"}, {"ZRANGE", "z1", "0", "-1"}} {
		res := execRESP(follower.srv, args[0], args[1:]...)
		if res.Type != resp.TypeError || !strings.HasPrefix(res.Str, "MOVED ") {
			t.Fatalf("expected MOVED error on follower strong %v, got %+v", args, res)
		}
	}

	// Control commands still work on follower in strong mode
	if res := execRESP(follower.srv, "PING"); res.Type != resp.TypeSimpleString || res.Str != "PONG" {
		t.Fatalf("expected PONG on follower, got %+v", res)
	}

	// Eventual mode allows follower to serve local reads
	if res := execRESP(follower.srv, "CONSISTENCY", "eventual"); res.Type != resp.TypeSimpleString || res.Str != "OK" {
		t.Fatalf("expected OK switching follower to eventual, got %+v", res)
	}
	if res := execRESP(follower.srv, "GET", "k1"); res.Type != resp.TypeBulkString || string(res.Bulk) != "v1" {
		t.Fatalf("follower eventual GET expected v1, got %+v", res)
	}

	// Partition old leader: strong reads MUST fail instead of serving stale data,
	// while a newly elected leader serves the updated value.
	leader.pt.SetPartitioned(true)

	newLeader, _ := waitForLeader(t, followers)
	if res := execRESP(newLeader.srv, "SET", "k1", "v2"); res.Type != resp.TypeSimpleString || res.Str != "OK" {
		t.Fatalf("SET on new leader failed: %+v", res)
	}

	// Partitioned old leader in strong mode rejects reads
	staleGuard := execRESP(leader.srv, "GET", "k1")
	if staleGuard.Type != resp.TypeError {
		t.Fatalf("expected error from partitioned old leader in strong mode, got %+v", staleGuard)
	}

	// Switching partitioned old leader to eventual demonstrates stale read (v1)
	_ = leader.srv.SetConsistency(ReadConsistencyEventual)
	staleRead := execRESP(leader.srv, "GET", "k1")
	if staleRead.Type != resp.TypeBulkString || string(staleRead.Bulk) != "v1" {
		t.Fatalf("expected stale v1 on partitioned old leader in eventual mode, got %+v", staleRead)
	}

	counterVal := func(c prometheus.Counter) float64 {
		var m dto.Metric
		if err := c.Write(&m); err != nil {
			t.Fatalf("failed to read metric: %v", err)
		}
		return m.GetCounter().GetValue()
	}

	// Verify read consistency metrics recorded strong successes, rejections, and eventual reads
	if got := counterVal(metrics.ReadsTotal.WithLabelValues("GET", "strong", "ok")); got < 1 {
		t.Fatalf("expected strong ok GET count >= 1, got %v", got)
	}
	if got := counterVal(metrics.ReadsTotal.WithLabelValues("GET", "eventual", "ok")); got < 2 {
		t.Fatalf("expected eventual ok GET count >= 2, got %v", got)
	}
	if got := counterVal(metrics.ReadVerificationFailures.WithLabelValues("not_leader")); got < 1 {
		t.Fatalf("expected not_leader verification failures >= 1, got %v", got)
	}
}

func TestRaftPartitionCommandAndDeposedLeader(t *testing.T) {
	// Standalone server without partition controller rejects RAFTPARTITION cleanly.
	standalone := New(0, store.New(), nil, ReadConsistencyStrong)
	if res := execRESP(standalone, "RAFTPARTITION"); res.Type != resp.TypeError {
		t.Fatalf("expected error without partition controller, got %+v", res)
	}

	nodes := startTestRaftCluster(t, 3)
	leader, followers := waitForLeader(t, nodes)

	if res := execRESP(leader.srv, "SET", "ver", "1"); res.Type != resp.TypeSimpleString || res.Str != "OK" {
		t.Fatalf("SET ver=1 failed: %+v", res)
	}

	// Verify RAFTPARTITION query and validation.
	if res := execRESP(leader.srv, "RAFTPARTITION"); res.Type != resp.TypeBulkString || string(res.Bulk) != "OFF" {
		t.Fatalf("expected RAFTPARTITION OFF, got %+v", res)
	}
	if res := execRESP(leader.srv, "RAFTPARTITION", "BOGUS"); res.Type != resp.TypeError {
		t.Fatalf("expected error for invalid RAFTPARTITION arg, got %+v", res)
	}

	// Partition the active leader via RESP command.
	if res := execRESP(leader.srv, "RAFTPARTITION", "ON"); res.Type != resp.TypeSimpleString || res.Str != "OK" {
		t.Fatalf("RAFTPARTITION ON failed: %+v", res)
	}
	if info := execRESP(leader.srv, "INFO", "REPLICATION"); !strings.Contains(string(info.Bulk), "raft_partitioned:true\r\n") {
		t.Fatalf("expected INFO REPLICATION to report raft_partitioned:true, got %q", string(info.Bulk))
	}

	// Wait for the unpartitioned majority to elect a new leader and advance the value.
	newLeader, _ := waitForLeader(t, followers)
	if res := execRESP(newLeader.srv, "SET", "ver", "2"); res.Type != resp.TypeSimpleString || res.Str != "OK" {
		t.Fatalf("SET ver=2 on new leader failed: %+v", res)
	}

	// Strong read on partitioned deposed leader must fail rather than returning stale "1".
	if res := execRESP(leader.srv, "GET", "ver"); res.Type != resp.TypeError {
		t.Fatalf("expected strong GET on partitioned deposed leader to error, got %+v", res)
	}

	// Eventual read on partitioned deposed leader exposes stale "1".
	if res := execRESP(leader.srv, "CONSISTENCY", "eventual"); res.Type != resp.TypeSimpleString {
		t.Fatalf("CONSISTENCY eventual failed: %+v", res)
	}
	if res := execRESP(leader.srv, "GET", "ver"); res.Type != resp.TypeBulkString || string(res.Bulk) != "1" {
		t.Fatalf("expected stale '1' on partitioned deposed leader in eventual mode, got %+v", res)
	}

	// Heal partition and verify old leader rejoins and catches up to "2".
	if res := execRESP(leader.srv, "RAFTPARTITION", "OFF"); res.Type != resp.TypeSimpleString || res.Str != "OK" {
		t.Fatalf("RAFTPARTITION OFF failed: %+v", res)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		res := execRESP(leader.srv, "GET", "ver")
		if res.Type == resp.TypeBulkString && string(res.Bulk) == "2" {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("healed node did not catch up to ver=2 in time")
}
