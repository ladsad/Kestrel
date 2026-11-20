package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ladsad/kestrel/pkg/resp"
)

type nodeSpec struct {
	id       string
	port     int
	raftPort int
	addr     string
	raftAddr string
	dataDir  string
	cmd      *exec.Cmd
}

type benchResult struct {
	mode             string
	writesOK         int64
	readsOK          int64
	readsRejected    int64
	readsConnErr     int64
	staleReads       int64
	p50              time.Duration
	p95              time.Duration
	p99              time.Duration
	partitionFailover time.Duration
	killFailover     time.Duration
}

func main() {
	mode := flag.String("mode", "both", "Consistency modes to benchmark: both, strong, or eventual")
	readersPerNode := flag.Int("readers-per-node", 2, "Concurrent reader goroutines per node")
	steadyDur := flag.Duration("steady", 2*time.Second, "Steady-state phase duration")
	partitionDur := flag.Duration("partition", 2500*time.Millisecond, "Leader partition phase duration")
	killDur := flag.Duration("kill", 2500*time.Millisecond, "Post-kill phase duration")
	strict := flag.Bool("strict", true, "Exit non-zero if strong mode observes any stale reads")
	flag.Parse()

	var modes []string
	switch strings.ToLower(strings.TrimSpace(*mode)) {
	case "both":
		modes = []string{"eventual", "strong"}
	case "strong", "eventual":
		modes = []string{strings.ToLower(strings.TrimSpace(*mode))}
	default:
		log.Fatalf("invalid --mode %q: must be both, strong, or eventual", *mode)
	}

	binPath, cleanupBin, err := buildKestrelBinary()
	if err != nil {
		log.Fatalf("Failed to build kestrel binary: %v", err)
	}
	defer cleanupBin()

	results := make([]benchResult, 0, len(modes))
	for _, m := range modes {
		res := runModeBenchmark(binPath, m, *readersPerNode, *steadyDur, *partitionDur, *killDur)
		results = append(results, res)
		printResult(res)
	}

	if len(results) == 2 {
		printComparison(results[0], results[1])
	}

	if *strict {
		for _, r := range results {
			if r.mode == "strong" && r.staleReads > 0 {
				log.Fatalf("LINEARIZABILITY VIOLATION: strong mode observed %d stale reads", r.staleReads)
			}
		}
	}
}

func buildKestrelBinary() (string, func(), error) {
	tmpDir, err := os.MkdirTemp("", "kestrel-failover-bench-*")
	if err != nil {
		return "", nil, err
	}
	binPath := filepath.Join(tmpDir, "kestrel-node.exe")
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/kestrel")
	out, err := cmd.CombinedOutput()
	if err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, fmt.Errorf("%v: %s", err, string(out))
	}
	return binPath, func() { _ = os.RemoveAll(tmpDir) }, nil
}

func startCluster(binPath, consistency string) ([]*nodeSpec, func()) {
	dataRoot, err := os.MkdirTemp("", "kestrel-bench-data-*")
	if err != nil {
		log.Fatalf("Failed to create temp data dir: %v", err)
	}

	nodes := []*nodeSpec{
		{id: "node1", port: 16380, raftPort: 17380, dataDir: filepath.Join(dataRoot, "node1")},
		{id: "node2", port: 16381, raftPort: 17381, dataDir: filepath.Join(dataRoot, "node2")},
		{id: "node3", port: 16382, raftPort: 17382, dataDir: filepath.Join(dataRoot, "node3")},
	}

	for i, n := range nodes {
		n.addr = fmt.Sprintf("127.0.0.1:%d", n.port)
		n.raftAddr = fmt.Sprintf("127.0.0.1:%d", n.raftPort)
		args := []string{
			"--port", strconv.Itoa(n.port),
			"--node-id", n.id,
			"--raft-bind", n.raftAddr,
			"--data-dir", n.dataDir,
			"--metrics-port", "0",
			"--read-consistency", consistency,
		}
		if i == 0 {
			args = append(args, "--bootstrap")
		}
		n.cmd = exec.Command(binPath, args...)
		if err := n.cmd.Start(); err != nil {
			log.Fatalf("Failed to start %s: %v", n.id, err)
		}
	}

	cleanup := func() {
		for _, n := range nodes {
			if n.cmd != nil && n.cmd.Process != nil {
				_ = n.cmd.Process.Kill()
				_, _ = n.cmd.Process.Wait()
			}
		}
		_ = os.RemoveAll(dataRoot)
	}

	// Wait for node1 to elect itself leader.
	if err := waitUntilLeader(nodes[0].addr, 6*time.Second); err != nil {
		cleanup()
		log.Fatalf("node1 failed to become leader: %v", err)
	}

	// Join node2 and node3.
	for _, follower := range nodes[1:] {
		val, err := sendSingleCommand(nodes[0].addr, 2*time.Second, "RAFTJOIN", follower.id, follower.raftAddr)
		if err != nil || val.Type == resp.TypeError {
			cleanup()
			log.Fatalf("RAFTJOIN %s failed: err=%v resp=%+v", follower.id, err, val)
		}
	}

	// Wait until all nodes answer INFO REPLICATION.
	deadline := time.Now().Add(5 * time.Second)
	for _, n := range nodes {
		for time.Now().Before(deadline) {
			if _, err := queryRole(n.addr); err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	return nodes, cleanup
}

func runModeBenchmark(binPath, mode string, readersPerNode int, steadyDur, partitionDur, killDur time.Duration) benchResult {
	log.Printf("=== Running failover-bench in %s mode ===", strings.ToUpper(mode))
	nodes, cleanup := startCluster(binPath, mode)
	defer cleanup()

	const targetKey = "lin_key"
	var committedVersion atomic.Int64
	var writesOK atomic.Int64
	var readsOK atomic.Int64
	var readsRejected atomic.Int64
	var readsConnErr atomic.Int64
	var staleReads atomic.Int64

	var activeLeader atomic.Value
	activeLeader.Store(nodes[0])

	var partitionFailover atomic.Int64
	var killFailover atomic.Int64

	// Initialize key to version 0 before readers start.
	if val, err := sendSingleCommand(nodes[0].addr, time.Second, "SET", targetKey, "0"); err != nil || val.Type == resp.TypeError {
		log.Fatalf("Initial SET failed: err=%v resp=%+v", err, val)
	}

	stopCh := make(chan struct{})
	var wg sync.WaitGroup

	var latMu sync.Mutex
	var readLatencies []time.Duration

	// Start concurrent readers pinned across all nodes (leader + followers).
	for _, node := range nodes {
		for r := 0; r < readersPerNode; r++ {
			wg.Add(1)
			go func(target *nodeSpec) {
				defer wg.Done()
				localLats := make([]time.Duration, 0, 2048)

				var conn net.Conn
				var rw *resp.Writer
				var rr *resp.Reader
				defer func() {
					if conn != nil {
						conn.Close()
					}
					latMu.Lock()
					readLatencies = append(readLatencies, localLats...)
					latMu.Unlock()
				}()

				for {
					select {
					case <-stopCh:
						return
					default:
					}

					if conn == nil {
						c, err := net.DialTimeout("tcp", target.addr, 100*time.Millisecond)
						if err != nil {
							readsConnErr.Add(1)
							time.Sleep(15 * time.Millisecond)
							continue
						}
						conn = c
						rw = resp.NewWriter(conn)
						rr = resp.NewReader(conn)
					}

					minAcceptable := committedVersion.Load()
					start := time.Now()
					_ = conn.SetDeadline(time.Now().Add(250 * time.Millisecond))
					err := rw.Write(resp.NewArray([]resp.Value{
						resp.NewBulkString([]byte("GET")),
						resp.NewBulkString([]byte(targetKey)),
					}))
					var val resp.Value
					if err == nil {
						val, err = rr.Read()
					}
					elapsed := time.Since(start)

					if err != nil {
						readsConnErr.Add(1)
						conn.Close()
						conn = nil
						continue
					}

					if val.Type == resp.TypeError {
						readsRejected.Add(1)
						time.Sleep(1 * time.Millisecond)
						continue
					}

					gotVer, parseErr := strconv.ParseInt(string(val.Bulk), 10, 64)
					if parseErr != nil {
						readsRejected.Add(1)
						continue
					}

					readsOK.Add(1)
					localLats = append(localLats, elapsed)
					if !isLinearizableRead(minAcceptable, gotVer) {
						staleReads.Add(1)
					}
				}
			}(node)
		}
	}

	// Start continuous writer advancing committedVersion only after leader ACK.
	wg.Add(1)
	go func() {
		defer wg.Done()
		var nextVer int64 = 1
		var conn net.Conn
		var rw *resp.Writer
		var rr *resp.Reader
		var curTarget *nodeSpec

		defer func() {
			if conn != nil {
				conn.Close()
			}
		}()

		for {
			select {
			case <-stopCh:
				return
			default:
			}

			desired := activeLeader.Load().(*nodeSpec)
			if conn == nil || curTarget != desired {
				if conn != nil {
					conn.Close()
					conn = nil
				}
				c, err := net.DialTimeout("tcp", desired.addr, 150*time.Millisecond)
				if err != nil {
					if leader := findCurrentLeader(nodes); leader != nil {
						activeLeader.Store(leader)
					}
					time.Sleep(20 * time.Millisecond)
					continue
				}
				conn = c
				rw = resp.NewWriter(conn)
				rr = resp.NewReader(conn)
				curTarget = desired
			}

			_ = conn.SetDeadline(time.Now().Add(300 * time.Millisecond))
			err := rw.Write(resp.NewArray([]resp.Value{
				resp.NewBulkString([]byte("SET")),
				resp.NewBulkString([]byte(targetKey)),
				resp.NewBulkString([]byte(strconv.FormatInt(nextVer, 10))),
			}))
			var val resp.Value
			if err == nil {
				val, err = rr.Read()
			}

			if err != nil || val.Type == resp.TypeError {
				if conn != nil {
					conn.Close()
					conn = nil
				}
				if leader := findCurrentLeader(nodes); leader != nil {
					activeLeader.Store(leader)
				}
				time.Sleep(15 * time.Millisecond)
				continue
			}

			committedVersion.Store(nextVer)
			writesOK.Add(1)
			nextVer++
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Phase 1: Steady-state reads/writes.
	log.Printf("[%s] Phase 1: Steady-state workload (%v)...", mode, steadyDur)
	time.Sleep(steadyDur)

	// Phase 2: Network partition the active leader while reads/writes continue.
	partitionedNode := findCurrentLeader(nodes)
	if partitionedNode == nil {
		partitionedNode = nodes[0]
	}
	log.Printf("[%s] Phase 2: Injecting Raft network partition on leader %s (%s)...", mode, partitionedNode.id, partitionedNode.addr)
	partStart := time.Now()
	if val, err := sendSingleCommand(partitionedNode.addr, time.Second, "RAFTPARTITION", "ON"); err != nil || val.Type == resp.TypeError {
		log.Fatalf("RAFTPARTITION ON failed on %s: err=%v resp=%+v", partitionedNode.id, err, val)
	}

	newLeader := waitForNewLeader(nodes, partitionedNode.id, 6*time.Second)
	if newLeader != nil {
		activeLeader.Store(newLeader)
		partitionFailover.Store(int64(time.Since(partStart)))
		log.Printf("[%s] Majority elected new leader %s in %v", mode, newLeader.id, time.Since(partStart))
	} else {
		log.Printf("[%s] Warning: new leader election timed out during partition", mode)
	}
	time.Sleep(partitionDur)

	// Heal partition before leader kill so 2/3 nodes remain alive for quorum.
	log.Printf("[%s] Healing partition on %s...", mode, partitionedNode.id)
	if val, err := sendSingleCommand(partitionedNode.addr, time.Second, "RAFTPARTITION", "OFF"); err != nil || val.Type == resp.TypeError {
		log.Printf("[%s] Warning: RAFTPARTITION OFF returned err=%v resp=%+v", mode, err, val)
	}
	time.Sleep(1500 * time.Millisecond)

	// Phase 3: Kill the active leader during live reads/writes.
	victim := findCurrentLeader(nodes)
	if victim == nil {
		victim = activeLeader.Load().(*nodeSpec)
	}
	log.Printf("[%s] Phase 3: Killing active leader %s (%s)...", mode, victim.id, victim.addr)
	killStart := time.Now()
	if victim.cmd != nil && victim.cmd.Process != nil {
		_ = victim.cmd.Process.Kill()
		_, _ = victim.cmd.Process.Wait()
		victim.cmd = nil
	}

	remaining := make([]*nodeSpec, 0, len(nodes)-1)
	for _, n := range nodes {
		if n.id != victim.id {
			remaining = append(remaining, n)
		}
	}
	survivorLeader := waitForNewLeader(remaining, victim.id, 6*time.Second)
	if survivorLeader != nil {
		activeLeader.Store(survivorLeader)
		killFailover.Store(int64(time.Since(killStart)))
		log.Printf("[%s] Post-kill new leader %s ready in %v", mode, survivorLeader.id, time.Since(killStart))
	} else {
		log.Printf("[%s] Warning: post-kill leader election timed out", mode)
	}
	time.Sleep(killDur)

	close(stopCh)
	wg.Wait()

	// Verify final committed version durability on surviving leader.
	finalCommitted := committedVersion.Load()
	if survivorLeader != nil {
		val, err := sendSingleCommand(survivorLeader.addr, time.Second, "GET", targetKey)
		if err != nil || val.Type != resp.TypeBulkString {
			log.Printf("[%s] Warning: final verification GET failed: err=%v resp=%+v", mode, err, val)
		} else {
			gotFinal, _ := strconv.ParseInt(string(val.Bulk), 10, 64)
			if gotFinal < finalCommitted {
				log.Printf("[%s] Data loss detected! Committed=%d, surviving leader=%d", mode, finalCommitted, gotFinal)
			} else {
				log.Printf("[%s] Durability verified on %s: final key=%d (committed=%d)", mode, survivorLeader.id, gotFinal, finalCommitted)
			}
		}
	}

	p50, p95, p99 := computePercentiles(readLatencies)
	return benchResult{
		mode:              mode,
		writesOK:          writesOK.Load(),
		readsOK:           readsOK.Load(),
		readsRejected:     readsRejected.Load(),
		readsConnErr:      readsConnErr.Load(),
		staleReads:        staleReads.Load(),
		p50:               p50,
		p95:               p95,
		p99:               p99,
		partitionFailover: time.Duration(partitionFailover.Load()),
		killFailover:      time.Duration(killFailover.Load()),
	}
}

// isLinearizableRead returns true iff a completed read returned at least the latest
// write version committed before the read started.
func isLinearizableRead(minCommittedAtStart, readVersion int64) bool {
	return readVersion >= minCommittedAtStart
}

func computePercentiles(samples []time.Duration) (p50, p95, p99 time.Duration) {
	if len(samples) == 0 {
		return 0, 0, 0
	}
	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return percentileAt(sorted, 0.50), percentileAt(sorted, 0.95), percentileAt(sorted, 0.99)
}

func percentileAt(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * q)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func parseRoleFromInfo(info string) string {
	for _, line := range strings.Split(info, "\r\n") {
		if strings.HasPrefix(line, "role:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "role:"))
		}
	}
	return ""
}

func queryRole(addr string) (string, error) {
	val, err := sendSingleCommand(addr, 250*time.Millisecond, "INFO", "REPLICATION")
	if err != nil {
		return "", err
	}
	if val.Type == resp.TypeError {
		return "", fmt.Errorf("%s", val.Str)
	}
	return parseRoleFromInfo(string(val.Bulk)), nil
}

func findCurrentLeader(nodes []*nodeSpec) *nodeSpec {
	for _, n := range nodes {
		if n.cmd == nil {
			continue
		}
		if role, err := queryRole(n.addr); err == nil && role == "Leader" {
			return n
		}
	}
	return nil
}

func waitUntilLeader(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if role, err := queryRole(addr); err == nil && role == "Leader" {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s to become Leader", addr)
}

func waitForNewLeader(nodes []*nodeSpec, excludeID string, timeout time.Duration) *nodeSpec {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.id == excludeID || n.cmd == nil {
				continue
			}
			if role, err := queryRole(n.addr); err == nil && role == "Leader" {
				return n
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	return nil
}

func sendSingleCommand(addr string, timeout time.Duration, args ...string) (resp.Value, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return resp.Value{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	arr := make([]resp.Value, len(args))
	for i, a := range args {
		arr[i] = resp.NewBulkString([]byte(a))
	}
	w := resp.NewWriter(conn)
	r := resp.NewReader(conn)
	if err := w.Write(resp.NewArray(arr)); err != nil {
		return resp.Value{}, err
	}
	return r.Read()
}

func printResult(r benchResult) {
	fmt.Printf("\n--- %s Mode Summary ---\n", strings.ToUpper(r.mode))
	fmt.Printf("Writes OK:          %d\n", r.writesOK)
	fmt.Printf("Reads OK:           %d\n", r.readsOK)
	fmt.Printf("Reads Rejected:     %d (MOVED / VerifyLeader errors)\n", r.readsRejected)
	fmt.Printf("Read Conn Errors:   %d\n", r.readsConnErr)
	fmt.Printf("Stale Reads:        %d\n", r.staleReads)
	fmt.Printf("Read Latency P50:   %v\n", r.p50)
	fmt.Printf("Read Latency P95:   %v\n", r.p95)
	fmt.Printf("Read Latency P99:   %v\n", r.p99)
	fmt.Printf("Partition Failover: %v\n", r.partitionFailover)
	fmt.Printf("Kill Failover:      %v\n", r.killFailover)
}

func printComparison(eventual, strong benchResult) {
	if eventual.mode == "strong" && strong.mode == "eventual" {
		eventual, strong = strong, eventual
	}
	p99Delta := strong.p99 - eventual.p99
	var ratio float64
	if eventual.p99 > 0 {
		ratio = float64(strong.p99) / float64(eventual.p99)
	}
	fmt.Printf("\n=== Consistency Comparison (Eventual vs Strong) ===\n")
	fmt.Printf("Stale Reads:       eventual=%d  |  strong=%d\n", eventual.staleReads, strong.staleReads)
	fmt.Printf("Read P50 Latency:  eventual=%v  |  strong=%v\n", eventual.p50, strong.p50)
	fmt.Printf("Read P99 Latency:  eventual=%v  |  strong=%v  (cost: +%v, %.2fx)\n", eventual.p99, strong.p99, p99Delta, ratio)
}
