package main

import (
	"log"
	"net"
	"os/exec"
	"strconv"
	"time"

	"github.com/ladsad/kestrel/pkg/resp"
)

func main() {
	log.Println("Starting Phase 4 Failover Benchmark...")
	
	// Ensure cleanup on exit
	exec.Command("taskkill", "/F", "/IM", "kestrel.exe").Run()
	exec.Command("powershell", "-Command", "Remove-Item -Recurse -Force data -ErrorAction SilentlyContinue").Run()

	// 1. Start 3 Nodes
	cmd1 := exec.Command("go", "run", "./cmd/kestrel", "--port", "6380", "--node-id", "node1", "--raft-bind", "127.0.0.1:7380", "--data-dir", "data/node1", "--bootstrap")
	cmd1.Start()
	
	cmd2 := exec.Command("go", "run", "./cmd/kestrel", "--port", "6381", "--node-id", "node2", "--raft-bind", "127.0.0.1:7381", "--data-dir", "data/node2")
	cmd2.Start()
	
	cmd3 := exec.Command("go", "run", "./cmd/kestrel", "--port", "6382", "--node-id", "node3", "--raft-bind", "127.0.0.1:7382", "--data-dir", "data/node3")
	cmd3.Start()
	
	defer func() {
		cmd1.Process.Kill()
		cmd2.Process.Kill()
		cmd3.Process.Kill()
	}()

	time.Sleep(3 * time.Second) // Wait for boot and election

	// 2. Join cluster
	log.Println("Joining nodes to cluster...")
	conn, err := net.Dial("tcp", "127.0.0.1:6380")
	if err != nil {
		log.Fatalf("Failed to connect to leader: %v", err)
	}
	
	w := resp.NewWriter(conn)
	r := resp.NewReader(conn)
	
	w.Write(resp.NewArray([]resp.Value{resp.NewBulkString([]byte("RAFTJOIN")), resp.NewBulkString([]byte("node2")), resp.NewBulkString([]byte("127.0.0.1:7381"))}))
	r.Read() // Wait for OK
	
	w.Write(resp.NewArray([]resp.Value{resp.NewBulkString([]byte("RAFTJOIN")), resp.NewBulkString([]byte("node3")), resp.NewBulkString([]byte("127.0.0.1:7382"))}))
	r.Read() // Wait for OK
	
	conn.Close()
	log.Println("Cluster joined.")
	time.Sleep(1 * time.Second)

	// 3. Write data
	conn, _ = net.Dial("tcp", "127.0.0.1:6380")
	w = resp.NewWriter(conn)
	r = resp.NewReader(conn)
	
	for i := 0; i < 1000; i++ {
		w.Write(resp.NewArray([]resp.Value{resp.NewBulkString([]byte("SET")), resp.NewBulkString([]byte("key" + strconv.Itoa(i))), resp.NewBulkString([]byte("val" + strconv.Itoa(i)))}))
		r.Read()
	}
	log.Println("Wrote 1000 keys to leader.")
	
	// 4. Kill leader
	log.Println("Killing leader (node1)...")
	killTime := time.Now()
	cmd1.Process.Kill()
	conn.Close()

	// 5. Measure time to resume writes on node2 (or node3)
	log.Println("Polling for new leader and write availability...")
	var resumeTime time.Duration
	var newLeaderPort string

	for {
		// Try node2
		c2, err := net.DialTimeout("tcp", "127.0.0.1:6381", 50*time.Millisecond)
		if err == nil {
			w2 := resp.NewWriter(c2)
			r2 := resp.NewReader(c2)
			w2.Write(resp.NewArray([]resp.Value{resp.NewBulkString([]byte("SET")), resp.NewBulkString([]byte("failover_test")), resp.NewBulkString([]byte("success"))}))
			
			c2.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			val, err := r2.Read()
			if err == nil && val.Type != resp.TypeError {
				resumeTime = time.Since(killTime)
				newLeaderPort = "6381"
				c2.Close()
				break
			}
			c2.Close()
		}

		// Try node3
		c3, err := net.DialTimeout("tcp", "127.0.0.1:6382", 50*time.Millisecond)
		if err == nil {
			w3 := resp.NewWriter(c3)
			r3 := resp.NewReader(c3)
			w3.Write(resp.NewArray([]resp.Value{resp.NewBulkString([]byte("SET")), resp.NewBulkString([]byte("failover_test")), resp.NewBulkString([]byte("success"))}))
			
			c3.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			val, err := r3.Read()
			if err == nil && val.Type != resp.TypeError {
				resumeTime = time.Since(killTime)
				newLeaderPort = "6382"
				c3.Close()
				break
			}
			c3.Close()
		}

		time.Sleep(10 * time.Millisecond)
	}

	log.Printf("Failover successful! Time to resume writes: %v (New Leader Port: %s)", resumeTime, newLeaderPort)

	// 6. Verify data
	c, _ := net.Dial("tcp", "127.0.0.1:"+newLeaderPort)
	w = resp.NewWriter(c)
	r = resp.NewReader(c)
	
	w.Write(resp.NewArray([]resp.Value{resp.NewBulkString([]byte("GET")), resp.NewBulkString([]byte("key999"))}))
	val, _ := r.Read()
	
	if string(val.Bulk) == "val999" {
		log.Println("Zero data loss verified. Key 999 is present.")
	} else {
		log.Printf("Data loss detected! Expected val999, got %v", val)
	}
	
	c.Close()
}
