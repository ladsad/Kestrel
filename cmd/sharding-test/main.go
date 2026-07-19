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
	log.Println("Starting Phase 5 Sharding Test...")

	// Cleanup existing instances
	exec.Command("taskkill", "/F", "/IM", "kestrel.exe").Run()
	exec.Command("taskkill", "/F", "/IM", "kestrel-proxy.exe").Run()
	exec.Command("powershell", "-Command", "Remove-Item -Recurse -Force data -ErrorAction SilentlyContinue").Run()

	// 1. Start Shard 0 (Ports 6380-6382)
	log.Println("Starting Shard 0 (Ports 6380-6382)...")
	cmds := []*exec.Cmd{
		exec.Command("go", "run", "./cmd/kestrel", "--port", "6380", "--node-id", "s0-n1", "--raft-bind", "127.0.0.1:7380", "--data-dir", "data/s0-n1", "--bootstrap"),
		exec.Command("go", "run", "./cmd/kestrel", "--port", "6381", "--node-id", "s0-n2", "--raft-bind", "127.0.0.1:7381", "--data-dir", "data/s0-n2"),
		exec.Command("go", "run", "./cmd/kestrel", "--port", "6382", "--node-id", "s0-n3", "--raft-bind", "127.0.0.1:7382", "--data-dir", "data/s0-n3"),
		
		// 2. Start Shard 1 (Ports 6480-6482)
		exec.Command("go", "run", "./cmd/kestrel", "--port", "6480", "--node-id", "s1-n1", "--raft-bind", "127.0.0.1:7480", "--data-dir", "data/s1-n1", "--bootstrap"),
		exec.Command("go", "run", "./cmd/kestrel", "--port", "6481", "--node-id", "s1-n2", "--raft-bind", "127.0.0.1:7481", "--data-dir", "data/s1-n2"),
		exec.Command("go", "run", "./cmd/kestrel", "--port", "6482", "--node-id", "s1-n3", "--raft-bind", "127.0.0.1:7482", "--data-dir", "data/s1-n3"),
	}

	for _, cmd := range cmds {
		cmd.Start()
		defer cmd.Process.Kill()
	}

	time.Sleep(3 * time.Second) // Wait for boot and election

	// Join Nodes to Shard 0
	log.Println("Forming Shard clusters...")
	conn0, _ := net.Dial("tcp", "127.0.0.1:6380")
	w0, r0 := resp.NewWriter(conn0), resp.NewReader(conn0)
	w0.Write(resp.NewArray([]resp.Value{resp.NewBulkString([]byte("RAFTJOIN")), resp.NewBulkString([]byte("s0-n2")), resp.NewBulkString([]byte("127.0.0.1:7381"))}))
	r0.Read()
	w0.Write(resp.NewArray([]resp.Value{resp.NewBulkString([]byte("RAFTJOIN")), resp.NewBulkString([]byte("s0-n3")), resp.NewBulkString([]byte("127.0.0.1:7382"))}))
	r0.Read()
	conn0.Close()

	// Join Nodes to Shard 1
	conn1, _ := net.Dial("tcp", "127.0.0.1:6480")
	w1, r1 := resp.NewWriter(conn1), resp.NewReader(conn1)
	w1.Write(resp.NewArray([]resp.Value{resp.NewBulkString([]byte("RAFTJOIN")), resp.NewBulkString([]byte("s1-n2")), resp.NewBulkString([]byte("127.0.0.1:7481"))}))
	r1.Read()
	w1.Write(resp.NewArray([]resp.Value{resp.NewBulkString([]byte("RAFTJOIN")), resp.NewBulkString([]byte("s1-n3")), resp.NewBulkString([]byte("127.0.0.1:7482"))}))
	r1.Read()
	conn1.Close()

	// 3. Start the Proxy
	log.Println("Starting Stateless Proxy on port 6379...")
	proxyCmd := exec.Command("go", "run", "./cmd/kestrel-proxy", "--port", "6379", "--shards", "127.0.0.1:6380,127.0.0.1:6480")
	proxyCmd.Start()
	defer proxyCmd.Process.Kill()

	time.Sleep(2 * time.Second)

	// 4. Send traffic through proxy
	log.Println("Sending 100 keys through the proxy...")
	pConn, err := net.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		log.Fatalf("Failed to connect to proxy: %v", err)
	}
	pw, pr := resp.NewWriter(pConn), resp.NewReader(pConn)

	for i := 0; i < 100; i++ {
		pw.Write(resp.NewArray([]resp.Value{resp.NewBulkString([]byte("SET")), resp.NewBulkString([]byte("key" + strconv.Itoa(i))), resp.NewBulkString([]byte("val" + strconv.Itoa(i)))}))
		pr.Read()
	}
	pConn.Close()

	// 5. Verify distribution
	log.Println("Verifying key distribution directly on shards...")
	shard0Count := 0
	shard1Count := 0

	conn0, _ = net.Dial("tcp", "127.0.0.1:6380")
	w0, r0 = resp.NewWriter(conn0), resp.NewReader(conn0)
	
	conn1, _ = net.Dial("tcp", "127.0.0.1:6480")
	w1, r1 = resp.NewWriter(conn1), resp.NewReader(conn1)

	for i := 0; i < 100; i++ {
		key := "key" + strconv.Itoa(i)
		
		w0.Write(resp.NewArray([]resp.Value{resp.NewBulkString([]byte("GET")), resp.NewBulkString([]byte(key))}))
		v0, _ := r0.Read()
		if v0.Type == resp.TypeBulkString {
			shard0Count++
		}

		w1.Write(resp.NewArray([]resp.Value{resp.NewBulkString([]byte("GET")), resp.NewBulkString([]byte(key))}))
		v1, _ := r1.Read()
		if v1.Type == resp.TypeBulkString {
			shard1Count++
		}
	}
	conn0.Close()
	conn1.Close()

	log.Printf("Distribution Results -> Shard 0: %d keys, Shard 1: %d keys", shard0Count, shard1Count)
	if shard0Count > 0 && shard1Count > 0 {
		log.Println("SUCCESS: Keys were successfully sharded across multiple Raft clusters!")
	} else {
		log.Println("FAILURE: One of the shards received no keys.")
	}
}
