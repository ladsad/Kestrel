package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/ladsad/kestrel/pkg/resp"
	"github.com/ladsad/kestrel/pkg/sharding"
)

func main() {
	port := flag.Int("port", 6379, "Port to run the proxy on")
	shardsFlag := flag.String("shards", "127.0.0.1:6380,127.0.0.1:6480", "Comma-separated list of bootstrap nodes for each shard")
	flag.Parse()

	shardList := strings.Split(*shardsFlag, ",")
	if len(shardList) == 0 {
		log.Fatal("At least one shard must be specified")
	}

	ring := sharding.NewHashRing(100)
	
	var leadersMu sync.RWMutex
	shardLeaders := make(map[string]string)

	for i, addr := range shardList {
		shardID := fmt.Sprintf("shard-%d", i)
		ring.AddNode(shardID)
		shardLeaders[shardID] = addr
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to bind port: %v", err)
	}
	defer listener.Close()

	log.Printf("Kestrel Proxy listening on port %d, routing across %d shards", *port, len(shardList))

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Proxy accept error: %v", err)
			continue
		}
		
		go handleClient(conn, ring, shardLeaders, &leadersMu)
	}
}

func handleClient(clientConn net.Conn, ring *sharding.HashRing, shardLeaders map[string]string, leadersMu *sync.RWMutex) {
	defer clientConn.Close()
	
	clientReader := resp.NewReader(clientConn)
	clientWriter := resp.NewWriter(clientConn)
	
	// Local cache of connections to backends to reuse them
	backendConns := make(map[string]net.Conn)
	defer func() {
		for _, bc := range backendConns {
			bc.Close()
		}
	}()

	for {
		val, err := clientReader.Read()
		if err != nil {
			if err != io.EOF {
				log.Printf("Client read error: %v", err)
			}
			return
		}

		if val.Type != resp.TypeArray || len(val.Array) == 0 {
			clientWriter.Write(resp.NewError("ERR expected array of bulk strings"))
			continue
		}

		cmd := strings.ToUpper(string(val.Array[0].Bulk))
		
		// Commands that don't have keys (like PING) or handle differently
		if cmd == "PING" {
			clientWriter.Write(resp.NewSimpleString("PONG"))
			continue
		}

		if len(val.Array) < 2 {
			clientWriter.Write(resp.NewError("ERR wrong number of arguments for command"))
			continue
		}

		// Extract the key to determine shard
		key := string(val.Array[1].Bulk)
		targetShard := ring.GetNode(key)
		
		if targetShard == "" {
			clientWriter.Write(resp.NewError("ERR cluster has no shards"))
			continue
		}

		// Forward the command and handle MOVED redirects
		maxRedirects := 3
		redirects := 0

		for {
			if redirects >= maxRedirects {
				clientWriter.Write(resp.NewError("ERR too many redirects"))
				break
			}

			// Get the current known leader for this shard
			leadersMu.RLock()
			targetAddr := shardLeaders[targetShard]
			leadersMu.RUnlock()

			// Connect to backend if not already connected
			bc, ok := backendConns[targetAddr]
			if !ok {
				var berr error
				bc, berr = net.Dial("tcp", targetAddr)
				if berr != nil {
					// Fallback: If we can't connect, maybe node is down. 
					// A real router would ping other nodes in the shard to find the new leader.
					clientWriter.Write(resp.NewError(fmt.Sprintf("ERR failed to connect to backend: %v", berr)))
					break
				}
				backendConns[targetAddr] = bc
			}

			// Forward request
			bw := resp.NewWriter(bc)
			br := resp.NewReader(bc)

			if err := bw.Write(val); err != nil {
				// Connection died, close it and retry
				bc.Close()
				delete(backendConns, targetAddr)
				redirects++
				continue
			}

			// Read response
			backendResp, err := br.Read()
			if err != nil {
				bc.Close()
				delete(backendConns, targetAddr)
				redirects++
				continue
			}

			// Check for MOVED error
			if backendResp.Type == resp.TypeError && strings.HasPrefix(backendResp.Str, "MOVED ") {
				parts := strings.Split(backendResp.Str, " ")
				if len(parts) == 2 {
					newAddr := parts[1]
					leadersMu.Lock()
					shardLeaders[targetShard] = newAddr
					leadersMu.Unlock()
					
					redirects++
					continue
				}
			}

			// Success, forward response to client
			clientWriter.Write(backendResp)
			break
		}
	}
}
