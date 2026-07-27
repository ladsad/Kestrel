package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ladsad/kestrel/pkg/resp"
	"github.com/ladsad/kestrel/pkg/server"
	"github.com/ladsad/kestrel/pkg/store"
	"github.com/ladsad/kestrel/pkg/vsr"
)

func main() {
	port := flag.Int("port", 6380, "Port to run Kestrel server on")
	nodeIdx := flag.Int("node-idx", 0, "Unique VSR Node Index (0, 1, 2...)")
	vsrBind := flag.String("vsr-bind", "127.0.0.1:7380", "Address to bind VSR transport on")
	vsrPeers := flag.String("vsr-peers", "127.0.0.1:7380,127.0.0.1:7381,127.0.0.1:7382", "Comma separated VSR peer addresses")
	dataDir := flag.String("data-dir", "data", "Directory to store VSR log")
	metricsPort := flag.Int("metrics-port", 9090, "Port to expose Prometheus metrics")
	flag.Parse()

	// 1. Initialize Store
	st := store.New()

	srv := server.New(*port, st, nil)
	
	fsmExec := func(cmd []byte) interface{} {
		// Parse RESP command from byte array back to args
		reader := resp.NewReader(bytes.NewReader(cmd))
		val, err := reader.Read()
		if err != nil || val.Type != resp.TypeArray || len(val.Array) == 0 {
			return err
		}
		
		cmdStr := strings.ToUpper(string(val.Array[0].Bulk))
		args := val.Array[1:]
		return srv.ApplyCommand(cmdStr, args)
	}

	// 2. Initialize VSR Log
	if err := os.MkdirAll(*dataDir, 0700); err != nil {
		log.Fatalf("Failed to create data dir: %v", err)
	}
	vLog, err := vsr.NewLog(filepath.Join(*dataDir, fmt.Sprintf("vsr-%d.log", *nodeIdx)))
	if err != nil {
		log.Fatalf("Failed to initialize VSR log: %v", err)
	}

	// 3. Initialize VSR Transport
	transport, err := vsr.NewTCPTransport(*vsrBind)
	if err != nil {
		log.Fatalf("Failed to initialize VSR transport: %v", err)
	}

	// 4. Initialize VSR Replica
	peers := strings.Split(*vsrPeers, ",")
	replica := vsr.NewReplica(*nodeIdx, peers, vLog, transport, fsmExec)

	// 5. Update Server with VSR Replica
	srv = server.New(*port, st, replica)

	// 6. Start Prometheus metrics server
	if *metricsPort > 0 {
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			log.Printf("Starting Prometheus metrics server on :%d", *metricsPort)
			if err := http.ListenAndServe(fmt.Sprintf(":%d", *metricsPort), nil); err != nil {
				log.Fatalf("Metrics server failed: %v", err)
			}
		}()
	}

	fmt.Printf("Starting Kestrel on port %d with VSR Node Index %d...\n", *port, *nodeIdx)
	if err := srv.Start(); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
