package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"

	"github.com/hashicorp/raft"
	"github.com/ladsad/kestrel/pkg/consensus"
	"github.com/ladsad/kestrel/pkg/raftfsm"
	"github.com/ladsad/kestrel/pkg/resp"
	"github.com/ladsad/kestrel/pkg/server"
	"github.com/ladsad/kestrel/pkg/store"
)

func main() {
	port := flag.Int("port", 6380, "Port to run Kestrel server on")
	nodeID := flag.String("node-id", "node1", "Unique Raft Node ID")
	raftBind := flag.String("raft-bind", "127.0.0.1:7380", "Address to bind Raft on")
	dataDir := flag.String("data-dir", "data", "Directory to store Raft data")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap a new cluster")
	flag.Parse()

	// 1. Initialize Store
	st := store.New()

	// 2. We need an Execute callback for FSM that avoids writing to network
	// We'll create the server first with a dummy raft pointer, then set it
	srv := server.New(*port, st, nil)
	
	fsmExec := func(cmd string, args []resp.Value) interface{} {
		return srv.ApplyCommand(cmd, args)
	}
	fsm := raftfsm.NewStoreFSM(st, fsmExec)

	// 3. Initialize Raft
	r, err := consensus.SetupRaft(filepath.Join(*dataDir, *nodeID), *nodeID, *raftBind, fsm)
	if err != nil {
		log.Fatalf("Failed to initialize Raft: %v", err)
	}

	// 4. Update Server with Raft
	srv = server.New(*port, st, r)

	if *bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(*nodeID),
					Address: raft.ServerAddress(*raftBind),
				},
			},
		}
		r.BootstrapCluster(configuration)
		log.Printf("Bootstrapped Raft cluster as %s at %s", *nodeID, *raftBind)
	}

	fmt.Printf("Starting Kestrel on port %d with Raft Node ID %s...\n", *port, *nodeID)
	if err := srv.Start(); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
