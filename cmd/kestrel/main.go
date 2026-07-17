package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/ladsad/kestrel/pkg/aof"
	"github.com/ladsad/kestrel/pkg/resp"
	"github.com/ladsad/kestrel/pkg/server"
	"github.com/ladsad/kestrel/pkg/store"
)

func main() {
	port := flag.Int("port", 6380, "Port to run Kestrel server on")
	fsyncPolicy := flag.String("fsync", "everysec", "Fsync policy: always, everysec, no")
	aofFile := flag.String("aof-file", "appendonly.aof", "Path to the AOF file")
	snapshotFile := flag.String("snapshot-file", "snapshot.rdb", "Path to the snapshot file")
	flag.Parse()

	policy := aof.FsyncPolicy(*fsyncPolicy)
	if policy != aof.FsyncAlways && policy != aof.FsyncEverySec && policy != aof.FsyncNo {
		log.Fatalf("Invalid fsync policy: %s", *fsyncPolicy)
	}

	st := store.New()

	// 1. Recovery: Load Snapshot
	if file, err := os.Open(*snapshotFile); err == nil {
		log.Printf("Loading snapshot from %s...", *snapshotFile)
		if err := st.LoadSnapshot(file); err != nil {
			log.Printf("Error loading snapshot: %v", err)
		}
		file.Close()
	}

	// 2. Recovery: Replay AOF
	// We instantiate the AOF instance to do this
	a, err := aof.NewAOF(*aofFile, policy)
	if err != nil {
		log.Fatalf("Failed to initialize AOF: %v", err)
	}
	defer a.Close()

	log.Printf("Replaying AOF from %s...", *aofFile)
	startReplay := time.Now()
	
	// Temporarily bypass Server and execute directly on store during replay
	dummyWriter := resp.NewWriter(io.Discard)
	// We need an execute command function to replay. Since executeCommand is inside Server,
	// let's create a temporary Server just for execution, without AOF so it doesn't log during replay
	replaySrv := server.New(*port, st, nil)

	var ops int
	err = a.Replay(func(cmd string, args []resp.Value) {
		ops++
		replaySrv.ExecuteCommand(cmd, args, dummyWriter)
	})
	if err != nil {
		log.Printf("AOF replay encountered error: %v (recovered %d ops)", err, ops)
	} else {
		log.Printf("AOF replayed %d ops in %v", ops, time.Since(startReplay))
	}

	// 3. Start Server
	srv := server.New(*port, st, a)

	// 4. Background Rotation (Snapshot + AOF Clear)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			log.Printf("Taking background snapshot and rotating AOF...")
			if err := srv.Rotate(*snapshotFile); err != nil {
				log.Printf("Rotation failed: %v", err)
			} else {
				log.Printf("Rotation successful")
			}
		}
	}()

	fmt.Printf("Starting Kestrel on port %d with fsync=%s...\n", *port, *fsyncPolicy)
	if err := srv.Start(); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
