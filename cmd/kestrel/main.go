package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/ladsad/kestrel/pkg/server"
	"github.com/ladsad/kestrel/pkg/store"
)

func main() {
	port := flag.Int("port", 6380, "Port to run Kestrel server on")
	flag.Parse()

	fmt.Printf("Starting Kestrel on port %d...\n", *port)
	
	st := store.New()
	srv := server.New(*port, st)

	if err := srv.Start(); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
