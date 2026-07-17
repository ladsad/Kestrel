package main

import (
	"flag"
	"fmt"
	"log"
)

func main() {
	port := flag.Int("port", 6380, "Port to run Kestrel server on")
	flag.Parse()

	fmt.Printf("Starting Kestrel on port %d...\n", *port)
	// TODO: Initialize TCP listener, command dispatcher, and store (Phase 1)
	log.Println("Server shutting down.")
}
