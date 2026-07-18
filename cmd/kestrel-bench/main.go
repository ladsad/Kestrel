package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/ladsad/kestrel/pkg/resp"
)

func main() {
	host := flag.String("host", "localhost:6380", "Server address (Leader)")
	readHost := flag.String("read-host", "", "Server address for reads (Follower, optional)")
	conns := flag.Int("conns", 50, "Number of concurrent connections")
	duration := flag.Duration("duration", 10*time.Second, "Benchmark duration")
	flag.Parse()

	if *readHost == "" {
		*readHost = *host
	}

	fmt.Printf("Running benchmark writes on %s, reads on %s with %d conns for %s...\n", *host, *readHost, *conns, *duration)

	var wg sync.WaitGroup
	var totalOps int64
	var totalErrs int64
	var opsMu sync.Mutex

	startTime := time.Now()

	for i := 0; i < *conns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			writeConn, err := net.Dial("tcp", *host)
			if err != nil {
				log.Printf("Dial write error: %v", err)
				opsMu.Lock()
				totalErrs++
				opsMu.Unlock()
				return
			}
			defer writeConn.Close()

			readConn, err := net.Dial("tcp", *readHost)
			if err != nil {
				log.Printf("Dial read error: %v", err)
				opsMu.Lock()
				totalErrs++
				opsMu.Unlock()
				return
			}
			defer readConn.Close()

			writerW := resp.NewWriter(writeConn)
			readerW := resp.NewReader(writeConn)

			writerR := resp.NewWriter(readConn)
			readerR := resp.NewReader(readConn)

			timeout := time.After(*duration)
			var ops int64
			var errs int64

			for {
				select {
				case <-timeout:
					opsMu.Lock()
					totalOps += ops
					totalErrs += errs
					opsMu.Unlock()
					return
				default:
					err = writerW.Write(resp.NewArray([]resp.Value{
						resp.NewBulkString([]byte("SET")),
						resp.NewBulkString([]byte("bench_key")),
						resp.NewBulkString([]byte("bench_value")),
					}))
					if err == nil {
						_, err = readerW.Read()
					}
					if err != nil {
						errs++
						continue
					}

					err = writerR.Write(resp.NewArray([]resp.Value{
						resp.NewBulkString([]byte("GET")),
						resp.NewBulkString([]byte("bench_key")),
					}))
					if err == nil {
						_, err = readerR.Read()
					}
					if err != nil {
						errs++
						continue
					}

					ops += 2
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(startTime)
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	fmt.Printf("--- Benchmark Results ---\n")
	fmt.Printf("Total Ops: %d\n", totalOps)
	fmt.Printf("Total Errs: %d\n", totalErrs)
	fmt.Printf("Elapsed: %s\n", elapsed)
	fmt.Printf("Ops/sec: %.2f\n", opsPerSec)
}
