package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"sort"
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
	var allLatencies []time.Duration

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

			var latencies []time.Duration

			for {
				select {
				case <-timeout:
					opsMu.Lock()
					totalOps += ops
					totalErrs += errs
					allLatencies = append(allLatencies, latencies...)
					opsMu.Unlock()
					return
				default:
					startOp := time.Now()
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
					latencies = append(latencies, time.Since(startOp))

					startOp = time.Now()
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
					latencies = append(latencies, time.Since(startOp))

					ops += 2
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(startTime)
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })
	var p50, p95, p99 time.Duration
	if len(allLatencies) > 0 {
		p50 = allLatencies[int(float64(len(allLatencies))*0.50)]
		p95 = allLatencies[int(float64(len(allLatencies))*0.95)]
		p99 = allLatencies[int(float64(len(allLatencies))*0.99)]
	}

	fmt.Printf("--- Benchmark Results ---\n")
	fmt.Printf("Total Ops: %d\n", totalOps)
	fmt.Printf("Total Errs: %d\n", totalErrs)
	fmt.Printf("Elapsed: %s\n", elapsed)
	fmt.Printf("Ops/sec: %.2f\n", opsPerSec)
	fmt.Printf("p50 Latency: %v\n", p50)
	fmt.Printf("p95 Latency: %v\n", p95)
	fmt.Printf("p99 Latency: %v\n", p99)
}
