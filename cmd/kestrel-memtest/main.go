package main

import (
	"flag"
	"fmt"
	"net"
	"sync"

	"github.com/ladsad/kestrel/pkg/resp"
)

func main() {
	host := flag.String("host", "localhost:6380", "Server address")
	count := flag.Int("count", 100000, "Number of keys to insert")
	flag.Parse()

	val := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	var wg sync.WaitGroup
	conns := 50
	keysPerConn := *count / conns

	for c := 0; c < conns; c++ {
		wg.Add(1)
		go func(connIdx int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", *host)
			if err != nil {
				return
			}
			defer conn.Close()

			writer := resp.NewWriter(conn)
			reader := resp.NewReader(conn)

			for i := 0; i < keysPerConn; i++ {
				key := fmt.Sprintf("k:mem:%d:%010d", connIdx, i) // 16 byte key
				
				err = writer.Write(resp.NewArray([]resp.Value{
					resp.NewBulkString([]byte("SET")),
					resp.NewBulkString([]byte(key)),
					resp.NewBulkString([]byte(val)),
				}))
				if err != nil {
					return
				}
				
				_, err = reader.Read()
				if err != nil {
					return
				}
			}
		}(c)
	}

	wg.Wait()
	fmt.Println("Done inserting keys.")
}
