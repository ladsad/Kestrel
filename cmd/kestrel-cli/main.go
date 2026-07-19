package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/ladsad/kestrel/pkg/resp"
)

func main() {
	host := flag.String("h", "localhost", "Server hostname")
	port := flag.Int("p", 6380, "Server port")
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", *host, *port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("Could not connect to Kestrel at %s: %v", addr, err)
	}
	defer conn.Close()

	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Printf("Connected to %s\n", addr)
	
	for {
		fmt.Printf("%s> ", addr)
		if !scanner.Scan() {
			break
		}
		
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		
		parts := strings.Fields(line)
		args := make([]resp.Value, len(parts))
		for i, part := range parts {
			args[i] = resp.NewBulkString([]byte(part))
		}
		
		if err := writer.Write(resp.NewArray(args)); err != nil {
			log.Fatalf("Write error: %v", err)
		}
		
		val, err := reader.Read()
		if err != nil {
			log.Fatalf("Read error: %v", err)
		}
		
		printValue(val)
	}
}

func printValue(v resp.Value) {
	switch v.Type {
	case resp.TypeSimpleString:
		fmt.Println(v.Str)
	case resp.TypeError:
		fmt.Printf("(error) %s\n", v.Str)
	case resp.TypeInteger:
		fmt.Printf("(integer) %d\n", v.Num)
	case resp.TypeBulkString:
		if v.IsNull {
			fmt.Println("(nil)")
		} else {
			fmt.Printf("\"%s\"\n", string(v.Bulk))
		}
	case resp.TypeArray:
		if v.IsNull {
			fmt.Println("(nil)")
		} else {
			for i, item := range v.Array {
				fmt.Printf("%d) ", i+1)
				// Simplified nested printing
				if item.Type == resp.TypeBulkString {
					fmt.Printf("\"%s\"\n", string(item.Bulk))
				} else {
					fmt.Println(item.Str)
				}
			}
		}
	}
}
