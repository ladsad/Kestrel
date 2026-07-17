package server

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/ladsad/kestrel/pkg/aof"
	"github.com/ladsad/kestrel/pkg/resp"
	"github.com/ladsad/kestrel/pkg/store"
)

type Server struct {
	port  int
	store *store.Store
	aof   *aof.AOF
}

func New(port int, st *store.Store, a *aof.AOF) *Server {
	return &Server{
		port:  port,
		store: st,
		aof:   a,
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return err
	}
	defer listener.Close()

	log.Printf("Kestrel server listening on port %d", s.port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) Rotate(snapshotPath string) error {
	if s.aof == nil {
		return nil
	}

	// 1. Lock AOF to block new writes from executing
	s.aof.LockForRotation()
	defer s.aof.UnlockForRotation()

	// 2. Lock Store to drain and pause any currently executing writes
	s.store.Pause()
	defer s.store.Resume()

	// 3. Take snapshot
	file, err := os.Create(snapshotPath + ".tmp")
	if err != nil {
		return err
	}

	if err := s.store.SnapshotNoLock(file); err != nil {
		file.Close()
		return err
	}
	file.Close() // Explicit close before rename!

	if err := os.Rename(snapshotPath+".tmp", snapshotPath); err != nil {
		return err
	}

	// 4. Clear AOF
	return s.aof.Clear()
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	for {
		val, err := reader.Read()
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading from connection: %v", err)
			}
			return
		}

		if val.Type != resp.TypeArray {
			writer.Write(resp.NewError("ERR expected array of bulk strings"))
			continue
		}

		if len(val.Array) == 0 {
			writer.Write(resp.NewError("ERR empty command"))
			continue
		}

		cmd := strings.ToUpper(string(val.Array[0].Bulk))
		args := val.Array[1:]

		s.ExecuteCommand(cmd, args, writer)
	}
}

func (s *Server) ExecuteCommand(cmd string, args []resp.Value, writer *resp.Writer) {
	var isWrite bool
	switch cmd {
	case "SET", "DEL", "HSET", "LPUSH", "RPUSH", "LPOP", "RPOP", "SADD", "ZADD":
		isWrite = true
	}

	if isWrite && s.aof != nil {
		// Reconstruct the full command for AOF
		fullCmd := make([]resp.Value, 0, len(args)+1)
		fullCmd = append(fullCmd, resp.NewBulkString([]byte(cmd)))
		fullCmd = append(fullCmd, args...)
		
		if err := s.aof.Write(fullCmd); err != nil {
			writer.Write(resp.NewError(fmt.Sprintf("ERR AOF write failed: %v", err)))
			return
		}
	}

	switch cmd {
	case "PING":
		if len(args) == 0 {
			writer.Write(resp.NewSimpleString("PONG"))
		} else if len(args) == 1 {
			writer.Write(resp.NewBulkString(args[0].Bulk))
		} else {
			writer.Write(resp.NewError("ERR wrong number of arguments for 'ping' command"))
		}
	case "ECHO":
		if len(args) != 1 {
			writer.Write(resp.NewError("ERR wrong number of arguments for 'echo' command"))
		} else {
			writer.Write(resp.NewBulkString(args[0].Bulk))
		}
	case "SET":
		if len(args) != 2 {
			writer.Write(resp.NewError("ERR wrong number of arguments for 'set' command"))
		} else {
			key := string(args[0].Bulk)
			val := string(args[1].Bulk)
			s.store.Set(key, val)
			writer.Write(resp.NewSimpleString("OK"))
		}
	case "GET":
		if len(args) != 1 {
			writer.Write(resp.NewError("ERR wrong number of arguments for 'get' command"))
		} else {
			key := string(args[0].Bulk)
			val, ok := s.store.Get(key)
			if !ok {
				writer.Write(resp.NewNullBulkString())
			} else {
				writer.Write(resp.NewBulkString([]byte(val)))
			}
		}
	case "DEL":
		if len(args) < 1 {
			writer.Write(resp.NewError("ERR wrong number of arguments for 'del' command"))
		} else {
			count := 0
			for _, arg := range args {
				key := string(arg.Bulk)
				count += s.store.Del(key)
			}
			writer.Write(resp.NewInteger(int64(count)))
		}
	case "HSET":
		if len(args) < 3 || len(args)%2 != 1 {
			writer.Write(resp.NewError("ERR wrong number of arguments for 'hset' command"))
		} else {
			key := string(args[0].Bulk)
			count := 0
			for i := 1; i < len(args); i += 2 {
				field := string(args[i].Bulk)
				val := string(args[i+1].Bulk)
				count += s.store.HSet(key, field, val)
			}
			writer.Write(resp.NewInteger(int64(count)))
		}
	case "HGET":
		if len(args) != 2 {
			writer.Write(resp.NewError("ERR wrong number of arguments for 'hget' command"))
		} else {
			key := string(args[0].Bulk)
			field := string(args[1].Bulk)
			val, ok := s.store.HGet(key, field)
			if !ok {
				writer.Write(resp.NewNullBulkString())
			} else {
				writer.Write(resp.NewBulkString([]byte(val)))
			}
		}
	case "HGETALL":
		if len(args) != 1 {
			writer.Write(resp.NewError("ERR wrong number of arguments for 'hgetall' command"))
		} else {
			key := string(args[0].Bulk)
			vals := s.store.HGetAll(key)
			arr := make([]resp.Value, len(vals))
			for i, v := range vals {
				arr[i] = resp.NewBulkString([]byte(v))
			}
			writer.Write(resp.NewArray(arr))
		}
	case "LPUSH":
		if len(args) < 2 {
			writer.Write(resp.NewError("ERR wrong number of arguments for 'lpush' command"))
		} else {
			key := string(args[0].Bulk)
			var vals []string
			for i := 1; i < len(args); i++ {
				vals = append(vals, string(args[i].Bulk))
			}
			count := s.store.LPush(key, vals)
			writer.Write(resp.NewInteger(int64(count)))
		}
	case "RPUSH":
		if len(args) < 2 {
			writer.Write(resp.NewError("ERR wrong number of arguments for 'rpush' command"))
		} else {
			key := string(args[0].Bulk)
			var vals []string
			for i := 1; i < len(args); i++ {
				vals = append(vals, string(args[i].Bulk))
			}
			count := s.store.RPush(key, vals)
			writer.Write(resp.NewInteger(int64(count)))
		}
	case "LPOP":
		if len(args) != 1 {
			writer.Write(resp.NewError("ERR wrong number of arguments for 'lpop' command"))
		} else {
			key := string(args[0].Bulk)
			val, ok := s.store.LPop(key)
			if !ok {
				writer.Write(resp.NewNullBulkString())
			} else {
				writer.Write(resp.NewBulkString([]byte(val)))
			}
		}
	case "RPOP":
		if len(args) != 1 {
			writer.Write(resp.NewError("ERR wrong number of arguments for 'rpop' command"))
		} else {
			key := string(args[0].Bulk)
			val, ok := s.store.RPop(key)
			if !ok {
				writer.Write(resp.NewNullBulkString())
			} else {
				writer.Write(resp.NewBulkString([]byte(val)))
			}
		}
	case "SADD":
		if len(args) < 2 {
			writer.Write(resp.NewError("ERR wrong number of arguments for 'sadd' command"))
		} else {
			key := string(args[0].Bulk)
			var vals []string
			for i := 1; i < len(args); i++ {
				vals = append(vals, string(args[i].Bulk))
			}
			count := s.store.SAdd(key, vals)
			writer.Write(resp.NewInteger(int64(count)))
		}
	case "SMEMBERS":
		if len(args) != 1 {
			writer.Write(resp.NewError("ERR wrong number of arguments for 'smembers' command"))
		} else {
			key := string(args[0].Bulk)
			vals := s.store.SMembers(key)
			arr := make([]resp.Value, len(vals))
			for i, v := range vals {
				arr[i] = resp.NewBulkString([]byte(v))
			}
			writer.Write(resp.NewArray(arr))
		}
	case "SISMEMBER":
		if len(args) != 2 {
			writer.Write(resp.NewError("ERR wrong number of arguments for 'sismember' command"))
		} else {
			key := string(args[0].Bulk)
			member := string(args[1].Bulk)
			res := s.store.SIsMember(key, member)
			writer.Write(resp.NewInteger(int64(res)))
		}
	case "ZADD":
		if len(args) != 3 { // Basic version, single score/member pair
			writer.Write(resp.NewError("ERR wrong number of arguments for 'zadd' command"))
		} else {
			key := string(args[0].Bulk)
			score, err := strconv.ParseFloat(string(args[1].Bulk), 64)
			if err != nil {
				writer.Write(resp.NewError("ERR value is not a valid float"))
			} else {
				member := string(args[2].Bulk)
				res := s.store.ZAdd(key, score, member)
				writer.Write(resp.NewInteger(int64(res)))
			}
		}
	case "ZRANGE":
		if len(args) != 3 {
			writer.Write(resp.NewError("ERR wrong number of arguments for 'zrange' command"))
		} else {
			key := string(args[0].Bulk)
			start, err1 := strconv.Atoi(string(args[1].Bulk))
			stop, err2 := strconv.Atoi(string(args[2].Bulk))
			if err1 != nil || err2 != nil {
				writer.Write(resp.NewError("ERR value is not an integer or out of range"))
			} else {
				vals := s.store.ZRange(key, start, stop)
				arr := make([]resp.Value, len(vals))
				for i, v := range vals {
					arr[i] = resp.NewBulkString([]byte(v))
				}
				writer.Write(resp.NewArray(arr))
			}
		}
	case "COMMAND":
		writer.Write(resp.NewSimpleString("OK"))
	default:
		writer.Write(resp.NewError(fmt.Sprintf("ERR unknown command '%s'", cmd)))
	}
}
