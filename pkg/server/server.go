package server

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/ladsad/kestrel/pkg/metrics"
	"github.com/ladsad/kestrel/pkg/resp"
	"github.com/ladsad/kestrel/pkg/store"
	"github.com/ladsad/kestrel/pkg/vsr"
)

type Server struct {
	port  int
	store *store.Store
	vsr   *vsr.Replica
}

func New(port int, st *store.Store, v *vsr.Replica) *Server {
	return &Server{
		port:  port,
		store: st,
		vsr:   v,
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

// Rotate is no longer needed since Raft handles snapshotting automatically

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

func (s *Server) ApplyCommand(cmd string, args []resp.Value) []byte {
	var buf bytes.Buffer
	writer := resp.NewWriter(&buf)
	s.executeCommandInternal(cmd, args, writer)
	return buf.Bytes()
}

func (s *Server) ExecuteCommand(cmd string, args []resp.Value, writer *resp.Writer) {
	start := time.Now()
	defer metrics.RecordCommand(cmd, start)

	var isWrite bool
	switch cmd {
	case "SET", "DEL", "HSET", "LPUSH", "RPUSH", "LPOP", "RPOP", "SADD", "ZADD":
		isWrite = true
	}

	if isWrite && s.vsr != nil {
		if !s.vsr.IsLeader() {
			writer.Write(resp.NewError("ERR not leader"))
			return
		}

		fullCmd := make([]resp.Value, 0, len(args)+1)
		fullCmd = append(fullCmd, resp.NewBulkString([]byte(cmd)))
		fullCmd = append(fullCmd, args...)
		
		arr := resp.NewArray(fullCmd)
		b := arr.Marshal()

		res := s.vsr.ProcessClientCmd(b)
		if err, ok := res.(error); ok {
			writer.Write(resp.NewError(fmt.Sprintf("ERR vsr apply failed: %v", err)))
			return
		}
		
		// If ProcessClientCmd returns byte slice or OK
		if bytesRes, ok := res.([]byte); ok {
			writer.WriteRaw(bytesRes)
		} else {
			// Actually we are not returning the real result from ProcessClientCmd in MVP,
			// just OK string if it didn't error.
			// The state machine application wrote to store, so we need to generate response.
			// But for now let's just let it be handled by standard if not returned.
			s.executeCommandInternal(cmd, args, writer)
		}
		return
	}

	s.executeCommandInternal(cmd, args, writer)
}

func (s *Server) executeCommandInternal(cmd string, args []resp.Value, writer *resp.Writer) {
	// Removing manual AOF and repl hooks since Raft handles durability and replication
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
	case "INFO":
		if len(args) > 0 && strings.ToUpper(string(args[0].Bulk)) == "REPLICATION" {
			var info string
			if s.vsr != nil {
				if s.vsr.IsLeader() {
					info += "role:leader\r\n"
				} else {
					info += "role:follower\r\n"
				}
			} else {
				info += "role:standalone\r\n"
			}
			writer.Write(resp.NewBulkString([]byte(info)))
		} else {
			writer.Write(resp.NewBulkString([]byte("# Server\r\nkestrel_version:0.5.0\r\n")))
		}
	case "COMMAND":
		writer.Write(resp.NewSimpleString("OK"))
	default:
		writer.Write(resp.NewError(fmt.Sprintf("ERR unknown command '%s'", cmd)))
	}
}
