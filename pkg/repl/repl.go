package repl

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/ladsad/kestrel/pkg/resp"
	"github.com/ladsad/kestrel/pkg/store"
)

type Leader struct {
	mu       sync.RWMutex
	replicas map[net.Conn]chan []byte
	store    *store.Store
	Offset   int64 // Number of write ops applied
}

func NewLeader(st *store.Store) *Leader {
	return &Leader{
		replicas: make(map[net.Conn]chan []byte),
		store:    st,
	}
}

// HandleSync is called when a replica connects and sends SYNC.
func (l *Leader) HandleSync(conn net.Conn, writer *resp.Writer) error {
	// 1. Take snapshot of the store
	var buf bytes.Buffer
	if err := l.store.Snapshot(&buf); err != nil {
		return err
	}

	// 2. Send the snapshot size as a bulk string, followed by the snapshot bytes
	// Wait, standard RESP bulk string is just exactly this!
	if err := writer.Write(resp.NewBulkString(buf.Bytes())); err != nil {
		return err
	}

	// 3. Register replica for future writes
	ch := make(chan []byte, 10000)
	l.mu.Lock()
	l.replicas[conn] = ch
	l.mu.Unlock()

	go func() {
		for b := range ch {
			if _, err := conn.Write(b); err != nil {
				l.RemoveReplica(conn)
				return
			}
		}
	}()

	log.Printf("Replica synced and registered: %v", conn.RemoteAddr())
	return nil
}

func (l *Leader) RemoveReplica(conn net.Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ch, ok := l.replicas[conn]; ok {
		close(ch)
		delete(l.replicas, conn)
		conn.Close()
		log.Printf("Replica disconnected: %v", conn.RemoteAddr())
	}
}

// ReplicateWrite forwards a write command to all connected replicas.
func (l *Leader) ReplicateWrite(cmd string, args []resp.Value) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.Offset++

	if len(l.replicas) == 0 {
		return
	}

	fullCmd := make([]resp.Value, 0, len(args)+1)
	fullCmd = append(fullCmd, resp.NewBulkString([]byte(cmd)))
	fullCmd = append(fullCmd, args...)
	arr := resp.NewArray(fullCmd)
	b := arr.Marshal()

	for _, ch := range l.replicas {
		select {
		case ch <- b:
		default:
			// Buffer full, drop or block. For simple replication, drop to avoid blocking leader
			// A real system would disconnect slow replicas.
		}
	}
}

type Replica struct {
	leaderAddr string
	store      *store.Store
	execute    func(cmd string, args []resp.Value) // Callback to execute on store/AOF
	Offset     int64
	done       chan struct{}
}

func NewReplica(leaderAddr string, st *store.Store, exec func(string, []resp.Value)) *Replica {
	return &Replica{
		leaderAddr: leaderAddr,
		store:      st,
		execute:    exec,
		done:       make(chan struct{}),
	}
}

func (r *Replica) Start() {
	go r.run()
}

func (r *Replica) run() {
	for {
		err := r.connectAndSync()
		if err != nil {
			log.Printf("Replication error: %v. Retrying in 2s...", err)
			time.Sleep(2 * time.Second)
			continue
		}
	}
}

func (r *Replica) connectAndSync() error {
	log.Printf("Connecting to leader %s...", r.leaderAddr)
	conn, err := net.Dial("tcp", r.leaderAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	writer := resp.NewWriter(conn)
	reader := resp.NewReader(conn)

	// Send SYNC
	writer.Write(resp.NewArray([]resp.Value{resp.NewBulkString([]byte("SYNC"))}))

	// Read Snapshot
	val, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read snapshot: %v", err)
	}
	if val.Type != resp.TypeBulkString {
		return fmt.Errorf("expected bulk string for snapshot, got %v", val.Type)
	}

	log.Printf("Received snapshot of %d bytes, loading into store...", len(val.Bulk))
	if err := r.store.LoadSnapshot(bytes.NewReader(val.Bulk)); err != nil {
		return fmt.Errorf("failed to load snapshot: %v", err)
	}
	log.Printf("Snapshot loaded.")

	// Continuous replication stream
	for {
		val, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("leader closed connection")
			}
			return fmt.Errorf("read error during replication: %v", err)
		}

		if val.Type == resp.TypeArray && len(val.Array) > 0 {
			cmd := string(val.Array[0].Bulk)
			args := val.Array[1:]
			r.execute(cmd, args)
			r.Offset++
		}
	}
}
