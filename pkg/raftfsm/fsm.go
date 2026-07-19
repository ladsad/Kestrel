package raftfsm

import (
	"bytes"
	"io"
	"log"
	"strings"

	"github.com/hashicorp/raft"
	"github.com/ladsad/kestrel/pkg/resp"
	"github.com/ladsad/kestrel/pkg/store"
)

type StoreFSM struct {
	store   *store.Store
	execute func(cmd string, args []resp.Value) interface{}
}

func NewStoreFSM(st *store.Store, exec func(string, []resp.Value) interface{}) *StoreFSM {
	return &StoreFSM{
		store:   st,
		execute: exec,
	}
}

func (f *StoreFSM) Apply(logEntry *raft.Log) interface{} {
	reader := resp.NewReader(bytes.NewReader(logEntry.Data))
	val, err := reader.Read()
	if err != nil {
		log.Printf("FSM Apply error: failed to parse log entry: %v", err)
		return err
	}

	if val.Type != resp.TypeArray || len(val.Array) == 0 {
		return nil
	}

	cmd := strings.ToUpper(string(val.Array[0].Bulk))
	args := val.Array[1:]

	return f.execute(cmd, args)
}

func (f *StoreFSM) Snapshot() (raft.FSMSnapshot, error) {
	// Take atomic snapshot using Store's RLock
	var buf bytes.Buffer
	if err := f.store.Snapshot(&buf); err != nil {
		return nil, err
	}
	return &StoreSnapshot{data: buf.Bytes()}, nil
}

func (f *StoreFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return f.store.LoadSnapshot(rc)
}

type StoreSnapshot struct {
	data []byte
}

func (s *StoreSnapshot) Persist(sink raft.SnapshotSink) error {
	_, err := sink.Write(s.data)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *StoreSnapshot) Release() {}
