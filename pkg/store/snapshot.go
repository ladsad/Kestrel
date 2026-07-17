package store

import (
	"encoding/gob"
	"io"
)

func init() {
	gob.Register(map[string]string{})
	gob.Register([]string{})
	gob.Register(map[string]struct{}{})
	gob.Register(&ZSet{})
}

// Snapshot writes the entire store to an io.Writer using gob.
// Must be called with a read or write lock held.
func (s *Store) SnapshotNoLock(w io.Writer) error {
	encoder := gob.NewEncoder(w)
	return encoder.Encode(s.data)
}

// Snapshot grabs an RLock and delegates to SnapshotNoLock
func (s *Store) Snapshot(w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.SnapshotNoLock(w)
}

// LoadSnapshot reads a store snapshot from an io.Reader.
func (s *Store) LoadSnapshot(r io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	decoder := gob.NewDecoder(r)
	var data map[string]interface{}
	if err := decoder.Decode(&data); err != nil {
		return err
	}
	s.data = data
	return nil
}
