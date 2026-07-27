package vsr

import (
	"encoding/binary"
	"os"
	"sync"
)

// Log manages the append-only file for VSR entries
type Log struct {
	mu      sync.RWMutex
	file    *os.File
	entries []LogEntry
}

// NewLog initializes or opens a VSR log
func NewLog(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	l := &Log{
		file:    f,
		entries: make([]LogEntry, 0),
	}

	if err := l.recover(); err != nil {
		return nil, err
	}

	return l, nil
}

// Append adds a new entry to the log and writes it to disk
func (l *Log) Append(entry LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Serialize
	cmdLen := uint32(len(entry.Command))
	buf := make([]byte, 12+cmdLen) // 8 bytes OpNumber, 4 bytes len, Command
	binary.BigEndian.PutUint64(buf[0:8], entry.OpNumber)
	binary.BigEndian.PutUint32(buf[8:12], cmdLen)
	copy(buf[12:], entry.Command)

	if _, err := l.file.Write(buf); err != nil {
		return err
	}

	// We don't fsync every write for performance, unless configured. For now, rely on OS buffers.
	l.entries = append(l.entries, entry)
	return nil
}

// GetEntry returns the log entry for a specific OpNumber (1-indexed for VSR)
func (l *Log) GetEntry(opNumber uint64) (LogEntry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Assuming sequential dense opNumbers starting from 1
	idx := int(opNumber) - 1
	if idx >= 0 && idx < len(l.entries) {
		return l.entries[idx], true
	}
	return LogEntry{}, false
}

// EntriesFrom returns a slice of entries starting from opNumber (1-indexed)
func (l *Log) EntriesFrom(opNumber uint64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	idx := int(opNumber) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(l.entries) {
		return nil
	}
	// return a copy
	res := make([]LogEntry, len(l.entries[idx:]))
	copy(res, l.entries[idx:])
	return res
}

func (l *Log) LastOp() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].OpNumber
}

// Replace replaces log entries from opNumber onwards, truncating the file appropriately.
func (l *Log) Replace(opNumber uint64, newEntries []LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	idx := int(opNumber) - 1
	if idx < 0 {
		idx = 0
	}
	if idx > len(l.entries) {
		idx = len(l.entries) // append at end if gap
	}

	l.entries = append(l.entries[:idx], newEntries...)

	// Truncate and rewrite file from scratch for simplicity during recovery
	// A more optimized version would seek and truncate to the exact byte offset.
	if err := l.file.Truncate(0); err != nil {
		return err
	}
	if _, err := l.file.Seek(0, 0); err != nil {
		return err
	}

	for _, entry := range l.entries {
		cmdLen := uint32(len(entry.Command))
		buf := make([]byte, 12+cmdLen)
		binary.BigEndian.PutUint64(buf[0:8], entry.OpNumber)
		binary.BigEndian.PutUint32(buf[8:12], cmdLen)
		copy(buf[12:], entry.Command)
		if _, err := l.file.Write(buf); err != nil {
			return err
		}
	}
	return l.file.Sync()
}

func (l *Log) recover() error {
	stat, err := l.file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() == 0 {
		return nil
	}

	buf := make([]byte, stat.Size())
	if _, err := l.file.ReadAt(buf, 0); err != nil {
		return err
	}

	offset := 0
	for offset < len(buf) {
		if offset+12 > len(buf) {
			break // Corrupted or partial write
		}
		opNum := binary.BigEndian.Uint64(buf[offset : offset+8])
		cmdLen := binary.BigEndian.Uint32(buf[offset+8 : offset+12])
		offset += 12

		if offset+int(cmdLen) > len(buf) {
			break
		}
		
		cmd := make([]byte, cmdLen)
		copy(cmd, buf[offset:offset+int(cmdLen)])
		offset += int(cmdLen)

		l.entries = append(l.entries, LogEntry{OpNumber: opNum, Command: cmd})
	}
	
	// Seek to end of recovered valid entries
	if _, err := l.file.Seek(int64(offset), 0); err != nil {
		return err
	}
	if err := l.file.Truncate(int64(offset)); err != nil {
		return err
	}

	return nil
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
