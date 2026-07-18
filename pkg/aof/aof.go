package aof

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/ladsad/kestrel/pkg/resp"
)

type FsyncPolicy string

const (
	FsyncAlways   FsyncPolicy = "always"
	FsyncEverySec FsyncPolicy = "everysec"
	FsyncNo       FsyncPolicy = "no"
)

type AOF struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
	policy FsyncPolicy
	done   chan struct{}
}

func NewAOF(path string, policy FsyncPolicy) (*AOF, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	a := &AOF{
		file:   file,
		writer: bufio.NewWriter(file),
		policy: policy,
		done:   make(chan struct{}),
	}

	if policy == FsyncEverySec {
		go a.backgroundSync()
	}

	return a, nil
}

func (a *AOF) Write(cmd []resp.Value) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	arr := resp.NewArray(cmd)
	bytes := arr.Marshal()

	_, err := a.writer.Write(bytes)
	if err != nil {
		return err
	}

	if a.policy == FsyncAlways {
		if err := a.writer.Flush(); err != nil {
			return err
		}
		if err := a.file.Sync(); err != nil {
			return err
		}
	} else if a.policy == FsyncNo {
		if a.writer.Buffered() > 4096 {
			a.writer.Flush()
		}
	}

	return nil
}

func (a *AOF) backgroundSync() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.mu.Lock()
			a.writer.Flush()
			a.file.Sync()
			a.mu.Unlock()
		case <-a.done:
			return
		}
	}
}

func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.policy == FsyncEverySec {
		close(a.done)
	}

	a.writer.Flush()
	a.file.Sync()
	return a.file.Close()
}

func (a *AOF) Replay(callback func(string, []resp.Value)) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, err := a.file.Seek(0, 0)
	if err != nil {
		return err
	}

	reader := resp.NewReader(a.file)
	for {
		val, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("aof replay error: %v", err)
		}

		if val.Type == resp.TypeArray && len(val.Array) > 0 {
			cmd := string(val.Array[0].Bulk)
			callback(cmd, val.Array[1:])
		}
	}

	_, err = a.file.Seek(0, 2)
	return err
}

func (a *AOF) LockForRotation() {
	a.mu.Lock()
}

func (a *AOF) UnlockForRotation() {
	a.mu.Unlock()
}

func (a *AOF) Clear() error {
	// Assumes LockForRotation has been called
	if err := a.writer.Flush(); err != nil {
		return err
	}
	name := a.file.Name()
	if err := a.file.Close(); err != nil {
		return err
	}
	if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
		return err
	}
	file, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	a.file = file
	a.writer.Reset(file)
	return nil
}
