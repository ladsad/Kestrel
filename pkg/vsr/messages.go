package vsr

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
)

type MsgType byte

const (
	MsgPrepare MsgType = iota
	MsgPrepareOK
	MsgCommit
	MsgStartViewChange
	MsgDoViewChange
	MsgStartView
	MsgRequest // From client proxy to leader
	MsgReply   // From leader to client proxy
)

type LogEntry struct {
	OpNumber uint64
	Command  []byte
}

type Prepare struct {
	ViewNumber uint64
	OpNumber   uint64
	CommitNum  uint64
	Command    []byte
}

type PrepareOK struct {
	ViewNumber uint64
	OpNumber   uint64
	ReplicaIdx int
}

type Commit struct {
	ViewNumber uint64
	CommitNum  uint64
}

type StartViewChange struct {
	ViewNumber uint64
	ReplicaIdx int
}

type DoViewChange struct {
	ViewNumber uint64
	ReplicaIdx int
	CommitNum  uint64
	LogEntries []LogEntry
}

type StartView struct {
	ViewNumber uint64
	CommitNum  uint64
	LogEntries []LogEntry
	OpNumber   uint64
}

type Request struct {
	Command   []byte
	ClientID  string
	RequestID uint64
}

type Reply struct {
	ViewNumber uint64
	RequestID  uint64
	Result     []byte
}

type Message struct {
	Type    MsgType
	Payload interface{}
}

// WriteMessage encodes and writes a message to the provided writer with a 4-byte length prefix.
func WriteMessage(w io.Writer, msg Message) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(&msg.Payload); err != nil {
		return err
	}

	payloadLen := uint32(buf.Len())
	header := make([]byte, 5)
	header[0] = byte(msg.Type)
	binary.BigEndian.PutUint32(header[1:], payloadLen)

	if _, err := w.Write(header); err != nil {
		return err
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		return err
	}
	return nil
}

// ReadMessage reads a message from the provided reader based on the 4-byte length prefix.
func ReadMessage(r io.Reader) (Message, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return Message{}, err
	}

	msgType := MsgType(header[0])
	payloadLen := binary.BigEndian.Uint32(header[1:])

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Message{}, err
	}

	buf := bytes.NewReader(payload)
	dec := gob.NewDecoder(buf)

	var msg Message
	msg.Type = msgType

	switch msgType {
	case MsgPrepare:
		var p Prepare
		err := dec.Decode(&p)
		msg.Payload = p
		return msg, err
	case MsgPrepareOK:
		var p PrepareOK
		err := dec.Decode(&p)
		msg.Payload = p
		return msg, err
	case MsgCommit:
		var p Commit
		err := dec.Decode(&p)
		msg.Payload = p
		return msg, err
	case MsgStartViewChange:
		var p StartViewChange
		err := dec.Decode(&p)
		msg.Payload = p
		return msg, err
	case MsgDoViewChange:
		var p DoViewChange
		err := dec.Decode(&p)
		msg.Payload = p
		return msg, err
	case MsgStartView:
		var p StartView
		err := dec.Decode(&p)
		msg.Payload = p
		return msg, err
	case MsgRequest:
		var p Request
		err := dec.Decode(&p)
		msg.Payload = p
		return msg, err
	case MsgReply:
		var p Reply
		err := dec.Decode(&p)
		msg.Payload = p
		return msg, err
	default:
		return Message{}, fmt.Errorf("unknown message type: %d", msgType)
	}
}

func init() {
	gob.Register(LogEntry{})
	gob.Register(Prepare{})
	gob.Register(PrepareOK{})
	gob.Register(Commit{})
	gob.Register(StartViewChange{})
	gob.Register(DoViewChange{})
	gob.Register(StartView{})
	gob.Register(Request{})
	gob.Register(Reply{})
}
