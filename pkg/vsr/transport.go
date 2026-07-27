package vsr

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// Transport defines how replicas communicate with each other
type Transport interface {
	Send(targetAddr string, msg Message) error
	Receive() <-chan Message
	Close() error
}

type TCPTransport struct {
	mu         sync.Mutex
	listener   net.Listener
	msgChan    chan Message
	conns      map[string]net.Conn
	addr       string
	closing    bool
}

func NewTCPTransport(bindAddr string) (*TCPTransport, error) {
	l, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}

	t := &TCPTransport{
		listener: l,
		msgChan:  make(chan Message, 1024),
		conns:    make(map[string]net.Conn),
		addr:     bindAddr,
	}

	go t.acceptLoop()

	return t, nil
}

func (t *TCPTransport) acceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			t.mu.Lock()
			closing := t.closing
			t.mu.Unlock()
			if closing {
				return
			}
			log.Printf("VSR TCPTransport accept error: %v", err)
			continue
		}
		go t.handleConn(conn)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		msg, err := ReadMessage(conn)
		if err != nil {
			return
		}
		t.msgChan <- msg
	}
}

func (t *TCPTransport) Send(targetAddr string, msg Message) error {
	t.mu.Lock()
	conn, ok := t.conns[targetAddr]
	t.mu.Unlock()

	var err error
	if !ok {
		conn, err = net.DialTimeout("tcp", targetAddr, 2*time.Second)
		if err != nil {
			return err
		}
		t.mu.Lock()
		t.conns[targetAddr] = conn
		t.mu.Unlock()
	}

	err = WriteMessage(conn, msg)
	if err != nil {
		// Connection might be dead, close it and remove from pool
		conn.Close()
		t.mu.Lock()
		delete(t.conns, targetAddr)
		t.mu.Unlock()
		return fmt.Errorf("failed to send message: %v", err)
	}
	return nil
}

func (t *TCPTransport) Receive() <-chan Message {
	return t.msgChan
}

func (t *TCPTransport) Close() error {
	t.mu.Lock()
	t.closing = true
	for _, conn := range t.conns {
		conn.Close()
	}
	t.mu.Unlock()
	return t.listener.Close()
}
