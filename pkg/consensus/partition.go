package consensus

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/raft"
)

// ErrNetworkPartitioned is returned by PartitionableTransport while isolated.
var ErrNetworkPartitioned = errors.New("network partitioned")

// PartitionableTransport wraps a raft.Transport to simulate bidirectional network partitions.
type PartitionableTransport struct {
	inner       raft.Transport
	partitioned atomic.Bool
	consumerCh  chan raft.RPC
	stopCh      chan struct{}
	closeOnce   sync.Once
}

// NewPartitionableTransport wraps inner and starts forwarding inbound RPCs when unpartitioned.
func NewPartitionableTransport(inner raft.Transport) *PartitionableTransport {
	p := &PartitionableTransport{
		inner:      inner,
		consumerCh: make(chan raft.RPC),
		stopCh:     make(chan struct{}),
	}
	go p.forwardConsumer()
	return p
}

// SetPartitioned toggles bidirectional Raft RPC isolation for this node.
func (p *PartitionableTransport) SetPartitioned(partitioned bool) {
	p.partitioned.Store(partitioned)
}

// IsPartitioned reports whether this node's Raft transport is currently isolated.
func (p *PartitionableTransport) IsPartitioned() bool {
	return p.partitioned.Load()
}

func (p *PartitionableTransport) forwardConsumer() {
	innerCh := p.inner.Consumer()
	for {
		select {
		case <-p.stopCh:
			return
		case rpc, ok := <-innerCh:
			if !ok {
				return
			}
			if p.IsPartitioned() {
				rpc.Respond(nil, ErrNetworkPartitioned)
				continue
			}
			select {
			case p.consumerCh <- rpc:
			case <-p.stopCh:
				return
			}
		}
	}
}

func (p *PartitionableTransport) Consumer() <-chan raft.RPC {
	return p.consumerCh
}

func (p *PartitionableTransport) LocalAddr() raft.ServerAddress {
	return p.inner.LocalAddr()
}

func (p *PartitionableTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	if p.IsPartitioned() {
		return nil, ErrNetworkPartitioned
	}
	pipe, err := p.inner.AppendEntriesPipeline(id, target)
	if err != nil {
		return nil, err
	}
	return &partitionablePipeline{inner: pipe, parent: p}, nil
}

func (p *PartitionableTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	if p.IsPartitioned() {
		return ErrNetworkPartitioned
	}
	return p.inner.AppendEntries(id, target, args, resp)
}

func (p *PartitionableTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	if p.IsPartitioned() {
		return ErrNetworkPartitioned
	}
	return p.inner.RequestVote(id, target, args, resp)
}

func (p *PartitionableTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	if p.IsPartitioned() {
		return ErrNetworkPartitioned
	}
	return p.inner.InstallSnapshot(id, target, args, resp, data)
}

func (p *PartitionableTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return p.inner.EncodePeer(id, addr)
}

func (p *PartitionableTransport) DecodePeer(buf []byte) raft.ServerAddress {
	return p.inner.DecodePeer(buf)
}

func (p *PartitionableTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	if cb == nil {
		p.inner.SetHeartbeatHandler(nil)
		return
	}
	p.inner.SetHeartbeatHandler(func(rpc raft.RPC) {
		if p.IsPartitioned() {
			rpc.Respond(nil, ErrNetworkPartitioned)
			return
		}
		cb(rpc)
	})
}

func (p *PartitionableTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	if p.IsPartitioned() {
		return ErrNetworkPartitioned
	}
	return p.inner.TimeoutNow(id, target, args, resp)
}

// Close stops the consumer forwarder and closes the underlying transport if supported.
func (p *PartitionableTransport) Close() error {
	p.closeOnce.Do(func() {
		close(p.stopCh)
	})
	if closer, ok := p.inner.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

type partitionablePipeline struct {
	inner  raft.AppendPipeline
	parent *PartitionableTransport
}

func (p *partitionablePipeline) AppendEntries(args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	if p.parent.IsPartitioned() {
		return nil, ErrNetworkPartitioned
	}
	return p.inner.AppendEntries(args, resp)
}

func (p *partitionablePipeline) Consumer() <-chan raft.AppendFuture {
	return p.inner.Consumer()
}

func (p *partitionablePipeline) Close() error {
	return p.inner.Close()
}
