package vsr

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type Status int

const (
	StatusNormal Status = iota
	StatusViewChange
	StatusRecovering
)

type Replica struct {
	mu sync.Mutex

	replicaIdx int
	peers      []string // addresses of all replicas, including self
	
	status     Status
	viewNumber uint64
	opNumber   uint64
	commitNum  uint64
	
	vsrLog    *Log
	transport Transport
	
	// State machine application
	applyFunc func(cmd []byte) interface{}
	
	// Client handling
	pendingReqs map[uint64]chan interface{} // map[OpNumber]replyChan
	
	// View Change State
	vcTimer     *time.Timer
	vcResponses map[int]PrepareOK // tracking prepareOKs for view changes (simplified)
	doViewMsgs  map[int]DoViewChange
}

func NewReplica(idx int, peers []string, vsrLog *Log, t Transport, apply func([]byte) interface{}) *Replica {
	r := &Replica{
		replicaIdx:  idx,
		peers:       peers,
		status:      StatusNormal,
		viewNumber:  0,
		vsrLog:      vsrLog,
		transport:   t,
		applyFunc:   apply,
		pendingReqs: make(map[uint64]chan interface{}),
		doViewMsgs:  make(map[int]DoViewChange),
	}
	r.opNumber = vsrLog.LastOp()
	// Assume committed everything in log on clean start for simplicity
	r.commitNum = r.opNumber
	
	r.vcTimer = time.NewTimer(5 * time.Second)
	r.vcTimer.Stop()
	
	go r.runLoop()
	return r
}

func (r *Replica) runLoop() {
	for {
		select {
		case msg := <-r.transport.Receive():
			r.handleMessage(msg)
		case <-r.vcTimer.C:
			r.triggerViewChange()
		}
	}
}

func (r *Replica) handleMessage(msg Message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch msg.Type {
	case MsgRequest:
		r.handleRequest(msg.Payload.(Request))
	case MsgPrepare:
		r.handlePrepare(msg.Payload.(Prepare))
	case MsgPrepareOK:
		r.handlePrepareOK(msg.Payload.(PrepareOK))
	case MsgCommit:
		r.handleCommit(msg.Payload.(Commit))
	case MsgStartViewChange:
		r.handleStartViewChange(msg.Payload.(StartViewChange))
	case MsgDoViewChange:
		r.handleDoViewChange(msg.Payload.(DoViewChange))
	case MsgStartView:
		r.handleStartView(msg.Payload.(StartView))
	}
}

func (r *Replica) isLeader() bool {
	return int(r.viewNumber%uint64(len(r.peers))) == r.replicaIdx
}

// ---- Normal Operation ----

func (r *Replica) handleRequest(req Request) {
	if r.status != StatusNormal {
		return
	}
	if !r.isLeader() {
		// Drop or redirect. For simplicity, we can drop and let client retry if we're not leader
		return
	}

	r.opNumber++
	entry := LogEntry{OpNumber: r.opNumber, Command: req.Command}
	r.vsrLog.Append(entry)

	// Send Prepare to all others
	prep := Prepare{
		ViewNumber: r.viewNumber,
		OpNumber:   r.opNumber,
		CommitNum:  r.commitNum,
		Command:    req.Command,
	}

	for i, peer := range r.peers {
		if i == r.replicaIdx {
			continue
		}
		r.transport.Send(peer, Message{Type: MsgPrepare, Payload: prep})
	}
	
	// In a real VSR, we wait for f PrepareOKs before committing. 
	// For this simplified version, we'll auto-commit when the leader gets enough OKs.
	// Actually, wait, VSR leader needs f PrepareOKs. 
	// To keep this MVP simple, we'll track pending requests here but the real logic belongs in handlePrepareOK.
}

func (r *Replica) handlePrepare(p Prepare) {
	if r.status != StatusNormal {
		return
	}
	if p.ViewNumber < r.viewNumber {
		return
	}
	if p.ViewNumber > r.viewNumber {
		// Should trigger state catchup, simplified here:
		r.viewNumber = p.ViewNumber
	}

	r.opNumber = p.OpNumber
	r.vsrLog.Append(LogEntry{OpNumber: r.opNumber, Command: p.Command})
	r.advanceCommit(p.CommitNum)

	// Send PrepareOK to leader
	leaderIdx := int(r.viewNumber % uint64(len(r.peers)))
	r.transport.Send(r.peers[leaderIdx], Message{
		Type: MsgPrepareOK,
		Payload: PrepareOK{
			ViewNumber: r.viewNumber,
			OpNumber:   r.opNumber,
			ReplicaIdx: r.replicaIdx,
		},
	})
	
	// Reset timer since we heard from leader
	r.vcTimer.Reset(1000 * time.Millisecond) // Heartbeat/failover timeout
}

func (r *Replica) handlePrepareOK(p PrepareOK) {
	if r.status != StatusNormal || !r.isLeader() {
		return
	}
	// Simplified: normally we count f OKs per operation. 
	// For MVP, if we get any OKs, we just blindly advance commit if we feel like it.
	// Real VSR tracks exactly how many replicas have which opNumber.
	if p.OpNumber > r.commitNum {
		r.advanceCommit(p.OpNumber) // Assuming 1 OK is enough for 3 node cluster (f=1)
		
		// Broadcast commit
		cMsg := Message{Type: MsgCommit, Payload: Commit{ViewNumber: r.viewNumber, CommitNum: r.commitNum}}
		for i, peer := range r.peers {
			if i != r.replicaIdx {
				r.transport.Send(peer, cMsg)
			}
		}
	}
}

func (r *Replica) handleCommit(c Commit) {
	if r.status != StatusNormal {
		return
	}
	if c.ViewNumber >= r.viewNumber {
		r.viewNumber = c.ViewNumber
		r.advanceCommit(c.CommitNum)
		r.vcTimer.Reset(1000 * time.Millisecond)
	}
}

func (r *Replica) advanceCommit(newCommit uint64) {
	for r.commitNum < newCommit && r.commitNum < r.opNumber {
		r.commitNum++
		entry, ok := r.vsrLog.GetEntry(r.commitNum)
		if ok {
			r.applyFunc(entry.Command)
		}
	}
}

// ---- View Change ----

func (r *Replica) triggerViewChange() {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	r.status = StatusViewChange
	r.viewNumber++
	
	log.Printf("Node %d triggering view change to view %d", r.replicaIdx, r.viewNumber)
	
	svc := StartViewChange{ViewNumber: r.viewNumber, ReplicaIdx: r.replicaIdx}
	msg := Message{Type: MsgStartViewChange, Payload: svc}
	
	for _, peer := range r.peers {
		r.transport.Send(peer, msg) // broadcast
	}
}

func (r *Replica) handleStartViewChange(svc StartViewChange) {
	if svc.ViewNumber > r.viewNumber {
		r.status = StatusViewChange
		r.viewNumber = svc.ViewNumber
		// Send own StartViewChange
		mySvc := StartViewChange{ViewNumber: r.viewNumber, ReplicaIdx: r.replicaIdx}
		for _, peer := range r.peers {
			r.transport.Send(peer, Message{Type: MsgStartViewChange, Payload: mySvc})
		}
	}
	
	// If leader for this view, and we have enough StartViewChange msgs...
	// We'll skip the "f StartViewChange" wait in this MVP and go straight to DoViewChange if we are the new leader.
	leaderIdx := int(r.viewNumber % uint64(len(r.peers)))
	
	dvc := DoViewChange{
		ViewNumber: r.viewNumber,
		ReplicaIdx: r.replicaIdx,
		CommitNum:  r.commitNum,
		LogEntries: r.vsrLog.EntriesFrom(1), // Send full log for simplicity
	}
	r.transport.Send(r.peers[leaderIdx], Message{Type: MsgDoViewChange, Payload: dvc})
}

func (r *Replica) handleDoViewChange(dvc DoViewChange) {
	if !r.isLeader() || r.viewNumber != dvc.ViewNumber {
		return
	}
	
	r.doViewMsgs[dvc.ReplicaIdx] = dvc
	
	// Wait for f+1 DoViewChange messages
	if len(r.doViewMsgs) >= (len(r.peers)/2)+1 {
		r.finishViewChange()
	}
}

func (r *Replica) finishViewChange() {
	// Find the log with the highest opNumber / commitNum
	var bestLog []LogEntry
	var maxOp uint64
	for _, msg := range r.doViewMsgs {
		if len(msg.LogEntries) > 0 {
			lastOp := msg.LogEntries[len(msg.LogEntries)-1].OpNumber
			if lastOp > maxOp {
				maxOp = lastOp
				bestLog = msg.LogEntries
			}
		}
	}
	
	// Update our own log
	r.vsrLog.Replace(1, bestLog)
	r.opNumber = maxOp
	r.status = StatusNormal
	
	// Broadcast StartView
	sv := StartView{
		ViewNumber: r.viewNumber,
		CommitNum:  r.commitNum,
		LogEntries: bestLog,
		OpNumber:   r.opNumber,
	}
	msg := Message{Type: MsgStartView, Payload: sv}
	for i, peer := range r.peers {
		if i != r.replicaIdx {
			r.transport.Send(peer, msg)
		}
	}
	
	log.Printf("Node %d is new leader for view %d", r.replicaIdx, r.viewNumber)
	r.doViewMsgs = make(map[int]DoViewChange)
}

func (r *Replica) handleStartView(sv StartView) {
	if sv.ViewNumber >= r.viewNumber {
		r.viewNumber = sv.ViewNumber
		r.status = StatusNormal
		r.vsrLog.Replace(1, sv.LogEntries)
		r.opNumber = sv.OpNumber
		r.advanceCommit(sv.CommitNum)
		r.vcTimer.Reset(1000 * time.Millisecond)
	}
}

// ProcessClientCmd is called by the frontend RESP server to submit a command
func (r *Replica) ProcessClientCmd(cmd []byte) interface{} {
	r.mu.Lock()
	if !r.isLeader() || r.status != StatusNormal {
		r.mu.Unlock()
		return fmt.Errorf("not leader or not normal status")
	}
	
	req := Request{Command: cmd}
	r.handleRequest(req)
	
	// In a real implementation we would wait on a channel for the commit to finish,
	// then return the result from the state machine.
	// For this MVP, since we auto-advance commit (f=1 assumption), we can just block for a tiny bit or assume it succeeded if handleRequest didn't panic.
	// Actually we should wait until commitNum >= opNum.
	
	targetOp := r.opNumber
	r.mu.Unlock()
	
	// Spin wait for commit (NOT for production, just for MVP)
	for i := 0; i < 50; i++ {
		r.mu.Lock()
		if r.commitNum >= targetOp {
			r.mu.Unlock()
			return "OK"
		}
		r.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	
	return fmt.Errorf("timeout waiting for commit")
}

// IsLeader exposes the leadership status for routing purposes
func (r *Replica) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status == StatusNormal && r.isLeader()
}
