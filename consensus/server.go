package consensus

import (
	"errors"
	"time"
)

// DefaultTick is the logical clock period (planning/phase-07 §1). With
// ElectionTimeout=15 the randomized timeout lands in [150ms, 300ms) — exactly
// the Raft paper's range — and HeartbeatTimeout=5 refreshes followers every
// 50ms, roughly three times per minimum election timeout.
const (
	DefaultTick             = 10 * time.Millisecond
	DefaultElectionTimeout  = 15
	DefaultHeartbeatTimeout = 5
)

// ErrStopped is returned when a call is made on a Server that is shutting down.
var ErrStopped = errors.New("consensus: server stopped")

// ErrProposalLost is returned by [Server.ProposeAndWait] when the proposed
// entry's index was reached by a *different* entry — this node lost
// leadership before the original proposal committed, and a later leader
// overwrote that index (Phase 8's divergence repair doing exactly its job).
// The write never went through under this node; the caller must retry,
// most likely against whoever the new leader turns out to be (phase-11 §3).
var ErrProposalLost = errors.New("consensus: proposal lost leadership before committing")

// ErrProposeTimeout is returned by [Server.ProposeAndWait] when the timeout
// elapses before the proposed entry is applied. Like any timeout in this
// project's transport layer, it is retryable — it says nothing about
// whether the write eventually lands.
var ErrProposeTimeout = errors.New("consensus: propose: timed out waiting for apply")

// Server is where real time lives.
//
// [Node] has no clock and no locks by design; something still has to turn
// wall-clock into Tick and serialize ticks against inbound messages. That is
// this type, and it is deliberately thin: **one goroutine owns the Driver**, so
// there is no mutex anywhere near Raft state. Tests that care about the
// algorithm skip Server entirely and drive Node by hand.
type Server struct {
	driver  *Driver
	inbound <-chan Message
	tick    time.Duration

	proposals   chan proposal
	proposeWait chan proposeWaitReq
	cancelWait  chan uint64
	statusReq   chan chan Status
	stop        chan struct{}
	done        chan struct{}

	// waiters holds one entry per index a ProposeAndWait caller is still
	// waiting on, keyed by the index assigned at propose time. Owned
	// exclusively by run() — the same single-goroutine rule as every other
	// piece of Raft state (Phase 7).
	waiters map[uint64]pendingWaiter
}

type proposal struct {
	cmd []byte
	err chan error
}

// proposeWaitReq asks run() to propose cmd and, on success, register a
// waiter for the resulting (index, term) instead of just returning.
type proposeWaitReq struct {
	cmd   []byte
	reply chan proposeWaitReply
}

// proposeWaitReply carries either an immediate error (e.g. [ErrNotLeader] —
// no waiter registered) or the assigned index plus a channel that fires
// once that index is resolved (applied under the same term, or lost).
type proposeWaitReply struct {
	index uint64
	done  chan error
	err   error
}

type pendingWaiter struct {
	term uint64
	done chan error
}

// Status is a point-in-time snapshot of a node, safe to read from any goroutine
// because it is produced *inside* the run loop.
type Status struct {
	ID          uint64
	Role        Role
	Term        uint64
	LeaderID    uint64
	CommitIndex uint64
	LastApplied uint64
	LastIndex   uint64
}

// NewServer builds a node and wires it to a transport. inbound is the stream the
// transport decodes into — for [TCPTransport], its Inbound channel.
func NewServer(cfg Config, sm StateMachine, tr Transport, inbound <-chan Message, tick time.Duration) (*Server, error) {
	d, err := NewDriver(cfg, sm, tr)
	if err != nil {
		return nil, err
	}
	if tick <= 0 {
		tick = DefaultTick
	}
	return &Server{
		driver:      d,
		inbound:     inbound,
		tick:        tick,
		proposals:   make(chan proposal),
		proposeWait: make(chan proposeWaitReq),
		cancelWait:  make(chan uint64),
		statusReq:   make(chan chan Status),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		waiters:     make(map[uint64]pendingWaiter),
	}, nil
}

// Start launches the run loop.
func (s *Server) Start() { go s.run() }

func (s *Server) run() {
	defer close(s.done)
	ticker := time.NewTicker(s.tick)
	defer ticker.Stop()

	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			_ = s.driver.Tick()
			s.resolveWaiters()
		case m := <-s.inbound:
			_ = s.driver.Step(m)
			s.resolveWaiters()
		case p := <-s.proposals:
			p.err <- s.driver.Propose(p.cmd)
			s.resolveWaiters()
		case req := <-s.proposeWait:
			if err := s.driver.Propose(req.cmd); err != nil {
				req.reply <- proposeWaitReply{err: err}
			} else {
				n := s.driver.Node()
				index := n.LastIndex()
				done := make(chan error, 1)
				s.waiters[index] = pendingWaiter{term: n.Term(), done: done}
				req.reply <- proposeWaitReply{index: index, done: done}
			}
			s.resolveWaiters()
		case index := <-s.cancelWait:
			delete(s.waiters, index)
		case reply := <-s.statusReq:
			n := s.driver.Node()
			reply <- Status{
				ID:          n.ID(),
				Role:        n.Role(),
				Term:        n.Term(),
				LeaderID:    n.LeaderID(),
				CommitIndex: n.CommitIndex(),
				LastApplied: n.LastApplied(),
				LastIndex:   n.LastIndex(),
			}
		}
	}
}

// resolveWaiters signals every pending ProposeAndWait waiter whose index has
// been reached by LastApplied. An index reached by an entry of a *different*
// term than the one the waiter proposed means this node lost leadership
// before that proposal committed and a later leader overwrote it —
// [ErrProposalLost], not success (phase-11 §3). Called from inside run()
// after everything that can move LastApplied, so it always sees a
// consistent snapshot of Node/Log — no locking needed (Phase 7's
// single-goroutine-owns-Raft-state rule).
func (s *Server) resolveWaiters() {
	if len(s.waiters) == 0 {
		return
	}
	n := s.driver.Node()
	applied := n.LastApplied()
	log := n.Log()
	for index, w := range s.waiters {
		if applied < index {
			continue
		}
		var err error
		if !log.Has(index) || log.Term(index) != w.term {
			err = ErrProposalLost
		}
		w.done <- err
		delete(s.waiters, index)
	}
}

// Propose submits a command. Returns [ErrNotLeader] on a non-leader and
// [ErrStopped] once the server is shutting down.
func (s *Server) Propose(cmd []byte) error {
	p := proposal{cmd: cmd, err: make(chan error, 1)}
	select {
	case s.proposals <- p:
		return <-p.err
	case <-s.stop:
		return ErrStopped
	}
}

// ProposeAndWait submits a command and blocks until it is actually durable —
// applied to the state machine under the term it was proposed in, not just
// appended to the leader's own log (phase-11 §3; DESIGN.md §5 step 5). This
// is additive to [Propose], which keeps its existing fire-and-forget
// behavior unchanged for every internal caller that doesn't need to wait.
//
// Returns the assigned log index on success. Returns [ErrNotLeader] if this
// node isn't leader, [ErrProposalLost] if it loses leadership before the
// entry commits, [ErrProposeTimeout] if timeout elapses first, and
// [ErrStopped] once the server is shutting down. All four are retryable —
// the caller (typically consensus/clientrpc) is expected to retry, possibly
// against a different node.
func (s *Server) ProposeAndWait(cmd []byte, timeout time.Duration) (uint64, error) {
	req := proposeWaitReq{cmd: cmd, reply: make(chan proposeWaitReply, 1)}
	select {
	case s.proposeWait <- req:
	case <-s.stop:
		return 0, ErrStopped
	}

	r := <-req.reply
	if r.err != nil {
		return 0, r.err
	}

	select {
	case err := <-r.done:
		return r.index, err
	case <-time.After(timeout):
		select {
		case s.cancelWait <- r.index:
		case <-s.stop:
		}
		return r.index, ErrProposeTimeout
	case <-s.stop:
		return r.index, ErrStopped
	}
}

// Status reads the node's state from inside the run loop, so it never races
// with Raft's own mutations.
func (s *Server) Status() (Status, error) {
	reply := make(chan Status, 1)
	select {
	case s.statusReq <- reply:
		return <-reply, nil
	case <-s.stop:
		return Status{}, ErrStopped
	}
}

// Stop halts the run loop and closes storage. It is safe to call more than once.
func (s *Server) Stop() error {
	select {
	case <-s.stop:
		return nil // already stopping
	default:
	}
	close(s.stop)
	<-s.done
	return s.driver.Close()
}
