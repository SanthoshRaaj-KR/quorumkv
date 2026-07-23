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

	proposals chan proposal
	statusReq chan chan Status
	stop      chan struct{}
	done      chan struct{}
}

type proposal struct {
	cmd []byte
	err chan error
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
		driver:    d,
		inbound:   inbound,
		tick:      tick,
		proposals: make(chan proposal),
		statusReq: make(chan chan Status),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
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
		case m := <-s.inbound:
			_ = s.driver.Step(m)
		case p := <-s.proposals:
			p.err <- s.driver.Propose(p.cmd)
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
