package consensus

import (
	"fmt"
	"sync"
)

// Sandbox is a small, introspectable Raft cluster meant for interactive
// exploration — the engine behind the test dashboard's live view. It is not
// part of Raft itself; it exists purely to let a human drive a real cluster
// by hand (propose, tick, crash, restart, partition) and see what happens,
// reusing the exact same Node/Driver/Bus code every test in this package
// exercises. Safe for concurrent use — every method takes an internal lock,
// since an HTTP server (its only caller today) handles requests concurrently.
//
// Sandbox is deliberately StateMachine-agnostic (SandboxConfig.NewStateMachine):
// it cannot import consensus/engine to wire up the real Rust sidecar itself —
// that package imports consensus for [StateMachine] and [Driver], so consensus
// importing it back would be a cycle. This is the same "consensus stays
// ignorant of the LSM" rule as everywhere else (consensus/state.go's own
// package doc), just enforced by the compiler instead of by convention. The
// caller that *can* see both packages (consensus/cmd/dashboard-backend) is
// where a real per-node sidecar process gets spawned and wired in.
type Sandbox struct {
	mu      sync.Mutex
	bus     *Bus
	ids     []uint64
	cfgs    map[uint64]Config
	newSM   func(id uint64) (StateMachine, error)
	drivers map[uint64]*Driver
	down    map[uint64]bool // crashed: stopped ticking, bus-isolated

	traceMu  sync.Mutex
	traceSeq int
	trace    []TraceEvent
}

// SandboxConfig configures a fresh Sandbox. Zero values fall back to the same
// defaults NewNode/NewDriver already use, except Nodes (defaults to 3).
type SandboxConfig struct {
	Nodes             int
	ElectionTimeout   int
	HeartbeatTimeout  int
	SnapshotThreshold int
	Seed              int64
	// NewStateMachine builds the StateMachine for node id — called once at
	// construction and again on every Restart. nil defaults to an in-memory
	// stub (sandboxSM) that just remembers strings, for a plain demo/test
	// with no real engine behind it.
	NewStateMachine func(id uint64) (StateMachine, error)
}

// sandboxSM is the Sandbox's StateMachine: it just remembers what it was
// told, the same shape as the test recorder. All access happens through
// Sandbox methods, which already hold sb.mu, so it needs no lock of its own.
type sandboxSM struct{ applied [][]byte }

func (s *sandboxSM) Apply(cmd []byte) error {
	s.applied = append(s.applied, append([]byte(nil), cmd...))
	return nil
}

func (s *sandboxSM) Snapshot() ([]byte, error) {
	var buf []byte
	for _, cmd := range s.applied {
		buf = append(buf, byte(len(cmd)), byte(len(cmd)>>8), byte(len(cmd)>>16), byte(len(cmd)>>24))
		buf = append(buf, cmd...)
	}
	return buf, nil
}

func (s *sandboxSM) Restore(data []byte) error {
	applied := make([][]byte, 0)
	for len(data) >= 4 {
		n := int(data[0]) | int(data[1])<<8 | int(data[2])<<16 | int(data[3])<<24
		data = data[4:]
		if len(data) < n {
			break
		}
		applied = append(applied, append([]byte(nil), data[:n]...))
		data = data[n:]
	}
	s.applied = applied
	return nil
}

// TraceEvent is one message actually delivered on the bus, recorded for the
// dashboard's "what's happening" timeline.
type TraceEvent struct {
	Seq    int    `json:"seq"`
	Type   string `json:"type"`
	From   uint64 `json:"from"`
	To     uint64 `json:"to"`
	Term   uint64 `json:"term"`
	Detail string `json:"detail"`
}

// SandboxEntry is one log entry as reported to the dashboard.
//
// Cmd is the raw command bytes — a []byte rather than a string deliberately:
// once real engine.Command encoding flows through here, the bytes are binary,
// and Go's encoding/json would silently mangle a string field holding
// non-UTF8 bytes (each invalid byte becomes U+FFFD, irreversibly). []byte
// gets Go's built-in base64 JSON encoding instead, which round-trips any
// content intact. A caller that knows the shape (consensus/engine) decodes
// it further for display; Sandbox itself stays agnostic.
type SandboxEntry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Cmd   []byte `json:"cmd"`
	NoOp  bool   `json:"noOp"`
}

// SandboxNodeState is one node's full observable Raft-level state. Engine
// data (what a node's state machine actually holds) is deliberately not
// here — Sandbox is StateMachine-agnostic, and reads bypass Raft entirely
// anyway (phase-10 §4); a caller that wired up a real engine reads it
// directly, in parallel with Sandbox, not through this struct.
type SandboxNodeState struct {
	ID          uint64         `json:"id"`
	Role        string         `json:"role"`
	Term        uint64         `json:"term"`
	LeaderID    uint64         `json:"leaderId"`
	CommitIndex uint64         `json:"commitIndex"`
	LastApplied uint64         `json:"lastApplied"`
	LastIndex   uint64         `json:"lastIndex"`
	Offset      uint64         `json:"offset"` // the snapshot boundary, 0 if none yet
	Down        bool           `json:"down"`
	Isolated    bool           `json:"isolated"`
	Entries     []SandboxEntry `json:"entries"`
}

// SandboxState is the whole cluster's observable state plus the recent
// message trace, as returned after every action.
type SandboxState struct {
	Nodes []SandboxNodeState `json:"nodes"`
	Trace []TraceEvent       `json:"trace"`
}

const sandboxTraceLimit = 300

// NewSandbox builds a fresh N-node cluster over an in-memory Bus, all using
// MemStorage (a "restart" reloads from the same in-memory instance, the same
// trick the test cluster's restart() helper uses to simulate a process
// coming back with its disk intact).
func NewSandbox(cfg SandboxConfig) (*Sandbox, error) {
	if cfg.Nodes <= 0 {
		cfg.Nodes = 3
	}
	if cfg.ElectionTimeout <= 0 {
		cfg.ElectionTimeout = DefaultElectionTimeout
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = DefaultHeartbeatTimeout
	}

	newSM := cfg.NewStateMachine
	if newSM == nil {
		newSM = func(uint64) (StateMachine, error) { return &sandboxSM{}, nil }
	}

	peers := make([]uint64, cfg.Nodes)
	for i := range peers {
		peers[i] = uint64(i + 1)
	}

	sb := &Sandbox{
		bus:     NewBus(),
		cfgs:    make(map[uint64]Config, cfg.Nodes),
		newSM:   newSM,
		drivers: make(map[uint64]*Driver, cfg.Nodes),
		down:    make(map[uint64]bool, cfg.Nodes),
	}
	sb.bus.OnMessage = sb.recordTrace

	for _, id := range peers {
		nodeCfg := Config{
			ID:                id,
			Peers:             peers,
			ElectionTimeout:   cfg.ElectionTimeout,
			HeartbeatTimeout:  cfg.HeartbeatTimeout,
			Storage:           NewMemStorage(),
			Seed:              cfg.Seed,
			SnapshotThreshold: cfg.SnapshotThreshold,
		}
		sm, err := newSM(id)
		if err != nil {
			return nil, fmt.Errorf("consensus: sandbox node %d: build state machine: %w", id, err)
		}
		d, err := NewDriver(nodeCfg, sm, sb.bus.Transport(id))
		if err != nil {
			return nil, fmt.Errorf("consensus: sandbox node %d: %w", id, err)
		}
		sb.ids = append(sb.ids, id)
		sb.cfgs[id] = nodeCfg
		sb.drivers[id] = d
	}
	return sb, nil
}

func (sb *Sandbox) recordTrace(m Message) {
	sb.traceMu.Lock()
	defer sb.traceMu.Unlock()
	sb.traceSeq++
	sb.trace = append(sb.trace, TraceEvent{
		Seq: sb.traceSeq, Type: m.Type.String(), From: m.From, To: m.To, Term: m.Term,
		Detail: summarizeMessage(m),
	})
	if len(sb.trace) > sandboxTraceLimit {
		sb.trace = sb.trace[len(sb.trace)-sandboxTraceLimit:]
	}
}

func summarizeMessage(m Message) string {
	switch m.Type {
	case MsgVoteReq:
		return fmt.Sprintf("RequestVote lastLogIndex=%d lastLogTerm=%d", m.LastLogIndex, m.LastLogTerm)
	case MsgVoteResp:
		return fmt.Sprintf("granted=%v", m.Granted)
	case MsgAppReq:
		if len(m.Entries) == 0 {
			return fmt.Sprintf("heartbeat prevLogIndex=%d prevLogTerm=%d leaderCommit=%d", m.PrevLogIndex, m.PrevLogTerm, m.LeaderCommit)
		}
		return fmt.Sprintf("AppendEntries prevLogIndex=%d prevLogTerm=%d entries=%d leaderCommit=%d", m.PrevLogIndex, m.PrevLogTerm, len(m.Entries), m.LeaderCommit)
	case MsgAppResp:
		if m.Success {
			return fmt.Sprintf("success matchIndex=%d", m.MatchIndex)
		}
		return fmt.Sprintf("rejected conflictIndex=%d conflictTerm=%d", m.ConflictIndex, m.ConflictTerm)
	case MsgSnap:
		return fmt.Sprintf("InstallSnapshot snapshotIndex=%d snapshotTerm=%d bytes=%d", m.SnapshotIndex, m.SnapshotTerm, len(m.SnapshotData))
	default:
		return ""
	}
}

// pumpLocked drains the bus to quiescence, the same fixed-point loop the test
// cluster's route() helper uses — except a sandbox never fails a test, it
// just stops after enough rounds (a real partition, for instance, means the
// bus legitimately never empties).
func (sb *Sandbox) pumpLocked() {
	for round := 0; round < 100; round++ {
		if sb.bus.Pending() == 0 {
			return
		}
		for _, id := range sb.ids {
			msgs := sb.bus.Take(id)
			if sb.down[id] {
				continue // a crashed node drops what was in flight to it
			}
			for _, m := range msgs {
				_ = sb.drivers[id].Step(m) // best-effort: this is a toy, not a test
			}
		}
	}
}

// Propose submits cmd on node id (must currently be that node's leader) and
// lets the resulting AppendEntries traffic settle.
func (sb *Sandbox) Propose(id uint64, cmd string) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	d, ok := sb.drivers[id]
	if !ok {
		return fmt.Errorf("no such node %d", id)
	}
	if err := d.Propose([]byte(cmd)); err != nil {
		return err
	}
	sb.pumpLocked()
	return nil
}

// Tick advances every live (non-crashed) node's logical clock by n steps,
// settling messages after each one.
func (sb *Sandbox) Tick(n int) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	if n <= 0 {
		n = 1
	}
	for i := 0; i < n; i++ {
		for _, id := range sb.ids {
			if sb.down[id] {
				continue
			}
			sb.drivers[id].Tick()
		}
		sb.pumpLocked()
	}
}

// Crash simulates `kill -9`: the node stops ticking entirely and the bus
// drops anything in flight to or from it — not a partition, a process death.
func (sb *Sandbox) Crash(id uint64) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	if _, ok := sb.drivers[id]; !ok {
		return fmt.Errorf("no such node %d", id)
	}
	sb.down[id] = true
	sb.bus.Isolate(id)
	return nil
}

// Restart brings a crashed node back from its persisted (in-memory) storage —
// the same instance its Config already points at, so this reloads exactly
// what a real process restart would find on disk.
func (sb *Sandbox) Restart(id uint64) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	cfg, ok := sb.cfgs[id]
	if !ok {
		return fmt.Errorf("no such node %d", id)
	}
	// Rebuilt via the factory, not reused — a real engine's StateMachine may
	// need to point at a freshly-respawned sidecar with a different address,
	// which only the caller managing that sidecar's lifecycle knows about.
	sm, err := sb.newSM(id)
	if err != nil {
		return fmt.Errorf("consensus: sandbox node %d: build state machine: %w", id, err)
	}
	d, err := NewDriver(cfg, sm, sb.bus.Transport(id))
	if err != nil {
		return err
	}
	sb.drivers[id] = d
	sb.down[id] = false
	sb.bus.Heal(id)
	sb.pumpLocked()
	return nil
}

// Isolate partitions node id away without killing it: it keeps ticking (and
// may time out and campaign against a leader it can no longer hear), but no
// message crosses in either direction.
func (sb *Sandbox) Isolate(id uint64) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	if _, ok := sb.drivers[id]; !ok {
		return fmt.Errorf("no such node %d", id)
	}
	sb.bus.Isolate(id)
	return nil
}

// Heal reconnects a node isolated by Isolate (or Crash — but a crashed node
// also needs Restart to start ticking again).
func (sb *Sandbox) Heal(id uint64) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	if _, ok := sb.drivers[id]; !ok {
		return fmt.Errorf("no such node %d", id)
	}
	sb.bus.Heal(id)
	sb.pumpLocked()
	return nil
}

// State reports every node's full observable state plus the recent message
// trace.
func (sb *Sandbox) State() SandboxState {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	nodes := make([]SandboxNodeState, 0, len(sb.ids))
	for _, id := range sb.ids {
		n := sb.drivers[id].Node()
		log := n.Log()
		real := log.Entries()

		entries := make([]SandboxEntry, 0, len(real))
		for _, e := range real {
			entries = append(entries, SandboxEntry{Index: e.Index, Term: e.Term, Cmd: e.Cmd, NoOp: e.IsNoOp()})
		}

		nodes = append(nodes, SandboxNodeState{
			ID:          id,
			Role:        n.Role().String(),
			Term:        n.Term(),
			LeaderID:    n.LeaderID(),
			CommitIndex: n.CommitIndex(),
			LastApplied: n.LastApplied(),
			LastIndex:   n.LastIndex(),
			Offset:      log.Offset(),
			Down:        sb.down[id],
			Isolated:    sb.bus.IsIsolated(id),
			Entries:     entries,
		})
	}

	sb.traceMu.Lock()
	trace := make([]TraceEvent, len(sb.trace))
	copy(trace, sb.trace)
	sb.traceMu.Unlock()

	return SandboxState{Nodes: nodes, Trace: trace}
}
