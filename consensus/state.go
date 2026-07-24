// Package consensus implements the Raft consensus algorithm for quorumkv.
//
// The core is a **deterministic driven state machine** (planning/phase-06 §1):
// [Node] owns no timers, no goroutines and no I/O. A driver advances logical
// time with [Node.Tick], delivers inbound messages with [Node.Step], and then
// asks [Node.Ready] what must happen next. See [Driver] for the loop.
//
// Nothing here knows about the LSM storage engine; commands are opaque bytes
// until the Phase 10 seam ([StateMachine]).
package consensus

import "fmt"

// None is the "no node" sentinel — VotedFor when no vote has been cast, and the
// recipient of a message with no specific destination.
const None uint64 = 0

// Role is a node's current Raft role.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("role(%d)", uint8(r))
	}
}

// Entry is one Raft log entry. Cmd is **opaque** — Raft never parses it (§5f);
// interpreting it is the state machine's job, wired up in Phase 10.
type Entry struct {
	Term  uint64
	Index uint64
	Cmd   []byte
}

// IsNoOp reports whether e is a leader's election no-op (§5c): a real, replicated,
// committed log entry that carries no command and is never handed to the state
// machine. It exists so a new leader can learn its own commitIndex safely.
func (e Entry) IsNoOp() bool { return len(e.Cmd) == 0 }

// HardState is the state Raft Figure 2 requires on stable storage **before
// responding to any RPC**.
//
// Note what is absent: commitIndex and lastApplied are volatile by decision
// (§6). They rebuild from 0 on restart, so committed entries are re-applied —
// safe only because Apply is idempotent for quorumkv's PUT/DELETE. Phase 10
// must re-verify that against the real engine.
type HardState struct {
	Term     uint64
	VotedFor uint64
}

// MessageType discriminates the Raft RPCs. Phase 6 exchanges no messages (there
// are no peers), but the vote types are handled for real so that Phase 7 adds
// only a transport — the state machine does not change.
type MessageType uint8

const (
	MsgVoteReq MessageType = iota + 1
	MsgVoteResp
	MsgAppReq  // heartbeats: Phase 7. Entries: Phase 8.
	MsgAppResp // Phase 7/8.
	MsgSnap    // InstallSnapshot (Phase 9): sent when nextIndex falls below what the log still holds.
)

func (t MessageType) String() string {
	switch t {
	case MsgVoteReq:
		return "vote-req"
	case MsgVoteResp:
		return "vote-resp"
	case MsgAppReq:
		return "app-req"
	case MsgAppResp:
		return "app-resp"
	case MsgSnap:
		return "snap"
	default:
		return fmt.Sprintf("msg(%d)", uint8(t))
	}
}

// Message is one Raft RPC or its reply, as a plain value. Keeping the wire
// vocabulary as data (rather than method calls) is what lets a test drive a
// whole cluster in one goroutine and lets Phase 12 replay a failing run exactly.
type Message struct {
	Type MessageType
	From uint64
	To   uint64
	Term uint64

	// RequestVote.
	LastLogIndex uint64
	LastLogTerm  uint64

	// AppendEntries (Phase 7/8).
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64

	// Replies.
	Granted    bool   // MsgVoteResp
	Success    bool   // MsgAppResp
	MatchIndex uint64 // MsgAppResp

	// Rejection hint (Raft §5.3's optimization, Phase 8 §1). A follower that
	// rejects an AppendEntries reports where the leader should resume, so the
	// leader skips a whole run of a conflicting term in one hop instead of
	// backing up one index per round trip.
	//
	// ConflictTerm == 0 means "my log is simply too short"; ConflictIndex is
	// then the follower's lastIndex+1. Otherwise ConflictTerm is the term the
	// follower holds at prevLogIndex and ConflictIndex is the first index it
	// holds for that term.
	//
	// Both zero means "no hint" — the leader falls back to the paper's linear
	// decrement, which is always correct. Correctness never depends on the hint.
	ConflictIndex uint64
	ConflictTerm  uint64

	// InstallSnapshot (Phase 9 §4). SnapshotData is opaque to Raft — it is
	// whatever the state machine's Snapshot() produced. The ack reuses
	// MsgAppResp{Success, MatchIndex} above; no new reply fields are needed.
	SnapshotIndex uint64
	SnapshotTerm  uint64
	SnapshotData  []byte
}

// Snapshot is a point-in-time capture of applied state. Index/Term are Raft's;
// Data is the state machine's opaque blob — Raft never looks inside it, the
// same rule as Entry.Cmd (§1).
type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// Ready is everything the driver must do before the node may take another step.
//
// The order is the contract (§2, extended by Phase 8 §3 and Phase 9 §4), and
// it is not negotiable:
//
//	persist Snapshot → fsync → compact log file → (restore SM)   (Phase 9)
//	                                             →  truncate TruncateFrom  (Phase 8)
//	                                             →  persist HardState + EntriesToPersist  →  fsync
//	                                             →  send Messages
//	                                             →  apply CommittedEntries
//	                                             →  Advance()
//
// A received snapshot goes first because it supersedes everything older.
// Sending before the fsync can promise a vote the node forgets across a crash;
// applying before the fsync can expose data a future leader overwrites;
// appending before the truncate leaves a crash window exposing the wrong suffix.
type Ready struct {
	// HardState is non-nil when currentTerm or votedFor changed.
	HardState *HardState
	// Snapshot, when non-nil, must be persisted (SaveSnapshot) and the log file
	// compacted to it before anything else in this Ready — a received snapshot
	// is always authoritative over whatever it replaces (Phase 9 §4). Set only
	// by a received InstallSnapshot; a locally-taken snapshot is driver
	// housekeeping and never flows through Ready (§3).
	Snapshot *Snapshot
	// RestoreFromSnapshot reports whether the state machine must also be
	// rebuilt from Snapshot.Data (always true when Snapshot is set via Step).
	RestoreFromSnapshot bool
	// TruncateFrom, when non-nil, must be durably applied to stable storage
	// **before** EntriesToPersist: discard every entry at or after this index.
	// Set only when a follower found a genuine conflict (Phase 8 §2).
	TruncateFrom *uint64
	// EntriesToPersist are new log entries, contiguous and ascending.
	EntriesToPersist []Entry
	// Messages must be sent only after the above is durable. Empty in Phase 6.
	Messages []Message
	// CommittedEntries are ready for the state machine, in index order.
	CommittedEntries []Entry
}

// IsEmpty reports whether there is nothing for the driver to do.
func (r Ready) IsEmpty() bool {
	return r.HardState == nil &&
		r.Snapshot == nil &&
		r.TruncateFrom == nil &&
		len(r.EntriesToPersist) == 0 &&
		len(r.Messages) == 0 &&
		len(r.CommittedEntries) == 0
}

// StateMachine consumes committed commands and, from Phase 9, captures and
// restores its own state for snapshotting. This interface *is* the Phase 10
// seam, declared four phases early: Phase 6 backs it with a slice, Phase 10
// swaps in the Rust LSM engine and nothing else in Track B changes.
//
// Apply must be idempotent — see the note on [HardState]. Snapshot and Restore
// keep Raft opaque to what the bytes mean (§1): the test SM serializes a
// slice, the LSM hands back its packaged SSTable set (phase-10 §6a).
//
// All three methods are fallible (phase-10 §2): Phase 6-9's stubs could never
// fail, so the interface didn't need to say what happens when they do. A real
// engine can — a sidecar unreachable, a full disk, a corrupt snapshot blob —
// and every other durable operation in [Driver.run] already treats failure as
// fatal and bubbles it (SaveHardState, AppendEntries, TruncateFrom,
// SaveSnapshot, CompactLog). These three were the only ones that couldn't
// join that discipline; now they do. A failed Apply must never be silently
// dropped — that would lose a majority-committed write — so it surfaces the
// same way a failed fsync does: loudly, stopping the node.
type StateMachine interface {
	Apply(cmd []byte) error
	// Snapshot captures applied state as an opaque blob, for the driver to
	// persist and, later, ship to a lagging follower via InstallSnapshot.
	Snapshot() ([]byte, error)
	// Restore replaces the state machine's state with a snapshot previously
	// returned by Snapshot — either this node's own or one installed from a
	// leader.
	Restore(data []byte) error
}

// Transport delivers outbound messages to peers. Phase 6 has no peers, so a nil
// Transport (messages dropped) is correct; Phase 7 plugs gRPC in here.
type Transport interface {
	Send(msgs []Message)
}
