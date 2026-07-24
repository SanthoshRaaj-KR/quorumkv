package engine

import (
	"fmt"

	"quorumkv/consensus"
)

// StateMachine adapts one node's local engine sidecar to
// [consensus.StateMachine] — the Phase 10 seam consensus/state.go's own
// package doc comment and Phase 6's interface were both built for.
type StateMachine struct {
	client *Client
}

var _ consensus.StateMachine = (*StateMachine)(nil)

// NewStateMachine builds a StateMachine bound to the sidecar at addr.
func NewStateMachine(addr string) *StateMachine {
	return &StateMachine{client: NewClient(addr)}
}

// Apply decodes cmd and applies it to the local engine. A failed apply
// (sidecar unreachable, disk full, ...) returns an error, which
// [consensus.Driver] treats as fatal and bubbles — never silently dropped
// (phase-10 §2).
func (sm *StateMachine) Apply(cmd []byte) error {
	c, err := DecodeCommand(cmd)
	if err != nil {
		return fmt.Errorf("engine: apply: %w", err)
	}
	switch c.Op {
	case OpPut:
		return sm.client.Put(c.Key, c.Value)
	case OpDelete:
		return sm.client.Delete(c.Key)
	default:
		return fmt.Errorf("engine: apply: unknown op %d", c.Op)
	}
}

// Snapshot packages the local engine's current SSTable set (phase-10 §6a).
func (sm *StateMachine) Snapshot() ([]byte, error) { return sm.client.Snapshot() }

// Restore replaces the local engine's state with a previously-captured
// snapshot, this node's own or an installed one.
func (sm *StateMachine) Restore(data []byte) error { return sm.client.Restore(data) }

// Get reads key directly from this node's local engine, bypassing Raft
// entirely — safe to call on any node, leader or follower; a follower's
// answer may lag the leader's by however long a heartbeat takes to arrive
// (DESIGN.md §5, phase-10 §4).
func (sm *StateMachine) Get(key []byte) ([]byte, bool, error) { return sm.client.Get(key) }

// Stats reports lightweight engine introspection (the sandbox UI), not used
// by Raft.
func (sm *StateMachine) Stats() (Stats, error) { return sm.client.Stats() }

// ProposePut encodes a PUT and proposes it — the encoding half of the seam
// Apply decodes on the other end.
func ProposePut(d *consensus.Driver, key, value []byte) error {
	return d.Propose(EncodeCommand(Command{Op: OpPut, Key: key, Value: value}))
}

// ProposeDelete encodes a DELETE and proposes it.
func ProposeDelete(d *consensus.Driver, key []byte) error {
	return d.Propose(EncodeCommand(Command{Op: OpDelete, Key: key}))
}
