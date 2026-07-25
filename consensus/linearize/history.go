// Package linearize checks whether a recorded history of key-value
// operations is linearizable (planning/phase-14-linearizability.md).
//
// This file and checker.go deliberately import nothing from [quorumkv/consensus]
// or [quorumkv/consensus/clientrpc] — the algorithm is a generic check over
// (key, op, interval, result) tuples, independent of what produced them.
// cluster.go is the only file in this package that knows a real Raft
// cluster exists; everything here would work identically fed a history
// from any other key-value store.
package linearize

import (
	"sync"
	"time"
)

// OpKind is the kind of operation one [Op] records.
type OpKind int

const (
	OpPut OpKind = iota
	OpDelete
	OpGet
)

func (k OpKind) String() string {
	switch k {
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	case OpGet:
		return "get"
	default:
		return "unknown"
	}
}

// Value is a register's observed or simulated content: either a specific
// string, or "absent" (never written, or the most recent op was a delete).
// The zero Value is absent — the correct starting state for any key that
// has never been touched.
type Value struct {
	Found bool
	Data  string
}

// Op is one recorded client operation against a single key.
//
// Start/End are wall-clock, as observed by the client that issued the
// call — the real-time interval the checker's search must respect (§1d).
// Ok reports whether the outcome is known: false means the client saw an
// error or timeout, so whether this op actually took effect on the
// register is genuinely unknown (§1c/§7) — never simply dropped, since
// that would hide exactly the "acknowledged as failed but actually
// committed" bug class this package exists to catch.
type Op struct {
	Key   string
	Kind  OpKind
	Ok    bool
	Start time.Time
	End   time.Time

	// Arg is the value a Put writes — expected to already be uniquely
	// tagged by the caller (e.g. with a global op-ID, §1c), so a Get's
	// result maps unambiguously to the one write that produced it. Unused
	// for Delete/Get.
	Arg string
	// Result is a Get's observed value. Unused for Put/Delete.
	Result Value
}

// History is every recorded operation across every key and every client,
// safe for concurrent recording from multiple goroutines.
type History struct {
	mu  sync.Mutex
	ops []Op
}

// NewHistory returns an empty History ready to record into.
func NewHistory() *History { return &History{} }

// Record appends op. Safe for concurrent use.
func (h *History) Record(op Op) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ops = append(h.ops, op)
}

// Do times fn — the actual client call — and records the resulting Op.
// fn performs the call and reports its result (ignored for Put/Delete)
// and whether the outcome is known (false on error/timeout, §1c).
//
//	h.Do(key, OpPut, taggedValue, func() (Value, bool) {
//	    err := client.Put([]byte(key), []byte(taggedValue))
//	    return Value{}, err == nil
//	})
func (h *History) Do(key string, kind OpKind, arg string, fn func() (Value, bool)) {
	start := time.Now()
	result, ok := fn()
	end := time.Now()
	h.Record(Op{Key: key, Kind: kind, Arg: arg, Ok: ok, Start: start, End: end, Result: result})
}

// Ops returns a copy of every recorded operation, in recording order.
func (h *History) Ops() []Op {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]Op, len(h.ops))
	copy(out, h.ops)
	return out
}

// Len reports how many operations have been recorded so far.
func (h *History) Len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.ops)
}

// ByKey groups every recorded operation by key (§1a: linearizability is
// checked per key, independently, never as one combined history).
func (h *History) ByKey() map[string][]Op {
	out := make(map[string][]Op)
	for _, op := range h.Ops() {
		out[op.Key] = append(out[op.Key], op)
	}
	return out
}
