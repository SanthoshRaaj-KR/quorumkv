package consensus

import "fmt"

// Log is the in-memory Raft log.
//
// # The sentinel (§3)
//
// entries[0] is a dummy Entry{Term: 0, Index: 0} that is never replicated,
// never committed and never applied. It exists so that "the entry before the
// first real entry" is a real object: an empty log has LastIndex()==0 and
// LastTerm()==0 with no special case, and Phase 8's prevLogIndex==0 walkback
// base case needs no branch. One wasted struct buys a whole class of
// off-by-one bugs.
//
// # Why every access goes through a method
//
// While there is no snapshot, entries[i].Index == i. Phase 9 truncates the
// *head* of the log and breaks that identity, forcing a firstIndex offset.
// Nothing outside this file may index the backing slice, so Phase 9 rewrites
// these method bodies and touches nothing else.
type Log struct {
	entries []Entry
}

// NewLog builds a log from persisted entries (which must start at index 1 and
// ascend contiguously), prepending the sentinel.
func NewLog(persisted []Entry) *Log {
	l := &Log{entries: make([]Entry, 1, len(persisted)+1)}
	l.entries[0] = Entry{Term: 0, Index: 0}
	l.entries = append(l.entries, persisted...)
	return l
}

// LastIndex is the highest index in the log, or 0 if it holds only the sentinel.
func (l *Log) LastIndex() uint64 { return l.entries[len(l.entries)-1].Index }

// LastTerm is the term of the entry at LastIndex (0 for an empty log).
func (l *Log) LastTerm() uint64 { return l.entries[len(l.entries)-1].Term }

// Has reports whether index i exists in the log. Index 0 (the sentinel) counts:
// it is a valid prevLogIndex.
func (l *Log) Has(i uint64) bool { return i <= l.LastIndex() }

// At returns the entry at index i. Panics if i is out of range — callers that
// cannot know must check Has first.
func (l *Log) At(i uint64) Entry {
	if !l.Has(i) {
		panic(fmt.Sprintf("consensus: log index %d out of range (last=%d)", i, l.LastIndex()))
	}
	return l.entries[i]
}

// Term returns the term of the entry at index i. Panics if i is out of range;
// check Has first. The sentinel's term is 0.
func (l *Log) Term(i uint64) uint64 { return l.At(i).Term }

// Slice returns a copy of entries in the inclusive range [lo, hi]. lo must be
// at least 1 (the sentinel is not returnable). An empty range yields nil.
func (l *Log) Slice(lo, hi uint64) []Entry {
	if lo < 1 {
		lo = 1
	}
	if hi > l.LastIndex() || lo > hi {
		return nil
	}
	out := make([]Entry, hi-lo+1)
	copy(out, l.entries[lo:hi+1])
	return out
}

// Append adds entries to the end of the log. Each must continue the sequence
// exactly (index == LastIndex()+1); a gap is a programming error, not a
// recoverable condition.
func (l *Log) Append(entries ...Entry) {
	for _, e := range entries {
		if e.Index != l.LastIndex()+1 {
			panic(fmt.Sprintf("consensus: non-contiguous append: got index %d, want %d", e.Index, l.LastIndex()+1))
		}
		l.entries = append(l.entries, e)
	}
}

// TruncateFrom discards every entry with index >= i, so LastIndex() becomes i-1.
// i must be at least 1 (the sentinel is not removable).
//
// This is Phase 8's log-repair tool — when a follower's log diverges, the leader
// overwrites the conflicting suffix. It is built and tested here, in Phase 6's
// quiet, so Phase 8 only has to call it.
func (l *Log) TruncateFrom(i uint64) {
	if i < 1 {
		panic("consensus: cannot truncate the log sentinel")
	}
	if i > l.LastIndex() {
		return // nothing to drop
	}
	l.entries = l.entries[:i]
}

// IsUpToDate implements Raft §5.4.1's election restriction: is a candidate whose
// log ends at (lastIndex, lastTerm) at least as up-to-date as this log?
//
// Compare the last term first; only on a tie does length decide. This is the
// safety check that stops a node which missed recent writes from winning an
// election and erasing committed data.
func (l *Log) IsUpToDate(lastIndex, lastTerm uint64) bool {
	if lastTerm != l.LastTerm() {
		return lastTerm > l.LastTerm()
	}
	return lastIndex >= l.LastIndex()
}

// Entries returns a copy of every real entry (the sentinel excluded).
func (l *Log) Entries() []Entry { return l.Slice(1, l.LastIndex()) }
