package consensus

import "fmt"

// Log is the in-memory Raft log.
//
// # The sentinel / boundary (§3, Phase 9 §2)
//
// entries[0] is never replicated, never committed and never applied. Before
// any snapshot exists it is the dummy Entry{Term: 0, Index: 0}; once a
// snapshot exists it becomes that snapshot's (lastIncludedIndex,
// lastIncludedTerm) — "the entry before the first real entry" is a real
// object either way, so an empty-above-the-boundary log still has a base case
// for prevLogIndex matching with no special-casing.
//
// # Why every access goes through a method
//
// entries[i].Index == i + Offset(): nothing outside this file may index the
// backing slice, which is exactly what let Phase 9 rewrite these method
// bodies — to introduce the offset — and touch nothing else.
type Log struct {
	entries []Entry
}

// NewLog builds a log from persisted entries strictly above boundary,
// prepending boundary itself as entries[0]. boundary is the {0,0} sentinel
// when no snapshot has ever been taken, or a snapshot's (lastIncludedIndex,
// lastIncludedTerm) once one exists (Phase 9 §5).
func NewLog(boundary Entry, persisted []Entry) *Log {
	l := &Log{entries: make([]Entry, 1, len(persisted)+1)}
	l.entries[0] = Entry{Term: boundary.Term, Index: boundary.Index}
	l.entries = append(l.entries, persisted...)
	return l
}

// Offset is the log's boundary index: 0 until a snapshot exists, thereafter
// the snapshot's lastIncludedIndex. It is never a real, applyable entry.
func (l *Log) Offset() uint64 { return l.entries[0].Index }

// LastIndex is the highest index in the log, or Offset() if nothing above the
// boundary has been appended yet.
func (l *Log) LastIndex() uint64 { return l.entries[len(l.entries)-1].Index }

// LastTerm is the term of the entry at LastIndex (the boundary's term if the
// log holds nothing above it).
func (l *Log) LastTerm() uint64 { return l.entries[len(l.entries)-1].Term }

// Has reports whether index i exists in the log. The boundary itself counts
// (i == Offset()): it is a valid prevLogIndex, exactly as index 0 was before
// any snapshot existed.
func (l *Log) Has(i uint64) bool { return i >= l.entries[0].Index && i <= l.LastIndex() }

// At returns the entry at index i. Panics if i is out of range — callers that
// cannot know must check Has first.
func (l *Log) At(i uint64) Entry {
	if !l.Has(i) {
		panic(fmt.Sprintf("consensus: log index %d out of range (offset=%d last=%d)", i, l.entries[0].Index, l.LastIndex()))
	}
	return l.entries[i-l.entries[0].Index]
}

// Term returns the term of the entry at index i. Panics if i is out of range;
// check Has first. The boundary's term is real once a snapshot exists.
func (l *Log) Term(i uint64) uint64 { return l.At(i).Term }

// Slice returns a copy of entries in the inclusive range [lo, hi]. lo below
// Offset()+1 is clamped (the boundary itself is not returnable — it is not a
// real entry). An empty range yields nil.
func (l *Log) Slice(lo, hi uint64) []Entry {
	if lo < l.entries[0].Index+1 {
		lo = l.entries[0].Index + 1
	}
	if hi > l.LastIndex() || lo > hi {
		return nil
	}
	offset := l.entries[0].Index
	out := make([]Entry, hi-lo+1)
	copy(out, l.entries[lo-offset:hi-offset+1])
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
// i must be strictly greater than Offset() (the boundary is not removable —
// there is nothing before it left to fall back to).
//
// This is Phase 8's log-repair tool — when a follower's log diverges, the leader
// overwrites the conflicting suffix. It is built and tested here, in Phase 6's
// quiet, so Phase 8 only has to call it.
func (l *Log) TruncateFrom(i uint64) {
	offset := l.entries[0].Index
	if i <= offset {
		panic(fmt.Sprintf("consensus: cannot truncate at or below the log's boundary (offset=%d)", offset))
	}
	if i > l.LastIndex() {
		return // nothing to drop
	}
	l.entries = l.entries[:i-offset]
}

// CompactTo drops the log's prefix up to and including index, making index the
// new boundary (Phase 9 §2). index must currently exist in the log (Has(index)
// must hold) — in particular, a caller must never compact past what it has
// applied, since that data would become unrecoverable.
func (l *Log) CompactTo(index uint64) {
	if !l.Has(index) {
		panic(fmt.Sprintf("consensus: cannot compact to index %d, not in log (offset=%d last=%d)", index, l.entries[0].Index, l.LastIndex()))
	}
	term := l.Term(index)
	pos := index - l.entries[0].Index
	newEntries := make([]Entry, 1, len(l.entries)-int(pos))
	newEntries[0] = Entry{Index: index, Term: term}
	newEntries = append(newEntries, l.entries[pos+1:]...)
	l.entries = newEntries
}

// RestoreToSnapshot throws the entire log away, leaving only the boundary
// (index, term). Used when installing a leader's snapshot (Phase 9 §4): an
// installed snapshot is authoritative and supersedes any existing log,
// committed or not.
func (l *Log) RestoreToSnapshot(index, term uint64) {
	l.entries = []Entry{{Index: index, Term: term}}
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

// Entries returns a copy of every real entry (the boundary excluded).
func (l *Log) Entries() []Entry { return l.Slice(l.entries[0].Index+1, l.LastIndex()) }
