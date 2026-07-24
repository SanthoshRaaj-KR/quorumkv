package consensus

import "testing"

func buildLog(terms ...uint64) *Log {
	entries := make([]Entry, 0, len(terms))
	for i, term := range terms {
		entries = append(entries, Entry{Term: term, Index: uint64(i + 1)})
	}
	return NewLog(Entry{}, entries)
}

func TestEmptyLogFallsOutOfTheSentinel(t *testing.T) {
	l := NewLog(Entry{}, nil)
	if l.LastIndex() != 0 {
		t.Errorf("LastIndex = %d, want 0", l.LastIndex())
	}
	if l.LastTerm() != 0 {
		t.Errorf("LastTerm = %d, want 0", l.LastTerm())
	}
	if !l.Has(0) {
		t.Error("index 0 (the sentinel) must exist — it is a valid prevLogIndex")
	}
	if l.Has(1) {
		t.Error("an empty log must not claim index 1")
	}
	if got := l.Entries(); len(got) != 0 {
		t.Errorf("Entries() = %v, want empty (the sentinel is not a real entry)", got)
	}
}

func TestAccessorsAndSlice(t *testing.T) {
	l := buildLog(1, 1, 2, 3)

	if l.LastIndex() != 4 || l.LastTerm() != 3 {
		t.Fatalf("last = (%d, %d), want (4, 3)", l.LastIndex(), l.LastTerm())
	}
	if l.Term(0) != 0 {
		t.Errorf("sentinel term = %d, want 0", l.Term(0))
	}
	if l.Term(3) != 2 {
		t.Errorf("Term(3) = %d, want 2", l.Term(3))
	}
	if got := l.Slice(2, 3); len(got) != 2 || got[0].Index != 2 || got[1].Index != 3 {
		t.Errorf("Slice(2,3) = %+v", got)
	}
	if got := l.Slice(5, 9); got != nil {
		t.Errorf("Slice past the end = %+v, want nil", got)
	}
	if got := l.Slice(3, 2); got != nil {
		t.Errorf("inverted Slice = %+v, want nil", got)
	}
	// Slice returns a copy: mutating it must not corrupt the log.
	s := l.Slice(1, 1)
	s[0].Term = 999
	if l.Term(1) != 1 {
		t.Error("Slice must return a copy")
	}
}

func TestAtPanicsOutOfRange(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("At past the end should panic — it is a programming error")
		}
	}()
	buildLog(1).At(5)
}

func TestAppendRejectsGaps(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("a non-contiguous append should panic")
		}
	}()
	buildLog(1, 1).Append(Entry{Term: 2, Index: 7})
}

func TestTruncateFromLog(t *testing.T) {
	l := buildLog(1, 1, 2, 3)

	l.TruncateFrom(3)
	if l.LastIndex() != 2 {
		t.Errorf("LastIndex after TruncateFrom(3) = %d, want 2", l.LastIndex())
	}
	if l.LastTerm() != 1 {
		t.Errorf("LastTerm = %d, want 1", l.LastTerm())
	}

	// Truncating past the end is a no-op.
	l.TruncateFrom(50)
	if l.LastIndex() != 2 {
		t.Errorf("no-op truncate changed the log: LastIndex = %d", l.LastIndex())
	}

	// The suffix can be replaced — Phase 8's repair.
	l.Append(Entry{Term: 4, Index: 3})
	if l.LastIndex() != 3 || l.LastTerm() != 4 {
		t.Errorf("after re-append, last = (%d, %d), want (3, 4)", l.LastIndex(), l.LastTerm())
	}

	l.TruncateFrom(1)
	if l.LastIndex() != 0 {
		t.Errorf("truncating everything should leave an empty log, got LastIndex %d", l.LastIndex())
	}
}

func TestTruncateFromRejectsTheSentinel(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("TruncateFrom(0) should panic — the sentinel is not removable")
		}
	}()
	buildLog(1).TruncateFrom(0)
}

// The §5.4.1 election restriction: term wins over length. This is the check that
// stops a node which missed recent writes from becoming leader and erasing
// committed data, so it is worth testing exhaustively before any network exists.
func TestIsUpToDate(t *testing.T) {
	// Local log ends at index 3, term 2.
	l := buildLog(1, 2, 2)

	cases := []struct {
		name                string
		candIndex, candTerm uint64
		want                bool
	}{
		{"identical log", 3, 2, true},
		{"longer, same term", 9, 2, true},
		{"shorter, same term", 1, 2, false},
		{"higher term but much shorter — term wins", 1, 5, true},
		{"lower term but much longer — term still wins", 99, 1, false},
		{"empty candidate log", 0, 0, false},
	}
	for _, c := range cases {
		if got := l.IsUpToDate(c.candIndex, c.candTerm); got != c.want {
			t.Errorf("%s: IsUpToDate(%d, %d) = %v, want %v", c.name, c.candIndex, c.candTerm, got, c.want)
		}
	}

	// An empty local log is never more up-to-date than anyone.
	empty := NewLog(Entry{}, nil)
	if !empty.IsUpToDate(0, 0) {
		t.Error("an empty candidate ties an empty local log")
	}
}
