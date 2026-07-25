package linearize

import (
	"testing"
	"time"
)

// A checker that always says "linearizable" is worse than no checker at
// all — it's a false proof. These tests exist to earn trust in Check
// before cluster.go ever points it at a real system (planning/
// phase-14-linearizability.md §5): known-good histories must pass, known-
// bad ones must fail, and the ambiguity rule for unknown-outcome writes
// must go both ways correctly.

// t0 is an arbitrary fixed instant; every test builds intervals as offsets
// from it in milliseconds, which is far easier to read and reason about
// than real wall-clock times.
var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

func put(key, arg string, startMs, endMs int) Op {
	return Op{Key: key, Kind: OpPut, Arg: arg, Ok: true, Start: at(startMs), End: at(endMs)}
}

func del(key string, startMs, endMs int) Op {
	return Op{Key: key, Kind: OpDelete, Ok: true, Start: at(startMs), End: at(endMs)}
}

func get(key string, result Value, startMs, endMs int) Op {
	return Op{Key: key, Kind: OpGet, Result: result, Ok: true, Start: at(startMs), End: at(endMs)}
}

func found(s string) Value { return Value{Found: true, Data: s} }

func absent() Value { return Value{} }

func historyOf(ops ...Op) *History {
	h := NewHistory()
	for _, op := range ops {
		h.Record(op)
	}
	return h
}

// ─── known-good histories ───────────────────────────────────────────────────

func TestSequentialPutThenGetIsLinearizable(t *testing.T) {
	h := historyOf(
		put("k", "v1", 0, 10),
		get("k", found("v1"), 20, 30),
	)
	ok, v := Check(h)
	if !ok {
		t.Fatalf("expected linearizable, got violation: %v", v)
	}
}

// Two writes fully complete (real time) before a Get that observes the
// later one — the only valid order.
func TestNonOverlappingWritesThenGetLatest(t *testing.T) {
	h := historyOf(
		put("k", "v1", 0, 10),
		put("k", "v2", 20, 30),
		get("k", found("v2"), 40, 50),
	)
	ok, v := Check(h)
	if !ok {
		t.Fatalf("expected linearizable, got violation: %v", v)
	}
}

// P1 and P2 genuinely overlap (concurrent) — a Get observing *either* value
// after both complete is valid, since real time doesn't force an order
// between them.
func TestConcurrentWritesEitherOrderIsValid(t *testing.T) {
	for _, want := range []string{"v1", "v2"} {
		h := historyOf(
			put("k", "v1", 0, 20),
			put("k", "v2", 10, 30),
			get("k", found(want), 40, 50),
		)
		ok, v := Check(h)
		if !ok {
			t.Fatalf("want=%s: expected linearizable, got violation: %v", want, v)
		}
	}
}

// A Get that overlaps a Put may observe the value either before or after
// it — both must be accepted.
func TestGetConcurrentWithPutCanObserveEitherState(t *testing.T) {
	h := historyOf(
		put("k", "v1", 0, 20),
		get("k", absent(), 5, 15), // ordered before the Put completes
	)
	ok, v := Check(h)
	if !ok {
		t.Fatalf("expected linearizable (Get before the concurrent Put), got violation: %v", v)
	}

	h2 := historyOf(
		put("k", "v1", 0, 20),
		get("k", found("v1"), 5, 15), // ordered after the Put, still concurrent by real time
	)
	ok2, v2 := Check(h2)
	if !ok2 {
		t.Fatalf("expected linearizable (Get after the concurrent Put), got violation: %v", v2)
	}
}

func TestDeleteMakesKeyAbsent(t *testing.T) {
	h := historyOf(
		put("k", "v1", 0, 10),
		del("k", 20, 30),
		get("k", absent(), 40, 50),
	)
	ok, v := Check(h)
	if !ok {
		t.Fatalf("expected linearizable, got violation: %v", v)
	}
}

func TestNeverWrittenKeyReadsAbsent(t *testing.T) {
	h := historyOf(get("k", absent(), 0, 10))
	ok, v := Check(h)
	if !ok {
		t.Fatalf("expected linearizable, got violation: %v", v)
	}
}

// Different keys are independent (§1a) — a violation on one key must not
// taint another.
func TestKeysAreCheckedIndependently(t *testing.T) {
	h := historyOf(
		put("a", "v1", 0, 10),
		get("a", found("v1"), 20, 30),
		put("b", "v1", 0, 10),
		put("b", "v2", 20, 30),
		get("b", found("v2"), 40, 50),
	)
	ok, v := Check(h)
	if !ok {
		t.Fatalf("expected linearizable, got violation: %v", v)
	}
}

// ─── known-bad histories ─────────────────────────────────────────────────────

// P1 fully completes before P2 starts (real time forces P1 then P2), so a
// Get after both must observe P2's value — observing P1's is a violation.
func TestStaleReadAfterNonOverlappingWriteIsDetected(t *testing.T) {
	h := historyOf(
		put("k", "v1", 0, 10),
		put("k", "v2", 20, 30),
		get("k", found("v1"), 40, 50), // stale: must have been "v2" by now
	)
	ok, v := Check(h)
	if ok {
		t.Fatal("expected a violation (stale read after a non-overlapping write), got none")
	}
	if v.Key != "k" {
		t.Errorf("violation key = %q, want %q", v.Key, "k")
	}
}

// A Get that fully precedes a Put (real time) must not observe that Put's
// value.
func TestReadBeforeWriteObservingItIsDetected(t *testing.T) {
	h := historyOf(
		get("k", found("v1"), 0, 10),
		put("k", "v1", 20, 30),
	)
	ok, _ := Check(h)
	if ok {
		t.Fatal("expected a violation (read observed a write that hadn't started yet), got none")
	}
}

func TestReadAfterDeleteObservingOldValueIsDetected(t *testing.T) {
	h := historyOf(
		put("k", "v1", 0, 10),
		del("k", 20, 30),
		get("k", found("v1"), 40, 50), // must be absent by now
	)
	ok, _ := Check(h)
	if ok {
		t.Fatal("expected a violation (read after a non-overlapping delete observed the old value), got none")
	}
}

// One bad key among several good ones must still be caught.
func TestOneBadKeyAmongGoodKeysIsCaught(t *testing.T) {
	h := historyOf(
		put("good", "v1", 0, 10),
		get("good", found("v1"), 20, 30),
		put("bad", "v1", 0, 10),
		put("bad", "v2", 20, 30),
		get("bad", found("v1"), 40, 50),
	)
	ok, v := Check(h)
	if ok {
		t.Fatal("expected a violation on key \"bad\", got none")
	}
	if v.Key != "bad" {
		t.Errorf("violation key = %q, want %q", v.Key, "bad")
	}
}

// ─── unknown-outcome (ambiguous) writes ──────────────────────────────────────

func unknownPut(key, arg string, startMs, endMs int) Op {
	return Op{Key: key, Kind: OpPut, Arg: arg, Ok: false, Start: at(startMs), End: at(endMs)}
}

// A Get that observes the value from *before* an unknown-outcome write is
// valid under the "it never happened" branch of the ambiguity.
func TestUnknownWriteMayBeExcluded(t *testing.T) {
	h := historyOf(
		put("k", "v1", 0, 10),
		unknownPut("k", "v2", 20, 30), // client saw an error/timeout
		get("k", found("v1"), 40, 50), // consistent with v2 never landing
	)
	ok, v := Check(h)
	if !ok {
		t.Fatalf("expected linearizable (unknown write excluded), got violation: %v", v)
	}
}

// A Get that observes the unknown write's own value is valid under the "it
// did happen" branch.
func TestUnknownWriteMayBeIncluded(t *testing.T) {
	h := historyOf(
		put("k", "v1", 0, 10),
		unknownPut("k", "v2", 20, 30),
		get("k", found("v2"), 40, 50), // consistent with v2 having landed
	)
	ok, v := Check(h)
	if !ok {
		t.Fatalf("expected linearizable (unknown write included), got violation: %v", v)
	}
}

// A Get observing a value consistent with *neither* branch is still a
// genuine violation — ambiguity isn't a blanket excuse.
func TestUnknownWriteStillCatchesAGenuineViolation(t *testing.T) {
	h := historyOf(
		put("k", "v1", 0, 10),
		unknownPut("k", "v2", 20, 30),
		get("k", found("v3"), 40, 50), // v3 was never written by anything
	)
	ok, _ := Check(h)
	if ok {
		t.Fatal("expected a violation (observed value matches no write, known or unknown), got none")
	}
}

// An unknown-outcome Get carries no information (nothing was observed) and
// must never affect the result either way.
func TestUnknownGetIsIgnored(t *testing.T) {
	h := historyOf(
		put("k", "v1", 0, 10),
		{Key: "k", Kind: OpGet, Ok: false, Start: at(15), End: at(18)}, // no data
		get("k", found("v1"), 20, 30),
	)
	ok, v := Check(h)
	if !ok {
		t.Fatalf("expected linearizable (unknown Get ignored), got violation: %v", v)
	}
}
