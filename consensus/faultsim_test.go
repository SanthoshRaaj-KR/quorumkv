package consensus

import "testing"

// Phase 13 scenario 1's Go-side mirror (planning/phase-13-fault-injection.md
// §2, faultsim.go) — raft-log uses the identical length-prefixed,
// CRC-checked framing as the Rust WAL, so the same "torn write, replay
// drops exactly the faulted record and everything after" property applies
// directly. TestTornTailIsDroppedAndPriorEntriesSurvive already proves this
// *offline* (hand-truncate the file after the fact); this is the same
// property proven live, at a call chosen by a seed.

func runTornAppend(t *testing.T, dir string, seed int64, count, target int) []Entry {
	t.Helper()
	s := mustOpen(t, dir)
	s.appendFault = newAppendFault(seed, target)

	var survivors []Entry
	for i := 1; i <= count; i++ {
		e := Entry{Term: 1, Index: uint64(i), Cmd: []byte("cmd")}
		_ = s.AppendEntries([]Entry{e}) // return value ignored: recovery is self-certifying via CRC
		if i < target {
			survivors = append(survivors, e)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return survivors
}

func TestTornAppendDropsExactlyTheFaultedEntryAndEverythingAfter(t *testing.T) {
	const count = 20
	for seed := int64(0); seed < 20; seed++ {
		dir := t.TempDir()
		target := 1 + int(seed%count) // which AppendEntries call is torn
		want := runTornAppend(t, dir, seed, count, target)

		s2 := mustOpen(t, dir)
		defer s2.Close()
		got, err := s2.LoadEntries()
		if err != nil {
			t.Fatalf("seed %d: LoadEntries: %v", seed, err)
		}
		entriesEqual(t, got, want)
	}
}

// TestTornAppendSeedReproducesIdentically is the determinism claim itself,
// tested directly (phase-13 §3.2, same shape as the Rust side's
// a_fixed_seed_reproduces_the_identical_fault): the same seed against the
// same workload produces the identical torn point and surviving entries,
// both times.
func TestTornAppendSeedReproducesIdentically(t *testing.T) {
	const (
		count  = 20
		seed   = 12345
		target = 11
	)

	dirA := t.TempDir()
	wantA := runTornAppend(t, dirA, seed, count, target)
	sA := mustOpen(t, dirA)
	defer sA.Close()
	gotA, err := sA.LoadEntries()
	if err != nil {
		t.Fatal(err)
	}

	dirB := t.TempDir()
	wantB := runTornAppend(t, dirB, seed, count, target)
	sB := mustOpen(t, dirB)
	defer sB.Close()
	gotB, err := sB.LoadEntries()
	if err != nil {
		t.Fatal(err)
	}

	entriesEqual(t, wantA, wantB)
	entriesEqual(t, gotA, gotB)
	entriesEqual(t, gotA, wantA)
}
