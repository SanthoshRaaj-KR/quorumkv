package consensus

// The Go-side mirror of storage/src/faultsim.rs's live, seed-reproducible
// fault injection (planning/phase-13-fault-injection.md §2, "each scenario
// gets a Go-side mirror where it makes sense" — scenario 1's shape applies
// directly here, since raft-log uses the identical length-prefixed,
// CRC-checked framing the WAL does).
//
// Deliberately narrower than the Rust seam: FileStorage's one log file
// handle is shared by AppendEntries, TruncateFrom, and replay's own
// Seek/ReadAt calls — unlike Rust's write-only WalWriter, there is no
// single "the writer" to wrap behind an interface without a much larger,
// riskier change touching every method on FileStorage. Rather than that,
// this hooks exactly the one call site scenario 1 needs: AppendEntries'
// write. Nothing else on FileStorage changes shape.

// appendFault is a deterministic, seed-reproducible plan for exactly one
// AppendEntries call: on the target-th call (1-based), tear its write to a
// seed-derived length and report success anyway — a torn write the caller
// doesn't even notice, matching what a real crash leaves behind (mirrors
// storage/src/faultsim.rs's FaultKind::TornWrite). Every call after that
// one is skipped entirely: a real crash stops the process once, so nothing
// after that instant reaches disk, even though the (unaware) caller keeps
// calling as if nothing happened.
//
// nil (FileStorage's zero value for this field) means "no fault" — only a
// test that builds one directly (white-box, same package) ever sets it;
// OpenFileStorage never does.
type appendFault struct {
	seed    int64
	target  int
	seen    int
	tripped bool
	rng     splitMix64
}

func newAppendFault(seed int64, target int) *appendFault {
	return &appendFault{seed: seed, target: target, rng: newSplitMix64(uint64(seed))}
}

// poll reports what this call should do. skip=true once tripped (nothing
// happens, not even a partial write); fire=true with a torn length exactly
// on the target call; otherwise the caller proceeds normally.
func (f *appendFault) poll(bufLen int) (tornLen int, fire bool, skip bool) {
	if f.tripped {
		return 0, false, true
	}
	f.seen++
	if f.seen != f.target {
		return 0, false, false
	}
	f.tripped = true
	if bufLen == 0 {
		return 0, true, false
	}
	return int(f.rng.next() % uint64(bufLen)), true, false
}

// splitMix64 mirrors storage/src/faultsim.rs's own hand-rolled PRNG — same
// reasoning: a dozen lines of arithmetic beats a new dependency for picking
// a reproducible torn length. Not cryptographic, not general-purpose.
type splitMix64 uint64

func newSplitMix64(seed uint64) splitMix64 { return splitMix64(seed) }

func (s *splitMix64) next() uint64 {
	*s += 0x9E3779B97F4A7C15
	z := uint64(*s)
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}
