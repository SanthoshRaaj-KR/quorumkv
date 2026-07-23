package consensus

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func mustOpen(t *testing.T, dir string) *FileStorage {
	t.Helper()
	s, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	return s
}

func entriesEqual(t *testing.T, got, want []Entry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i].Term != want[i].Term || got[i].Index != want[i].Index || !bytes.Equal(got[i].Cmd, want[i].Cmd) {
			t.Errorf("entry %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestEntryCodecRoundTrips(t *testing.T) {
	cases := []Entry{
		{Term: 0, Index: 0},
		{Term: 1, Index: 1, Cmd: []byte("PUT k v")},
		{Term: 7, Index: 9001},                                     // no-op: empty command
		{Term: 3, Index: 2, Cmd: bytes.Repeat([]byte{0xAB}, 5000)}, // large
	}
	for _, want := range cases {
		enc := encodeEntry(want)
		got, n, err := decodeEntry(enc)
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if n != len(enc) {
			t.Errorf("consumed %d bytes, want %d", n, len(enc))
		}
		if got.Term != want.Term || got.Index != want.Index || !bytes.Equal(got.Cmd, want.Cmd) {
			t.Errorf("round-trip = %+v, want %+v", got, want)
		}
	}
}

func TestDecodeRejectsCorruptionAndShortBuffers(t *testing.T) {
	enc := encodeEntry(Entry{Term: 2, Index: 5, Cmd: []byte("hello")})

	for i := 1; i < len(enc); i++ {
		if _, _, err := decodeEntry(enc[:i]); err == nil {
			t.Errorf("decode of a %d-byte prefix should fail", i)
		}
	}
	bad := append([]byte(nil), enc...)
	bad[len(bad)-1] ^= 0xFF // flip a payload bit
	if _, _, err := decodeEntry(bad); err == nil {
		t.Error("decode of a corrupted record should fail the CRC")
	}
}

func TestAppendAndReloadEntries(t *testing.T) {
	dir := t.TempDir()
	want := []Entry{
		{Term: 1, Index: 1, Cmd: []byte("a")},
		{Term: 1, Index: 2, Cmd: []byte("b")},
		{Term: 2, Index: 3}, // a no-op
	}

	s := mustOpen(t, dir)
	if err := s.AppendEntries(want); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := mustOpen(t, dir)
	defer s2.Close()
	got, err := s2.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries: %v", err)
	}
	entriesEqual(t, got, want)
}

func TestHardStateRoundTripsAndDefaultsToZero(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()

	hs, err := s.LoadHardState()
	if err != nil {
		t.Fatalf("LoadHardState on a fresh dir: %v", err)
	}
	if hs != (HardState{}) {
		t.Errorf("fresh HardState = %+v, want zero", hs)
	}

	want := HardState{Term: 42, VotedFor: 3}
	if err := s.SaveHardState(want); err != nil {
		t.Fatalf("SaveHardState: %v", err)
	}
	got, err := s.LoadHardState()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("HardState = %+v, want %+v", got, want)
	}

	// And it survives a reopen.
	s2 := mustOpen(t, dir)
	defer s2.Close()
	if got, _ := s2.LoadHardState(); got != want {
		t.Errorf("reloaded HardState = %+v, want %+v", got, want)
	}
}

func TestHardStateDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()
	if err := s.SaveHardState(HardState{Term: 9, VotedFor: 1}); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, HardStateName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] ^= 0xFF // corrupt the term
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LoadHardState(); err == nil {
		t.Error("a corrupted hardstate must not load silently")
	}
}

// ─── §7.5 torn tail ──────────────────────────────────────────────────────────

// A crash mid-append leaves a partial record. Replay drops it — that entry was
// never acknowledged, so losing it is correct — and every prior entry survives.
// Same rule, same reasoning as the Phase 1 WAL.
func TestTornTailIsDroppedAndPriorEntriesSurvive(t *testing.T) {
	dir := t.TempDir()
	want := []Entry{
		{Term: 1, Index: 1, Cmd: []byte("first")},
		{Term: 1, Index: 2, Cmd: []byte("second")},
		{Term: 1, Index: 3, Cmd: []byte("third")},
	}
	s := mustOpen(t, dir)
	if err := s.AppendEntries(want); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Chop the last few bytes off: a torn final write.
	path := filepath.Join(dir, LogName)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-4); err != nil {
		t.Fatal(err)
	}

	s2 := mustOpen(t, dir)
	defer s2.Close()
	got, err := s2.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries after a torn tail: %v", err)
	}
	entriesEqual(t, got, want[:2])

	// The file was healed, so the next append lands at a clean boundary.
	if err := s2.AppendEntries([]Entry{{Term: 2, Index: 3, Cmd: []byte("replacement")}}); err != nil {
		t.Fatal(err)
	}
	got, err = s2.LoadEntries()
	if err != nil {
		t.Fatal(err)
	}
	entriesEqual(t, got, append(want[:2], Entry{Term: 2, Index: 3, Cmd: []byte("replacement")}))
}

// ─── §7.6 TruncateFrom — Phase 8's tool, tested in Phase 6's quiet ───────────

func TestTruncateFromIsDurable(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)

	var all []Entry
	for i := uint64(1); i <= 5; i++ {
		all = append(all, Entry{Term: 1, Index: i, Cmd: []byte{byte('a' + i - 1)}})
	}
	if err := s.AppendEntries(all); err != nil {
		t.Fatal(err)
	}

	if err := s.TruncateFrom(3); err != nil {
		t.Fatalf("TruncateFrom: %v", err)
	}
	got, err := s.LoadEntries()
	if err != nil {
		t.Fatal(err)
	}
	entriesEqual(t, got, all[:2])

	// Overwriting the truncated suffix works — this is exactly Phase 8's repair.
	repl := []Entry{{Term: 2, Index: 3, Cmd: []byte("X")}, {Term: 2, Index: 4, Cmd: []byte("Y")}}
	if err := s.AppendEntries(repl); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2 := mustOpen(t, dir)
	defer s2.Close()
	got, err = s2.LoadEntries()
	if err != nil {
		t.Fatal(err)
	}
	entriesEqual(t, got, append(append([]Entry(nil), all[:2]...), repl...))
}

func TestTruncateFromEdges(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()
	if err := s.AppendEntries([]Entry{{Term: 1, Index: 1, Cmd: []byte("a")}}); err != nil {
		t.Fatal(err)
	}
	if err := s.TruncateFrom(0); err == nil {
		t.Error("TruncateFrom(0) must be rejected — index 0 is the sentinel")
	}
	if err := s.TruncateFrom(99); err != nil {
		t.Errorf("TruncateFrom past the end should be a no-op, got %v", err)
	}
	got, _ := s.LoadEntries()
	if len(got) != 1 {
		t.Errorf("a no-op truncate changed the log: %d entries", len(got))
	}
	if err := s.TruncateFrom(1); err != nil {
		t.Fatal(err)
	}
	got, _ = s.LoadEntries()
	if len(got) != 0 {
		t.Errorf("after TruncateFrom(1) the log should be empty, got %d", len(got))
	}
}
