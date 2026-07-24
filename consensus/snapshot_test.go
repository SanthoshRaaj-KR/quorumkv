package consensus

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Phase 9 — snapshotting. The log stops growing forever: a driver-triggered
// local snapshot compacts the log file, and InstallSnapshot lets a far-behind
// follower catch up in one message instead of entry-by-entry replay.

// ─── helpers ─────────────────────────────────────────────────────────────────

// newSingleNodeWithThreshold is [newSingleNode] with a small SnapshotThreshold
// so tests can trip the driver's local-snapshot policy without proposing
// thousands of commands.
func newSingleNodeWithThreshold(t *testing.T, dir string, threshold int) (*Driver, *recorder) {
	t.Helper()
	st, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	cfg := testConfig(t, st)
	cfg.SnapshotThreshold = threshold
	sm := &recorder{}
	d, err := NewDriver(cfg, sm, nil)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	return d, sm
}

// newClusterWithSnapshotThreshold rebuilds every node's Driver with a small
// SnapshotThreshold, the same rebuild-in-place pattern §6.8's batch-cap test
// uses to override per-node Config after newCluster's defaults.
func newClusterWithSnapshotThreshold(t *testing.T, n, threshold int) *cluster {
	t.Helper()
	c := newCluster(t, n)
	for _, id := range c.ids {
		cfg := c.cfgs[id]
		cfg.SnapshotThreshold = threshold
		cfg.Storage = NewMemStorage()
		c.cfgs[id] = cfg
		sm := &recorder{}
		d, err := NewDriver(cfg, sm, c.bus.Transport(id))
		if err != nil {
			t.Fatalf("node %d: %v", id, err)
		}
		c.nodes[id] = d
		c.sms[id] = sm
	}
	return c
}

// ─── §8.1/§8.10 done-when: recover via InstallSnapshot, not replay ───────────

func TestLaggingFollowerRecoversViaInstallSnapshotNotReplay(t *testing.T) {
	const threshold = 10
	c := newClusterWithSnapshotThreshold(t, 3, threshold)
	leader := c.waitLeader(200)

	var lagging uint64
	for _, id := range c.ids {
		if id != leader {
			lagging = id
			break
		}
	}
	c.crash(lagging)

	var want []string
	for i := 0; i < 40; i++ {
		cmd := fmt.Sprintf("write-%02d", i)
		c.propose(leader, cmd)
		want = append(want, cmd)
	}
	c.tickN(10)

	leaderOffset := c.node(leader).Log().Offset()
	if leaderOffset == 0 {
		t.Fatal("setup: the leader never snapshotted despite crossing the threshold repeatedly")
	}

	var snapCount int
	var badAppend bool
	c.bus.Reorder = func(msgs []Message) []Message {
		for _, m := range msgs {
			if m.To != lagging {
				continue
			}
			switch m.Type {
			case MsgSnap:
				snapCount++
			case MsgAppReq:
				if m.PrevLogIndex < leaderOffset {
					badAppend = true
				}
			}
		}
		return msgs
	}

	c.restart(lagging)
	converged := false
	for i := 0; i < 300 && !converged; i++ {
		c.tick()
		converged = c.logsIdentical()
	}
	if !converged {
		t.Fatal("the restarted follower never converged to the leader's log")
	}

	if snapCount != 1 {
		t.Errorf("delivered %d MsgSnap to the restarted follower, want exactly 1", snapCount)
	}
	if badAppend {
		t.Error("an AppendEntries carried a PrevLogIndex below the leader's snapshot boundary — that should have been a MsgSnap instead")
	}
	if got := c.node(lagging).Log().Offset(); got == 0 {
		t.Error("the restarted follower's log has no boundary — it should have installed the leader's snapshot")
	}

	c.tickN(20) // let any remaining entries above the snapshot commit and apply
	for _, id := range c.ids {
		got := c.applied(id)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("node %d applied\n got %v\nwant %v", id, got, want)
		}
	}
}

// ─── §8.2 the log actually shrinks ───────────────────────────────────────────

func TestSnapshotShrinksTheLogFile(t *testing.T) {
	const threshold = 5
	dir := t.TempDir()

	d, _ := newSingleNodeWithThreshold(t, dir, threshold)
	defer d.Close()
	elect(t, d)

	for i := 0; i < 20; i++ {
		if err := d.Propose([]byte(fmt.Sprintf("cmd-%02d", i))); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}

	wantOffset := d.Node().Log().Offset()
	if wantOffset == 0 {
		t.Fatal("no snapshot was taken despite crossing the threshold repeatedly")
	}

	// Inspect the on-disk log with an independent handle: it must hold only
	// entries above the boundary, not all 21 (20 commands + the election no-op).
	st2, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()

	entries, err := st2.LoadEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) >= 21 {
		t.Errorf("on-disk log still holds %d entries after compaction, want fewer than 21", len(entries))
	}
	for _, e := range entries {
		if e.Index <= wantOffset {
			t.Errorf("on-disk entry at index %d is at or below the boundary %d — should have been compacted away", e.Index, wantOffset)
		}
	}

	snap, ok, err := st2.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("no snapshot found on disk")
	}
	if snap.Index != wantOffset {
		t.Errorf("persisted snapshot index = %d, want %d (matching the log's boundary)", snap.Index, wantOffset)
	}
	t.Logf("snapshotted at index %d, %d entries survive on disk", wantOffset, len(entries))
}

// ─── §8.3/§8.7 restart restores from the snapshot, not entry-by-entry replay ─

func TestRestartRestoresFromSnapshotNotReplay(t *testing.T) {
	const threshold = 5
	dir := t.TempDir()

	d, sm := newSingleNodeWithThreshold(t, dir, threshold)
	elect(t, d)

	// The election no-op occupies the first applied slot, so the threshold
	// trips after threshold-1 real commands — right when this loop ends, with
	// nothing left above the boundary.
	var want []string
	for i := 0; i < threshold-1; i++ {
		cmd := fmt.Sprintf("v%02d", i)
		if err := d.Propose([]byte(cmd)); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		want = append(want, cmd)
	}

	snapIndex := d.Node().Log().Offset()
	if snapIndex == 0 {
		t.Fatal("setup: no snapshot was taken")
	}
	if fmt.Sprint(sm.strings()) != fmt.Sprint(want) {
		t.Fatalf("setup: pre-restart applied = %v, want %v", sm.strings(), want)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: nothing has ticked, elected or replayed anything yet.
	d2, sm2 := newSingleNodeWithThreshold(t, dir, threshold)
	defer d2.Close()
	n2 := d2.Node()

	if got := n2.CommitIndex(); got != snapIndex {
		t.Errorf("reloaded commitIndex = %d, want %d (the snapshot index, not 0 — Phase 9 §5)", got, snapIndex)
	}
	if got := n2.LastApplied(); got != snapIndex {
		t.Errorf("reloaded lastApplied = %d, want %d", got, snapIndex)
	}
	if got := len(n2.Log().Entries()); got != 0 {
		t.Errorf("reloaded log holds %d entries above the boundary, want 0 — there is nothing left to replay", got)
	}
	// The state came back from Restore(), not from re-walking a log — the log
	// above the boundary is empty, so there is nothing to walk.
	if fmt.Sprint(sm2.strings()) != fmt.Sprint(want) {
		t.Errorf("restored applied = %v, want %v", sm2.strings(), want)
	}

	// And it still elects and keeps serving correctly from here (§5.4's
	// single-node restart path — most likely to expose a commitIndex-reset bug).
	elect(t, d2)
	if err := d2.Propose([]byte("after-restart")); err != nil {
		t.Fatal(err)
	}
	want = append(want, "after-restart")
	if fmt.Sprint(sm2.strings()) != fmt.Sprint(want) {
		t.Errorf("applied after restart = %v, want %v", sm2.strings(), want)
	}
}

// ─── §8.4 a stale InstallSnapshot is a no-op ─────────────────────────────────

func TestStaleInstallSnapshotIsANoOp(t *testing.T) {
	st := NewMemStorage()
	if err := st.AppendEntries([]Entry{
		{Term: 1, Index: 1, Cmd: []byte("a")},
		{Term: 1, Index: 2, Cmd: []byte("b")},
		{Term: 1, Index: 3, Cmd: []byte("c")},
	}); err != nil {
		t.Fatal(err)
	}
	n, err := NewNode(Config{
		ID: 1, Peers: []uint64{1, 2, 3},
		ElectionTimeout: DefaultElectionTimeout, HeartbeatTimeout: DefaultHeartbeatTimeout,
		Storage: st, Seed: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	n.commitIndex = 3
	n.lastApplied = 3

	if err := n.Step(Message{
		Type: MsgSnap, From: 2, To: 1, Term: 1,
		SnapshotIndex: 2, SnapshotTerm: 1, SnapshotData: []byte("stale"),
	}); err != nil {
		t.Fatal(err)
	}
	rd := n.Ready()
	n.Advance()

	if rd.Snapshot != nil {
		t.Fatal("a stale InstallSnapshot (index 2 <= commitIndex 3) must install nothing")
	}
	if n.Log().Offset() != 0 || n.LastIndex() != 3 {
		t.Errorf("the log was touched by a stale snapshot: offset=%d lastIndex=%d", n.Log().Offset(), n.LastIndex())
	}
	if len(rd.Messages) != 1 {
		t.Fatalf("got %d replies, want 1", len(rd.Messages))
	}
	resp := rd.Messages[0]
	if !resp.Success || resp.MatchIndex != 3 {
		t.Errorf("ack = %+v, want Success=true MatchIndex=3", resp)
	}
}

// ─── §8.5 the log offset holds across compaction ─────────────────────────────

func TestLogOffsetHoldsAcrossCompaction(t *testing.T) {
	l := buildLog(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) // index i has term i

	l.CompactTo(5)
	if got := l.Offset(); got != 5 {
		t.Fatalf("Offset() = %d, want 5", got)
	}
	if got := l.LastIndex(); got != 10 {
		t.Fatalf("LastIndex() = %d, want 10 (unaffected)", got)
	}
	if l.Has(4) {
		t.Error("Has(4) should be false — below the boundary")
	}
	if !l.Has(5) {
		t.Error("Has(5) should be true — the boundary itself is a valid prevLogIndex")
	}
	if !l.Has(10) {
		t.Error("Has(10) should be true")
	}
	if l.Has(11) {
		t.Error("Has(11) should be false — past the end")
	}
	if got := l.Term(5); got != 5 {
		t.Errorf("Term(5) (the boundary) = %d, want 5", got)
	}
	if got := l.Term(6); got != 6 {
		t.Errorf("Term(6) = %d, want 6", got)
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Error("At(4) should panic — below the boundary")
			}
		}()
		l.At(4)
	}()

	// Slice clamps to the boundary rather than returning entries it already covers.
	if got := l.Slice(1, 7); len(got) != 2 || got[0].Index != 6 || got[1].Index != 7 {
		t.Errorf("Slice(1,7) = %+v, want indices [6 7]", got)
	}
	if got := l.Slice(11, 20); got != nil {
		t.Errorf("Slice past the end = %+v, want nil", got)
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Error("TruncateFrom(5) should panic — at or below the boundary")
			}
		}()
		l.TruncateFrom(5)
	}()
	l.TruncateFrom(6) // strictly above the boundary: legal
	if got := l.LastIndex(); got != 5 {
		t.Errorf("LastIndex after TruncateFrom(6) = %d, want 5", got)
	}

	l.RestoreToSnapshot(20, 7)
	if got := l.Offset(); got != 20 {
		t.Errorf("Offset after RestoreToSnapshot = %d, want 20", got)
	}
	if got := l.LastIndex(); got != 20 || l.LastTerm() != 7 {
		t.Errorf("last = (%d, %d) after RestoreToSnapshot, want (20, 7)", l.LastIndex(), l.LastTerm())
	}
	if got := l.Entries(); len(got) != 0 {
		t.Errorf("Entries() after RestoreToSnapshot should be empty, got %v", got)
	}
}

// ─── §8.6 a snapshot supersedes a divergent tail ─────────────────────────────

func TestSnapshotSupersedesADivergentTail(t *testing.T) {
	st := NewMemStorage()
	// A follower carrying an uncommitted, divergent suffix from a dead term.
	if err := st.AppendEntries([]Entry{
		{Term: 1, Index: 1, Cmd: []byte("keep")},
		{Term: 2, Index: 2, Cmd: []byte("doomed-a")},
		{Term: 2, Index: 3, Cmd: []byte("doomed-b")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveHardState(HardState{Term: 2}); err != nil {
		t.Fatal(err)
	}
	n, err := NewNode(Config{
		ID: 1, Peers: []uint64{1, 2, 3},
		ElectionTimeout: DefaultElectionTimeout, HeartbeatTimeout: DefaultHeartbeatTimeout,
		Storage: st, Seed: 21,
	})
	if err != nil {
		t.Fatal(err)
	}

	// A term-9 leader's snapshot covers well past the divergent suffix.
	if err := n.Step(Message{
		Type: MsgSnap, From: 2, To: 1, Term: 9,
		SnapshotIndex: 10, SnapshotTerm: 5, SnapshotData: []byte("state-at-10"),
	}); err != nil {
		t.Fatal(err)
	}
	rd := n.Ready()
	n.Advance()

	if rd.Snapshot == nil || !rd.RestoreFromSnapshot {
		t.Fatal("expected a snapshot to install")
	}
	if got := n.Log().Offset(); got != 10 {
		t.Errorf("Offset() = %d, want 10", got)
	}
	if got := n.LastIndex(); got != 10 {
		t.Errorf("LastIndex() = %d, want 10 — the divergent suffix must be entirely gone", got)
	}
	if got := n.CommitIndex(); got != 10 {
		t.Errorf("CommitIndex() = %d, want 10", got)
	}
	if got := n.LastApplied(); got != 10 {
		t.Errorf("LastApplied() = %d, want 10", got)
	}
	if got := n.Log().Entries(); len(got) != 0 {
		t.Errorf("Entries() should be empty right after install, got %v", got)
	}
}

// ─── §8.8 snapshot data round-trips through storage ──────────────────────────

func TestSnapshotRoundTripsThroughStorageAndDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()

	if _, ok, err := s.LoadSnapshot(); err != nil || ok {
		t.Fatalf("fresh dir should have no snapshot: ok=%v err=%v", ok, err)
	}

	want := Snapshot{Index: 42, Term: 7, Data: []byte("the-applied-state")}
	if err := s.SaveSnapshot(want); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	got, ok, err := s.LoadSnapshot()
	if err != nil || !ok {
		t.Fatalf("LoadSnapshot: ok=%v err=%v", ok, err)
	}
	if got.Index != want.Index || got.Term != want.Term || !bytes.Equal(got.Data, want.Data) {
		t.Errorf("LoadSnapshot = %+v, want %+v", got, want)
	}

	// Survives a reopen.
	s2 := mustOpen(t, dir)
	defer s2.Close()
	got2, ok2, err := s2.LoadSnapshot()
	if err != nil || !ok2 {
		t.Fatalf("reloaded LoadSnapshot: ok=%v err=%v", ok2, err)
	}
	if got2.Index != want.Index || got2.Term != want.Term || !bytes.Equal(got2.Data, want.Data) {
		t.Errorf("reloaded snapshot = %+v, want %+v", got2, want)
	}

	// Corruption must not load silently.
	path := filepath.Join(dir, SnapshotName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFileStorage(dir); err == nil {
		t.Error("a corrupted snapshot must not load silently")
	}
}

// A crash between SaveSnapshot and the log-file compaction that follows it
// (Phase 9 §7) must recover safely: the snapshot is authoritative, so a log
// that doesn't line up with it is dropped rather than trusted, never
// resurrected as if it were still valid content.
func TestReplayToleratesALogNotYetCompactedToASavedSnapshot(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)

	if err := s.AppendEntries([]Entry{
		{Term: 1, Index: 1, Cmd: []byte("a")},
		{Term: 1, Index: 2, Cmd: []byte("b")},
		{Term: 1, Index: 3, Cmd: []byte("c")},
	}); err != nil {
		t.Fatal(err)
	}
	// Simulate the crash window: the snapshot is durable, but raft-log was
	// never trimmed to match it (CompactLog never ran).
	if err := s.SaveSnapshot(Snapshot{Index: 3, Term: 1, Data: []byte("state")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage after a simulated crash: %v", err)
	}
	defer s2.Close()

	entries, err := s2.LoadEntries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("stale pre-snapshot entries survived replay: %v — a mismatched log must be dropped, not trusted", entries)
	}
	snap, ok, err := s2.LoadSnapshot()
	if err != nil || !ok || snap.Index != 3 {
		t.Fatalf("snapshot should still be intact: snap=%+v ok=%v err=%v", snap, ok, err)
	}

	// Storage must still be usable: new entries append cleanly above the boundary.
	if err := s2.AppendEntries([]Entry{{Term: 2, Index: 4, Cmd: []byte("d")}}); err != nil {
		t.Fatalf("AppendEntries after recovery: %v", err)
	}
	got, err := s2.LoadEntries()
	if err != nil {
		t.Fatal(err)
	}
	entriesEqual(t, got, []Entry{{Term: 2, Index: 4, Cmd: []byte("d")}})
}
