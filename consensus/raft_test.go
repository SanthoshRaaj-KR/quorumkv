package consensus

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

// recorder is the Phase 6 state machine: it just remembers what it was told.
// Phase 10 replaces it with the Rust LSM engine. Snapshot/Restore (Phase 9)
// serialize the applied slice as length-prefixed commands — good enough for a
// test SM; the LSM will hand back its SSTable set instead.
type recorder struct{ applied [][]byte }

func (r *recorder) Apply(cmd []byte) error {
	r.applied = append(r.applied, append([]byte(nil), cmd...))
	return nil
}

func (r *recorder) Snapshot() ([]byte, error) {
	var buf []byte
	for _, cmd := range r.applied {
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(cmd)))
		buf = append(buf, lenBuf[:]...)
		buf = append(buf, cmd...)
	}
	return buf, nil
}

func (r *recorder) Restore(data []byte) error {
	applied := make([][]byte, 0)
	for len(data) >= 4 {
		n := binary.LittleEndian.Uint32(data[:4])
		data = data[4:]
		if uint64(len(data)) < uint64(n) {
			break
		}
		applied = append(applied, append([]byte(nil), data[:n]...))
		data = data[n:]
	}
	r.applied = applied
	return nil
}

func (r *recorder) strings() []string {
	out := make([]string, len(r.applied))
	for i, c := range r.applied {
		out[i] = string(c)
	}
	return out
}

func testConfig(t *testing.T, st Storage) Config {
	t.Helper()
	return Config{
		ID:               1,
		Peers:            []uint64{1},
		ElectionTimeout:  10,
		HeartbeatTimeout: 3,
		Storage:          st,
		Seed:             42,
	}
}

// newSingleNode builds a cluster of one over a real on-disk store in dir.
func newSingleNode(t *testing.T, dir string) (*Driver, *recorder) {
	t.Helper()
	st, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("OpenFileStorage: %v", err)
	}
	sm := &recorder{}
	d, err := NewDriver(testConfig(t, st), sm, nil)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	return d, sm
}

// elect ticks until the node becomes leader (or fails the test).
func elect(t *testing.T, d *Driver) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if d.Node().Role() == Leader {
			return
		}
		if err := d.Tick(); err != nil {
			t.Fatalf("Tick: %v", err)
		}
	}
	t.Fatalf("node never became leader (role=%s)", d.Node().Role())
}

// ─── §7.1 done-when: ordered commit with correct numbering ───────────────────

func TestCommandsCommitInOrderWithCorrectNumbering(t *testing.T) {
	d, sm := newSingleNode(t, t.TempDir())
	defer d.Close()
	elect(t, d)

	// The leader's first entry is the election no-op (§5c) at index 1, so the
	// commands land at 2, 3, 4.
	for _, cmd := range []string{"A", "B", "C"} {
		if err := d.Propose([]byte(cmd)); err != nil {
			t.Fatalf("Propose(%s): %v", cmd, err)
		}
	}

	n := d.Node()
	if got, want := n.LastIndex(), uint64(4); got != want {
		t.Errorf("LastIndex = %d, want %d", got, want)
	}
	if got, want := n.CommitIndex(), uint64(4); got != want {
		t.Errorf("CommitIndex = %d, want %d", got, want)
	}
	if got, want := n.LastApplied(), uint64(4); got != want {
		t.Errorf("LastApplied = %d, want %d", got, want)
	}

	// Applied in order, and the no-op never reached the state machine.
	if got, want := fmt.Sprint(sm.strings()), fmt.Sprint([]string{"A", "B", "C"}); got != want {
		t.Errorf("applied = %s, want %s", got, want)
	}

	// Contiguous indexes, all in the leader's term.
	for i := uint64(1); i <= 4; i++ {
		e := n.Log().At(i)
		if e.Index != i {
			t.Errorf("entry %d has Index %d", i, e.Index)
		}
		if e.Term != n.Term() {
			t.Errorf("entry %d has Term %d, want %d", i, e.Term, n.Term())
		}
	}
	if !n.Log().At(1).IsNoOp() {
		t.Error("index 1 should be the election no-op")
	}
}

// ─── §7.2 done-when: restart reloads term + log ──────────────────────────────

func TestRestartReloadsTermAndLog(t *testing.T) {
	dir := t.TempDir()

	d, _ := newSingleNode(t, dir)
	elect(t, d)
	for _, cmd := range []string{"x=1", "y=2", "z=3"} {
		if err := d.Propose([]byte(cmd)); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}
	wantTerm := d.Node().Term()
	wantVote := d.Node().VotedFor()
	wantLog := d.Node().Log().Entries()
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen.
	d2, sm2 := newSingleNode(t, dir)
	defer d2.Close()
	n2 := d2.Node()

	if n2.Term() != wantTerm {
		t.Errorf("reloaded Term = %d, want %d", n2.Term(), wantTerm)
	}
	if n2.VotedFor() != wantVote {
		t.Errorf("reloaded VotedFor = %d, want %d", n2.VotedFor(), wantVote)
	}
	if n2.Role() != Follower {
		t.Errorf("restarted node role = %s, want follower", n2.Role())
	}
	gotLog := n2.Log().Entries()
	if len(gotLog) != len(wantLog) {
		t.Fatalf("reloaded log has %d entries, want %d", len(gotLog), len(wantLog))
	}
	for i := range gotLog {
		if gotLog[i].Term != wantLog[i].Term || gotLog[i].Index != wantLog[i].Index ||
			!bytes.Equal(gotLog[i].Cmd, wantLog[i].Cmd) {
			t.Errorf("entry %d = %+v, want %+v", i, gotLog[i], wantLog[i])
		}
	}

	// commitIndex is volatile (§6): nothing is applied until this node leads
	// again, at which point every committed command is re-applied.
	if len(sm2.applied) != 0 {
		t.Errorf("nothing should be applied before re-election, got %v", sm2.strings())
	}
	elect(t, d2)
	if got, want := fmt.Sprint(sm2.strings()), fmt.Sprint([]string{"x=1", "y=2", "z=3"}); got != want {
		t.Errorf("re-applied = %s, want %s", got, want)
	}
}

// ─── §7.3 the election runs the real candidate path ──────────────────────────

func TestElectionRunsRealCandidatePath(t *testing.T) {
	d, _ := newSingleNode(t, t.TempDir())
	defer d.Close()
	n := d.Node()

	if n.Role() != Follower {
		t.Fatalf("fresh node role = %s, want follower", n.Role())
	}
	if n.Term() != 0 {
		t.Fatalf("fresh node term = %d, want 0", n.Term())
	}

	// Not yet timed out → still a follower.
	for i := 0; i < 5; i++ {
		if err := d.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if n.Role() != Follower {
		t.Errorf("role after 5 ticks = %s, want follower (timeout is >= 10)", n.Role())
	}

	elect(t, d)
	if n.Term() != 1 {
		t.Errorf("term after election = %d, want 1", n.Term())
	}
	if n.VotedFor() != n.ID() {
		t.Errorf("VotedFor = %d, want self (%d)", n.VotedFor(), n.ID())
	}
	if n.Quorum() != 1 {
		t.Errorf("Quorum() at N=1 = %d, want 1", n.Quorum())
	}
}

// ─── §7.4 term monotonicity and vote clearing ────────────────────────────────

func TestHigherTermDemotesAndClearsVote(t *testing.T) {
	d, _ := newSingleNode(t, t.TempDir())
	defer d.Close()
	elect(t, d)
	n := d.Node()

	if n.Role() != Leader || n.VotedFor() != n.ID() {
		t.Fatalf("setup: role=%s votedFor=%d", n.Role(), n.VotedFor())
	}
	before := n.Term()

	// A message from a later term: defer, adopt the term, forget the vote.
	// The vote request also loses on the up-to-date check (empty candidate log
	// vs our committed entry), so no vote is granted either.
	err := d.Step(Message{
		Type: MsgVoteReq, From: 2, To: n.ID(), Term: before + 5,
		LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatalf("Step: %v", err)
	}

	if n.Role() != Follower {
		t.Errorf("role = %s, want follower after a higher term", n.Role())
	}
	if n.Term() != before+5 {
		t.Errorf("term = %d, want %d", n.Term(), before+5)
	}
	if n.VotedFor() != None {
		t.Errorf("VotedFor = %d, want None — a stale vote must not carry into a new term", n.VotedFor())
	}

	// And terms never go backwards.
	if err := d.Step(Message{Type: MsgVoteReq, From: 2, To: n.ID(), Term: 1}); err != nil {
		t.Fatal(err)
	}
	if n.Term() != before+5 {
		t.Errorf("term = %d after a stale message, want %d", n.Term(), before+5)
	}
}

// ─── §7.8 Propose on a non-leader ────────────────────────────────────────────

func TestProposeOnFollowerIsRejected(t *testing.T) {
	d, sm := newSingleNode(t, t.TempDir())
	defer d.Close()

	if err := d.Propose([]byte("nope")); err != ErrNotLeader {
		t.Errorf("Propose on follower = %v, want ErrNotLeader", err)
	}
	if d.Node().LastIndex() != 0 {
		t.Errorf("a rejected proposal must not touch the log (LastIndex=%d)", d.Node().LastIndex())
	}
	if len(sm.applied) != 0 {
		t.Errorf("a rejected proposal must not apply anything")
	}
}

// ─── §7.9 the commit rule refuses previous-term entries ──────────────────────

// A 3-node leader must not commit an entry from an earlier term just because a
// majority holds it (Raft §5.4.2) — it commits it only once a current-term entry
// commits. This is the rule that stops committed data being lost across a
// leader change, and it is written in Phase 6 so Phase 8 adds no commit logic.
func TestCommitRuleRejectsPreviousTermEntry(t *testing.T) {
	st := NewMemStorage()
	// A log left over from term 1, restored into a node now in term 2.
	if err := st.AppendEntries([]Entry{{Term: 1, Index: 1, Cmd: []byte("old")}}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveHardState(HardState{Term: 2}); err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		ID: 1, Peers: []uint64{1, 2, 3},
		ElectionTimeout: 10, HeartbeatTimeout: 3, Storage: st, Seed: 7,
	}
	n, err := NewNode(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Force leadership at term 3 without the no-op, so index 1 (term 1) is the
	// only entry, and pretend a majority holds it.
	n.role = Leader
	n.currentTerm = 3
	n.nextIndex = map[uint64]uint64{1: 2, 2: 2, 3: 2}
	n.matchIndex = map[uint64]uint64{1: 1, 2: 1, 3: 1}
	n.maybeAdvanceCommit()

	if n.CommitIndex() != 0 {
		t.Fatalf("CommitIndex = %d: a previous-term entry must not be committed by replica count", n.CommitIndex())
	}

	// Now append a current-term entry and replicate it to a majority: both commit.
	n.appendEntry(Entry{Term: 3, Index: 2, Cmd: []byte("new")})
	n.matchIndex[2] = 2
	n.maybeAdvanceCommit()

	if n.CommitIndex() != 2 {
		t.Errorf("CommitIndex = %d, want 2 once a current-term entry is replicated", n.CommitIndex())
	}
}

// ─── §7.7 determinism ────────────────────────────────────────────────────────

// The property Phase 12's chaos suite is built on: the same script of
// Tick/Step/Propose against the same seed produces a byte-identical Ready
// sequence. If this ever fails, replaying a failing chaos run is impossible.
func TestReadySequenceIsDeterministic(t *testing.T) {
	script := func() string {
		n, err := NewNode(Config{
			ID: 1, Peers: []uint64{1, 2, 3},
			ElectionTimeout: 10, HeartbeatTimeout: 3,
			Storage: NewMemStorage(), Seed: 99,
		})
		if err != nil {
			t.Fatal(err)
		}
		var trace bytes.Buffer
		record := func() {
			rd := n.Ready()
			fmt.Fprintf(&trace, "hs=%v entries=%v msgs=%v committed=%v role=%s term=%d\n",
				rd.HardState, rd.EntriesToPersist, rd.Messages, rd.CommittedEntries, n.Role(), n.Term())
			n.Advance()
		}
		for i := 0; i < 40; i++ {
			n.Tick()
			record()
		}
		// Grant the votes so it becomes leader, then propose.
		for _, peer := range []uint64{2, 3} {
			_ = n.Step(Message{Type: MsgVoteResp, From: peer, To: 1, Term: n.Term(), Granted: true})
			record()
		}
		for i := 0; i < 5; i++ {
			_ = n.Propose([]byte(fmt.Sprintf("cmd-%d", i)))
			record()
		}
		return trace.String()
	}

	first, second := script(), script()
	if first != second {
		t.Errorf("Ready sequence is not deterministic across runs:\n--- first ---\n%s\n--- second ---\n%s", first, second)
	}
	if first == "" {
		t.Error("trace is empty — the script did nothing")
	}
}

// A companion to the determinism test: votes arriving as messages elect the
// leader through the same code path the N=1 case uses (§5b).
func TestMajorityOfThreeElectsViaVoteMessages(t *testing.T) {
	n, err := NewNode(Config{
		ID: 1, Peers: []uint64{1, 2, 3},
		ElectionTimeout: 10, HeartbeatTimeout: 3,
		Storage: NewMemStorage(), Seed: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if n.Quorum() != 2 {
		t.Fatalf("Quorum() at N=3 = %d, want 2", n.Quorum())
	}
	for i := 0; i < 40 && n.Role() != Candidate; i++ {
		n.Tick()
	}
	if n.Role() != Candidate {
		t.Fatalf("role = %s, want candidate", n.Role())
	}
	// One granted vote plus its own = 2 = quorum.
	if err := n.Step(Message{Type: MsgVoteResp, From: 2, To: 1, Term: n.Term(), Granted: true}); err != nil {
		t.Fatal(err)
	}
	if n.Role() != Leader {
		t.Errorf("role = %s, want leader after reaching quorum", n.Role())
	}
}

// ─── driver contract ─────────────────────────────────────────────────────────

// Entries must be durable before they are applied. Checking the storage file
// directly is the only way to see the ordering the contract promises (§2).
func TestEntriesAreDurableBeforeTheyAreApplied(t *testing.T) {
	dir := t.TempDir()
	d, sm := newSingleNode(t, dir)
	defer d.Close()
	elect(t, d)
	if err := d.Propose([]byte("durable")); err != nil {
		t.Fatal(err)
	}
	if len(sm.applied) != 1 {
		t.Fatalf("applied %d commands, want 1", len(sm.applied))
	}

	// Read the log file with a separate handle: if the command was applied, it
	// must already be on disk.
	st2, err := OpenFileStorage(filepath.Dir(filepath.Join(dir, LogName)))
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	entries, err := st2.LoadEntries()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range entries {
		if bytes.Equal(e.Cmd, []byte("durable")) {
			found = true
		}
	}
	if !found {
		t.Error("command was applied but is not on stable storage — the contract is broken")
	}
}
