package consensus

import (
	"fmt"
	"testing"
	"time"
)

// Phase 8 — log replication. Every test here runs against the deterministic Bus
// cluster, whose checkInvariant now enforces three safety properties after
// *every* step: one leader per term, no committed entry ever lost, and the Log
// Matching Property.

// propose submits cmd on the current leader and lets the cluster settle.
func (c *cluster) propose(leader uint64, cmd string) {
	c.t.Helper()
	if err := c.nodes[leader].Propose([]byte(cmd)); err != nil {
		c.t.Fatalf("Propose(%s) on node %d: %v", cmd, leader, err)
	}
	c.route()
}

// applied returns what a node's state machine has consumed, as strings.
func (c *cluster) applied(id uint64) []string { return c.sms[id].strings() }

// ─── §6.1 done-when: same order everywhere ───────────────────────────────────

func TestWritesReplicateToAllFollowersInOrder(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader(200)

	var want []string
	for i := 0; i < 20; i++ {
		cmd := fmt.Sprintf("PUT k%02d v%02d", i, i)
		c.propose(leader, cmd)
		want = append(want, cmd)
	}
	// Let the commit index propagate so followers apply.
	c.tickN(20)

	if !c.logsIdentical() {
		t.Fatal("logs diverged across the cluster")
	}
	for _, id := range c.ids {
		got := c.applied(id)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("node %d applied\n got %v\nwant %v", id, got, want)
		}
	}

	// The commit rule actually fired — Phase 7 could not get here.
	n := c.node(leader)
	if n.CommitIndex() != n.LastIndex() {
		t.Errorf("leader commitIndex = %d, want %d (its lastIndex)", n.CommitIndex(), n.LastIndex())
	}
}

// ─── §6.2 done-when: a lagging follower catches up, in few round trips ───────

func TestStoppedFollowerCatchesUpAfterFiftyWrites(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader(200)

	var lagging uint64
	for _, id := range c.ids {
		if id != leader {
			lagging = id
			break
		}
	}
	c.crash(lagging)

	for i := 0; i < 50; i++ {
		c.propose(leader, fmt.Sprintf("write-%02d", i))
	}
	c.tickN(10)

	before := c.appReqs[lagging]
	c.restart(lagging)

	// Tick only until convergence, so the round-trip count measures catch-up
	// rather than the heartbeats that keep flowing afterwards.
	converged := false
	for i := 0; i < 200 && !converged; i++ {
		c.tick()
		converged = c.logsIdentical()
	}
	if !converged {
		t.Fatal("the restarted follower did not converge to the leader's log")
	}
	if got, want := c.node(lagging).LastIndex(), c.node(leader).LastIndex(); got != want {
		t.Errorf("restarted follower lastIndex = %d, want %d", got, want)
	}

	// Catch-up must not cost one round trip per entry. With the §1 hint the
	// leader jumps straight to the follower's end; the batch cap (64) then
	// carries all 50 in a single message.
	roundTrips := c.appReqs[lagging] - before
	if roundTrips > 5 {
		t.Errorf("catch-up took %d AppendEntries; the conflict hint should need a handful, not one per entry", roundTrips)
	}
	t.Logf("caught up 50 entries in %d AppendEntries (linear decrement would need ~50)", roundTrips)
}

// ─── §6.5 THE test: a delayed duplicate must truncate nothing ────────────────

// The trap in Raft §5.3. A leader sends 1–3, the follower appends and replies,
// the reply is lost. The follower then receives 4–6 and appends those too. The
// delayed duplicate of 1–3 arrives again. "Truncate at prevLogIndex+1 then
// append" would discard 4–6 — entries that may already be committed on a
// majority. AppendEntries must be idempotent.
func TestDelayedDuplicateAppendEntriesTruncatesNothing(t *testing.T) {
	n, err := NewNode(Config{
		ID: 1, Peers: []uint64{1, 2, 3},
		ElectionTimeout: DefaultElectionTimeout, HeartbeatTimeout: DefaultHeartbeatTimeout,
		Storage: NewMemStorage(), Seed: 11,
	})
	if err != nil {
		t.Fatal(err)
	}

	first := []Entry{
		{Term: 1, Index: 1, Cmd: []byte("a")},
		{Term: 1, Index: 2, Cmd: []byte("b")},
		{Term: 1, Index: 3, Cmd: []byte("c")},
	}
	second := []Entry{
		{Term: 1, Index: 4, Cmd: []byte("d")},
		{Term: 1, Index: 5, Cmd: []byte("e")},
		{Term: 1, Index: 6, Cmd: []byte("f")},
	}

	deliver := func(prevIdx, prevTerm uint64, entries []Entry, leaderCommit uint64) Ready {
		msg := Message{
			Type: MsgAppReq, From: 2, To: 1, Term: 1,
			PrevLogIndex: prevIdx, PrevLogTerm: prevTerm,
			Entries: entries, LeaderCommit: leaderCommit,
		}
		if err := n.Step(msg); err != nil {
			t.Fatal(err)
		}
		rd := n.Ready()
		n.Advance()
		return rd
	}

	deliver(0, 0, first, 0)
	deliver(3, 1, second, 6)
	if n.LastIndex() != 6 {
		t.Fatalf("setup: lastIndex = %d, want 6", n.LastIndex())
	}
	if n.CommitIndex() != 6 {
		t.Fatalf("setup: commitIndex = %d, want 6", n.CommitIndex())
	}

	// The delayed duplicate of 1–3 arrives.
	rd := deliver(0, 0, first, 3)

	if rd.TruncateFrom != nil {
		t.Fatalf("a duplicate AppendEntries truncated from index %d — committed entries destroyed", *rd.TruncateFrom)
	}
	if len(rd.EntriesToPersist) != 0 {
		t.Errorf("a duplicate re-persisted %d entries; it must be a no-op", len(rd.EntriesToPersist))
	}
	if n.LastIndex() != 6 {
		t.Fatalf("lastIndex = %d after the duplicate, want 6 — entries 4–6 were lost", n.LastIndex())
	}
	for i, want := range []string{"a", "b", "c", "d", "e", "f"} {
		if got := string(n.Log().At(uint64(i + 1)).Cmd); got != want {
			t.Errorf("index %d = %q, want %q", i+1, got, want)
		}
	}
	// And commitIndex must not regress on the stale leaderCommit.
	if n.CommitIndex() != 6 {
		t.Errorf("commitIndex = %d after a stale leaderCommit=3, want 6", n.CommitIndex())
	}
}

// The follower's commit bound must be the last *new* entry, not its local last
// index — otherwise a delayed message with a high leaderCommit commits entries
// it never covered (§2).
func TestFollowerCommitIsBoundedByLastNewEntry(t *testing.T) {
	n, err := NewNode(Config{
		ID: 1, Peers: []uint64{1, 2, 3},
		ElectionTimeout: DefaultElectionTimeout, HeartbeatTimeout: DefaultHeartbeatTimeout,
		Storage: NewMemStorage(), Seed: 12,
	})
	if err != nil {
		t.Fatal(err)
	}

	// The follower holds five entries...
	err = n.Step(Message{
		Type: MsgAppReq, From: 2, To: 1, Term: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []Entry{
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 1, Index: 4}, {Term: 1, Index: 5},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	n.Ready()
	n.Advance()

	// ...but a message covering only up to index 2 claims leaderCommit 5.
	if err := n.Step(Message{
		Type: MsgAppReq, From: 2, To: 1, Term: 1,
		PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 5,
	}); err != nil {
		t.Fatal(err)
	}
	if n.CommitIndex() != 2 {
		t.Errorf("commitIndex = %d, want 2 — bounded by the last entry this RPC covered", n.CommitIndex())
	}
}

// ─── §6.6 a genuinely divergent suffix is replaced ───────────────────────────

func TestDivergentSuffixIsTruncatedAndReplaced(t *testing.T) {
	st := NewMemStorage()
	// A follower carrying entries from a dead term 2.
	if err := st.AppendEntries([]Entry{
		{Term: 1, Index: 1, Cmd: []byte("keep")},
		{Term: 2, Index: 2, Cmd: []byte("doomed-a")},
		{Term: 2, Index: 3, Cmd: []byte("doomed-b")},
		{Term: 2, Index: 4, Cmd: []byte("doomed-c")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveHardState(HardState{Term: 2}); err != nil {
		t.Fatal(err)
	}
	n, err := NewNode(Config{
		ID: 1, Peers: []uint64{1, 2, 3},
		ElectionTimeout: DefaultElectionTimeout, HeartbeatTimeout: DefaultHeartbeatTimeout,
		Storage: st, Seed: 13,
	})
	if err != nil {
		t.Fatal(err)
	}

	// A term-5 leader overwrites from index 2.
	if err := n.Step(Message{
		Type: MsgAppReq, From: 2, To: 1, Term: 5,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []Entry{
			{Term: 5, Index: 2, Cmd: []byte("real-a")},
			{Term: 5, Index: 3, Cmd: []byte("real-b")},
		},
	}); err != nil {
		t.Fatal(err)
	}
	rd := n.Ready()
	n.Advance()

	if rd.TruncateFrom == nil || *rd.TruncateFrom != 2 {
		t.Fatalf("TruncateFrom = %v, want 2", rd.TruncateFrom)
	}
	if len(rd.EntriesToPersist) != 2 {
		t.Errorf("persisted %d entries, want 2", len(rd.EntriesToPersist))
	}
	if n.LastIndex() != 3 {
		t.Errorf("lastIndex = %d, want 3 — the divergent suffix must be gone", n.LastIndex())
	}
	if got := string(n.Log().At(1).Cmd); got != "keep" {
		t.Errorf("index 1 = %q; the matching prefix must survive", got)
	}
	for i, want := range map[uint64]string{2: "real-a", 3: "real-b"} {
		if got := string(n.Log().At(i).Cmd); got != want {
			t.Errorf("index %d = %q, want %q", i, got, want)
		}
	}
}

// The rejection hint must let the leader skip a whole run of a dead term in one
// hop rather than backing up one index at a time (§1).
func TestRejectionHintSkipsAWholeTerm(t *testing.T) {
	st := NewMemStorage()
	var entries []Entry
	entries = append(entries, Entry{Term: 1, Index: 1})
	for i := uint64(2); i <= 40; i++ {
		entries = append(entries, Entry{Term: 2, Index: i}) // a long run of term 2
	}
	if err := st.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}
	n, err := NewNode(Config{
		ID: 1, Peers: []uint64{1, 2, 3},
		ElectionTimeout: DefaultElectionTimeout, HeartbeatTimeout: DefaultHeartbeatTimeout,
		Storage: st, Seed: 14,
	})
	if err != nil {
		t.Fatal(err)
	}

	// A leader probes at index 40 claiming term 7 — a mismatch.
	if err := n.Step(Message{
		Type: MsgAppReq, From: 2, To: 1, Term: 7, PrevLogIndex: 40, PrevLogTerm: 7,
	}); err != nil {
		t.Fatal(err)
	}
	rd := n.Ready()
	n.Advance()

	if len(rd.Messages) != 1 {
		t.Fatalf("got %d replies, want 1", len(rd.Messages))
	}
	resp := rd.Messages[0]
	if resp.Success {
		t.Fatal("the probe should have been rejected")
	}
	if resp.ConflictTerm != 2 {
		t.Errorf("ConflictTerm = %d, want 2", resp.ConflictTerm)
	}
	if resp.ConflictIndex != 2 {
		t.Errorf("ConflictIndex = %d, want 2 (the first index of that term)", resp.ConflictIndex)
	}
}

// ─── §6.7 a minority cannot commit ───────────────────────────────────────────

func TestMinorityCannotCommit(t *testing.T) {
	c := newCluster(t, 5)
	leader := c.waitLeader(300)

	// Kill three of five: the leader survives but can never reach a majority.
	killed := 0
	for _, id := range c.ids {
		if id != leader && killed < 3 {
			c.crash(id)
			killed++
		}
	}

	before := c.node(leader).CommitIndex()
	for i := 0; i < 10; i++ {
		if err := c.nodes[leader].Propose([]byte(fmt.Sprintf("doomed-%d", i))); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		c.route()
	}
	c.tickN(100)

	if got := c.node(leader).CommitIndex(); got != before {
		t.Errorf("commitIndex advanced from %d to %d with only 2 of 5 alive", before, got)
	}
	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		if n := len(c.applied(id)); n != 0 {
			t.Errorf("node %d applied %d commands without a majority", id, n)
		}
	}
}

// ─── §6.8 batch caps ─────────────────────────────────────────────────────────

// The caps only bind when a follower is actually behind. Proposing with the
// cluster settled after every write sends one entry per message and proves
// nothing, so this builds a real backlog first by crashing a follower.
func TestAppendEntriesRespectsBatchCaps(t *testing.T) {
	const (
		maxEntries = 8
		maxBytes   = 512
		cmdSize    = 100 // ~120 bytes on the wire: the byte cap binds before the entry cap
	)

	c := newCluster(t, 3)
	for _, id := range c.ids {
		cfg := c.cfgs[id]
		cfg.MaxEntriesPerAppend = maxEntries
		cfg.MaxBytesPerAppend = maxBytes
		cfg.Storage = NewMemStorage()
		c.cfgs[id] = cfg
		sm := &recorder{}
		d, err := NewDriver(cfg, sm, c.bus.Transport(id))
		if err != nil {
			t.Fatal(err)
		}
		c.nodes[id] = d
		c.sms[id] = sm
	}

	var worstEntries, worstBytes int
	c.bus.Reorder = func(msgs []Message) []Message {
		for _, m := range msgs {
			if m.Type != MsgAppReq || len(m.Entries) == 0 {
				continue
			}
			size := 0
			for _, e := range m.Entries {
				size += len(e.Cmd) + 20
			}
			if len(m.Entries) > worstEntries {
				worstEntries = len(m.Entries)
			}
			if size > worstBytes {
				worstBytes = size
			}
		}
		return msgs
	}

	leader := c.waitLeader(300)
	var lagging uint64
	for _, id := range c.ids {
		if id != leader {
			lagging = id
			break
		}
	}

	// Build a backlog this follower cannot see.
	c.crash(lagging)
	payload := make([]byte, cmdSize)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	for i := 0; i < 200; i++ {
		c.propose(leader, fmt.Sprintf("%04d-%s", i, payload))
	}
	c.tickN(10)

	// Now let it catch up: every one of those AppendEntries is a full batch.
	c.restart(lagging)
	c.tickN(200)

	if worstEntries > maxEntries {
		t.Errorf("an AppendEntries carried %d entries, cap is %d", worstEntries, maxEntries)
	}
	if worstBytes > maxBytes {
		t.Errorf("an AppendEntries carried %d bytes, cap is %d", worstBytes, maxBytes)
	}
	if worstEntries < 2 {
		t.Errorf("largest batch was %d entries — no backlog formed, so the caps were never exercised", worstEntries)
	}
	if !c.logsIdentical() {
		t.Error("capped batching failed to converge the logs")
	}
	t.Logf("largest batch: %d entries / %d bytes (caps: %d / %d)", worstEntries, worstBytes, maxEntries, maxBytes)
}

// ─── §6.9 restart preserves the replicated log ───────────────────────────────

func TestRollingRestartPreservesTheLog(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader(200)
	for i := 0; i < 15; i++ {
		c.propose(leader, fmt.Sprintf("v%02d", i))
	}
	c.tickN(20)
	wantLen := len(c.node(leader).Log().Entries())

	for _, id := range c.ids {
		c.crash(id)
		c.tickN(40)
		c.restart(id)
		c.tickN(60)

		if got := len(c.node(id).Log().Entries()); got < wantLen {
			t.Errorf("node %d came back with %d entries, want at least %d", id, got, wantLen)
		}
	}

	c.waitLeader(300)
	c.tickN(60)
	if !c.logsIdentical() {
		t.Error("logs diverged across a rolling restart")
	}
}

// Followers must never apply an entry before it is committed — otherwise a
// leader change could expose data that is then overwritten.
func TestFollowersApplyOnlyCommittedEntries(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader(200)

	for i := 0; i < 10; i++ {
		c.propose(leader, fmt.Sprintf("cmd-%d", i))
	}
	c.tickN(20)

	for _, id := range c.ids {
		n := c.node(id)
		if n.LastApplied() > n.CommitIndex() {
			t.Errorf("node %d applied %d but committed only %d", id, n.LastApplied(), n.CommitIndex())
		}
		// Every applied command must exist in the log at a committed index.
		if uint64(len(c.applied(id))) > n.CommitIndex() {
			t.Errorf("node %d applied %d commands, commitIndex is %d", id, len(c.applied(id)), n.CommitIndex())
		}
	}
}

// ─── §6.10 TCP end to end ────────────────────────────────────────────────────

func TestTCPClusterReplicatesToAllNodes(t *testing.T) {
	c := newTCPCluster(t, 3)
	defer c.stopAll()

	leader, _ := c.waitLeader(10*time.Second, nil)

	want := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		cmd := fmt.Sprintf("PUT key%d val%d", i, i)
		if err := c.servers[leader].Propose([]byte(cmd)); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		want = append(want, cmd)
	}

	// Poll until every node's commitIndex covers all 10 commands plus the
	// leader's election no-op.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		done := 0
		for _, id := range c.ids {
			st, err := c.servers[id].Status()
			if err == nil && st.LastApplied >= 11 {
				done++
			}
		}
		if done == len(c.ids) {
			t.Logf("all 3 nodes applied 10 commands over real sockets")
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, id := range c.ids {
		st, _ := c.servers[id].Status()
		t.Logf("node %d: %+v", id, st)
	}
	t.Fatal("replication did not reach all nodes within 15s")
}
