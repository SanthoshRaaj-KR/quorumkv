package consensus

import (
	"bytes"
	"fmt"
	"testing"
)

// ─── deterministic in-process cluster ────────────────────────────────────────

// cluster runs N nodes in one goroutine over a [Bus]. No sockets, no timers, no
// scheduler: every test below is exactly reproducible. This helper is the
// scaffolding Phase 12's chaos suite grows out of.
type cluster struct {
	t     *testing.T
	bus   *Bus
	ids   []uint64
	nodes map[uint64]*Driver
	sms   map[uint64]*recorder
	cfgs  map[uint64]Config
	down  map[uint64]bool

	// Safety invariants, all enforced after *every* step (see checkInvariant).
	leaderByTerm map[uint64]uint64 // at most one leader per term
	committed    map[uint64]uint64 // index → term, once committed anywhere
	// appReqs counts AppendEntries delivered per destination, so a test can
	// assert catch-up takes few round trips rather than one per entry.
	appReqs map[uint64]int
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{
		t:            t,
		bus:          NewBus(),
		nodes:        make(map[uint64]*Driver),
		sms:          make(map[uint64]*recorder),
		cfgs:         make(map[uint64]Config),
		down:         make(map[uint64]bool),
		leaderByTerm: make(map[uint64]uint64),
		committed:    make(map[uint64]uint64),
		appReqs:      make(map[uint64]int),
	}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, uint64(i))
	}
	for _, id := range c.ids {
		cfg := Config{
			ID:               id,
			Peers:            c.ids,
			ElectionTimeout:  DefaultElectionTimeout,
			HeartbeatTimeout: DefaultHeartbeatTimeout,
			Storage:          NewMemStorage(),
			Seed:             int64(id) * 7, // distinct: identical seeds mean identical timeouts
		}
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

// tick advances every live node one tick, then delivers messages to quiescence.
func (c *cluster) tick() {
	c.t.Helper()
	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		if err := c.nodes[id].Tick(); err != nil {
			c.t.Fatalf("node %d Tick: %v", id, err)
		}
		c.checkInvariant()
	}
	c.route()
}

// route delivers queued messages until nothing is pending.
func (c *cluster) route() {
	c.t.Helper()
	for round := 0; round < 100; round++ {
		if c.bus.Pending() == 0 {
			return
		}
		for _, id := range c.ids {
			msgs := c.bus.Take(id)
			if c.down[id] {
				continue // a crashed node drops what was in flight to it
			}
			for _, m := range msgs {
				if m.Type == MsgAppReq {
					c.appReqs[id]++
				}
				if err := c.nodes[id].Step(m); err != nil {
					c.t.Fatalf("node %d Step(%v): %v", id, m.Type, err)
				}
				c.checkInvariant()
			}
		}
	}
	c.t.Fatal("message routing did not settle in 100 rounds")
}

func (c *cluster) tickN(n int) {
	c.t.Helper()
	for i := 0; i < n; i++ {
		c.tick()
	}
}

// checkInvariant asserts Raft's central safety property: **at most one leader
// per term**. Two leaders in one term would need two intersecting majorities,
// so a violation means the vote accounting is broken. Checked after every single
// step, not merely at the end of a test.
func (c *cluster) checkInvariant() {
	c.t.Helper()
	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		n := c.nodes[id].Node()
		if n.Role() != Leader {
			continue
		}
		if prev, ok := c.leaderByTerm[n.Term()]; ok && prev != id {
			c.t.Fatalf("two leaders in term %d: node %d and node %d", n.Term(), prev, id)
		}
		c.leaderByTerm[n.Term()] = id
	}
	c.checkCommittedNeverLost()
	c.checkLogMatching()
}

// checkCommittedNeverLost is the third done-when of Phase 8, as an invariant:
// once an index is committed anywhere, no node may ever hold a different entry
// at that index, and no node that had it may lose it.
//
// A node may have compacted its log past some of these indices (Phase 9):
// that is not loss, since the snapshot itself is the durable record of them,
// so the scan starts above whatever boundary this node currently holds.
func (c *cluster) checkCommittedNeverLost() {
	c.t.Helper()
	// Record everything newly committed.
	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		n := c.node(id)
		for i := n.Log().Offset() + 1; i <= n.CommitIndex(); i++ {
			term := n.Log().Term(i)
			if prev, ok := c.committed[i]; ok && prev != term {
				c.t.Fatalf("committed entry %d changed term %d → %d on node %d", i, prev, term, id)
			}
			c.committed[i] = term
		}
	}
	// And nobody may contradict or drop one.
	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		n := c.node(id)
		for i, term := range c.committed {
			if !n.Log().Has(i) {
				continue // not replicated here yet — allowed
			}
			if got := n.Log().Term(i); got != term {
				c.t.Fatalf("node %d holds term %d at committed index %d, want %d", id, got, i, term)
			}
		}
	}
}

// checkLogMatching asserts Raft's Log Matching Property: if two logs contain an
// entry with the same index and term, the logs are identical in every preceding
// entry. Equivalently — once two logs diverge at some index, they may never
// agree again above it.
//
// The comparison only covers indices both logs still hold (Phase 9): a node
// that has compacted past some of these has traded that range for a snapshot,
// which is a separate guarantee, not a Log Matching violation.
func (c *cluster) checkLogMatching() {
	c.t.Helper()
	for ai := 0; ai < len(c.ids); ai++ {
		for bi := ai + 1; bi < len(c.ids); bi++ {
			a, b := c.ids[ai], c.ids[bi]
			if c.down[a] || c.down[b] {
				continue
			}
			la, lb := c.node(a).Log(), c.node(b).Log()
			start := la.Offset()
			if lb.Offset() > start {
				start = lb.Offset()
			}
			limit := min(la.LastIndex(), lb.LastIndex())
			diverged := uint64(0)
			for i := start + 1; i <= limit; i++ {
				if la.Term(i) != lb.Term(i) {
					if diverged == 0 {
						diverged = i
					}
					continue
				}
				if diverged != 0 {
					c.t.Fatalf(
						"log matching violated: nodes %d and %d differ at index %d but agree again at %d",
						a, b, diverged, i)
				}
			}
		}
	}
}

// logsIdentical reports whether every live node's log agrees — same end point,
// same content everywhere both still hold it. Raw entries-list equality
// (Entries()) stopped being meaningful once nodes may sit at different
// snapshot boundaries (Phase 9): two logs can be the same replicated history
// while one holds more of its prefix than the other.
func (c *cluster) logsIdentical() bool {
	var ref *Log
	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		got := c.node(id).Log()
		if ref == nil {
			ref = got
			continue
		}
		if !logsMatch(ref, got) {
			return false
		}
	}
	return true
}

// logsMatch reports whether a and b end at the same (index, term) and agree
// on every entry in their overlapping range.
func logsMatch(a, b *Log) bool {
	if a.LastIndex() != b.LastIndex() || a.LastTerm() != b.LastTerm() {
		return false
	}
	start := a.Offset()
	if b.Offset() > start {
		start = b.Offset()
	}
	for i := start + 1; i <= a.LastIndex(); i++ {
		ea, eb := a.At(i), b.At(i)
		if ea.Term != eb.Term || !bytes.Equal(ea.Cmd, eb.Cmd) {
			return false
		}
	}
	return true
}

func (c *cluster) leaders() []uint64 {
	var out []uint64
	for _, id := range c.ids {
		if !c.down[id] && c.nodes[id].Node().Role() == Leader {
			out = append(out, id)
		}
	}
	return out
}

// waitLeader ticks until exactly one leader exists, or fails after maxTicks.
func (c *cluster) waitLeader(maxTicks int) uint64 {
	c.t.Helper()
	for i := 0; i < maxTicks; i++ {
		if l := c.leaders(); len(l) == 1 {
			return l[0]
		}
		c.tick()
	}
	c.t.Fatalf("no single leader after %d ticks (leaders=%v)", maxTicks, c.leaders())
	return 0
}

// crash stops a node ticking and drops messages to it — a kill -9, not a
// partition: the node simply stops existing until restart.
func (c *cluster) crash(id uint64) {
	c.down[id] = true
	c.bus.Isolate(id)
}

// restart brings a crashed node back from its persisted state. It returns as a
// follower, as Raft requires.
func (c *cluster) restart(id uint64) {
	c.t.Helper()
	sm := &recorder{}
	d, err := NewDriver(c.cfgs[id], sm, c.bus.Transport(id))
	if err != nil {
		c.t.Fatalf("restart node %d: %v", id, err)
	}
	c.nodes[id] = d
	c.sms[id] = sm
	c.down[id] = false
	c.bus.Heal(id)
}

func (c *cluster) node(id uint64) *Node { return c.nodes[id].Node() }

// ─── §6.1 done-when: exactly one leader ──────────────────────────────────────

func TestThreeNodesElectExactlyOneLeader(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader(200)

	term := c.node(leader).Term()
	followers := 0
	for _, id := range c.ids {
		n := c.node(id)
		if id == leader {
			continue
		}
		if n.Role() != Follower {
			t.Errorf("node %d role = %s, want follower", id, n.Role())
		}
		if n.Term() != term {
			t.Errorf("node %d term = %d, want %d (the leader's)", id, n.Term(), term)
		}
		if n.LeaderID() != leader {
			t.Errorf("node %d LeaderID = %d, want %d", id, n.LeaderID(), leader)
		}
		followers++
	}
	if followers != 2 {
		t.Errorf("got %d followers, want 2", followers)
	}
}

// ─── §6.2 done-when: kill the leader → a new one within a second ─────────────

func TestKillingTheLeaderElectsANewOne(t *testing.T) {
	c := newCluster(t, 3)
	old := c.waitLeader(200)
	oldTerm := c.node(old).Term()

	c.crash(old)

	// A tick is 10ms in production, so "within a second" is 100 ticks.
	var fresh uint64
	for i := 0; i < 100; i++ {
		c.tick()
		if l := c.leaders(); len(l) == 1 {
			fresh = l[0]
			break
		}
	}
	if fresh == 0 {
		t.Fatalf("no new leader within 100 ticks (~1s) of killing node %d", old)
	}
	if fresh == old {
		t.Fatalf("the crashed node %d cannot be the new leader", old)
	}
	if got := c.node(fresh).Term(); got <= oldTerm {
		t.Errorf("new leader term = %d, want > %d — a new leader means a new term", got, oldTerm)
	}
}

// ─── §6.3 done-when: the old leader returns as a follower ────────────────────

func TestRestartedLeaderRejoinsAsFollowerWithoutDisruption(t *testing.T) {
	c := newCluster(t, 3)
	old := c.waitLeader(200)
	c.crash(old)

	var fresh uint64
	for i := 0; i < 100 && fresh == 0; i++ {
		c.tick()
		if l := c.leaders(); len(l) == 1 {
			fresh = l[0]
		}
	}
	if fresh == 0 {
		t.Fatal("no new leader after the kill")
	}
	freshTerm := c.node(fresh).Term()

	c.restart(old)
	if r := c.node(old).Role(); r != Follower {
		t.Errorf("a restarted node comes back as %s, want follower", r)
	}

	// Let it hear the incumbent's heartbeats.
	c.tickN(60)

	if l := c.leaders(); len(l) != 1 || l[0] != fresh {
		t.Errorf("leaders after the old leader rejoined = %v, want just %d", l, fresh)
	}
	if got := c.node(fresh).Term(); got != freshTerm {
		t.Errorf("the incumbent's term changed from %d to %d — the rejoin was disruptive", freshTerm, got)
	}
	if got := c.node(old).LeaderID(); got != fresh {
		t.Errorf("rejoined node follows leader %d, want %d", got, fresh)
	}
}

// ─── §6.6 heartbeats suppress elections ──────────────────────────────────────

func TestHeartbeatsPreventFurtherElections(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitLeader(200)
	term := c.node(leader).Term()

	// 20 × the election timeout with a healthy leader: nobody should ever even
	// become a candidate.
	for i := 0; i < 20*DefaultElectionTimeout; i++ {
		c.tick()
		for _, id := range c.ids {
			if r := c.node(id).Role(); r == Candidate {
				t.Fatalf("node %d became a candidate at tick %d despite a live leader", id, i)
			}
		}
	}

	if l := c.leaders(); len(l) != 1 || l[0] != leader {
		t.Errorf("leaders = %v, want just %d", l, leader)
	}
	if got := c.node(leader).Term(); got != term {
		t.Errorf("term drifted from %d to %d under a healthy leader", term, got)
	}
}

// ─── §6.7 a split vote resolves ──────────────────────────────────────────────

// One cluster-wide seed — the natural way to make a chaos run reproducible.
// Every node must still draw an *independent* timeout stream, or a vote split
// can never break and the cluster livelocks forever. NewNode mixes the node ID
// in for exactly this reason; this test is the guard on that.
func TestSplitVoteResolvesUnderAClusterWideSeed(t *testing.T) {
	c := newCluster(t, 3)
	for _, id := range c.ids {
		cfg := c.cfgs[id]
		cfg.Seed = 1234
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

	leader := c.waitLeader(500)
	if leader == 0 {
		t.Fatal("the cluster livelocked on a split vote")
	}
	t.Logf("resolved to leader %d in term %d", leader, c.node(leader).Term())
}

// ─── §6.8 majority arithmetic on five nodes ──────────────────────────────────

func TestFiveNodeClusterToleratesTwoFailures(t *testing.T) {
	c := newCluster(t, 5)
	if q := c.node(1).Quorum(); q != 3 {
		t.Fatalf("Quorum() at N=5 = %d, want 3", q)
	}

	leader := c.waitLeader(300)
	c.crash(leader)

	// 4 alive, quorum 3 → still electable.
	second := c.waitLeader(300)
	c.crash(second)

	// 3 alive, quorum 3 → still electable, but with zero slack.
	third := c.waitLeader(300)

	// Now kill a third: 2 alive of 5, no majority possible. Nobody may lead.
	c.crash(third)
	c.tickN(300)
	if l := c.leaders(); len(l) != 0 {
		t.Errorf("leaders = %v with only 2 of 5 alive — a minority must never elect", l)
	}
}

// ─── §6.5 a stale log cannot win ─────────────────────────────────────────────

// The §5.4.1 election restriction, end to end: a node that missed recent writes
// must be denied every vote. This is what stops committed data being erased.
func TestStaleNodeIsDeniedEveryVote(t *testing.T) {
	ids := []uint64{1, 2, 3}
	mk := func(id uint64, entries []Entry, term uint64) *Node {
		st := NewMemStorage()
		if len(entries) > 0 {
			if err := st.AppendEntries(entries); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.SaveHardState(HardState{Term: term}); err != nil {
			t.Fatal(err)
		}
		n, err := NewNode(Config{
			ID: id, Peers: ids,
			ElectionTimeout: DefaultElectionTimeout, HeartbeatTimeout: DefaultHeartbeatTimeout,
			Storage: st, Seed: int64(id),
		})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}

	// Nodes 1 and 2 hold entries through term 5; node 3 fell behind at term 1.
	upToDate := []Entry{
		{Term: 1, Index: 1, Cmd: []byte("a")},
		{Term: 5, Index: 2, Cmd: []byte("b")},
	}
	stale := []Entry{{Term: 1, Index: 1, Cmd: []byte("a")}}

	n1 := mk(1, upToDate, 5)
	n2 := mk(2, upToDate, 5)
	n3 := mk(3, stale, 5)

	// Node 3 campaigns with its stale log.
	for i := 0; i < 100 && n3.Role() != Candidate; i++ {
		n3.Tick()
	}
	if n3.Role() != Candidate {
		t.Fatal("node 3 never campaigned")
	}
	req := Message{
		Type: MsgVoteReq, From: 3, Term: n3.Term(),
		LastLogIndex: n3.LastIndex(), LastLogTerm: n3.Log().LastTerm(),
	}

	for _, voter := range []*Node{n1, n2} {
		req.To = voter.ID()
		if err := voter.Step(req); err != nil {
			t.Fatal(err)
		}
		rd := voter.Ready()
		voter.Advance()
		if len(rd.Messages) != 1 {
			t.Fatalf("node %d sent %d replies, want 1", voter.ID(), len(rd.Messages))
		}
		if rd.Messages[0].Granted {
			t.Errorf("node %d granted a vote to the stale node 3 — §5.4.1 is broken", voter.ID())
		}
	}

	// And the up-to-date node still wins with the same voters.
	for i := 0; i < 100 && n1.Role() != Candidate; i++ {
		n1.Tick()
	}
	n1.Ready()
	n1.Advance()
	req2 := Message{
		Type: MsgVoteReq, From: 1, To: 2, Term: n1.Term(),
		LastLogIndex: n1.LastIndex(), LastLogTerm: n1.Log().LastTerm(),
	}
	if err := n2.Step(req2); err != nil {
		t.Fatal(err)
	}
	rd := n2.Ready()
	n2.Advance()
	if len(rd.Messages) != 1 || !rd.Messages[0].Granted {
		t.Errorf("node 2 denied the up-to-date candidate: %+v", rd.Messages)
	}
}

// A candidate that hears a valid heartbeat has lost, and must step down at once.
func TestCandidateStepsDownOnValidHeartbeat(t *testing.T) {
	n, err := NewNode(Config{
		ID: 1, Peers: []uint64{1, 2, 3},
		ElectionTimeout: DefaultElectionTimeout, HeartbeatTimeout: DefaultHeartbeatTimeout,
		Storage: NewMemStorage(), Seed: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100 && n.Role() != Candidate; i++ {
		n.Tick()
	}
	if n.Role() != Candidate {
		t.Fatal("never campaigned")
	}

	if err := n.Step(Message{Type: MsgAppReq, From: 2, To: 1, Term: n.Term()}); err != nil {
		t.Fatal(err)
	}
	if n.Role() != Follower {
		t.Errorf("role = %s after a same-term heartbeat, want follower", n.Role())
	}
	if n.LeaderID() != 2 {
		t.Errorf("LeaderID = %d, want 2", n.LeaderID())
	}
}

// A heartbeat that fails the log-match check must still count as proof of life
// (§3) — otherwise a follower with a stale log campaigns against a healthy leader.
func TestHeartbeatResetsTimerEvenWhenLogMismatches(t *testing.T) {
	n, err := NewNode(Config{
		ID: 1, Peers: []uint64{1, 2, 3},
		ElectionTimeout: DefaultElectionTimeout, HeartbeatTimeout: DefaultHeartbeatTimeout,
		Storage: NewMemStorage(), Seed: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	// A heartbeat referring to entries this empty node has never seen.
	hb := Message{Type: MsgAppReq, From: 2, To: 1, Term: 4, PrevLogIndex: 9, PrevLogTerm: 3, LeaderCommit: 9}

	for i := 0; i < 10*DefaultElectionTimeout; i++ {
		if err := n.Step(hb); err != nil {
			t.Fatal(err)
		}
		rd := n.Ready()
		n.Advance()
		for _, m := range rd.Messages {
			if m.Type == MsgAppResp && m.Success {
				t.Fatal("the log-match check should have failed")
			}
		}
		n.Tick()
		if n.Role() != Follower {
			t.Fatalf("node became %s despite a live leader (tick %d)", n.Role(), i)
		}
	}
	if n.CommitIndex() != 0 {
		t.Errorf("CommitIndex = %d: a follower must not commit entries it cannot prove it holds", n.CommitIndex())
	}
}

// Duplicated and reordered messages must be harmless — terms plus the
// once-per-term vote make both non-events.
func TestDuplicateAndReorderedMessagesAreHarmless(t *testing.T) {
	c := newCluster(t, 3)
	// Deliver every batch reversed, and duplicated.
	c.bus.Reorder = func(msgs []Message) []Message {
		out := make([]Message, 0, len(msgs)*2)
		for i := len(msgs) - 1; i >= 0; i-- {
			out = append(out, msgs[i], msgs[i])
		}
		return out
	}
	leader := c.waitLeader(300)
	c.tickN(100)
	if l := c.leaders(); len(l) != 1 || l[0] != leader {
		t.Errorf("leaders = %v under duplication+reordering, want just %d", l, leader)
	}
}

func TestClusterInvariantHoldsAcrossRepeatedFailures(t *testing.T) {
	c := newCluster(t, 5)
	for round := 0; round < 4; round++ {
		leader := c.waitLeader(400)
		c.crash(leader)
		c.tickN(50)
		c.restart(leader)
		c.tickN(50)
	}
	// checkInvariant ran after every step above; a violation would already have
	// failed the test. Confirm the cluster is still functional.
	if l := c.leaders(); len(l) != 1 {
		t.Errorf("leaders after 4 crash/restart rounds = %v, want exactly one", l)
	}
	t.Logf("leader history by term: %v", fmt.Sprint(c.leaderByTerm))
}
