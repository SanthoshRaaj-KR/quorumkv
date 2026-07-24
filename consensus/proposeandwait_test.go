package consensus

import (
	"testing"
	"time"
)

// ─── §11.3 ProposeAndWait ────────────────────────────────────────────────────
//
// These reuse tcp_test.go's newTCPCluster helper — ProposeAndWait's whole
// point is to observe real commit/apply timing, which only exists with a real
// clock and real sockets (the deterministic Bus/Sandbox tests drive Driver
// directly and skip Server entirely).

// TestProposeAndWaitResolvesOnApply proves the wait is on LastApplied, not
// just on the leader's own local append — a GET immediately after a
// successful ProposeAndWait must see the write (phase-11 §3, done-when item 4).
func TestProposeAndWaitResolvesOnApply(t *testing.T) {
	c := newTCPCluster(t, 3)
	defer c.stopAll()

	leader, _ := c.waitLeader(10*time.Second, nil)

	idx, err := c.servers[leader].ProposeAndWait([]byte("PUT k v"), 5*time.Second)
	if err != nil {
		t.Fatalf("ProposeAndWait: %v", err)
	}
	if idx == 0 {
		t.Fatal("ProposeAndWait returned index 0")
	}

	st, err := c.servers[leader].Status()
	if err != nil {
		t.Fatal(err)
	}
	if st.LastApplied < idx {
		t.Errorf("LastApplied=%d, want >= %d — ProposeAndWait returned before apply", st.LastApplied, idx)
	}
}

// TestProposeAndWaitTimesOutWithoutQuorum proves a proposal that can never
// reach a majority (both followers stopped) times out rather than hanging
// forever or falsely reporting success.
func TestProposeAndWaitTimesOutWithoutQuorum(t *testing.T) {
	c := newTCPCluster(t, 3)
	defer c.stopAll()

	leader, _ := c.waitLeader(10*time.Second, nil)

	for _, id := range c.ids {
		if id == leader {
			continue
		}
		_ = c.servers[id].Stop()
		_ = c.trs[id].Close()
	}

	_, err := c.servers[leader].ProposeAndWait([]byte("PUT k v"), 300*time.Millisecond)
	if err != ErrProposeTimeout {
		t.Errorf("ProposeAndWait with no reachable quorum = %v, want ErrProposeTimeout", err)
	}
}

// TestProposeAndWaitDetectsLostProposal forces the scenario ErrProposalLost
// exists for: the leader that accepted a proposal loses leadership before it
// commits, and a new leader overwrites that same index with a different
// entry. The old (now stale) node must report the loss, not hang and not
// falsely report success (phase-11 §3, done-when item 5).
func TestProposeAndWaitDetectsLostProposal(t *testing.T) {
	c := newTCPCluster(t, 3)
	defer c.stopAll()

	leader, term := c.waitLeader(10*time.Second, nil)
	var followers []uint64
	for _, id := range c.ids {
		if id != leader {
			followers = append(followers, id)
		}
	}

	// Cut the leader off from both followers, bidirectionally, before it
	// proposes: closes any already-open sockets and clears every transport's
	// address book so nothing can redial across the cut.
	c.isolate(leader)

	waitErr := make(chan error, 1)
	go func() {
		_, err := c.servers[leader].ProposeAndWait([]byte("PUT k v-old"), 10*time.Second)
		waitErr <- err
	}()

	// The isolated leader can't heartbeat; the two followers should elect a
	// new leader among themselves once their election timeout fires.
	newLeader, newTerm := c.waitLeaderAmong(10*time.Second, followers)
	if newTerm <= term {
		t.Fatalf("new term %d, want > old term %d", newTerm, term)
	}

	// The new leader's log is still exactly where the old leader's was before
	// the cut (the old leader's proposal never replicated), so this proposal
	// lands at the same index the isolated leader is waiting on, just under
	// the new term.
	if err := c.servers[newLeader].Propose([]byte("PUT k v-new")); err != nil {
		t.Fatalf("propose on new leader: %v", err)
	}

	// Reconnect everyone: the old leader will hear the new term, step down,
	// and accept the new leader's AppendEntries, truncating and overwriting
	// its own pending entry.
	c.heal()

	select {
	case err := <-waitErr:
		if err != ErrProposalLost {
			t.Errorf("ProposeAndWait on the deposed leader = %v, want ErrProposalLost", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("ProposeAndWait never resolved after healing the partition")
	}
}

// isolate cuts id off from every other node in c, bidirectionally: closes any
// already-open connections and clears each transport's address book so
// neither side can redial. TCPTransport has no first-class partition
// primitive (unlike Bus.Isolate) — this reconstructs one from its existing
// per-peer connection bookkeeping, for tests only.
func (c *tcpCluster) isolate(id uint64) {
	for _, other := range c.ids {
		if other == id {
			continue
		}
		c.trs[other].drop(id)
		c.trs[id].drop(other)
	}
	c.trs[id].SetPeers(nil)
	for _, other := range c.ids {
		if other == id {
			continue
		}
		addrs := make(map[uint64]string)
		for _, x := range c.ids {
			if x != other && x != id {
				addrs[x] = c.trs[x].Addr()
			}
		}
		c.trs[other].SetPeers(addrs)
	}
}

// heal restores every transport's full address book, undoing isolate.
func (c *tcpCluster) heal() {
	addrs := make(map[uint64]string, len(c.ids))
	for _, id := range c.ids {
		addrs[id] = c.trs[id].Addr()
	}
	for _, id := range c.ids {
		full := make(map[uint64]string, len(addrs)-1)
		for peer, a := range addrs {
			if peer != id {
				full[peer] = a
			}
		}
		c.trs[id].SetPeers(full)
	}
}

// waitLeaderAmong is [tcpCluster.waitLeader] restricted to a candidate subset
// of nodes, for scenarios where the excluded node is deliberately partitioned
// off and must not be counted.
func (c *tcpCluster) waitLeaderAmong(timeout time.Duration, among []uint64) (uint64, uint64) {
	c.t.Helper()
	skip := make(map[uint64]bool, len(c.ids))
	for _, id := range c.ids {
		skip[id] = true
	}
	for _, id := range among {
		skip[id] = false
	}
	return c.waitLeader(timeout, skip)
}
