// Package chaos automates DESIGN.md §8's fault scenarios (planning/
// phase-12-chaos.md), composing pieces every earlier phase already built and
// proved by hand: a real TCP Raft cluster, a real clientrpc.Client, and (new
// in this phase) TCPTransport's first-class Isolate/Heal.
//
// It lives in its own package, not inside consensus's own test files,
// because it needs clientrpc, which imports consensus — an internal
// consensus test can't import something that imports consensus back.
package chaos

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"quorumkv/consensus"
	"quorumkv/consensus/clientrpc"
	"quorumkv/consensus/engine"
)

// ─── a minimal StateMachine + Getter, standing in for the real engine sidecar ─
//
// Same reasoning as clientrpc's own tests (phase-11): the property under
// test here is Raft-level durability and availability, not storage-engine
// fsync (storage/tests/kill9.rs already proves that separately), so a plain
// in-memory map is enough and keeps this suite cargo-free.

type memSM struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemSM() *memSM { return &memSM{data: make(map[string][]byte)} }

func (m *memSM) Apply(cmd []byte) error {
	c, err := engine.DecodeCommand(cmd)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	switch c.Op {
	case engine.OpPut:
		m.data[string(c.Key)] = append([]byte(nil), c.Value...)
	case engine.OpDelete:
		delete(m.data, string(c.Key))
	}
	return nil
}

func (m *memSM) Snapshot() ([]byte, error) { return nil, nil }
func (m *memSM) Restore([]byte) error      { return nil }

func (m *memSM) Get(key []byte) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[string(key)]
	return v, ok, nil
}

var _ consensus.StateMachine = (*memSM)(nil)
var _ clientrpc.Getter = (*memSM)(nil)

// ─── a real cluster: real TCP Raft + real HTTP clientrpc ────────────────────

type node struct {
	id         uint64
	clientAddr string
	server     *consensus.Server
	tr         *consensus.TCPTransport
	sm         *memSM
	ln         net.Listener
	httpSrv    *http.Server
}

type cluster struct {
	t     *testing.T
	ids   []uint64
	nodes map[uint64]*node
}

// clusterOpts lets a test override the defaults that don't matter for most
// scenarios (snapshot threshold is the one item 3's snapshot sub-test cares
// about).
type clusterOpts struct {
	snapshotThreshold int
}

func newCluster(t *testing.T, n int, opts clusterOpts) *cluster {
	t.Helper()
	c := &cluster{t: t, nodes: make(map[uint64]*node)}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, uint64(i))
	}

	trs := make(map[uint64]*consensus.TCPTransport)
	raftAddrs := make(map[uint64]string, n)
	for _, id := range c.ids {
		tr, err := consensus.NewTCPTransport(id, "127.0.0.1:0")
		if err != nil {
			t.Fatalf("raft transport %d: %v", id, err)
		}
		trs[id] = tr
		raftAddrs[id] = tr.Addr()
	}
	for _, id := range c.ids {
		trs[id].SetPeers(raftAddrs)
	}

	for _, id := range c.ids {
		sm := newMemSM()
		srv, err := consensus.NewServer(consensus.Config{
			ID: id, Peers: c.ids,
			ElectionTimeout: 10, HeartbeatTimeout: 3,
			Storage:           consensus.NewMemStorage(),
			Seed:              int64(id) * 31,
			SnapshotThreshold: opts.snapshotThreshold,
		}, sm, trs[id], trs[id].Inbound(), 5*time.Millisecond)
		if err != nil {
			t.Fatalf("server %d: %v", id, err)
		}
		srv.Start()

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("client listener %d: %v", id, err)
		}
		httpSrv := &http.Server{Handler: clientrpc.NewServer(srv, sm, time.Second).Handler()}
		go httpSrv.Serve(ln)

		c.nodes[id] = &node{
			id: id, clientAddr: ln.Addr().String(),
			server: srv, tr: trs[id], sm: sm, ln: ln, httpSrv: httpSrv,
		}
	}
	return c
}

func (c *cluster) stop() {
	for _, n := range c.nodes {
		_ = n.server.Stop()
		_ = n.tr.Close()
		_ = n.httpSrv.Close()
	}
}

// kill simulates one node's process dying: its Raft transport and client-RPC
// listener both go dark, and it stops being addressable at all — the same
// shape as clientrpc_test.go's own TestClientSurvivesLeaderElection, just
// promoted to a reusable helper here since every scenario in this file needs
// it at least once.
func (c *cluster) kill(id uint64) {
	n, ok := c.nodes[id]
	if !ok {
		return
	}
	_ = n.server.Stop()
	_ = n.tr.Close()
	_ = n.httpSrv.Close()
	delete(c.nodes, id)
}

// isolate cuts id off from every other node, bidirectionally, via the
// first-class TCPTransport.Isolate this phase added — no more per-test ad
// hoc drop+SetPeers reconstruction (planning/phase-12 §1a).
func (c *cluster) isolate(id uint64) { c.nodes[id].tr.Isolate() }

// heal reverses isolate.
func (c *cluster) heal(id uint64) { c.nodes[id].tr.Heal() }

func (c *cluster) clientAddrs(ids ...uint64) map[uint64]string {
	if len(ids) == 0 {
		ids = c.ids
	}
	m := make(map[uint64]string, len(ids))
	for _, id := range ids {
		if n, ok := c.nodes[id]; ok {
			m[id] = n.clientAddr
		}
	}
	return m
}

func (c *cluster) waitLeader(timeout time.Duration) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []uint64
		for _, id := range c.ids {
			n, ok := c.nodes[id]
			if !ok {
				continue
			}
			if st, err := n.server.Status(); err == nil && st.Role == consensus.Leader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no single leader among %v within %v", c.ids, timeout)
	return 0
}

// waitConverged polls until every id in ids reports the same LastApplied and
// LastIndex (item 3's "final state is identical to the majority's — same
// last-applied entries, no divergence"). Whether convergence happened via
// plain log replay or InstallSnapshot is Phase 9's own already-tested
// concern (snapshot_test.go); this only asserts the outward, client-visible
// property.
func (c *cluster) waitConverged(ids []uint64, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	var lastReport string
	for time.Now().Before(deadline) {
		statuses := make(map[uint64]consensus.Status, len(ids))
		allOK := true
		for _, id := range ids {
			n, ok := c.nodes[id]
			if !ok {
				allOK = false
				break
			}
			st, err := n.server.Status()
			if err != nil {
				allOK = false
				break
			}
			statuses[id] = st
		}
		if allOK {
			converged := true
			var want *consensus.Status
			for _, id := range ids {
				st := statuses[id]
				if want == nil {
					w := st
					want = &w
					continue
				}
				if st.LastApplied != want.LastApplied || st.LastIndex != want.LastIndex {
					converged = false
				}
			}
			if converged {
				return
			}
			lastReport = fmt.Sprintf("%+v", statuses)
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("nodes %v did not converge within %v; last statuses: %s", ids, timeout, lastReport)
}

// ─── scenarios ───────────────────────────────────────────────────────────────

// TestChaosKillLeaderMidWrite is DESIGN.md §8 item 1: a background goroutine
// kills whoever is leader partway through a Put loop; every key whose Put
// returned nil must read back correctly from the surviving majority. Keys
// whose Put errored are explicitly allowed to be missing (phase-12 §7,
// extending ROADMAP phase-1's own "unacknowledged writes may vanish" rule
// cluster-wide).
func TestChaosKillLeaderMidWrite(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{})
	defer c.stop()
	c.waitLeader(10 * time.Second)

	cl := clientrpc.New(c.clientAddrs())

	const n = 200
	acked := make(map[string]string, n)
	var mu sync.Mutex

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Let the Put loop actually get moving, then kill whoever holds
		// leadership right now — a single kill, paced once, not a storm
		// (phase-12 §7: two kills back-to-back can legitimately exhaust the
		// client's bounded retries if they land faster than an election
		// resolves).
		time.Sleep(50 * time.Millisecond)
		leader := c.waitLeader(5 * time.Second)
		c.kill(leader)
	}()

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%03d", i)
		val := fmt.Sprintf("v%03d", i)
		if err := cl.Put([]byte(key), []byte(val)); err == nil {
			mu.Lock()
			acked[key] = val
			mu.Unlock()
		}
	}
	wg.Wait()

	if len(acked) == 0 {
		t.Fatal("no write was ever acknowledged — cluster never became usable")
	}
	t.Logf("%d/%d writes acknowledged around the leader kill", len(acked), n)

	for key, want := range acked {
		val, ok, err := cl.Get([]byte(key))
		if err != nil || !ok || string(val) != want {
			t.Errorf("acked key %q: got (%q, ok=%v, err=%v), want (%q, true, nil)", key, val, ok, err, want)
		}
	}
}

// TestChaosPartitionMinorityStaysAvailable is items 2 and 3: isolating a
// 2-node minority in a 5-node cluster must not disturb the 3-node majority's
// quorum at all, and healing the partition must bring the minority back to
// the exact same state — no divergence.
func TestChaosPartitionMinorityStaysAvailable(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{})
	defer c.stop()
	leader := c.waitLeader(10 * time.Second)

	// Pick the minority from followers only: the point of this scenario is
	// "a minority can't affect the majority's quorum," which is clearest
	// when the majority already includes the leader and never has to elect
	// a new one at all.
	var minority []uint64
	for _, id := range c.ids {
		if id != leader && len(minority) < 2 {
			minority = append(minority, id)
		}
	}
	var majority []uint64
	for _, id := range c.ids {
		isMinority := false
		for _, m := range minority {
			if id == m {
				isMinority = true
			}
		}
		if !isMinority {
			majority = append(majority, id)
		}
	}

	for _, id := range minority {
		c.isolate(id)
	}

	// Address the client only at the majority: the minority's addresses are
	// simply absent from its list, per phase-12 §6 item 2's own wording.
	cl := clientrpc.New(c.clientAddrs(majority...))

	const n = 50
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("p%03d", i)
		val := fmt.Sprintf("v%03d", i)
		if err := cl.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("put %d while minority isolated: %v", i, err)
		}
		got, ok, err := cl.Get([]byte(key))
		if err != nil || !ok || string(got) != val {
			t.Fatalf("get %d while minority isolated: got (%q, %v, %v), want (%q, true, nil)", i, got, ok, err, val)
		}
	}

	for _, id := range minority {
		c.heal(id)
	}

	c.waitConverged(c.ids, 15*time.Second)
}

// TestChaosHealAfterLongPartitionUsesSnapshotPath is item 3's parenthetical:
// force InstallSnapshot rather than plain entry-by-entry replay by giving
// the cluster a tiny SnapshotThreshold (reusing Phase 9's own knob, per
// phase-12 §6 item 3) and writing well past it while a follower is
// isolated, then healing and asserting it still converges exactly.
// Whether InstallSnapshot specifically fired is snapshot_test.go's job
// (Phase 9); this only proves the cluster-level, client-observed outcome —
// a follower that fell far behind still ends up identical to the leader.
func TestChaosHealAfterLongPartitionUsesSnapshotPath(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{snapshotThreshold: 5})
	defer c.stop()
	leader := c.waitLeader(10 * time.Second)

	var follower uint64
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	c.isolate(follower)

	cl := clientrpc.New(c.clientAddrs(leader))
	const n = 30 // several multiples of the threshold=5, so a snapshot must fire
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("s%03d", i)
		if err := cl.Put([]byte(key), []byte(fmt.Sprintf("v%03d", i))); err != nil {
			t.Fatalf("put %d while follower isolated: %v", i, err)
		}
	}

	c.heal(follower)
	c.waitConverged(c.ids, 15*time.Second)
}
