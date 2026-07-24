package clientrpc

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"quorumkv/consensus"
	"quorumkv/consensus/engine"
)

// ─── a minimal StateMachine + Getter, standing in for the real engine sidecar ─
//
// These tests prove the RPC layer (redirect-following, retry, leader-only
// reads) is correct. They deliberately don't spin up a real Rust sidecar
// process — that's consensus/engine's own integration tests' job — so a
// plain in-memory map is enough here.

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
var _ Getter = (*memSM)(nil)

// ─── a real 3-node cluster: real TCP Raft + real HTTP clientrpc ─────────────

type testNode struct {
	clientAddr string
	server     *consensus.Server
	tr         *consensus.TCPTransport
	sm         *memSM
	ln         net.Listener
	httpSrv    *http.Server
}

type testCluster struct {
	t     *testing.T
	ids   []uint64
	nodes map[uint64]*testNode
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	c := &testCluster{t: t, nodes: make(map[uint64]*testNode)}
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
			Storage: consensus.NewMemStorage(), Seed: int64(id) * 31,
		}, sm, trs[id], trs[id].Inbound(), 5*time.Millisecond)
		if err != nil {
			t.Fatalf("server %d: %v", id, err)
		}
		srv.Start()

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("client listener %d: %v", id, err)
		}
		httpSrv := &http.Server{Handler: NewServer(srv, sm, time.Second).Handler()}
		go httpSrv.Serve(ln)

		c.nodes[id] = &testNode{
			clientAddr: ln.Addr().String(),
			server:     srv, tr: trs[id], sm: sm, ln: ln, httpSrv: httpSrv,
		}
	}
	return c
}

func (c *testCluster) stop() {
	for _, n := range c.nodes {
		_ = n.server.Stop()
		_ = n.tr.Close()
		_ = n.httpSrv.Close()
	}
}

func (c *testCluster) clientAddrs() map[uint64]string {
	m := make(map[uint64]string, len(c.nodes))
	for id, n := range c.nodes {
		m[id] = n.clientAddr
	}
	return m
}

func (c *testCluster) waitLeader(timeout time.Duration) uint64 {
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
	c.t.Fatalf("no single leader within %v", timeout)
	return 0
}

// ─── tests ────────────────────────────────────────────────────────────────

func TestClientPutGetRoundTrip(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.stop()
	c.waitLeader(10 * time.Second)

	cl := New(c.clientAddrs())
	if err := cl.Put([]byte("name"), []byte("quorumkv")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	val, ok, err := cl.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || !bytes.Equal(val, []byte("quorumkv")) {
		t.Errorf("Get = (%q, %v), want (\"quorumkv\", true)", val, ok)
	}

	if err := cl.Delete([]byte("name")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, err := cl.Get([]byte("name")); err != nil || ok {
		t.Errorf("Get after Delete = ok=%v err=%v, want ok=false", ok, err)
	}
}

// TestClientWorksFromAnyStartingNode is the done-when, literally: a client
// pointed at any of the three addresses first must still succeed, not just
// the one that happens to already be leader.
func TestClientWorksFromAnyStartingNode(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.stop()
	c.waitLeader(10 * time.Second)

	for _, start := range c.ids {
		cl := New(c.clientAddrs())
		cl.last = start // force this call's first attempt, whitebox (internal test)
		if err := cl.Put([]byte("k"), []byte("v")); err != nil {
			t.Errorf("starting from node %d: Put: %v", start, err)
		}
	}
}

// TestClientSurvivesLeaderElection is ROADMAP's phase-11 done-when: the
// caller writes a single Put/Get, the leader dies mid-session, and the same
// Client keeps working through the resulting election with zero
// caller-written retry logic.
func TestClientSurvivesLeaderElection(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.stop()
	leader := c.waitLeader(10 * time.Second)

	cl := New(c.clientAddrs())
	if err := cl.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("initial put: %v", err)
	}

	n := c.nodes[leader]
	_ = n.server.Stop()
	_ = n.tr.Close()
	_ = n.httpSrv.Close()
	delete(c.nodes, leader)

	if err := cl.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("put after leader death: %v", err)
	}
	val, ok, err := cl.Get([]byte("b"))
	if err != nil || !ok || !bytes.Equal(val, []byte("2")) {
		t.Fatalf("get after election: val=%q ok=%v err=%v", val, ok, err)
	}
}

// TestHandlePutRedirectsOnFollower proves the wire-level shape of §2a's
// redirect: a follower answers 503 with a leaderId hint, not a bare error a
// client would have no way to act on.
func TestHandlePutRedirectsOnFollower(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.stop()
	leader := c.waitLeader(10 * time.Second)

	var follower uint64
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}

	body, _ := json.Marshal(putRequest{Key: []byte("k"), Value: []byte("v")})
	resp, err := http.Post("http://"+c.nodes[follower].clientAddr+"/put", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post to follower: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	var errResp errorResponse
	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if errResp.LeaderID != leader {
		t.Errorf("leaderId = %d, want %d", errResp.LeaderID, leader)
	}
}
