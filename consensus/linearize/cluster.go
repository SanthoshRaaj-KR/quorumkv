package linearize

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"quorumkv/consensus"
	"quorumkv/consensus/clientrpc"
	"quorumkv/consensus/engine"
)

// This file is the only one in the package that knows a real Raft cluster
// exists (package doc, history.go) — it assembles Phase 11's own building
// blocks (consensus.Server + TCPTransport + clientrpc.Server) into a
// reusable harness instead of test-only scaffolding, so both the fast
// integration test and the slow flagship benchmark (checkpoints 2 and 4)
// can share it. Backed by an in-memory stub StateMachine, not the real
// Rust engine: this package checks Raft-level consistency, which the
// storage engine's own durability has nothing to do with (phase-14 §2) —
// the real engine's crash safety is Phase 1/13's job, already proven.

// memSM is the harness's StateMachine: an in-memory map, decoding commands
// the same way the real engine.StateMachine does (clientrpc always encodes
// via engine.EncodeCommand regardless of what backs it), so it's a drop-in
// stand-in behind a real clientrpc.Server.
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

// ─── the cluster itself ──────────────────────────────────────────────────

type clusterNode struct {
	clientAddr string
	server     *consensus.Server
	tr         *consensus.TCPTransport
	sm         *memSM
	ln         net.Listener
	httpSrv    *http.Server
}

// cluster is a real N-node Raft cluster over real TCP sockets, each node
// also serving the real client-facing HTTP protocol (Phase 11) — the exact
// shape a real deployment (cmd/quorumkv-node) has, minus the Rust sidecar.
type cluster struct {
	ids   []uint64
	nodes map[uint64]*clusterNode
}

// newCluster builds a cluster with the real, unmodified client-facing
// protocol on every node.
func newCluster(n int, seed int64) (*cluster, error) {
	return buildCluster(n, seed, func(srv *consensus.Server, sm *memSM) http.Handler {
		return clientrpc.NewServer(srv, sm, 2*time.Second).Handler()
	})
}

// buildCluster is newCluster generalized over what serves the client-facing
// HTTP protocol on each node. The real path (newCluster) always uses the
// genuine clientrpc.Server; the mutation test (mutation_test.go, §5.3)
// is the only other caller, substituting a deliberately buggy handler to
// prove Check() actually catches a real violation instead of just agreeing
// with itself on synthetic histories.
func buildCluster(n int, seed int64, handlerFor func(srv *consensus.Server, sm *memSM) http.Handler) (*cluster, error) {
	c := &cluster{nodes: make(map[uint64]*clusterNode)}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, uint64(i))
	}

	trs := make(map[uint64]*consensus.TCPTransport)
	raftAddrs := make(map[uint64]string, n)
	for _, id := range c.ids {
		tr, err := consensus.NewTCPTransport(id, "127.0.0.1:0")
		if err != nil {
			return nil, fmt.Errorf("linearize: raft transport %d: %w", id, err)
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
			Storage: consensus.NewMemStorage(), Seed: seed + int64(id)*31,
		}, sm, trs[id], trs[id].Inbound(), 5*time.Millisecond)
		if err != nil {
			return nil, fmt.Errorf("linearize: server %d: %w", id, err)
		}
		srv.Start()

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, fmt.Errorf("linearize: client listener %d: %w", id, err)
		}
		httpSrv := &http.Server{Handler: handlerFor(srv, sm)}
		go httpSrv.Serve(ln)

		c.nodes[id] = &clusterNode{
			clientAddr: ln.Addr().String(),
			server:     srv, tr: trs[id], sm: sm, ln: ln, httpSrv: httpSrv,
		}
	}
	return c, nil
}

func (c *cluster) stop() {
	for _, n := range c.nodes {
		_ = n.server.Stop()
		_ = n.tr.Close()
		_ = n.httpSrv.Close()
	}
}

func (c *cluster) clientAddrs() map[uint64]string {
	m := make(map[uint64]string, len(c.nodes))
	for id, n := range c.nodes {
		m[id] = n.clientAddr
	}
	return m
}

func (c *cluster) waitLeader(timeout time.Duration) (uint64, error) {
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
			return leaders[0], nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return 0, fmt.Errorf("linearize: no single leader within %v", timeout)
}

// killLeader stops the current leader's server, transport and HTTP
// listener — a crash, not a graceful shutdown — and removes it from
// cluster bookkeeping so later waitLeader calls correctly exclude it.
// Clients already holding its address in their static list (Phase 11 §5)
// simply treat it like any other unreachable node and hop past it — the
// harness doesn't need to special-case this at all.
func (c *cluster) killLeader() (uint64, error) {
	id, err := c.waitLeader(10 * time.Second)
	if err != nil {
		return 0, err
	}
	n := c.nodes[id]
	_ = n.server.Stop()
	_ = n.tr.Close()
	_ = n.httpSrv.Close()
	delete(c.nodes, id)
	return id, nil
}

// ─── the workload ────────────────────────────────────────────────────────

// RunConfig configures a fault-injected run against a real cluster (§4).
type RunConfig struct {
	Nodes      int           // cluster size (default 3)
	Keys       int           // shared keyspace size clients contend over (default 20)
	Clients    int           // concurrent client goroutines (default 10)
	Duration   time.Duration // how long the workload runs (default 2s)
	KillLeader bool          // kill the current leader partway through, once
	Seed       int64         // op-selection randomness
}

func (cfg RunConfig) withDefaults() RunConfig {
	if cfg.Nodes <= 0 {
		cfg.Nodes = 3
	}
	if cfg.Keys <= 0 {
		cfg.Keys = 20
	}
	if cfg.Clients <= 0 {
		cfg.Clients = 10
	}
	if cfg.Duration <= 0 {
		cfg.Duration = 2 * time.Second
	}
	return cfg
}

// RunResult is what a fault-injected run produces.
type RunResult struct {
	History      *History
	LeaderKilled uint64 // 0 if cfg.KillLeader was false or the kill never completed
}

// Run drives cfg against a fresh real cluster: cfg.Clients goroutines issue
// Put/Get/Delete against a shared keyspace of only cfg.Keys keys —
// deliberately small relative to the client count, so operations on the
// same key genuinely overlap in real time (§4.1, the case that actually
// exercises the checker; disjoint per-client keys would prove nothing) —
// for cfg.Duration, through a real clientrpc.Client that finds the leader
// and survives an election entirely on its own (Phase 11). If
// cfg.KillLeader, a separate goroutine kills the current leader about a
// third of the way through, in real time, while the workload keeps running.
//
// Every call's outcome — including failures and timeouts — is recorded
// into the returned History (§4.3); an unacknowledged write is never
// simply dropped, since that would hide exactly the bug class this package
// exists to catch.
func Run(cfg RunConfig) (*RunResult, error) {
	cfg = cfg.withDefaults()

	c, err := newCluster(cfg.Nodes, cfg.Seed)
	if err != nil {
		return nil, err
	}
	defer c.stop()

	if _, err := c.waitLeader(10 * time.Second); err != nil {
		return nil, err
	}

	result := &RunResult{History: NewHistory()}
	keys := make([]string, cfg.Keys)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%03d", i)
	}
	var opID atomic.Uint64

	stop := make(chan struct{})
	var wg sync.WaitGroup

	if cfg.KillLeader {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(cfg.Duration / 3)
			if id, err := c.killLeader(); err == nil {
				result.LeaderKilled = id
			}
		}()
	}

	addrs := c.clientAddrs()
	for i := 0; i < cfg.Clients; i++ {
		wg.Add(1)
		go func(clientSeed int64) {
			defer wg.Done()
			runClient(clientrpc.New(addrs), keys, result.History, &opID, clientSeed, stop)
		}(cfg.Seed + int64(i)*97)
	}

	time.Sleep(cfg.Duration)
	close(stop)
	wg.Wait()

	return result, nil
}

// runClient loops Put/Get/Delete against a random key from keys until stop
// closes, recording every call into h.
func runClient(cl *clientrpc.Client, keys []string, h *History, opID *atomic.Uint64, seed int64, stop <-chan struct{}) {
	rng := rand.New(rand.NewSource(seed))
	for {
		select {
		case <-stop:
			return
		default:
		}
		key := keys[rng.Intn(len(keys))]
		switch rng.Intn(3) {
		case 0:
			tagged := fmt.Sprintf("op-%d", opID.Add(1))
			h.Do(key, OpPut, tagged, func() (Value, bool) {
				err := cl.Put([]byte(key), []byte(tagged))
				return Value{}, err == nil
			})
		case 1:
			h.Do(key, OpDelete, "", func() (Value, bool) {
				err := cl.Delete([]byte(key))
				return Value{}, err == nil
			})
		case 2:
			h.Do(key, OpGet, "", func() (Value, bool) {
				val, found, err := cl.Get([]byte(key))
				if err != nil {
					return Value{}, false
				}
				return Value{Found: found, Data: string(val)}, true
			})
		}
	}
}
