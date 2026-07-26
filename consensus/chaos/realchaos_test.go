//go:build realchaos

// Real-process variant of item 1 (planning/phase-12-chaos.md §6 item 1's
// "real-process variant"): actual compiled quorumkv-node binaries (each
// with its own real Rust engine sidecar subprocess), a real OS kill -9 on
// the leader's whole process tree, mirroring storage/tests/kill9.rs's own
// philosophy — a real induced crash, not a simulated one — at the cluster
// level instead of the storage layer alone.
//
// Gated behind the realchaos build tag, deliberately not part of the
// default `go test ./...` run (phase-12 §6/§7): it needs `cargo` on PATH,
// compiles a real Go binary, and pays real wall-clock seconds for real
// elections and real sidecar startup. Run explicitly:
//
//	go test -tags=realchaos -run TestRealChaos -v -timeout 3m ./consensus/chaos/
package chaos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"quorumkv/consensus/clientrpc"
)

// realStatus mirrors clientrpc's own (unexported) statusResponse shape —
// duplicated here rather than exported from clientrpc solely for a test,
// same call this file makes for every other wire type it needs.
type realStatus struct {
	ID       uint64 `json:"id"`
	Role     string `json:"role"`
	Term     uint64 `json:"term"`
	LeaderID uint64 `json:"leaderId"`
}

func fetchStatus(addr string) (realStatus, error) {
	resp, err := http.Get("http://" + addr + "/status")
	if err != nil {
		return realStatus{}, err
	}
	defer resp.Body.Close()
	var st realStatus
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return realStatus{}, err
	}
	return st, nil
}

// syncBuffer collects a real subprocess's stdout+stderr for post-mortem
// logging (dumped only on failure) without racing the pipe-reading goroutine
// net/http/exec sets up internally against the test goroutine reading it back.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func nodeBinName() string {
	if runtime.GOOS == "windows" {
		return "quorumkv-node.exe"
	}
	return "quorumkv-node"
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate a free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

// killTree kills a node's whole OS process tree in one shot — the point of
// this variant: a genuine crash takes the engine sidecar down with it too,
// since nothing survives to run quorumkv-node's own graceful-shutdown path
// (its os.Interrupt/SIGTERM handler, cmd/quorumkv-node/main.go's
// killSidecar) that would otherwise kill the sidecar cleanly. `taskkill /T`
// walks the real OS process tree by parent-child relationship, no advance
// process-group setup required. Non-Windows falls back to killing just the
// direct child — this test suite's own dev/CI environment is Windows; a
// future non-Windows run would need the process-group equivalent
// (Setpgid + kill(-pid)) added here.
func killTree(pid int) {
	if runtime.GOOS == "windows" {
		_ = exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(pid)).Run()
		return
	}
	if p, err := os.FindProcess(pid); err == nil {
		_ = p.Kill()
	}
}

type realNode struct {
	id         uint64
	clientAddr string
	cmd        *exec.Cmd
	log        *syncBuffer
}

type realCluster struct {
	t     *testing.T
	ids   []uint64
	nodes map[uint64]*realNode
}

func newRealCluster(t *testing.T, n int) *realCluster {
	t.Helper()
	if _, err := exec.LookPath("cargo"); err != nil {
		t.Skip("cargo not on PATH; skipping real-process chaos test")
	}

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	consensusDir := filepath.Dir(wd) // consensus/chaos -> consensus

	binPath := filepath.Join(t.TempDir(), nodeBinName())
	build := exec.Command("go", "build", "-o", binPath, "./cmd/quorumkv-node")
	build.Dir = consensusDir
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build quorumkv-node: %v\n%s", err, out)
	}

	c := &realCluster{t: t, nodes: make(map[uint64]*realNode)}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, uint64(i))
	}

	raftAddrs := make(map[uint64]string, n)
	clientAddrs := make(map[uint64]string, n)
	for _, id := range c.ids {
		raftAddrs[id] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
		clientAddrs[id] = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	}
	peerParts := make([]string, 0, n)
	for _, id := range c.ids {
		peerParts = append(peerParts, fmt.Sprintf("%d=%s", id, raftAddrs[id]))
	}
	peersStr := strings.Join(peerParts, ",")

	dataRoot := t.TempDir()
	for _, id := range c.ids {
		dataDir := filepath.Join(dataRoot, fmt.Sprintf("node%d", id))
		cmd := exec.Command(binPath,
			"-id", strconv.FormatUint(id, 10),
			"-data", dataDir,
			"-client", clientAddrs[id],
			"-peers", peersStr,
		)
		cmd.Dir = consensusDir // so quorumkv-node's default -storage-dir=../storage resolves
		logBuf := &syncBuffer{}
		cmd.Stdout = logBuf
		cmd.Stderr = logBuf
		if err := cmd.Start(); err != nil {
			t.Fatalf("start node %d: %v", id, err)
		}
		c.nodes[id] = &realNode{id: id, clientAddr: clientAddrs[id], cmd: cmd, log: logBuf}
	}

	// Every node's client RPC — and, transitively, its own real Rust sidecar,
	// which quorumkv-node waits on before it ever binds this listener — must
	// be reachable before anything else in the test can rely on the cluster.
	deadline := time.Now().Add(60 * time.Second)
	for _, id := range c.ids {
		nd := c.nodes[id]
		for {
			if _, err := fetchStatus(nd.clientAddr); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d never became reachable within 60s; log:\n%s", id, nd.log.String())
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	t.Cleanup(func() {
		if t.Failed() {
			for _, id := range c.ids {
				if nd, ok := c.nodes[id]; ok {
					t.Logf("node %d output:\n%s", id, nd.log.String())
				}
			}
		}
	})

	return c
}

func (c *realCluster) stopAll() {
	for id := range c.nodes {
		c.kill(id)
	}
}

// kill is a real kill -9 (Windows: TerminateProcess via taskkill /T /F,
// including the node's own sidecar child) — not a graceful shutdown.
func (c *realCluster) kill(id uint64) {
	nd, ok := c.nodes[id]
	if !ok {
		return
	}
	delete(c.nodes, id)
	if nd.cmd.Process != nil {
		killTree(nd.cmd.Process.Pid)
	}
	_ = nd.cmd.Wait()
}

func (c *realCluster) clientAddrs(ids ...uint64) map[uint64]string {
	if len(ids) == 0 {
		ids = c.ids
	}
	m := make(map[uint64]string, len(ids))
	for _, id := range ids {
		if nd, ok := c.nodes[id]; ok {
			m[id] = nd.clientAddr
		}
	}
	return m
}

func (c *realCluster) waitLeader(timeout time.Duration) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []uint64
		for _, id := range c.ids {
			nd, ok := c.nodes[id]
			if !ok {
				continue
			}
			st, err := fetchStatus(nd.clientAddr)
			if err != nil {
				continue // still starting, or just killed
			}
			if st.Role == "leader" {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("no single leader among %v within %v", c.ids, timeout)
	return 0
}

// TestRealChaosKillLeaderMidWrite is DESIGN.md §8 item 1, at real-process
// fidelity: three real quorumkv-node binaries (six OS processes counting
// sidecars), a real HTTP clientrpc.Client, and a real `kill -9` of the
// leader's whole process tree partway through a Put loop. Every key whose
// Put returned nil must still read back correctly from the surviving
// majority; keys whose Put errored are allowed to be missing, the same
// unacknowledged-write rule the in-process TestChaosKillLeaderMidWrite
// asserts.
func TestRealChaosKillLeaderMidWrite(t *testing.T) {
	c := newRealCluster(t, 3)
	defer c.stopAll()

	const leaderTimeout = 60 * time.Second
	c.waitLeader(leaderTimeout)

	cl := clientrpc.New(c.clientAddrs())

	// Fewer iterations than the in-process variant: each Put here is a real
	// HTTP round trip through a real Raft commit to a real Rust sidecar
	// write, not an in-memory stub — this suite's whole point is fidelity,
	// not iteration count.
	const n = 40
	acked := make(map[string]string, n)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(300 * time.Millisecond) // let real writes get moving first
		leader := c.waitLeader(leaderTimeout)
		t.Logf("real kill -9: node %d (current leader)", leader)
		c.kill(leader)
	}()

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("rk%03d", i)
		val := fmt.Sprintf("rv%03d", i)
		if err := cl.Put([]byte(key), []byte(val)); err == nil {
			acked[key] = val
		}
	}
	wg.Wait()

	if len(acked) == 0 {
		t.Fatal("no write was ever acknowledged — cluster never became usable")
	}
	t.Logf("%d/%d real writes acknowledged around the real kill -9", len(acked), n)

	for key, want := range acked {
		val, ok, err := cl.Get([]byte(key))
		if err != nil || !ok || string(val) != want {
			t.Errorf("acked key %q: got (%q, ok=%v, err=%v), want (%q, true, nil)", key, val, ok, err, want)
		}
	}
}
