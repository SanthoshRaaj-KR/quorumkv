// Command dashboard-backend is a tiny local HTTP server exposing a live,
// introspectable Raft cluster (consensus.Sandbox) for the quorumkv test
// dashboard's interactive view. It is not part of the library — a thin JSON
// wrapper so a browser can propose commands, tick the clock, and crash,
// restart, or partition nodes, watching the real Node/Driver/Bus code do its
// thing rather than a simulation of it.
//
// Phase 10 (planning/phase-10-apply-seam.md §7): each node is now paired with
// a real Rust sidecar process (storage/src/bin/sidecar.rs) holding its own
// LSM engine. This binary is exactly the layer that plan called for — it can
// import both consensus and consensus/engine (consensus itself cannot, on
// pain of an import cycle), so the per-node sidecar lifecycle and the
// Sandbox's StateMachine factory both live here.
//
//	go run ./cmd/dashboard-backend
//
// Listens on 127.0.0.1:5056. Normally launched automatically by the Flask
// dashboard (dashboard/app.py), which proxies to it. Requires `cargo` on
// PATH (to run the sidecar) in addition to `go`.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"quorumkv/consensus"
	"quorumkv/consensus/engine"
)

// ─── sidecar process lifecycle ────────────────────────────────────────────
//
// One real Rust sidecar per node, each its own on-disk directory under one
// temp root per sandbox "generation." Killing a node's sidecar and killing
// its Raft driver are two independent actions (§7's Crash/Restart both do
// both); a directory outlives its sidecar process, which is what makes
// "restart" mean something at the storage layer, not just the Raft layer.

type sidecarManager struct {
	dataDir string // this generation's temp root; each node gets dataDir/nodeN

	mu      sync.Mutex
	procs   map[uint64]*exec.Cmd
	addrs   map[uint64]string
	clients map[uint64]*engine.Client
}

func newSidecarManager() (*sidecarManager, error) {
	root, err := os.MkdirTemp("", "quorumkv-sandbox-*")
	if err != nil {
		return nil, fmt.Errorf("create sandbox temp dir: %w", err)
	}
	return &sidecarManager{
		dataDir: root,
		procs:   make(map[uint64]*exec.Cmd),
		addrs:   make(map[uint64]string),
		clients: make(map[uint64]*engine.Client),
	}, nil
}

func (m *sidecarManager) nodeDir(id uint64) string {
	return filepath.Join(m.dataDir, fmt.Sprintf("node%d", id))
}

// storageCrateDir locates the Rust storage crate relative to this process's
// working directory (the dashboard sets it to the consensus/ directory, so
// "../storage" is its sibling — same relationship the engine package's own
// integration tests rely on, just one directory shallower here).
func storageCrateDir() (string, error) {
	dir, err := filepath.Abs("../storage")
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(filepath.Join(dir, "Cargo.toml")); err != nil {
		return "", fmt.Errorf("storage crate not found at %s: %w", dir, err)
	}
	return dir, nil
}

// spawn starts the sidecar for node id (creating its directory if new),
// waits for its "port=N" readiness line, and records its address.
func (m *sidecarManager) spawn(id uint64) error {
	crateDir, err := storageCrateDir()
	if err != nil {
		return err
	}
	nodeDir := m.nodeDir(id)
	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		return fmt.Errorf("node %d: create data dir: %w", id, err)
	}

	cmd := exec.Command("cargo", "run", "--quiet", "--bin", "sidecar", "--", nodeDir)
	cmd.Dir = crateDir
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("node %d: stdout pipe: %w", id, err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("node %d: start sidecar: %w", id, err)
	}

	portCh := make(chan int, 1)
	errCh := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if p, ok := strings.CutPrefix(scanner.Text(), "port="); ok {
				if n, err := strconv.Atoi(p); err == nil {
					portCh <- n
					return
				}
			}
		}
		errCh <- fmt.Errorf("node %d: sidecar exited before announcing a port", id)
	}()

	var port int
	select {
	case port = <-portCh:
	case err := <-errCh:
		return err
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		return fmt.Errorf("node %d: sidecar never announced a port within 30s", id)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	m.mu.Lock()
	m.procs[id] = cmd
	m.addrs[id] = addr
	m.clients[id] = engine.NewClient(addr)
	m.mu.Unlock()
	log.Printf("dashboard-backend: node %d sidecar up at %s (dir=%s)", id, addr, nodeDir)
	return nil
}

// kill stops node id's sidecar process if one is running. Its data directory
// is left untouched — a later spawn/respawn at the same directory picks up
// exactly where it left off.
func (m *sidecarManager) kill(id uint64) {
	m.mu.Lock()
	cmd := m.procs[id]
	delete(m.procs, id)
	m.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
}

// respawn kills (if running) and starts a fresh sidecar at the same
// directory — the storage-engine half of a node Restart.
func (m *sidecarManager) respawn(id uint64) error {
	m.kill(id)
	return m.spawn(id)
}

// client returns the client currently bound to node id's sidecar, for a
// direct read that bypasses Raft entirely (phase-10 §4).
func (m *sidecarManager) client(id uint64) (*engine.Client, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.clients[id]
	return c, ok
}

// stateMachine builds the consensus.StateMachine for node id, bound to
// whatever address is on record *right now* — used as
// consensus.SandboxConfig.NewStateMachine. Looking the address up at call
// time (not baking one in) is what lets a post-Restart address change take
// effect: the caller must respawn (updating the address) before triggering
// Sandbox.Restart, which is exactly the order handleRestart below follows.
func (m *sidecarManager) stateMachine(id uint64) (consensus.StateMachine, error) {
	m.mu.Lock()
	addr, ok := m.addrs[id]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no sidecar address recorded for node %d", id)
	}
	return engine.NewStateMachine(addr), nil
}

// killAll stops every tracked sidecar — called when a sandbox generation is
// replaced (reset) or the process shuts down, so sidecars never outlive it.
func (m *sidecarManager) killAll() {
	m.mu.Lock()
	ids := make([]uint64, 0, len(m.procs))
	for id := range m.procs {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	for _, id := range ids {
		m.kill(id)
	}
}

// ─── global state ─────────────────────────────────────────────────────────

var (
	mu   sync.Mutex
	sb   *consensus.Sandbox
	side *sidecarManager
)

func current() *consensus.Sandbox {
	mu.Lock()
	defer mu.Unlock()
	return sb
}

func currentSidecars() *sidecarManager {
	mu.Lock()
	defer mu.Unlock()
	return side
}

// reset replaces the whole sandbox generation: spin up cfg.Nodes fresh
// sidecars, build a Sandbox whose StateMachine factory is bound to them, and
// only then kill the *previous* generation's sidecars — so a failed reset
// never leaves the dashboard with no working cluster at all.
func reset(cfg consensus.SandboxConfig) (consensus.SandboxState, error) {
	if cfg.Nodes <= 0 {
		cfg.Nodes = 3
	}
	oldSidecars := currentSidecars()

	newSidecars, err := newSidecarManager()
	if err != nil {
		return consensus.SandboxState{}, err
	}
	for i := 1; i <= cfg.Nodes; i++ {
		if err := newSidecars.spawn(uint64(i)); err != nil {
			newSidecars.killAll()
			return consensus.SandboxState{}, fmt.Errorf("spawn sidecar for node %d: %w", i, err)
		}
	}

	cfg.NewStateMachine = newSidecars.stateMachine
	next, err := consensus.NewSandbox(cfg)
	if err != nil {
		newSidecars.killAll()
		return consensus.SandboxState{}, err
	}

	mu.Lock()
	sb, side = next, newSidecars
	mu.Unlock()

	if oldSidecars != nil {
		oldSidecars.killAll()
	}
	return next.State(), nil
}

// ─── HTTP plumbing ─────────────────────────────────────────────────────────

func withCORS(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			return
		}
		h(w, r)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	writeJSON(w, map[string]string{"error": err.Error()})
}

type resetReq struct {
	Nodes             int   `json:"nodes"`
	ElectionTimeout   int   `json:"election_timeout"`
	HeartbeatTimeout  int   `json:"heartbeat_timeout"`
	SnapshotThreshold int   `json:"snapshot_threshold"`
	Seed              int64 `json:"seed"`
}

func handleReset(w http.ResponseWriter, r *http.Request) {
	var req resetReq
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req) // a missing/empty body just means "use defaults"
	}
	state, err := reset(consensus.SandboxConfig{
		Nodes: req.Nodes, ElectionTimeout: req.ElectionTimeout,
		HeartbeatTimeout: req.HeartbeatTimeout, SnapshotThreshold: req.SnapshotThreshold,
		Seed: req.Seed,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, state)
}

func handleState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, current().State())
}

func handleTick(w http.ResponseWriter, r *http.Request) {
	n := 1
	if v := r.URL.Query().Get("n"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			n = parsed
		}
	}
	current().Tick(n)
	writeJSON(w, current().State())
}

// handlePropose builds a real engine.Command (PUT needs key+value, DELETE
// needs just key) and proposes its encoded bytes — no more opaque test
// strings (phase-10 §3). Sandbox.Propose takes a Go string, but that's just
// a byte carrier here: Go strings aren't UTF-8-validated at runtime, so the
// binary-encoded command survives untouched.
func handlePropose(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Node  uint64 `json:"node"`
		Op    string `json:"op"` // "put" (default) or "delete"
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, err)
		return
	}

	var cmd engine.Command
	switch strings.ToLower(req.Op) {
	case "", "put":
		cmd = engine.Command{Op: engine.OpPut, Key: []byte(req.Key), Value: []byte(req.Value)}
	case "delete":
		cmd = engine.Command{Op: engine.OpDelete, Key: []byte(req.Key)}
	default:
		writeErr(w, fmt.Errorf("unknown op %q (want \"put\" or \"delete\")", req.Op))
		return
	}

	if err := current().Propose(req.Node, string(engine.EncodeCommand(cmd))); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, current().State())
}

// handleGet reads directly from a node's own local engine — bypassing Raft
// and Sandbox both, per phase-10 §4: reads were never a Raft-level concern,
// so they don't go through Sandbox.State() either.
func handleGet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Node uint64 `json:"node"`
		Key  string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, err)
		return
	}
	client, ok := currentSidecars().client(req.Node)
	if !ok {
		writeErr(w, fmt.Errorf("no sidecar for node %d", req.Node))
		return
	}
	val, found, err := client.Get([]byte(req.Key))
	if err != nil {
		writeErr(w, err)
		return
	}
	if !found {
		writeJSON(w, map[string]any{"found": false})
		return
	}
	writeJSON(w, map[string]any{"found": true, "value": string(val)})
}

func decodeNodeReq(r *http.Request) (uint64, error) {
	var req struct {
		Node uint64 `json:"node"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return 0, err
	}
	return req.Node, nil
}

// handleCrash stops both halves of a node: the Raft driver (Sandbox.Crash)
// and its sidecar process — a real "kill -9", not just a Raft-level pause.
func handleCrash(w http.ResponseWriter, r *http.Request) {
	id, err := decodeNodeReq(r)
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := current().Crash(id); err != nil {
		writeErr(w, err)
		return
	}
	currentSidecars().kill(id)
	writeJSON(w, current().State())
}

// handleRestart brings both halves back: respawn the sidecar first (same
// directory, a fresh address) so Sandbox.Restart's factory call finds it,
// then rebuild the Raft driver. This is the concrete proof of phase-10's
// done-when — both the log and the engine survive a restart.
func handleRestart(w http.ResponseWriter, r *http.Request) {
	id, err := decodeNodeReq(r)
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := currentSidecars().respawn(id); err != nil {
		writeErr(w, err)
		return
	}
	if err := current().Restart(id); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, current().State())
}

// nodeAction wires up Isolate/Heal — pure network-partition semantics, no
// sidecar involvement (a partition doesn't kill a process).
func nodeAction(fn func(*consensus.Sandbox, uint64) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := decodeNodeReq(r)
		if err != nil {
			writeErr(w, err)
			return
		}
		if err := fn(current(), id); err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, current().State())
	}
}

func main() {
	// Best-effort cleanup on a graceful shutdown (Ctrl+C). A hard kill from
	// the Flask parent (Popen.terminate) may not give this a chance to run —
	// same limitation the dashboard's own README already notes for the Go
	// backend process itself.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("dashboard-backend: shutting down, killing sidecars...")
		if s := currentSidecars(); s != nil {
			s.killAll()
		}
		os.Exit(0)
	}()

	if _, err := reset(consensus.SandboxConfig{Nodes: 3}); err != nil {
		log.Fatalf("initial sandbox: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/reset", withCORS(handleReset))
	mux.HandleFunc("/state", withCORS(handleState))
	mux.HandleFunc("/tick", withCORS(handleTick))
	mux.HandleFunc("/propose", withCORS(handlePropose))
	mux.HandleFunc("/get", withCORS(handleGet))
	mux.HandleFunc("/crash", withCORS(handleCrash))
	mux.HandleFunc("/restart", withCORS(handleRestart))
	mux.HandleFunc("/isolate", withCORS(nodeAction((*consensus.Sandbox).Isolate)))
	mux.HandleFunc("/heal", withCORS(nodeAction((*consensus.Sandbox).Heal)))

	const addr = "127.0.0.1:5056"
	log.Printf("dashboard-backend listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
