// Command quorumkv-node is one real quorumkv cluster node: a Raft
// consensus.Server paired with its own local engine sidecar (phase-10),
// speaking the phase-11 client-facing RPC protocol — the standalone binary
// planning/phase-10-apply-seam.md §7 explicitly deferred to this phase.
//
// Each real node is two OS processes: this one plus its sidecar
// (storage/src/bin/sidecar.rs). A 3-node cluster is six processes total.
// Run once per node, with each node's own -id/-client/-data and the same
// -peers map everywhere:
//
//	go run ./cmd/quorumkv-node -id 1 -data ./data/node1 -client 127.0.0.1:6001 \
//	    -peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
//	go run ./cmd/quorumkv-node -id 2 -data ./data/node2 -client 127.0.0.1:6002 \
//	    -peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
//	go run ./cmd/quorumkv-node -id 3 -data ./data/node3 -client 127.0.0.1:6003 \
//	    -peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
//
// Requires `cargo` on PATH (to run the sidecar), same as
// cmd/dashboard-backend — and, like that binary, assumes it runs from the
// consensus/ directory unless -storage-dir says otherwise.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"quorumkv/consensus"
	"quorumkv/consensus/clientrpc"
	"quorumkv/consensus/engine"
)

func main() {
	var (
		id                = flag.Uint64("id", 0, "this node's ID (must appear in -peers)")
		peersFlag         = flag.String("peers", "", "comma-separated id=host:port for every node's Raft address, including this one")
		clientBind        = flag.String("client", "", "address to bind the client-facing RPC server (e.g. 127.0.0.1:6001)")
		dataDir           = flag.String("data", "", "directory for this node's Raft log and engine data")
		storageDir        = flag.String("storage-dir", "../storage", "path to the Rust storage crate, for the sidecar")
		electionTimeout   = flag.Int("election-timeout", consensus.DefaultElectionTimeout, "election timeout in ticks")
		heartbeatTimeout  = flag.Int("heartbeat-timeout", consensus.DefaultHeartbeatTimeout, "heartbeat timeout in ticks")
		tick              = flag.Duration("tick", consensus.DefaultTick, "logical tick period")
		snapshotThreshold = flag.Int("snapshot-threshold", 0, "applied entries beyond the last snapshot before taking another (0 = default)")
		seed              = flag.Int64("seed", 0, "election-timeout random seed (0 = derived from the current time)")
	)
	flag.Parse()

	if *id == 0 {
		log.Fatal("quorumkv-node: -id is required")
	}
	if *clientBind == "" {
		log.Fatal("quorumkv-node: -client is required")
	}
	if *dataDir == "" {
		log.Fatal("quorumkv-node: -data is required")
	}
	raftAddrs, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatalf("quorumkv-node: -peers: %v", err)
	}
	myRaftAddr, ok := raftAddrs[*id]
	if !ok {
		log.Fatalf("quorumkv-node: -peers does not include this node's id %d", *id)
	}
	peers := make([]uint64, 0, len(raftAddrs))
	for pid := range raftAddrs {
		peers = append(peers, pid)
	}

	seedVal := *seed
	if seedVal == 0 {
		seedVal = time.Now().UnixNano()
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("quorumkv-node: create data dir: %v", err)
	}
	raftDir := filepath.Join(*dataDir, "raft")
	engineDir := filepath.Join(*dataDir, "engine")

	// ── engine sidecar (phase-10 §7: one per node, this process's pair) ────
	sidecarAddr, sidecarCmd, err := spawnSidecar(*storageDir, engineDir)
	if err != nil {
		log.Fatalf("quorumkv-node: sidecar: %v", err)
	}
	log.Printf("quorumkv-node: node %d engine sidecar up at %s (dir=%s)", *id, sidecarAddr, engineDir)
	sm := engine.NewStateMachine(sidecarAddr)

	// ── raft ────────────────────────────────────────────────────────────
	tr, err := consensus.NewTCPTransport(*id, myRaftAddr)
	if err != nil {
		killSidecar(sidecarCmd)
		log.Fatalf("quorumkv-node: raft transport: %v", err)
	}
	tr.SetPeers(raftAddrs)

	storage, err := consensus.OpenFileStorage(raftDir)
	if err != nil {
		killSidecar(sidecarCmd)
		log.Fatalf("quorumkv-node: open raft storage: %v", err)
	}

	raftServer, err := consensus.NewServer(consensus.Config{
		ID: *id, Peers: peers,
		ElectionTimeout: *electionTimeout, HeartbeatTimeout: *heartbeatTimeout,
		Storage: storage, Seed: seedVal, SnapshotThreshold: *snapshotThreshold,
	}, sm, tr, tr.Inbound(), *tick)
	if err != nil {
		killSidecar(sidecarCmd)
		log.Fatalf("quorumkv-node: build raft server: %v", err)
	}
	raftServer.Start()
	log.Printf("quorumkv-node: node %d raft listening on %s (peers=%v)", *id, myRaftAddr, peers)

	// ── client-facing RPC (phase-11 §2) ────────────────────────────────
	rpcServer := clientrpc.NewServer(raftServer, sm, clientrpc.DefaultProposeTimeout)
	ln, err := net.Listen("tcp", *clientBind)
	if err != nil {
		_ = raftServer.Stop()
		_ = tr.Close()
		killSidecar(sidecarCmd)
		log.Fatalf("quorumkv-node: client listener: %v", err)
	}
	httpSrv := &http.Server{Handler: rpcServer.Handler()}
	go func() {
		if err := httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("quorumkv-node: client server: %v", err)
		}
	}()
	log.Printf("quorumkv-node: node %d client RPC listening on %s", *id, ln.Addr())

	// ── shutdown ────────────────────────────────────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Printf("quorumkv-node: node %d shutting down", *id)
	_ = httpSrv.Close()
	_ = raftServer.Stop()
	_ = tr.Close()
	killSidecar(sidecarCmd)
}

// parsePeers turns "1=host:port,2=host:port,..." into an id -> address map,
// the same static "no discovery" shape [consensus.TCPTransport.SetPeers] and
// [clientrpc.Client] both already take (standing project rule: no
// membership change, ever).
func parsePeers(s string) (map[uint64]string, error) {
	out := make(map[uint64]string)
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("must not be empty, want id=host:port[,id=host:port...]")
	}
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("malformed entry %q (want id=host:port)", part)
		}
		id, err := strconv.ParseUint(kv[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("malformed id in %q: %w", part, err)
		}
		out[id] = kv[1]
	}
	return out, nil
}

// spawnSidecar starts this node's engine sidecar over engineDir and waits
// for its "port=N" readiness line on stdout — the same protocol
// cmd/dashboard-backend's sidecarManager.spawn uses, narrowed from N
// simulated nodes sharing one process to exactly one real node.
func spawnSidecar(storageDir, engineDir string) (addr string, cmd *exec.Cmd, err error) {
	if _, err := os.Stat(filepath.Join(storageDir, "Cargo.toml")); err != nil {
		return "", nil, fmt.Errorf("storage crate not found at %s (pass -storage-dir): %w", storageDir, err)
	}
	if err := os.MkdirAll(engineDir, 0o755); err != nil {
		return "", nil, fmt.Errorf("create engine dir: %w", err)
	}

	c := exec.Command("cargo", "run", "--quiet", "--bin", "sidecar", "--", engineDir)
	c.Dir = storageDir
	c.Stderr = os.Stderr
	stdout, err := c.StdoutPipe()
	if err != nil {
		return "", nil, fmt.Errorf("stdout pipe: %w", err)
	}
	if err := c.Start(); err != nil {
		return "", nil, fmt.Errorf("start sidecar: %w", err)
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
		errCh <- fmt.Errorf("sidecar exited before announcing a port")
	}()

	select {
	case port := <-portCh:
		return fmt.Sprintf("127.0.0.1:%d", port), c, nil
	case err := <-errCh:
		return "", nil, err
	case <-time.After(30 * time.Second):
		_ = c.Process.Kill()
		return "", nil, fmt.Errorf("sidecar never announced a port within 30s")
	}
}

func killSidecar(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
}
