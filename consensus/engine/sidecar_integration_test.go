package engine

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"quorumkv/consensus"
)

// These tests spawn the *real* Rust sidecar binary (storage/src/bin/sidecar.rs)
// and drive it over the real hand-rolled HTTP protocol — no mocks anywhere in
// this file. They're slower than a unit test (a cold `cargo run` pays Rust's
// compile check even when nothing changed) and need `cargo` on PATH; that's
// the honest cost of actually exercising the phase-10 seam end to end rather
// than asserting against a stand-in.

// storageDir locates the Rust storage crate relative to this file, so the
// test works regardless of the caller's working directory.
func storageDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("..", "..", "storage"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "Cargo.toml")); err != nil {
		t.Skipf("storage crate not found at %s, skipping real-sidecar test: %v", dir, err)
	}
	return dir
}

// startSidecar builds (if needed) and runs the real sidecar binary against
// dbDir, waits for its "port=N" readiness line on stdout, and returns the
// address to connect to plus a cleanup func that kills the process.
func startSidecar(t *testing.T, dbDir string) (addr string, cleanup func()) {
	t.Helper()
	dir := storageDir(t)

	cmd := exec.Command("cargo", "run", "--quiet", "--bin", "sidecar", "--", dbDir)
	cmd.Dir = dir
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sidecar: %v", err)
	}

	portCh := make(chan int, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if p, ok := strings.CutPrefix(line, "port="); ok {
				if n, err := strconv.Atoi(p); err == nil {
					portCh <- n
					return
				}
			}
		}
	}()

	select {
	case port := <-portCh:
		return fmt.Sprintf("127.0.0.1:%d", port), func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	case <-time.After(60 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("sidecar never printed its port within 60s (a cold `cargo build` can be slow — check `cargo` is on PATH)")
		return "", nil
	}
}

// ─── §8.1: Apply/Get/Delete against the real engine ──────────────────────────

func TestApplyPutThenGetThroughRealEngine(t *testing.T) {
	addr, cleanup := startSidecar(t, t.TempDir())
	defer cleanup()

	sm := NewStateMachine(addr)
	if err := sm.Apply(EncodeCommand(Command{Op: OpPut, Key: []byte("foo"), Value: []byte("bar")})); err != nil {
		t.Fatalf("Apply(put): %v", err)
	}

	val, ok, err := sm.Get([]byte("foo"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(val) != "bar" {
		t.Errorf("Get(foo) = (%q, %v), want (bar, true)", val, ok)
	}

	if err := sm.Apply(EncodeCommand(Command{Op: OpDelete, Key: []byte("foo")})); err != nil {
		t.Fatalf("Apply(delete): %v", err)
	}
	if _, ok, err := sm.Get([]byte("foo")); err != nil {
		t.Fatalf("Get after delete: %v", err)
	} else if ok {
		t.Error("key should be absent after delete")
	}
}

// ─── §8.3: kill and restart a node → consistent engine state ────────────────

func TestRestartSurvivesAgainstRealEngine(t *testing.T) {
	dbDir := t.TempDir()
	addr, cleanup := startSidecar(t, dbDir)

	sm := NewStateMachine(addr)
	for i := 0; i < 20; i++ {
		key, val := []byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i))
		if err := sm.Apply(EncodeCommand(Command{Op: OpPut, Key: key, Value: val})); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	cleanup() // kill the sidecar process — its disk survives

	addr2, cleanup2 := startSidecar(t, dbDir) // a fresh process, the same directory
	defer cleanup2()
	sm2 := NewStateMachine(addr2)
	for i := 0; i < 20; i++ {
		key, want := fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i)
		val, ok, err := sm2.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get after restart: %v", err)
		}
		if !ok || string(val) != want {
			t.Errorf("%s = (%q, %v) after restart, want (%s, true)", key, val, ok, want)
		}
	}
}

// ─── §8.5: Snapshot/Restore round-trips through the real engine ─────────────

func TestSnapshotRestoreThroughRealEngine(t *testing.T) {
	addr, cleanup := startSidecar(t, t.TempDir())
	defer cleanup()

	sm := NewStateMachine(addr)
	for i := 0; i < 10; i++ {
		key, val := []byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i))
		if err := sm.Apply(EncodeCommand(Command{Op: OpPut, Key: key, Value: val})); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}

	blob, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(blob) == 0 {
		t.Fatal("snapshot blob is empty")
	}

	addr2, cleanup2 := startSidecar(t, t.TempDir()) // a different, fresh directory
	defer cleanup2()
	sm2 := NewStateMachine(addr2)
	if err := sm2.Restore(blob); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	for i := 0; i < 10; i++ {
		key, want := fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i)
		val, ok, err := sm2.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get after restore: %v", err)
		}
		if !ok || string(val) != want {
			t.Errorf("%s = (%q, %v) after restore, want (%s, true)", key, val, ok, want)
		}
	}
}

// ─── §8.2/§8.4: the full Raft path, and a failed Apply is fatal ─────────────

func newSingleNodeDriver(t *testing.T, sm consensus.StateMachine, seed int64) *consensus.Driver {
	t.Helper()
	d, err := consensus.NewDriver(consensus.Config{
		ID: 1, Peers: []uint64{1},
		ElectionTimeout: 10, HeartbeatTimeout: 3,
		Storage: consensus.NewMemStorage(),
		Seed:    seed,
	}, sm, nil)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	for i := 0; i < 100 && d.Node().Role() != consensus.Leader; i++ {
		if err := d.Tick(); err != nil {
			t.Fatalf("Tick: %v", err)
		}
	}
	if d.Node().Role() != consensus.Leader {
		t.Fatal("single node never elected itself")
	}
	return d
}

// The done-when, through the actual propose -> commit -> apply pipeline —
// not just calling StateMachine.Apply directly.
func TestSingleNodeDriverAppliesThroughRealEngine(t *testing.T) {
	addr, cleanup := startSidecar(t, t.TempDir())
	defer cleanup()

	sm := NewStateMachine(addr)
	d := newSingleNodeDriver(t, sm, 1)
	defer d.Close()

	if err := ProposePut(d, []byte("hello"), []byte("world")); err != nil {
		t.Fatalf("ProposePut: %v", err)
	}
	val, ok, err := sm.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(val) != "world" {
		t.Errorf("Get(hello) = (%q, %v), want (world, true)", val, ok)
	}

	if err := ProposeDelete(d, []byte("hello")); err != nil {
		t.Fatalf("ProposeDelete: %v", err)
	}
	if _, ok, err := sm.Get([]byte("hello")); err != nil {
		t.Fatalf("Get after delete: %v", err)
	} else if ok {
		t.Error("key should be absent after a committed delete")
	}
}

// The test that justifies phase-10 §2's interface change: a local apply
// failure must stop the node loudly, never vanish.
func TestFailedApplyIsFatalNotSilent(t *testing.T) {
	dbDir := t.TempDir()
	addr, cleanup := startSidecar(t, dbDir)

	sm := NewStateMachine(addr)
	d := newSingleNodeDriver(t, sm, 2)
	defer d.Close()

	if err := ProposePut(d, []byte("a"), []byte("1")); err != nil {
		t.Fatalf("first ProposePut: %v", err)
	}

	cleanup() // kill the sidecar without touching the Driver

	err := ProposePut(d, []byte("b"), []byte("2"))
	if err == nil {
		t.Fatal("Propose against a dead sidecar must return an error, not silently succeed")
	}
	t.Logf("got the expected error: %v", err)
}
