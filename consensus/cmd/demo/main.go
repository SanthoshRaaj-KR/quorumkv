// Command demo is a tiny end-to-end run of the Phase 6 single-node Raft: it
// elects itself, commits a few commands, then restarts and proves term and log
// were reloaded from disk.
//
//	go run ./cmd/demo
//
// The Track A equivalent is storage/examples/demo.rs.
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"quorumkv/consensus"
)

// printer is the Phase 6 state machine: commands go to stdout. Phase 10 swaps
// the Rust LSM engine in behind this same interface.
type printer struct{ session int }

func (p *printer) Apply(cmd []byte) {
	fmt.Printf("  [session %d] apply: %s\n", p.session, cmd)
}

// Snapshot/Restore (Phase 9): the demo never accumulates enough entries to
// trigger one, but the interface still has to be satisfied. Phase 10 replaces
// this whole type with the Rust LSM engine, which hands back its SSTable set.
func (p *printer) Snapshot() []byte { return []byte(fmt.Sprintf("session-%d", p.session)) }

func (p *printer) Restore(data []byte) {
	fmt.Printf("  [session %d] restore: %s\n", p.session, data)
}

func main() {
	dir := filepath.Join(os.TempDir(), "quorumkv-raft-demo")
	_ = os.RemoveAll(dir) // start clean each run

	fmt.Printf("data dir: %s\n\n", dir)

	// ── Session 1: elect, propose, commit ────────────────────────────────────
	fmt.Println("session 1: fresh node")
	d, n := open(dir, 1)

	fmt.Printf("  role=%s term=%d (nobody has ticked yet)\n", n.Role(), n.Term())
	for n.Role() != consensus.Leader {
		must(d.Tick())
	}
	fmt.Printf("  elected: role=%s term=%d votedFor=%d quorum=%d\n",
		n.Role(), n.Term(), n.VotedFor(), n.Quorum())
	fmt.Printf("  index 1 is the election no-op: %v\n", n.Log().At(1).IsNoOp())

	for _, cmd := range []string{"PUT name quorumkv", "PUT lang go", "DELETE lang"} {
		must(d.Propose([]byte(cmd)))
	}
	fmt.Printf("  lastIndex=%d commitIndex=%d lastApplied=%d\n\n",
		n.LastIndex(), n.CommitIndex(), n.LastApplied())
	must(d.Close())

	// ── Session 2: restart and prove the state came back ─────────────────────
	fmt.Println("session 2: after restart")
	d2, n2 := open(dir, 2)
	defer d2.Close()

	fmt.Printf("  reloaded: role=%s term=%d votedFor=%d lastIndex=%d\n",
		n2.Role(), n2.Term(), n2.VotedFor(), n2.LastIndex())
	fmt.Println("  (a restarted node always comes back a follower — Raft keeps no")
	fmt.Println("   persistent leader state, and restoring as leader is a split brain)")
	fmt.Println("  commitIndex is volatile, so committed entries re-apply on re-election:")

	for n2.Role() != consensus.Leader {
		must(d2.Tick())
	}
	fmt.Printf("  re-elected at term %d; lastApplied=%d\n", n2.Term(), n2.LastApplied())
}

func open(dir string, session int) (*consensus.Driver, *consensus.Node) {
	st, err := consensus.OpenFileStorage(dir)
	must(err)
	d, err := consensus.NewDriver(consensus.Config{
		ID:               1,
		Peers:            []uint64{1}, // a cluster of one; peers arrive in Phase 7
		ElectionTimeout:  10,
		HeartbeatTimeout: 3,
		Storage:          st,
	}, &printer{session: session}, nil)
	must(err)
	return d, d.Node()
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
