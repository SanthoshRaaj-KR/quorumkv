//go:build flagship

package linearize

import (
	"testing"
	"time"
)

// The flagship run (planning/phase-14-linearizability.md §6.3) — the
// resume number this phase exists to produce. Deliberately gated behind
// the "flagship" build tag rather than testing.Short()'s inverse: `go test
// ./...` does not pass -short by default, so a Short-gated skip wouldn't
// actually keep this out of a normal run. A build tag does — this file
// isn't even compiled unless asked for, the same "slow, real, not CI's
// problem" carve-out planning/phase-12-chaos.md §6.1 already established
// for its real-process kill-9 variant.
//
// Run it explicitly:
//
//	go test -tags=flagship -run TestFlagship -v -timeout 15m ./consensus/linearize/
//
// A 5-node cluster (not 3): this harness kills leaders permanently, no
// restart, so the run needs enough surviving nodes to keep a majority
// after every kill. 5 nodes tolerates 2 permanent kills (3 remain, still a
// majority of 5) — two induced leader failures across one run is the
// point; 3 nodes only ever tolerates one (already covered by the fast
// integration test, checkpoint 2).
func TestFlagshipLinearizabilityUnderRepeatedLeaderFailure(t *testing.T) {
	const (
		nodes    = 5
		keys     = 40
		clients  = 25
		duration = 90 * time.Second
		kills    = 2
		seed     = 42
	)

	t.Logf("flagship run starting: %d nodes, %d clients, %d keys, %s, %d leader kills, seed=%d",
		nodes, clients, keys, duration, kills, seed)

	start := time.Now()
	result, err := Run(RunConfig{
		Nodes: nodes, Keys: keys, Clients: clients,
		Duration:    duration,
		LeaderKills: kills,
		Seed:        seed,
	})
	runElapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	opCount := result.History.Len()
	t.Logf("workload done in %s: %d operations (%.0f ops/sec), %d leader(s) killed: %v",
		runElapsed, opCount, float64(opCount)/runElapsed.Seconds(), len(result.LeaderKills), result.LeaderKills)

	if opCount < 10000 {
		t.Fatalf("only %d operations recorded — below the 10,000 the flagship number needs; "+
			"raise clients/duration in this test", opCount)
	}
	if len(result.LeaderKills) < kills {
		t.Fatalf("only %d of %d planned leader kills completed — the cluster likely lost quorum early", len(result.LeaderKills), kills)
	}

	checkStart := time.Now()
	ok, v := Check(result.History)
	checkElapsed := time.Since(checkStart)
	if !ok {
		t.Fatalf("linearizability violated after %d ops:\n%s", opCount, v.Dump())
	}

	t.Logf("checked in %s", checkElapsed)
	t.Logf(
		"RESULT: linearizability verified across %d operations under %d induced leader failures "+
			"(%d-node cluster, %d concurrent clients, %s wall-clock, %.0f ops/sec)",
		opCount, len(result.LeaderKills), nodes, clients, runElapsed.Round(time.Millisecond), float64(opCount)/runElapsed.Seconds(),
	)
}
