package linearize

import (
	"testing"
	"time"
)

// The integration run (planning/phase-14-linearizability.md §6.2): a real
// cluster, real concurrent clients contending over a small keyspace, one
// mid-run leader kill, checked against the same Check() the adversarial
// tests in checker_test.go already earned trust in. Small and fast enough
// for the normal test suite — the flagship, resume-number-sized run lives
// separately (checkpoint 4) and is not part of `go test ./...`.
func TestClusterRunIsLinearizableUnderLeaderCrash(t *testing.T) {
	result, err := Run(RunConfig{
		Nodes: 3, Keys: 15, Clients: 8,
		Duration:   3 * time.Second,
		KillLeader: true,
		Seed:       1,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("recorded %d ops; leader killed: node %d", result.History.Len(), result.LeaderKilled)
	if result.History.Len() < 50 {
		t.Fatalf("workload barely ran: only %d ops recorded", result.History.Len())
	}
	if result.LeaderKilled == 0 {
		t.Error("expected a leader kill to have completed during the run")
	}

	ok, v := Check(result.History)
	if !ok {
		t.Fatalf("linearizability violated:\n%s", v.Dump())
	}
}

// Same shape, no fault injection — the quiescent-cluster baseline. Cheap
// insurance that a passing chaos run isn't just the checker being lucky
// with easy histories; this one has no leader-crash-induced ambiguity at
// all, so it's the simplest possible real-cluster case.
func TestClusterRunIsLinearizableWithoutFaults(t *testing.T) {
	result, err := Run(RunConfig{
		Nodes: 3, Keys: 15, Clients: 8,
		Duration: 2 * time.Second,
		Seed:     2,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.History.Len() < 30 {
		t.Fatalf("workload barely ran: only %d ops recorded", result.History.Len())
	}

	ok, v := Check(result.History)
	if !ok {
		t.Fatalf("linearizability violated:\n%s", v.Dump())
	}
}
