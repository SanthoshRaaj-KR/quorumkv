package linearize

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Violation is the counterexample produced when a key's recorded history
// admits no linearization consistent with real time — enough to reproduce
// and inspect the failing key's operations directly.
type Violation struct {
	Key string
	Ops []Op
}

func (v *Violation) Error() string {
	return fmt.Sprintf("linearize: key %q: no valid linearization for %d recorded op(s)", v.Key, len(v.Ops))
}

// Dump renders every op in the violation, ordered by start time, with
// enough detail (kind, arg/result, real-time interval, outcome) to
// reconstruct why no linearization exists by hand — the same "give the
// human what they need to reproduce it" instinct as a fault-injection seed
// (planning/phase-13-fault-injection.md §1c).
func (v *Violation) Dump() string {
	ops := append([]Op(nil), v.Ops...)
	sort.Slice(ops, func(i, j int) bool { return ops[i].Start.Before(ops[j].Start) })

	var b strings.Builder
	fmt.Fprintf(&b, "key %q, %d op(s):\n", v.Key, len(ops))
	for _, op := range ops {
		outcome := "ok"
		if !op.Ok {
			outcome = "UNKNOWN (client saw an error/timeout)"
		}
		switch op.Kind {
		case OpPut:
			fmt.Fprintf(&b, "  [%s .. %s] PUT   arg=%q            (%s)\n", fmtT(op.Start), fmtT(op.End), op.Arg, outcome)
		case OpDelete:
			fmt.Fprintf(&b, "  [%s .. %s] DELETE                  (%s)\n", fmtT(op.Start), fmtT(op.End), outcome)
		case OpGet:
			fmt.Fprintf(&b, "  [%s .. %s] GET   result=%s   (%s)\n", fmtT(op.Start), fmtT(op.End), fmtValue(op.Result), outcome)
		}
	}
	return b.String()
}

func fmtT(t time.Time) string { return t.Format("15:04:05.000000") }

func fmtValue(v Value) string {
	if !v.Found {
		return "absent"
	}
	return fmt.Sprintf("%q", v.Data)
}

// Check reports whether h is linearizable. Decomposed key by key (§1a):
// linearizability is compositional across independent objects, so a
// key-value store — where every operation touches exactly one key — is
// linearizable as a whole iff each key's own history is. This turns an
// otherwise NP-hard search into dozens of small, independent ones.
//
// Returns the first violating key found, or nil if every key checks out.
func Check(h *History) (bool, *Violation) {
	for key, ops := range h.ByKey() {
		if !checkKey(ops) {
			return false, &Violation{Key: key, Ops: ops}
		}
	}
	return true, nil
}

// checkKey decides whether ops — every recorded operation for one key —
// admits a linearization.
//
// An unknown-outcome Get (Ok=false) is dropped outright: a Get that
// observed no result has nothing to verify and never mutates the
// register, so it can neither pass nor fail the check (§7).
//
// An unknown-outcome Put/Delete is genuinely ambiguous — it may or may not
// have taken effect (§1c/§7) — so every subset of them is tried as
// "happened" vs. "never happened" until one subset's resulting definite
// history admits a valid order. Real fault-injection runs should only ever
// produce a handful of these per key, so the 2^n enumeration here is not a
// practical concern.
func checkKey(ops []Op) bool {
	var definite, unknownWrites []Op
	for _, op := range ops {
		switch {
		case !op.Ok && op.Kind == OpGet:
			continue
		case !op.Ok:
			unknownWrites = append(unknownWrites, op)
		default:
			definite = append(definite, op)
		}
	}

	n := len(unknownWrites)
	for mask := 0; mask < (1 << n); mask++ {
		trial := append([]Op(nil), definite...)
		for i, op := range unknownWrites {
			if mask&(1<<i) != 0 {
				trial = append(trial, op)
			}
		}
		if linearizes(trial) {
			return true
		}
	}
	return false
}

// linearizes runs the backtracking search (§1d) over a fixed set of
// operations already resolved to "did happen": true iff some total order
// consistent with real time reproduces every recorded Get result.
func linearizes(ops []Op) bool {
	placed := make([]bool, len(ops))
	memo := make(map[string]bool)
	return search(ops, placed, Value{}, memo)
}

// search tries to extend a partial linearization by one more operation.
// state is the register's value after everything already placed. memo
// keys on (which ops are placed, current state) so the same dead end is
// never re-explored.
func search(ops []Op, placed []bool, state Value, memo map[string]bool) bool {
	key := fmt.Sprintf("%v|%v|%s", placed, state.Found, state.Data)
	if result, seen := memo[key]; seen {
		return result
	}

	allPlaced := true
	for i := range ops {
		if placed[i] {
			continue
		}
		allPlaced = false
		if !isCandidate(ops, placed, i) {
			continue
		}
		next, ok := apply(ops[i], state)
		if !ok {
			continue // this op's recorded Get result can't happen here — dead end
		}
		placed[i] = true
		if search(ops, placed, next, memo) {
			placed[i] = false
			memo[key] = true
			return true
		}
		placed[i] = false
	}

	memo[key] = allPlaced
	return allPlaced
}

// isCandidate reports whether ops[i] may be placed next: every operation
// that real time forces to precede it (its interval ends before ops[i]'s
// begins) must already be placed. Operations with overlapping intervals
// impose no ordering between each other — either may come first.
func isCandidate(ops []Op, placed []bool, i int) bool {
	for j := range ops {
		if j == i || placed[j] {
			continue
		}
		if ops[j].End.Before(ops[i].Start) {
			return false
		}
	}
	return true
}

// apply advances the simulated register by op at this point in the trial
// order. ok is false only when op is a Get whose recorded result disagrees
// with the simulated state — the signal to abandon this branch.
func apply(op Op, state Value) (Value, bool) {
	switch op.Kind {
	case OpPut:
		return Value{Found: true, Data: op.Arg}, true
	case OpDelete:
		return Value{}, true
	case OpGet:
		return state, op.Result == state
	default:
		panic(fmt.Sprintf("linearize: unknown op kind %v", op.Kind))
	}
}
