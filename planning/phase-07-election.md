# Phase 7 — Leader election over real RPC

> **Concept:** electing exactly one leader among many, tolerating crashes.
> **Done when:** 3 nodes → exactly one leader; kill the leader → a new leader
> within ~1s; bring the old one back → it becomes a follower without disruption.

Now the network arrives. Three real nodes over gRPC, `RequestVote`, randomized
election timeouts, and heartbeats. The whole phase turns on one safety check —
the "log at least as up-to-date" rule — and one liveness trick — randomized
timeouts.

---

## 1. Algorithm options

### 2a. RPC transport

| Option | Notes |
|---|---|
| **gRPC** | DESIGN §6 specifies it; typed protobuf messages, streaming, mature Go support. The obvious choice |
| net/rpc or raw TCP | fewer deps but you hand-roll framing/serialization we get free from gRPC |

**gRPC — LOCKED.** Define `RequestVote` and `AppendEntries` (Phase 8) in a
`.proto`; codegen the Go stubs.

### 2b. Election timeout randomization

Every follower waits a **randomized** `[150ms, 300ms]` for a heartbeat before
becoming a candidate. Randomization is the whole point: if all followers used the
same timeout they'd all become candidates at once, split the vote, and loop.
Randomizing means one node almost always times out first, wins, and heartbeats
the rest back to followers. LOCKED (range configurable).

### 2c. The up-to-date check (the safety-critical rule)

A voter grants its vote only if **both**:
1. candidate's `term ≥` voter's `currentTerm`, and
2. candidate's log is **at least as up-to-date**: compare `lastLogTerm` first,
   then `lastLogIndex` as tiebreak.

Rule 2 is what stops a node that missed recent committed writes from winning and
erasing them. It is *the* correctness check of the phase. LOCKED.

### 2d. Pre-vote — recommended extension

Vanilla Raft has a disruption bug: a partitioned node keeps timing out and
bumping its term; when it rejoins, its high term forces a needless election even
though the real leader was fine. **Pre-vote** fixes it: before actually
incrementing its term, a candidate first asks "would you vote for me?" and only
starts a real election if a majority say yes.

**Recommendation: implement pre-vote.** It's a small addition that directly
serves the "old leader rejoins without disruption" done-when criterion, and it's
standard in production Raft. LOCKED as a recommended extension (build the basic
election first, then layer pre-vote).

---

## 2. Implementation approach

- Roles: `Follower` → (timeout) → `Candidate` → (majority) → `Leader`.
- **Candidate:** increment `currentTerm` (persist), vote for self, send
  `RequestVote{term, candidateId, lastLogIndex, lastLogTerm}` to all peers in
  parallel; on majority of grants → become leader and immediately heartbeat.
- **Voter:** apply §2c; if granting, persist `votedFor` **before** replying (a
  node must never forget a vote — persist-before-ack from Phase 6).
- **Leader:** send empty `AppendEntries` heartbeats every ~50ms (well under the
  election timeout) so followers stay followers.
- **Term discipline:** any RPC carrying a higher term → step down to follower,
  adopt the term, persist. This single rule keeps at most one leader per term.
- Concurrency: a mutex around the node's state; RPC handlers and timers all
  serialize through it (Go: a single goroutine + channels, or a guarded struct).

Files: `election.go` (timers, RequestVote, roles), `rpc.go` (gRPC server/client).

---

## 3. Edge cases

- **Split vote** — no majority in a term; everyone times out again (randomized),
  next term resolves. Must not deadlock.
- **Two candidates same term** — at most one gets a majority (a voter votes once
  per term via `votedFor`). The other steps down on seeing the winner's term.
- **Stale candidate** — fails the §2c up-to-date check; correctly loses.
- **Old leader returns** — sees a higher term (or is rejected), steps down to
  follower. Pre-vote prevents it from disrupting on the way back.
- **Clock/timer skew** — timeouts are randomized per election, not fixed, so
  transient skew doesn't cause permanent live-lock.

---

## 4. Test plan

1. **Single leader** — bring up 3 nodes; assert exactly one leader emerges and
   the other two are followers.
2. **Leader failover** — kill the leader; assert a new leader within ~1s and that
   it has an up-to-date log.
3. **Non-disruptive rejoin** — restart the old leader; assert it joins as a
   follower and no spurious election happens (this is the pre-vote payoff).
4. **Split-vote recovery** — force simultaneous candidacies; assert the cluster
   still converges to one leader within a couple of terms.
5. **Stale node can't win** — give one node a short log; partition the others
   briefly; assert the stale node never becomes leader (§2c).

Test 5 guards the safety rule; test 3 guards the liveness/disruption behaviour.

---

## 5. Decisions locked

| Decision | Choice |
|---|---|
| Transport | gRPC, protobuf-defined RPCs |
| Election timeout | randomized [150,300]ms (configurable); heartbeat ~50ms |
| Up-to-date check | term, then (lastLogTerm, lastLogIndex) — the safety rule |
| Vote persistence | `votedFor` fsync'd before replying |
| Pre-vote | implemented (basic election first, then layer it) |
| Term rule | higher term seen → step down, adopt, persist |
