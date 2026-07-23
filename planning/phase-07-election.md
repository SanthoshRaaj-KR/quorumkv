# Phase 7 — Leader election over real RPC

> **Concept:** electing exactly one leader among many, tolerating crashes.
> **Done when:** bring up 3 nodes → exactly one becomes leader. Kill the leader
> → a new leader is elected within a second. Bring the old one back → it becomes
> a follower and doesn't disrupt anything.

Phase 6 built the state machine; it already counts votes, applies the §5.4.1
up-to-date rule, and reaches quorum through the general `Quorum()` path. This
phase does **not** change it. Phase 7 adds the two things a cluster of one
never needed: a way for messages to reach other nodes, and a place for real
time to live.

That split is the payoff of the Phase 6 §1 decision. Everything below sits
*outside* `Node`.

---

## 1. Algorithm — where real time lives

`Node` has no clock, by decision. Something must still turn wall-clock into
`Tick()`, and something must serialize ticks against inbound messages, because
`Node` is single-threaded.

| | Timers inside the node | A `Server` loop outside it |
|---|---|---|
| Who calls `Tick()` | the node's own goroutine | a `time.Ticker` in one `select` loop |
| Serialization | locks inside the algorithm | one goroutine owns the `Driver`; nothing else touches it |
| Deterministic tests | impossible | the `Node` is still drivable by hand — tests skip `Server` entirely |

**LOCKED: a `Server` type owns the `Driver`, a `time.Ticker`, an inbound message
channel and a proposal channel, in a single `select` loop.** One goroutine, no
mutexes around Raft state. Tests that care about the algorithm bypass `Server`
and drive `Node` directly, exactly as Phase 6 does.

### Tick granularity

ROADMAP calls for a 150–300 ms randomized election timeout.

| Knob | Value | Why |
|---|---|---|
| tick | 10 ms | fine enough for 150 ms resolution, coarse enough that a 5-node cluster isn't busy-looping |
| `ElectionTimeout` | 15 ticks → randomized to [150 ms, 300 ms) | exactly the paper's range; the randomization is what breaks vote splits |
| `HeartbeatTimeout` | 5 ticks = 50 ms | the standard rule of thumb: heartbeat ≈ ⅓ of the minimum election timeout, so a healthy leader refreshes followers ~3× before any of them times out |

**LOCKED.**

---

## 2. Algorithm — the wire

This is the phase's one genuinely open decision, and it is a **deviation from
`ROADMAP.md`, which says gRPC.** Stating it plainly rather than quietly picking.

| | gRPC + protobuf | `net/rpc` + gob | Own TCP framing |
|---|---|---|---|
| Toolchain | needs `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc` — **none are installed on this machine**, and `GOPATH/bin` is empty | stdlib | stdlib |
| Dependencies | ~40 transitive modules | zero | zero |
| Codegen step | yes, checked-in generated files | no | no |
| Resume value | high — it's what etcd/TiKV/Cockroach use | low; `net/rpc` is frozen | medium — but it *is* the framing you already designed twice |
| Fit for Raft | RPC semantics (request/response) fight Raft's fire-and-forget message model slightly | same | natural: Raft is a message-passing algorithm |
| Effort here | install toolchain, write `.proto`, wire codegen into the build | small | small |

### The call: own TCP framing now, gRPC deferred behind the same seam

Three reasons:

1. **The `Transport` interface already exists** (Phase 6, `state.go`). Whatever
   goes behind it is a leaf, not an architecture. Swapping in gRPC later is a
   mechanical change to one file, and no Raft code moves.
2. **Zero new dependencies keeps Phase 7 about elections.** The done-when is
   "exactly one leader, survives a kill" — not "protobuf compiles."
3. **You already own this framing.** CRC32C + length prefix is the Phase 1 WAL
   record format and the Phase 6 `raft-log` format. A third use costs almost
   nothing and keeps one codec to reason about.

The honest counter-argument: gRPC is the more resume-credible answer, and
`DESIGN.md` §9 names `rpc.go`. That's why this is **flagged for sign-off** rather
than quietly locked — the decision below is provisional, and swapping it is a
one-file change by construction.

**PROVISIONAL: own TCP transport with CRC32C length-prefixed frames. gRPC
remains the stated target, deferred to a later pass behind `Transport`.**

### Two transports, not one

| `Loopback` | in-process router; no sockets, no goroutines, fully deterministic |
| `TCPTransport` | real sockets over `127.0.0.1`, static id→address map |

`Loopback` is not a toy — it is what makes Phase 12 possible. A whole 3-node
cluster runs in one goroutine, a partition is "drop these messages," and a
failing run replays from a seed. `TCPTransport` proves the same code survives a
real socket. **LOCKED: ship both.**

### Message loss policy

Fire-and-forget. A message to an unreachable peer is **dropped**, not queued;
the connection is redialled lazily on the next send. Raft is designed to
tolerate loss, duplication and reordering — a retry queue would add a second,
worse consensus protocol underneath the real one. **LOCKED.**

---

## 3. Algorithm — heartbeats, and what they are *not*

ROADMAP: *"Add heartbeats (empty `AppendEntries`) so a live leader keeps
followers from timing out."* The subtlety is what a follower does with one.

A heartbeat carries `prevLogIndex`/`prevLogTerm`, so a follower can check the
log-matching property — but in Phase 7 there is no replication to repair a
mismatch. So:

- **A heartbeat from a current-or-newer term always resets the election timer**,
  even when the log-match check *fails*. Leader liveness and log agreement are
  different questions; conflating them causes a follower with a stale log to
  time out and start a pointless election against a perfectly healthy leader.
- **`commitIndex` advances only when the match check succeeds**, to
  `min(leaderCommit, lastIndex)`. A follower must never expose an entry it
  cannot prove it holds.
- The follower replies `MsgAppResp{Success}` honestly. The leader **ignores a
  failure** in Phase 7 — walking `nextIndex` backward and shipping the missing
  entries is precisely Phase 8's job, and doing half of it here is how the log
  matching property gets subtly broken.

**LOCKED.** This is the smallest heartbeat that is not a lie.

---

## 4. Implementation approach

### 4a. What changes in `Node` (small, and only additive)

- `stepFollower`/`stepCandidate` handle `MsgAppReq` per §3: adopt the leader,
  reset the timer, range-check, reply.
- A candidate receiving `MsgAppReq` at `term >= currentTerm` **steps down** —
  someone else won.
- Track `leaderID` so Phase 11's client redirect has something to redirect to.
- `MsgAppResp` updates `matchIndex`/`nextIndex` on success; failures are logged
  and dropped (Phase 8).

Everything else — `campaign`, `becomeLeader`, `handleVoteRequest`,
`maybeAdvanceCommit`, the no-op entry, the up-to-date rule — is untouched. That
is the Phase 6 §1 bet paying out.

### 4b. `Server` — the runtime shell

```go
for {
    select {
    case <-ticker.C:       driver.Tick()
    case m := <-inbound:   driver.Step(m)
    case p := <-proposals: p.err <- driver.Propose(p.cmd)
    case <-stop:           return
    }
}
```

The transport's reader goroutines push into `inbound`; they never touch Raft
state. `Driver.run` sends via `Transport` *after* the fsync, unchanged from
Phase 6.

### 4c. Addressing

A static `map[uint64]string` of id → `host:port` in the config. No discovery,
no membership change — both out of scope for the project (`DESIGN.md` §1).

---

## 5. Edge cases

- **Split vote** — two candidates, neither reaching quorum. Resolved by the
  randomized timeout.
- **A cluster-wide seed must not disable the randomization.** Found during
  implementation, and it is a genuine trap: `Config.Seed` exists so a chaos run
  replays exactly, so setting *one* seed for the whole cluster is the natural
  thing to do — at which point every node draws the **same** timeout, campaigns
  on the same tick forever, and the split vote never breaks. `NewNode` therefore
  mixes the node ID into the seed: runs stay replayable *and* the streams stay
  independent. `TestSplitVoteResolvesUnderAClusterWideSeed` is the guard.
- **A stale node must not win.** It has a lower `lastLogTerm`, so §5.4.1 denies
  it every vote. Tested directly — it is the check that protects committed data.
- **Two leaders in one term is impossible** — each node votes once per term and
  a majority is required, so two majorities would have to intersect. Asserted as
  a cluster-wide invariant after *every* step, not just at the end.
- **The old leader returns** with a stale term, hears a higher one in the first
  heartbeat or vote request, and steps down without disrupting anything.
- **A candidate that hears a valid heartbeat** steps down immediately.
- **Duplicate/reordered messages** — terms and the once-per-term vote make both
  harmless. The loopback transport reorders deliberately in one test.
- **Self-addressed messages** are never sent; `campaign` skips `n.id`.
- **A peer that is down** — dial fails, message dropped, election proceeds with
  the remaining majority. No queue, no backlog to replay later.

---

## 6. Test plan (expands the done-when)

Deterministic in-process cluster unless noted:

1. **Done-when: exactly one leader.** 3 nodes, tick until one wins; assert
   exactly one leader and two followers, all on the same term.
2. **Done-when: kill the leader → a new one within a second.** Stop delivering
   to/from the leader; assert a new leader in a higher term within
   `2 × ElectionTimeout` ticks.
3. **Done-when: the old leader returns as a follower** and the cluster keeps its
   existing leader.
4. **One leader per term, always** — invariant asserted after every single step
   of every cluster test, not as a final check.
5. **A stale log cannot win.** Give one node a longer/newer log; force the stale
   one to campaign; assert it is denied and the up-to-date node can still win.
6. **Heartbeats suppress elections.** With the leader alive and delivering,
   run 20 × the election timeout; assert no follower ever becomes a candidate
   and the term never changes.
7. **Split vote resolves.** Two nodes campaign simultaneously; assert the
   cluster converges to one leader rather than livelocking.
8. **A 5-node cluster** elects with `Quorum()==3`, and survives 2 failures but
   not 3 — the majority arithmetic, tested rather than assumed.
9. **TCP smoke test** — 3 real `Server`s on `127.0.0.1`, real sockets: exactly
   one leader; kill it; a new one appears.
10. **Message codec round-trip** + corruption rejection, mirroring the Phase 6
    entry codec tests.

Tests 4 and 5 are the safety core; 1–3 are the done-when; 9 is the proof it
isn't only true in simulation.

---

## 7. Explicitly out of scope

No log replication — `AppendEntries` carries no entries and the leader does not
repair followers (Phase 8). No snapshots (Phase 9). No LSM contact (Phase 10).
No client, no redirect (Phase 11) — though `leaderID` is tracked here so Phase 11
has something to read. No membership change, ever.

---

## 8. Decisions locked

| Decision | Choice |
|---|---|
| Real time | a `Server` loop outside `Node`; one goroutine owns the `Driver` |
| Tick | 10 ms; election 15 ticks → [150 ms, 300 ms); heartbeat 5 ticks (50 ms) |
| Wire | **PROVISIONAL** — own TCP + CRC32C length-prefixed frames; gRPC deferred behind `Transport` (deviates from ROADMAP; flagged for sign-off) |
| Transports | ship two: deterministic `Loopback` + real `TCPTransport` |
| Message loss | fire-and-forget; drop on unreachable, lazy redial, no queue |
| Heartbeat | resets the election timer even on log mismatch; commit advances only on match |
| `MsgAppResp` failure | ignored in Phase 7; `nextIndex` backoff is Phase 8 |
| Addressing | static id → host:port map; no discovery |
| Leader tracking | `leaderID` recorded now for Phase 11's redirect |
| Seed mixing | node ID folded into `Config.Seed`, so a cluster-wide seed keeps runs replayable without collapsing every node onto one timeout |

**Milestone: a real 3-node cluster that elects exactly one leader, re-elects
within a second when the leader dies, and absorbs the old leader as a follower.
It orders nothing yet — that's Phase 8.**

---

## 9. Status — shipped

Built and green: 42 Go tests, `go vet` clean, stable over 5 repeated runs.
The real-socket cluster elects in ~140 ms and re-elects after a leader kill.

`Node` changed only additively, as §4a predicted: `handleAppendEntries`,
`handleAppendResponse`, `leaderID`, and an immediate heartbeat on winning.
`campaign`, `becomeLeader`, `handleVoteRequest`, `maybeAdvanceCommit`, the no-op
entry and the up-to-date rule were **not touched** — the Phase 6 §1 bet paid out.

Two things worth carrying forward:

- **Nothing commits in Phase 7, by construction.** A leader's heartbeat probes
  `nextIndex-1`, followers with shorter logs fail the match, so no follower's
  `matchIndex` ever advances and the general commit rule correctly refuses to
  commit anything — including the leader's own no-op. That is honest for an
  election-only phase; Phase 8 is what makes `matchIndex` move.
- **gRPC is still owed.** The wire decision in §2 is PROVISIONAL and awaiting
  sign-off. `TCPTransport` implements `Transport` in one file with no Raft code
  in it, so the swap stays mechanical.
