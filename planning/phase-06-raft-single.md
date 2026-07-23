# Phase 6 — Single-node Raft (no networking)

> **Concept:** Raft's state machine and log mechanics, in isolation.
> **Done when:** you append a sequence of commands and they commit in order,
> with correct term/index numbering. Restarting reloads term + log from disk.

Track B starts here. This phase knows nothing about the LSM engine, nothing
about gRPC, and nothing about other nodes — it is a "cluster" of exactly one.
That isolation is the whole point: term handling and log indexing are where
every hard Raft bug lives, and finding them here costs minutes instead of the
days they cost once a network is in the picture.

Toolchain check: Go 1.26.4 is installed. Phase 6 uses **stdlib only** — no
dependency lands until gRPC arrives in Phase 7.

---

## 1. Algorithm — the headline decision: self-driving actor vs. driven core

This is the architectural fork for all of Track B, and it must be settled
before a line of Raft is written, because Phases 7–12 inherit it.

| | Self-driving actor (`hashicorp/raft` style) | Deterministic driven core (`etcd/raft` style) |
|---|---|---|
| Timers | internal goroutines, `time.After`, real clock | caller calls `Tick()`; the module has no clock |
| I/O | the module writes its own disk and network | the module *returns* a `Ready` describing what to persist and send; the caller does it |
| Shape | `raft` owns goroutines and channels | `raft` is a pure state machine: `Step(msg) → state change` |
| Testing an election | spin real nodes, sleep past a real 300ms timeout, hope | call `Tick()` 10 times in a loop; whole election in microseconds |
| Reproducing a failure | flaky; timing-dependent | replay the exact message sequence from a seed |
| Concurrency bugs | inside the module, tangled with the algorithm | outside the module, in a thin driver you can read in one sitting |
| Cost | simpler to get the first node running | one more layer of indirection up front |

### The call: deterministic driven core

Three reasons, in order of weight:

1. **Phase 12 is a chaos suite, and it is the phase that makes this project
   resume-credible.** With a driven core you can run a 3-node cluster inside a
   single goroutine, inject a partition as "drop these messages," and replay a
   failing run exactly. With timers inside the module, Phase 12 degrades into
   sleep-and-pray tests that fail once a week in CI and can't be reproduced.
2. **It matches how Track A was built.** The LSM engine kept its hard parts as
   pure, directly-testable units (`Merge`, the SSTable codec, the Bloom filter)
   and pushed I/O to the edges. That's why 111 unit tests run in 0.33s. Same
   discipline, same payoff.
3. **Phases 7 and 8 become additive.** Adding the network is "carry `Message`
   values over gRPC and call `Step`" — not a rewrite. The single-node code path
   written here is the *same* code path a 3-node cluster runs.

The honest cost: more ceremony before the first commit lands, and you must be
disciplined about the persist-before-send ordering in §2 — the driven core
hands you that responsibility instead of hiding it.

**LOCKED: deterministic driven core. No timers, no goroutines, and no I/O
inside the Raft state machine.**

---

## 2. The core API surface and its one hard contract

```go
type Node struct { /* ... */ }

func (n *Node) Tick()                        // one logical clock tick
func (n *Node) Step(m Message) error         // deliver an inbound message
func (n *Node) Propose(cmd []byte) error     // leader-only: append a command
func (n *Node) Ready() Ready                 // what the driver must now do
func (n *Node) Advance()                     // driver reports it's done
```

```go
type Ready struct {
    HardState        *HardState  // non-nil if currentTerm/votedFor/commit changed
    EntriesToPersist []Entry     // append to stable storage before anything else
    Messages         []Message   // send only AFTER the above is fsync'd
    CommittedEntries []Entry     // hand to the state machine
}
```

**The contract — the thing that keeps Raft safe:**

> persist `HardState` + `EntriesToPersist` → **fsync** → send `Messages` →
> apply `CommittedEntries` → `Advance()`

Get that order wrong and you can promise a vote you forget, or tell a peer an
entry is durable before it is. It is the same shape as Phase 5's compaction
contract (*outputs durable → MANIFEST edit fsync'd → inputs deleted*) and Phase
1's WAL contract (*fsync before ack*) — one more instance of a rule you've now
implemented twice.

Phase 6 has no peers, so `Messages` is always empty. The step exists in the
loop anyway, unused, so that Phase 7 fills a slot rather than restructuring.

---

## 3. Log indexing — the off-by-one that eats weeks

The Raft paper is 1-based. Go slices are 0-based. Every implementation pays
for this somewhere; the only question is where.

| | Dummy sentinel at index 0 | Offset arithmetic |
|---|---|---|
| Access | `log[i]` *is* entry `i` | `log[i-firstIndex]`, everywhere |
| Empty log | falls out: `lastIndex()==0`, `lastTerm()==0` | special-cased at every call site |
| `prevLogIndex=0` (Phase 8) | natural base case, no branch | the branch you forget |
| Cost | one wasted `Entry{Term:0, Index:0}` | none |

**LOCKED: dummy sentinel `{Index: 0, Term: 0}` at slot 0.** It is never
applied and never replicated; it exists so that "the entry before the first
real entry" is a real object rather than a special case. Phase 8's log-matching
walkback is exactly where the absence of it bites.

**But** — Phase 9 (snapshotting) truncates the *head* of the log, at which
point a `firstIndex` offset becomes unavoidable. So: expose the log only
through **methods** now —

```go
func (l *Log) At(i uint64) Entry
func (l *Log) Term(i uint64) uint64
func (l *Log) LastIndex() uint64
func (l *Log) LastTerm() uint64
func (l *Log) Slice(lo, hi uint64) []Entry
func (l *Log) TruncateFrom(i uint64) error   // Phase 8's tool; built here
```

— and never index the backing slice from outside `log.go`. Phase 9 then changes
those six method bodies and nothing else in the codebase.

---

## 4. Persistent state — what must survive a crash, and where

Raft Figure 2 is unambiguous: `currentTerm`, `votedFor`, and `log[]` must be on
stable storage **before responding to any RPC**. `commitIndex` and `lastApplied`
are volatile and rebuilt.

### Storage backend

| | Verdict |
|---|---|
| **bbolt** (what etcd uses) | battle-tested, but a whole B-tree dependency for a workload that is 99% append. And a KV store inside a KV store is a confusing thing to explain. |
| **Own append-only file** | mirrors Phase 1 exactly — you have already designed, debugged, and crash-tested this framing. Zero dependencies. You understand every byte. |
| **Reuse the Rust LSM engine** | creates a Track A ↔ Track B dependency that the roadmap explicitly forbids before Phase 10. Rejected outright. |

**LOCKED: own append-only file, reusing Phase 1's framing verbatim — length
prefix + CRC32C per record, torn tail dropped on replay.** Track B stays
self-contained, and the failure mode is one you have already reasoned about.

### Two files, two lifetimes

- **`raft-hardstate`** — `currentTerm` + `votedFor`. ~16 bytes, rewritten
  constantly (every term bump, every vote granted). Write to `.tmp` → fsync →
  rename → fsync dir. The file is smaller than a sector so a torn write is
  arguably impossible, but rename is nearly free here and removes the argument.
- **`raft-log`** — the entries, append-only, Phase-1 framed.

### The one operation the LSM WAL never needed

Raft's log is **not** the LSM's WAL. Phase 8's log repair *overwrites a
conflicting suffix* — an append-only file must support truncation:

```
TruncateFrom(i):  seek to the byte offset of record i  →  File.Truncate  →  fsync
```

This needs an in-memory `index → byte offset` table, built during replay and
extended on every append. Build it in Phase 6 while there's no network noise;
Phase 8 then just *calls* it. **LOCKED.**

---

## 5. Implementation approach

### 5a. Shape

`role` enum (`Follower` / `Candidate` / `Leader`) with a `Step` that dispatches
to `stepFollower` / `stepCandidate` / `stepLeader`. Even at N=1, write the real
three-role machine.

### 5b. Election at N=1 — run the real path, do not shortcut

The temptation is `if len(peers) == 1 { role = Leader }`. Don't. Run it:

```
Tick() past electionTimeout
  → become Candidate
  → currentTerm++
  → votedFor = self          (persist before counting it)
  → record self-vote
  → votes(1) >= quorum(1)    → become Leader
```

Write `quorum()` as `len(peers)/2 + 1` **once**. At N=1 it returns 1, and
"a majority of one is trivial" falls out of the general rule instead of being
a special case that Phase 7 has to delete. Phase 7 then changes exactly one
thing: votes arrive as messages instead of being the only vote in the box.

### 5c. The no-op entry on election

A new leader appends an empty entry in its own term immediately on winning
(Raft §5.4.2). It's how a leader safely learns its own `commitIndex` without
committing an entry from a previous term. Trivial to add now; genuinely
annoying to retrofit once Phase 8's commit logic exists. **Do it in Phase 6.**

### 5d. Commit advance — write the general rule now

Even though N=1 makes it trivial (`matchIndex[self] == lastIndex`), implement
the real thing:

1. collect `matchIndex` across all peers, sort,
2. take the highest index replicated on a majority,
3. **commit it only if `log.Term(i) == currentTerm`** (Raft §5.4.2 — the rule
   that stops a leader from committing a previous term's entry by counting
   replicas).

Phase 8 then adds no new commit logic at all.

### 5e. Apply

Entries in `(lastApplied, commitIndex]` go into `Ready.CommittedEntries`. The
driver hands them to:

```go
type StateMachine interface { Apply(cmd []byte) }
```

Phase 6's implementation appends to a slice. **Phase 10 swaps in the Rust LSM
seam and nothing else changes** — that interface *is* the Phase 10 seam,
declared three phases early.

### 5f. Commands are opaque

`cmd []byte`. Raft never parses it, never knows `PUT` from `DELETE`. The moment
Raft understands the payload, the Phase 10 boundary is gone.

---

## 6. Edge cases

- **Restart always comes back as a Follower** — even if the node was leader.
  Raft has no persistent leader state; a restored-as-leader node is a classic
  self-inflicted split brain.
- **`votedFor` is persisted *before* the vote is granted**, not after. At N=1,
  self-voting, this looks like pointless ceremony. It is the single thing
  preventing a double vote in Phase 7 after a crash mid-election.
- **`currentTerm` is persisted before any response** carrying it. Same
  reasoning.
- **Torn tail in `raft-log`** — dropped, exactly as Phase 1 does. An entry whose
  CRC doesn't check was never acknowledged, so losing it is correct.
- **`commitIndex` / `lastApplied` are not persisted** — `commitIndex` restarts
  at 0 and re-advances; committed entries are therefore **re-applied** after a
  restart. This is only safe if `Apply` is idempotent. `PUT k v` and `DELETE k`
  both are, so it holds for quorumkv — but it is an assumption, and Phase 10
  must re-check it against the LSM engine. Flagged here, resolved there.
- **Empty log** — `lastIndex()==0`, `lastTerm()==0`, straight from the sentinel.
- **`Propose` on a non-leader** returns `ErrNotLeader`. Phase 11 turns that
  error into the client's redirect.
- **A message with a higher term** demotes the node to follower, sets
  `currentTerm` to it, and **clears `votedFor`**. Forgetting the clear is a
  standard bug — it silently disenfranchises the node for a whole term.

---

## 7. Test plan (expands the done-when)

1. **Done-when: ordered commit with correct numbering.** Propose `A`,`B`,`C` →
   all committed, in order, contiguous indexes, all in the leader's term.
2. **Done-when: restart reloads term + log.** Propose, close, reopen → same
   `currentTerm`, same `votedFor`, byte-identical log, entries re-applied.
3. **Election runs the real path.** Fresh node, zero ticks → still Follower.
   Tick past the timeout → Leader, `currentTerm==1`, `votedFor==self`.
4. **Term monotonicity.** `currentTerm` never decreases; `Step` with a higher
   term demotes to Follower **and clears `votedFor`**.
5. **Torn tail.** Chop bytes off the last `raft-log` record, reopen → that entry
   is gone, every prior entry intact.
6. **`TruncateFrom`.** Append 5, truncate from 3 → `lastIndex()==2`; reopen and
   assert the file agrees. (Phase 8's tool, tested in Phase 6's quiet.)
7. **Determinism.** A fixed script of `Tick`/`Step`/`Propose` produces an
   identical `Ready` sequence across two runs. **This is the property Phase 12
   is built on** — if it ever fails, chaos testing is worthless.
8. **`Propose` on a follower** returns `ErrNotLeader`.
9. **Commit rule.** A hand-built log with a previous-term entry replicated on a
   majority is *not* committed until a current-term entry is (§5d/§5.4.2).

Tests 4, 7 and 9 are the ones that earn their keep; 1 and 2 are the done-when.

---

## 8. Explicitly out of scope

No RPC, no gRPC, no protobuf, no real network (Phase 7/8). No snapshots or
`InstallSnapshot` (Phase 9). No LSM engine contact of any kind (Phase 10). No
read-index or lease reads (Phase 11). No batching or pipelining. No cluster
membership change — that's out of scope for the whole project per `DESIGN.md` §1.

---

## 9. Module layout

Matches `DESIGN.md` §9, which names the Go tree `consensus/`:

```
consensus/
├── go.mod          module .../quorumkv/consensus  (stdlib only in Phase 6)
├── raft.go         Node, roles, Step/Tick/Propose/Ready/Advance
├── state.go        HardState, Entry, Message types
├── log.go          Log + the six accessors (§3), TruncateFrom
├── storage.go      raft-hardstate + raft-log files, Phase-1 framing
└── *_test.go
```

---

## 10. Decisions locked

| Decision | Choice |
|---|---|
| Raft core style | **deterministic driven core** (etcd model) — no timers, goroutines, or I/O inside the state machine |
| Driver contract | persist HardState+entries → fsync → send messages → apply committed → `Advance()` |
| Log indexing | dummy sentinel `{0,0}` at slot 0; slice reached **only** through `Log` methods (Phase 9 changes bodies only) |
| Raft persistence | own append-only file, **Phase-1 framing** (length prefix + CRC32C, torn tail dropped) |
| File split | `raft-hardstate` (tmp→rename) + `raft-log` (append-only) |
| Log truncation | `TruncateFrom(i)` via in-memory index→offset table, built in Phase 6, used by Phase 8 |
| Election at N=1 | run the **real** candidate path; `quorum() = len(peers)/2+1`, no N=1 shortcut |
| Leader no-op entry | appended on election win (§5.4.2) |
| Commit rule | general majority-`matchIndex` rule + current-term-only restriction, written now |
| Apply seam | `StateMachine{ Apply([]byte) }` — Phase 6 uses a slice, **Phase 10 swaps in the LSM** |
| Command payload | opaque `[]byte`; Raft never parses it |
| Restart role | always Follower |
| `commitIndex`/`lastApplied` | volatile; committed entries re-applied on restart (requires idempotent `Apply` — re-verified in Phase 10) |
| Dependencies | stdlib only; first dependency (gRPC) arrives in Phase 7 |

**Milestone: a Raft node that elects itself, orders commands, commits them, and
survives a restart — with a state machine you can drive deterministically.**
Phase 7 adds peers and a transport; the state machine written here does not
change.
