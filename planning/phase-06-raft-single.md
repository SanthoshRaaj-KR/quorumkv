# Phase 6 — Single-node Raft (no networking)

> **Concept:** Raft's state machine and log mechanics, in isolation.
> **Done when:** append a sequence of commands and they commit in order with
> correct term/index numbering; restart reloads term + log from disk.

Track B starts here, in **Go**, and starts *without a network* on purpose. A
cluster of exactly one node elects itself leader trivially (majority of 1) and
commits immediately — so we can nail term handling, log indexing, and durable
persistence with zero network noise. Every hard Raft bug is easier to find here
first, before RPC and timers are in the picture.

---

## 1. What we're building

The per-node state from DESIGN §3, persisted correctly:

- **Persistent (survives restart, fsync'd before responding):** `currentTerm`,
  `votedFor`, `log[]` (each entry `{term, index, command}`).
- **Volatile:** `commitIndex`, `lastApplied`, role (`Leader`/`Follower`/`Candidate`).

Plus the operations: append a command to the log, advance `commitIndex`, and
`apply` committed entries to a state machine (a stub for now — prints or appends
to a slice; the real LSM engine arrives at Phase 10).

---

## 2. Algorithm options

### 2a. Raft log storage format

| Option | Notes |
|---|---|
| **Reuse the WAL discipline** (length-prefix + CRC32C + fsync) | Same append-only, crash-safe format we validated in Track A Phase 1 — just storing Raft entries instead of KV records. Consistent, proven |
| A dedicated embedded store (bbolt/BadgerDB) | Less code to write, but hides the mechanics and adds a dependency; the point of this phase is to *own* the log |
| In-memory only | Fails the restart requirement immediately — rejected |

**Reuse the Phase 1 WAL discipline for the Raft log** — LOCKED. An entry is
`{term, index, command}`, length-prefixed, CRC32C'd, fsync'd before the append
is acknowledged. `currentTerm`/`votedFor` are tiny; persist them in a small
separate state file, fsync'd on every change (they change rarely).

### 2b. The `apply` boundary — how Raft hands off committed entries

Define a narrow interface now so Phase 10 can slot the LSM engine in without
touching Raft:

```go
type StateMachine interface { Apply(entry LogEntry) error }
```

Phase 6's implementation just records the entry. Phase 10's implementation calls
the Rust LSM engine. Raft never knows the difference. LOCKED.

### 2c. Persist-before-ack ordering

The Track A invariant carries straight over: a log entry must be **durably
appended (fsync) before** it counts toward commit, and `currentTerm`/`votedFor`
must be **durable before** the node acts on them. This is the Raft safety
foundation — a node that forgets it voted, or forgets a log entry it
acknowledged, breaks consensus.

---

## 3. Implementation approach

- A `RaftNode` struct holding the persistent + volatile state and a
  `StateMachine`.
- `Propose(command)`: append `{currentTerm, nextIndex, command}` to the log
  (fsync); with a cluster of one, it's immediately replicated to a "majority,"
  so advance `commitIndex` and `apply` up to it.
- `applyLoop`: while `lastApplied < commitIndex`, apply the next entry, advance
  `lastApplied`.
- Persistence: on startup, replay the Raft-log WAL to rebuild `log[]`, load
  `currentTerm`/`votedFor` from the state file. `commitIndex`/`lastApplied` start
  at the snapshot/log base and re-advance.
- Roles exist but are trivial here (always leader). The election *machinery*
  (timers, terms) is stubbed structurally so Phase 7 fills it in without a
  redesign.

Keep this in `consensus/` (DESIGN §9): `raft.go` (state + Propose/apply),
`log.go` (the log WAL), `persist.go` (term/vote state file).

---

## 4. Edge cases

- **Restart mid-log** — a torn final entry is dropped on replay (CRC catches it),
  exactly like Track A's WAL. Committed entries survive.
- **Empty log** — index numbering starts clean; first entry is index 1 (index 0
  is a reserved sentinel, simplifies `prevLogIndex` math in Phase 8).
- **Apply is idempotent-friendly** — re-applying an already-applied entry after a
  crash must be harmless; PUT/DELETE are naturally idempotent, and `lastApplied`
  tracking avoids double-apply anyway.

---

## 5. Test plan

1. **In-order commit** — propose commands A,B,C; assert they commit at indices
   1,2,3 with the right term, applied in order.
2. **Term/index numbering** — assert every entry's index is contiguous and terms
   are monotonic.
3. **Restart reload** — propose, restart, assert `currentTerm` and full `log[]`
   reload from disk and `commitIndex` re-advances correctly.
4. **Torn-tail recovery** — corrupt the last log record; restart; assert clean
   recovery of all prior entries (reuses the Phase 1 corruption story).

---

## 6. Decisions locked

| Decision | Choice |
|---|---|
| Language | Go (Track B) |
| Log storage | reuse Phase 1 WAL discipline (length-prefix + CRC32C + fsync) |
| Term/vote persistence | small separate state file, fsync on change |
| `apply` boundary | `StateMachine` interface; stub now, LSM at Phase 10 |
| Index base | index 0 reserved sentinel; first real entry is index 1 |
| Ordering invariant | persist-before-ack for log entries and term/vote |
