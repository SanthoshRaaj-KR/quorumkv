# Phase 9 — Snapshotting

> **Concept:** stopping the log from growing forever.
> **Done when:** let one node lag far behind, trigger a snapshot on the leader,
> and watch the lagging node recover via `InstallSnapshot` rather than
> entry-by-entry replay — then serve reads correctly.

The log is append-only and, so far, immortal. Every committed command lives in
it forever, which means disk grows without bound and a restarting node replays
its entire history. Snapshotting fixes both: periodically capture the applied
state, discard every log entry the snapshot already covers, and ship the
snapshot in one shot to a follower that has fallen too far behind to catch up
entry-by-entry.

This is the phase Phases 6–8 were built to receive. Two hooks are already
waiting:

- **The log accessor discipline** (`log.go`) — nothing outside `log.go` indexes
  the backing slice, precisely so this phase can introduce a head offset by
  rewriting six method bodies and nothing else.
- **The `sendAppend` panic** (`raft.go`) — *"needs index N, compacted away —
  InstallSnapshot is Phase 9"*. That exact condition is now reachable, and this
  phase turns the panic into an RPC.

---

## 1. Algorithm — what a snapshot *is*, and who owns the bytes

A Raft snapshot is three things: `lastIncludedIndex`, `lastIncludedTerm`, and an
opaque blob of application state. The index/term are Raft's; the blob is the
state machine's, and Raft must never look inside it — the same opacity rule as
the command payload (§5f, Phase 6).

The headline decision: **who serializes the state, and who holds the bytes.**

| | Raft owns a copy of the state | State machine serializes on demand |
|---|---|---|
| Who can snapshot | only what Raft has been shown | the SM, which *is* the state |
| Coupling | Raft must understand the data | Raft stays opaque-only |
| Fit for Phase 10 | wrong — Raft can't hold the LSM's SSTables | right — the LSM hands back a reference to its SSTable set (`DESIGN.md` §3) |

**LOCKED: the state machine serializes itself.** `StateMachine` gains two
methods, additively on the Phase 6 seam:

```go
type StateMachine interface {
    Apply(cmd []byte)
    Snapshot() []byte     // capture applied state
    Restore(data []byte)  // replace state from a snapshot
}
```

`DESIGN.md` §3 is explicit that the production snapshot "can just be the LSM
engine's own current SSTable set, since those are already an immutable
point-in-time view." So this interface is exactly the Phase 10 seam again: the
test SM serializes a slice; the LSM will serialize a manifest reference. **The
one caveat**, flagged now: Phase 9 holds the snapshot *bytes* in the node's
memory to serve `InstallSnapshot`. That is fine for a slice and wrong for a
multi-gigabyte SSTable set — Phase 10 will serve the blob from storage instead of
RAM. Noted so it isn't a surprise.

---

## 2. Algorithm — the log's head offset (the structural change)

Until now `entries[i].Index == i`, and `entries[0]` is the `{0,0}` sentinel. A
snapshot up to index *k* discards entries `1..k`. The elegant move — the reason
the sentinel exists — is that **the boundary becomes the new sentinel**:

```
entries[0] = { Index: lastIncludedIndex, Term: lastIncludedTerm }
```

It is still "the entry before the first real entry," still never applied, still
the base case for `prevLogIndex` matching — it just no longer sits at index 0.
Every accessor gains `offset := entries[0].Index`:

| Accessor | Before | After |
|---|---|---|
| `At(i)` | `entries[i]` | `entries[i-offset]` |
| `Has(i)` | `i <= LastIndex()` | `offset <= i <= LastIndex()` |
| `TruncateFrom(i)` | `i >= 1` | `i > offset` (can't truncate into the snapshot) |
| `LastIndex/LastTerm` | unchanged | unchanged (still the tail) |

Two new methods:

- `CompactTo(index)` — drop the prefix up to `index`, making it the new
  boundary. Keeps the boundary term for future `prevLogTerm` answers.
- `RestoreToSnapshot(index, term)` — throw the log away entirely; the boundary
  is all that remains. Used when installing a leader's snapshot.

**`Has(offset)` stays true**, exactly as `Has(0)` was: a leader may probe at the
snapshot boundary, and the follower must be able to answer `Term(offset)`.

**LOCKED.** This is the change the whole `log.go` accessor discipline was set up
for; it touches no other file.

---

## 3. Algorithm — when to snapshot, and who triggers it

Raft's node has no I/O and no clock — it cannot decide "the log is too big" and
go write a file. So, consistent with the driven-core split:

- **The driver triggers**, on a size policy: `Config.SnapshotThreshold` applied
  entries beyond the last snapshot (default 10000; tests use a tiny value).
- **The driver owns the bytes**: after applying, if the policy trips, it calls
  `sm.Snapshot()`, persists the result, compacts the log file, and *then* tells
  the node "your log is now compacted to *k*."

This keeps `SaveSnapshot` in exactly one place and keeps the node pure. Local
snapshotting therefore does **not** flow through `Ready` — it is driver-side
housekeeping. Only a *received* snapshot flows through `Ready`, because it
originates in `Step` (§4).

**LOCKED: driver-triggered on a size threshold; the node is told after the fact.**

---

## 4. Algorithm — `InstallSnapshot`, and the two directions

### Sending (leader)

`sendAppend` currently panics when a follower's `nextIndex` falls at or below the
compacted boundary. Replace the panic with: send `MsgSnap` carrying the node's
held snapshot (index, term, data). One message replaces thousands of entries —
that is the entire point.

### Receiving (follower)

`Step(MsgSnap)`:

1. Stale term → reject, exactly like `AppendEntries`.
2. `SnapshotIndex <= commitIndex` → already have this state; ack with
   `MatchIndex = commitIndex` and install nothing. (A delayed or redundant
   snapshot must be a no-op — the §2-style idempotence rule, one level up.)
3. Otherwise: `RestoreToSnapshot(index, term)`, set
   `commitIndex = lastApplied = index`, hold the snapshot, and surface it on the
   next `Ready` with a **restore** flag.

The ack reuses `MsgAppResp{Success, MatchIndex}` — so the leader's existing
`handleAppendResponse` advances `matchIndex`/`nextIndex` with no new code.

### The `Ready` addition

```go
type Ready struct {
    ...
    Snapshot *Snapshot  // NEW: persist durably, then compact the log file to Snapshot.Index
    RestoreFromSnapshot bool // NEW: if set, sm.Restore(Snapshot.Data) too
}
```

Driver order, extending the contract once more — snapshot **first**, because a
received snapshot supersedes everything older:

```
persist Snapshot → fsync → compact log file → (restore SM) →
    truncate → persist HardState+entries → fsync → send → apply → Advance
```

Persisting the snapshot before compacting the log file is the same
durable-before-destructive rule as Phase 8's truncate-before-append: a crash
between them must leave the *old* state recoverable, never a gap.

**LOCKED.**

---

## 5. Algorithm — startup after a snapshot exists

This is where the volatile-`commitIndex` decision from Phase 6 gets refined, and
it must be got right or a restart loses committed data.

Before Phase 9, `commitIndex`/`lastApplied` restarted at 0 and every committed
entry was re-applied (safe because `Apply` is idempotent). But after a snapshot,
**the entries below the boundary are gone** — they cannot be re-applied. So on
startup:

1. `OpenFileStorage` loads the snapshot (meta + data) and the surviving log
   (entries strictly above the boundary).
2. `NewNode` builds the log with `entries[0] = {snapshotIndex, snapshotTerm}` and
   sets `commitIndex = lastApplied = snapshotIndex` — **not 0**. Everything the
   snapshot covers is by definition already committed and applied.
3. `NewDriver` calls `sm.Restore(data)` before the first `run()`, so the state
   machine starts from the snapshot, and only entries *above* it are replayed.

**LOCKED.** The Phase 6 note "committed entries re-applied on restart (requires
idempotent Apply)" now reads: *entries above the last snapshot* are re-applied;
the snapshot itself restores in one shot.

---

## 6. Storage — the log file gains a prefix drop

`FileStorage` gains:

- `SaveSnapshot(Snapshot)` — write meta + data to `raft-snapshot.tmp`, fsync,
  rename, fsync dir. Whole-file replace, same discipline as `raft-hardstate`.
- `LoadSnapshot() (Snapshot, bool, error)` — the boundary and blob, or "none."
- `CompactLog(index)` — drop every log record at or below `index`. The log is
  append-only, so this **rewrites** it: copy the surviving tail to
  `raft-log.tmp`, fsync, rename, rebuild the in-memory offset table. O(surviving
  log), run rarely. Not clever; correct and obvious.

The snapshot file is a single blob (`crc32c || len || index(8) term(8) data`),
the same frame family as everything else in both tracks.

**LOCKED.**

---

## 7. Edge cases

- **A redundant/stale `InstallSnapshot`** (`index <= commitIndex`) installs
  nothing — the idempotence rule of §4. Its own test.
- **A snapshot that arrives while the follower is mid-log** — `RestoreToSnapshot`
  discards the whole log, including any uncommitted tail. Correct: an installed
  snapshot is authoritative and strictly newer than anything it replaces.
- **A crash between `SaveSnapshot` and `CompactLog`** — recovers with the old log
  intact plus a new snapshot; the overlap is harmless (the snapshot just covers
  entries the log still holds). A crash the other way is prevented by ordering.
- **`CompactTo` must never pass `lastApplied`** — you cannot snapshot state you
  have not applied. Asserted.
- **A leader compacting away an entry a slow follower still needs** — precisely
  the case `InstallSnapshot` exists for; no longer a panic.
- **The boundary term must survive compaction** — a follower probed at the
  boundary index must still answer `prevLogTerm`. `CompactTo` keeps it.
- **A single-node cluster snapshots too** — and its restart path (§5) is the one
  most likely to expose a `commitIndex`-reset bug, so it gets an explicit test.
- **`lastIndexOfTerm` / hinted backtracking across a boundary** — the leader's
  conflict search must stop at the boundary rather than walk into compacted
  indices. Checked.

---

## 8. Test plan (expands the done-when)

Deterministic `Bus` cluster unless noted.

1. **Done-when: recover via `InstallSnapshot`, not replay.** Crash a follower,
   propose many writes, snapshot the leader (compacting the log), restart the
   follower. Assert it converges, its SM state matches, **exactly one `MsgSnap`
   was delivered**, and no `AppendEntries` carried an entry below the boundary.
2. **The log actually shrinks.** After a snapshot, assert the leader's persisted
   log holds only entries above the boundary and the on-disk log file is smaller.
3. **Restart restores from the snapshot.** Snapshot, restart a node, assert its
   SM equals the pre-restart SM and `commitIndex == snapshotIndex` immediately —
   entries below the boundary were *not* replayed one by one.
4. **A stale `InstallSnapshot` is a no-op** (§7). Deliver a snapshot with
   `index <= commitIndex`; assert nothing is restored and the log is untouched.
5. **The log offset holds** — after `CompactTo`, `At`/`Has`/`Term`/`Slice` all
   answer correctly for indices above, at, and below the boundary; below panics.
6. **A snapshot supersedes a divergent tail.** A follower with an uncommitted
   conflicting suffix receives a newer snapshot; assert the whole log is
   replaced and it matches the leader.
7. **Single-node snapshot + restart** — the §5/§7 path, in isolation.
8. **Snapshot data round-trips through storage** — `SaveSnapshot`/`LoadSnapshot`
   plus corruption rejection, mirroring the hardstate tests.
9. **N=1 and all Phase 6–8 tests still pass unchanged** — the log-offset change
   must be invisible when no snapshot has been taken.
10. **TCP end-to-end** — a real lagging node recovers via `InstallSnapshot` over
    sockets, then serves the correct applied state.

Tests 1 and 3 are the done-when; 4 and 5 guard the correctness core.

---

## 9. Explicitly out of scope

No LSM contact — the SM is still a slice (Phase 10 swaps it, and §1 marks where
the snapshot bytes move from RAM to storage). No client (Phase 11). No chunked
`InstallSnapshot` — the blob is one message, capped by the same 64 MiB frame
limit; chunking is a real-world need this project scope doesn't require, noted
not built. No log-size *auto*-tuning beyond the flat threshold. No membership
change, ever.

---

## 10. Decisions locked

| Decision | Choice |
|---|---|
| Snapshot ownership | the **state machine** serializes/restores; Raft stays opaque. `StateMachine` gains `Snapshot()`/`Restore()` |
| Snapshot bytes location | held in the node's RAM in Phase 9; **Phase 10 serves from storage** (LSM SSTable set), flagged |
| Log head | `entries[0]` becomes the snapshot boundary; every accessor gains an `offset`. `CompactTo`/`RestoreToSnapshot` added |
| Trigger | driver-side, `Config.SnapshotThreshold` applied entries past the last snapshot; node told after the fact |
| Local vs received | local snapshot is driver housekeeping (no `Ready`); a received snapshot flows through `Ready` with a restore flag |
| `InstallSnapshot` | `MsgSnap` replaces the `sendAppend` panic; ack reuses `MsgAppResp` so no new leader code |
| `Ready` contract | gains `Snapshot`/`RestoreFromSnapshot`; order becomes snapshot → compact → restore → truncate → append → send → apply |
| Startup | `commitIndex = lastApplied = snapshotIndex` (not 0); driver `Restore`s the SM before replay — refines the Phase 6 volatile-commit rule |
| Storage | `SaveSnapshot`/`LoadSnapshot` (tmp→rename) + `CompactLog` (rewrite surviving tail) |

**Milestone: Track B is a complete replicated log that no longer grows without
bound — it elects, replicates, commits, compacts, and heals a far-behind node in
one shot. It still stores commands in a slice; connecting that slice to the Rust
LSM engine is Phase 10, the seam this whole track was built toward.**
