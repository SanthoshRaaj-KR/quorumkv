# Phase 8 — Log replication over RPC

> **Concept:** getting all nodes to agree on the same ordered log.
> **Done when:** writes sent to the leader appear on all followers in the same
> order. Stop a follower, send 50 writes, restart it → it catches up and
> converges to the identical log. No committed entry ever disappears.

Phase 7 elected a leader that says nothing. This phase gives its `AppendEntries`
a payload, teaches a follower to detect and repair divergence, and makes
`matchIndex` actually move — which is what finally lets the commit rule written
back in Phase 6 commit something.

**Nothing commits today, by construction** (phase-07 §9): heartbeats probe
`nextIndex-1`, every follower with a shorter log fails the match, so no
`matchIndex` advances. Phase 8 is the phase that closes that loop.

---

## 0. What is already built

Unusually much, because Phases 6 and 7 deliberately front-loaded it:

| Piece | Where | State |
|---|---|---|
| `Log.TruncateFrom` | `log.go` | built + tested in Phase 6 |
| `FileStorage.TruncateFrom` (index→offset table) | `storage.go` | built + tested in Phase 6 |
| General commit rule + current-term restriction | `raft.go` `maybeAdvanceCommit` | built + tested in Phase 6 |
| `Message.Entries` field and its codec | `state.go`, `transport.go` | built + tested in Phase 7 |
| Log-match check on the follower | `handleAppendEntries` | built in Phase 7 |
| `nextIndex`-anchored probes | `broadcastHeartbeat` | built in Phase 7 |
| `matchIndex`/`nextIndex` success path | `handleAppendResponse` | built in Phase 7 |

So Phase 8 is genuinely additive. The new work is: carry entries, truncate on
conflict, backtrack on rejection, and one addition to the `Ready` contract.

---

## 1. Algorithm — how the leader finds the point of agreement

When a follower rejects, the leader must find the last index where the two logs
agree. This is the phase's headline decision.

| | Linear decrement (paper, basic) | Follower-hinted backtracking (paper §5.3 optimization) | Binary search |
|---|---|---|---|
| Mechanism | `nextIndex--`, retry | follower returns the conflicting term and the first index it holds for that term; leader jumps straight past it | leader probes midpoints |
| Round trips for a follower 50 entries behind | **50** | typically **1–2** | ~6 |
| Round trips for a divergent *term* (100 entries) | 100 | 1 | ~7 |
| Extra state | none | two `uint64` on the response message | none |
| Complexity | trivial | ~15 lines | fiddly, and it interacts badly with concurrent appends |

### The call: hinted backtracking, with linear decrement as the fallback

1. **The done-when directly exercises this path.** "Stop a follower, send 50
   writes, restart it" with linear decrement is 50 round trips × a 50 ms
   heartbeat ≈ **2.5 seconds** to converge. It would pass, barely, and it would
   be the wrong lesson.
2. **Retrofitting it later is a wire-format change.** `Message` gains
   `ConflictIndex`/`ConflictTerm`, so `EncodeMessage`/`DecodeMessage` change.
   Doing that once, now, is cheaper than doing it twice.
3. **Correctness never depends on the optimization.** If a response carries no
   hint (zero values), the leader falls back to `nextIndex--`. The hint is a
   speedup over a path that is already correct — which is the only safe way to
   ship an optimization in a consensus algorithm.

**LOCKED: hinted backtracking, linear decrement as the fallback.**

### The hint, precisely

When a follower rejects at `prevLogIndex`:

- **Its log is too short** — it has no entry at `prevLogIndex`:
  `ConflictIndex = lastIndex + 1`, `ConflictTerm = 0`. The leader jumps
  `nextIndex` straight to the follower's end.
- **It has the index but the term differs** — `ConflictTerm` = the term it holds
  there, `ConflictIndex` = the *first* index it holds for that term. The leader
  skips its own entire run of that term in one hop.

---

## 2. Algorithm — the correctness core: truncate **only** on a real conflict

This is Phase 8's equivalent of Phase 5's tombstone-drop rule: the one thing
that, done wrong, silently destroys committed data.

Raft §5.3: *"If an existing entry conflicts with a new one (same index but
different terms), delete the existing entry and all that follow it."*

The trap is the word **conflicts**. The naive reading — "on every
`AppendEntries`, truncate at `prevLogIndex+1` and append what arrived" — is
wrong, and it fails in a way tests rarely catch:

> A leader sends entries 5–7. The follower appends them and replies. The reply is
> lost. The leader retries. Meanwhile the follower also received entries 8–10
> from a later, larger batch and appended those too. The **delayed duplicate** of
> 5–7 arrives again. Truncate-then-append discards 8–10 — entries that may
> already be **committed** on a majority.

So the rule is:

1. Walk the incoming entries against the local log, index by index.
2. Find the first index where the terms **differ**. That, and only that, is a
   conflict.
3. Truncate from there; append the remainder.
4. If every incoming entry matches what is already there, **append nothing**.
   The RPC is a no-op — correctly, because it is a duplicate.

`AppendEntries` must be **idempotent**. **LOCKED**, and it gets a dedicated
test (§6.5) because it is the one that matters most.

### The commit-index bound has the same shape

Figure 2 says a follower sets
`commitIndex = min(leaderCommit, index of last new entry)` — **not**
`min(leaderCommit, localLastIndex)`. Phase 7 currently does the latter
(`raft.go`, `handleAppendEntries`). It is harmless today because nothing
commits, and it is wrong the moment entries flow: a delayed `AppendEntries`
carrying a stale-but-high `leaderCommit` could mark entries committed that this
RPC never covered. **Fix it in Phase 8** — noted here so it isn't lost.

---

## 3. Algorithm — one addition to the `Ready` contract

Truncation is a **durable** operation. `Ready` currently describes only appends,
so there is nowhere to say "discard from index i before you append." Add it:

```go
type Ready struct {
    HardState        *HardState
    TruncateFrom     *uint64   // NEW: durably discard >= this index, first
    EntriesToPersist []Entry
    Messages         []Message
    CommittedEntries []Entry
}
```

The driver's order becomes:

```
truncate → append → fsync → send → apply → Advance
```

Truncate-before-append is not stylistic: appending first and truncating after
leaves a window where a crash exposes a log with the wrong suffix.

**This is the only change to the Phase 6 contract, and it extends it rather than
bending it.** Everything else — persist before send, send before apply — stands
unchanged. `Storage.TruncateFrom` already exists and is already tested; the
driver just gains one call. **LOCKED.**

Worth noticing: a follower replying `Success` only *after* its entries are on
disk falls out for free. `handleAppendEntries` queues the reply into
`pendingMsgs`, and the driver persists `EntriesToPersist` before it sends
`Messages`. The durability guarantee the majority-commit rule depends on is
already structural — it needs no new code, because Phase 6 §2 put the ordering
in one place.

---

## 4. Algorithm — batching and when to send

### How many entries per RPC

| One at a time | correct, and pathologically slow — 1 RTT per entry |
| Everything from `nextIndex` to `lastIndex` | a follower a million entries behind gets a million-entry message; `ReadMessage` already refuses frames over 64 MiB, so this doesn't just get slow, it **deadlocks the catch-up** |
| Capped batch | bounded memory, bounded frame size, converges in `n/cap` round trips |

**LOCKED: capped batch.** `MaxEntriesPerAppend` (default **64**) and
`MaxBytesPerAppend` (default **1 MiB**), both on `Config`. The byte cap matters
independently — 64 entries of 1 MiB each would blow the frame limit.

### When the leader sends

| Heartbeat only | commit latency = the heartbeat interval (50 ms), regardless of load |
| Immediately on `Propose`, heartbeats for repair/liveness | commit latency = 1 RTT |

**LOCKED: send immediately on `Propose`; heartbeats remain the repair and
liveness mechanism.** A heartbeat that finds a follower behind carries entries
too — that is what makes a restarted follower catch up without a write arriving.

---

## 5. Edge cases

- **Delayed duplicate `AppendEntries`** — the §2 trap. Idempotent, no truncation.
- **A follower whose log is longer than the leader's** — an *empty*
  `AppendEntries` must never truncate it. The extra entries are uncommitted and
  will be overwritten when the leader actually sends conflicting entries.
- **A committed entry must never be truncated.** Guaranteed upstream by the
  §5.4.1 election restriction (already built and tested in Phase 7): a candidate
  missing committed entries can never win, so no leader ever orders their
  removal.
- **`matchIndex` must never move backwards** — always `max(old, new)`. A stale,
  reordered response would otherwise un-commit an entry.
- **`nextIndex` must never fall below 1** — clamp it; index 0 is the sentinel.
- **A rejection from a stale term** is ignored (already handled by the `Step`
  term preamble).
- **`nextIndex` below the log's first index** — impossible until Phase 9 adds
  snapshots and truncates the log's head; that is where `InstallSnapshot` comes
  from. Assert-and-fail loudly for now rather than silently mis-serving.
- **The leader counting its own entries** — the leader's `matchIndex[self]`
  tracks its log's last index, which may include entries not yet fsync'd. Safe
  only because the driver persists before sending, so nothing is observable
  before it is durable. Re-verified here since Phase 8 is where it starts to
  matter.
- **A single-node cluster** must keep behaving exactly as it does today — the
  N=1 regression tests from Phase 6 stay green throughout.

---

## 6. Test plan (expands the done-when)

Deterministic `Bus` cluster unless noted.

1. **Done-when: same order everywhere.** Propose 20 commands on the leader;
   assert all three logs are byte-identical and every state machine applied the
   same sequence.
2. **Done-when: a lagging follower catches up.** Crash a follower, propose 50
   writes, restart it; assert it converges to the identical log — and in **few
   round trips**, not 50 (asserting the §1 hint actually works).
3. **Done-when: no committed entry ever disappears.** A cluster-wide invariant
   checked after *every* step, alongside the existing one-leader-per-term check:
   once an index is committed anywhere, no node's entry at that index may ever
   change term or vanish.
4. **Log Matching Property as an invariant** — if two logs agree at index `i`,
   they are identical for every `j <= i`. Asserted cluster-wide after every step.
5. **A delayed duplicate `AppendEntries` truncates nothing** (§2). Hand-built:
   deliver 5–7, then 8–10, then replay 5–7; assert 8–10 survive. **The test that
   matters most in this phase.**
6. **A genuinely divergent suffix is replaced.** Give a follower entries from a
   dead term; assert they are truncated and replaced, and that the follower's log
   then matches the leader's exactly.
7. **A minority cannot commit.** 5 nodes, 3 crashed; propose; assert
   `commitIndex` never advances and nothing is applied anywhere.
8. **Batch caps hold** — propose 500 entries; assert no `AppendEntries` exceeds
   `MaxEntriesPerAppend` or `MaxBytesPerAppend`.
9. **Restart preserves the replicated log.** Crash and restart every node in
   turn; assert each returns with its log intact and rejoins without divergence.
10. **TCP end-to-end** — 3 real `Server`s: propose on the leader, poll until all
    three state machines have applied the same sequence.
11. **N=1 regression** — every Phase 6 single-node test still passes unchanged.

Tests 3, 4 and 5 are the safety core. 1, 2 and 9 are the done-when.

---

## 7. Explicitly out of scope

No snapshots or `InstallSnapshot` (Phase 9 — and §5 marks exactly where it will
plug in). No LSM contact (Phase 10). No client or redirect (Phase 11) — though
`leaderID` is already tracked. No read-index/lease reads. No pipelining or
flow control beyond the batch caps. No membership change, ever.

---

## 8. Decisions locked

| Decision | Choice |
|---|---|
| Divergence repair | follower-hinted backtracking (`ConflictIndex`/`ConflictTerm`); **linear `nextIndex--` as the correctness fallback** |
| Truncation rule | truncate **only** at the first index whose term actually differs; `AppendEntries` is idempotent |
| Follower commit bound | `min(leaderCommit, index of last new entry)` — fixes the Phase 7 `localLastIndex` bound |
| `Ready` contract | gains `TruncateFrom *uint64`; driver order becomes truncate → append → fsync → send → apply |
| Batching | `MaxEntriesPerAppend` = 64, `MaxBytesPerAppend` = 1 MiB, both on `Config` |
| Send timing | immediately on `Propose`; heartbeats carry entries when a follower is behind |
| `matchIndex` | monotonic — always `max(old, new)` |
| `nextIndex` | clamped to `>= 1`; below the log's first index is a Phase 9 (`InstallSnapshot`) condition, loud failure until then |
| Wire format | `Message` gains two `uint64` fields; codec updated. No compatibility concern — nothing is deployed |

**Milestone: Track B is a working replicated log. It orders and replicates
opaque commands across a real cluster and never loses a committed one — it just
doesn't store them anywhere real yet. That is Phase 10.**
