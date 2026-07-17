# quorumkv — phase-by-phase execution plan

Derived from `DESIGN.md` §6. The design roadmap has 7 steps, but several
bundle multiple concepts together. Here each phase teaches **exactly one
idea** and has a concrete "done when…" test you can run before moving on.

**Rule of thumb:** don't start a phase until the previous phase's test passes.
Don't add a feature a phase doesn't ask for (no networking in phase 1, no
compaction in phase 3, etc.). Building in isolation is what keeps each bug
traceable to one concept.

There are two independent tracks that merge at Phase 10:

- **Track A — Rust LSM storage engine** (Phases 1–5): local disk storage.
  Knows nothing about clusters.
- **Track B — Go Raft consensus** (Phases 6–9): replication + ordering.
  Knows nothing about how data is stored.
- **Merge + polish** (Phases 10–12): connect them, add a client, break it on
  purpose.

You can build Track A and Track B in either order — they don't depend on each
other until Phase 10.

---

## Track A — Rust LSM storage engine

### Phase 1 — Write-ahead log (WAL)
**Concept:** durability. Nothing else.

**Build:** an append-only file. Every `PUT`/`DELETE` is serialized, appended,
and `fsync`'d before the call returns. On startup, replay the file top to
bottom to rebuild in-memory state (for now, just a plain `HashMap`).

**Done when:** you can write 100 keys, kill the process mid-run (`kill -9`),
restart, and every *acknowledged* write is still there. An unacknowledged
write (killed before fsync returned) is allowed to be missing — that's correct.

**Why it's first:** it's the smallest thing that is actually a database. If
this is wrong, nothing above it can be trusted.

---

### Phase 2 — Memtable
**Concept:** the in-memory sorted layer that serves live reads and writes.

**Build:** replace the `HashMap` with a **sorted** map. Start with Rust's
`std::collections::BTreeMap` — it's O(log n) and already sorted. (The design
says skip list; that's an optimization for concurrency you can swap in later.
Don't start there — it's a distraction from the LSM structure.) Order of
operations per write: append to WAL first, *then* insert into the memtable.

**Done when:** `PUT`/`GET`/`DELETE` all work in memory, and after a crash the
memtable is rebuilt exactly by replaying the WAL. `GET` on a deleted key
returns "not found."

**Why sorted matters:** the next phase flushes this to disk, and SSTables must
be sorted. Sorting in memory now makes the flush a straight copy.

---

### Phase 3 — Flush to SSTable
**Concept:** getting data out of RAM onto disk, immutably.

**Build:** when the memtable passes a size threshold (start tiny, e.g. 1MB, so
you can trigger it easily in tests), freeze it and write it out as a sorted
on-disk file (the SSTable). After a successful flush, the WAL entries it
covered can be discarded and the memtable reset. Update the read path:
check the live memtable first, then each SSTable newest-to-oldest.

**Done when:** you write enough keys to trigger 2–3 flushes, restart the
process, and every key reads back correctly from disk — including a key that
was overwritten (newest SSTable wins).

**Key property to internalize:** an SSTable is **never edited after writing**.
Overwrites and deletes don't touch it; they live in newer files and win by
being newer. Immutability is what makes reads lock-free later.

---

### Phase 4 — Bloom filter
**Concept:** skipping disk files you don't need to read.

**Build:** when writing an SSTable, also write a Bloom filter over all its
keys. On read, before opening an SSTable's data, ask its Bloom filter "could
this key be here?" If it says no, skip the file entirely — zero disk I/O.

**Done when:** you can instrument reads and show that a `GET` for a key only
present in the newest SSTable skips the older ones instead of scanning them.
A `GET` for a totally absent key touches *no* SSTable data blocks.

**Why it matters:** without this, every read scans every file and gets slower
as data grows. This is the trick that keeps LSM reads fast.

---

### Phase 5 — Compaction
**Concept:** garbage collection for the storage engine.

**Build:** size-tiered compaction (simplest). Pick several similarly-sized
SSTables, merge them into one larger sorted file, dropping (a) older values of
keys that were overwritten and (b) "tombstones" for deleted keys. **Write the
merged file to a new path and only delete the inputs after it's fully written
and fsync'd** — never mutate in place. This is also the disk-full safety
property from `DESIGN.md` §8.

**Done when:** write the same 10 keys 1000 times each, run compaction, and
verify (a) total disk size drops dramatically, (b) every key still returns its
latest value, and (c) a deleted key stays deleted.

**Milestone:** Track A is now a complete standalone LSM key-value engine. You
could ship it as a library on its own.

---

## Track B — Go Raft consensus

### Phase 6 — Single-node Raft (no networking)
**Concept:** Raft's state machine and log mechanics, in isolation.

**Build:** the per-node state — `currentTerm`, `votedFor`, `log[]`,
`commitIndex`, `lastApplied` — as a "cluster" of exactly one node. It elects
itself leader (a majority of 1 is trivial), appends commands to its own log,
and commits them immediately. No RPC, no timers-that-matter yet. `apply` for
now just prints or appends to a slice.

**Done when:** you append a sequence of commands and they commit in order, with
correct term/index numbering. Restarting reloads term + log from disk.

**Why isolate this:** it lets you get term handling and log indexing correct
with zero network noise. Every hard Raft bug is easier to find here first.

---

### Phase 7 — Leader election over real RPC
**Concept:** electing exactly one leader among many, tolerating crashes.

**Build:** 3 nodes over gRPC. Implement `RequestVote` and randomized election
timeouts (150–300ms). A node votes yes only if the candidate's term ≥ its own
**and** the candidate's log is at least as up-to-date (`lastLogTerm` then
`lastLogIndex`). Add heartbeats (empty `AppendEntries`) so a live leader keeps
followers from timing out.

**Done when:** bring up 3 nodes → exactly one becomes leader. Kill the leader →
a new leader is elected within a second. Bring the old one back → it becomes a
follower, doesn't disrupt anything.

**The one check that matters most:** the "log at least as up-to-date" rule.
It's what stops a stale node from winning and erasing committed data.

---

### Phase 8 — Log replication over RPC
**Concept:** getting all nodes to agree on the same ordered log.

**Build:** full `AppendEntries` with `prevLogIndex`/`prevLogTerm`. A follower
rejects if its log doesn't match at `prevLogIndex` (the **log matching
property**). On rejection, the leader walks `prevLogIndex` backward until it
finds agreement, then overwrites the follower forward. Mark an entry committed
once a **majority** have durably appended it; propagate `commitIndex` on
heartbeats.

**Done when:** writes sent to the leader appear on all followers in the same
order. Stop a follower, send 50 writes, restart it → it catches up and
converges to the identical log. No committed entry ever disappears.

---

### Phase 9 — Snapshotting
**Concept:** stopping the log from growing forever.

**Build:** periodically snapshot applied state and discard log entries older
than it. Add the `InstallSnapshot` RPC so a follower that's fallen too far
behind gets the snapshot in one shot instead of thousands of entries.

**Done when:** let one node lag far behind, trigger a snapshot on the leader,
and watch the lagging node recover via `InstallSnapshot` rather than
entry-by-entry replay — then serve reads correctly.

**Milestone:** Track B is now a working replicated log. It orders and
replicates opaque commands; it just doesn't store them anywhere real yet.

---

## Merge + polish

### Phase 10 — Connect Raft's `apply` to the LSM engine
**Concept:** the single seam where the two halves meet.

**Decision to make first (see "Open decisions" below):** *how* does Go call
into the Rust engine — FFI, a local gRPC sidecar, or Rust-as-subprocess? Pick
one; the simplest to start is a local gRPC service the Go node talks to.

**Build:** when Raft commits entry `{PUT k v}`, its `apply` callback hands that
command to the local Rust LSM engine, which does a normal WAL+memtable write.
`GET` reads from the local engine.

**Done when:** `PUT(k,v)` sent to the leader ends up readable via the LSM
engine on **all three** nodes. Kill and restart a node → its LSM state is
rebuilt and consistent with the others.

**This is the payoff phase** — the first moment it's genuinely a *replicated*
key-value store.

---

### Phase 11 — Client library
**Concept:** making the cluster usable from outside.

**Decision to make first:** pick a read consistency mode from `DESIGN.md` §5
(linearizable / leader-only / follower read-index) and document the tradeoff.
Start with **leader-only** — simplest — and note the stale-read window.

**Build:** a client that finds the leader, follows `NotLeaderError` redirects,
and retries on leader change. `PUT`/`GET`/`DELETE` from a single call site.

**Done when:** a client keeps working through a leader election without the
caller writing any retry logic themselves.

---

### Phase 12 — Chaos test suite
**Concept:** proving correctness under the failures that matter. This is the
phase that makes the project resume-credible (`DESIGN.md` §8).

**Build the five fault injections:**
1. Kill the leader mid-write → no *acknowledged* write is ever lost.
2. Partition a minority away → majority keeps serving reads and writes.
3. Heal the partition → the isolated side catches up via log repair (or
   snapshot) without diverging.
4. Fill the disk mid-compaction → no existing SSTable is corrupted.
5. *(stretch)* Jepsen-style harness that injects faults automatically and
   checks the operation history is linearizable.

**Done when:** all five run in CI and pass repeatably.

---

## Open decisions (resolve before the phase that needs them)

| Decision | Blocks | Recommended start |
|---|---|---|
| Memtable structure: BTreeMap vs skip list | Phase 2 | BTreeMap; swap to skip list only if concurrency demands it |
| Compaction: size-tiered vs leveled | Phase 5 | Size-tiered (simpler) |
| Rust ↔ Go boundary: FFI / gRPC sidecar / subprocess | Phase 10 | Local gRPC sidecar (easiest to debug) |
| Read consistency mode | Phase 11 | Leader-only, document the stale-read window |

## Suggested order

Track A and Track B are independent until Phase 10. If you want to see a
"database" work end-to-end soonest, do **Track A first** (Phases 1–5) — you get
a usable local KV store at Phase 5. Then Track B (6–9), then merge.
