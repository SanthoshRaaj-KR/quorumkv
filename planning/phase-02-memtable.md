# Phase 2 — Memtable

> **Concept:** the in-memory sorted layer that serves live reads and writes.
> **Done when:** `PUT`/`GET`/`DELETE` work in memory; after a crash the memtable
> is rebuilt exactly by replaying the WAL; `GET` on a deleted key returns
> "not found."

Phase 1 gave us durability with a plain `HashMap`. Phase 2 swaps that `HashMap`
for a **sorted** in-memory map. That single word — *sorted* — is the whole
point, and the reason will only fully pay off in Phase 3.

---

## 1. Why sorted, and why now

The memtable is where live writes accumulate before they're flushed to disk.
In Phase 3 we flush it to an **SSTable** (Sorted String Table) — a file whose
entire contract is that keys are in sorted order (that's what makes range scans
and the sparse index work). If the memtable is *already* sorted, the flush is a
straight sequential copy: walk the map in order, write bytes. If it's a
`HashMap`, the flush would have to collect and sort every key first.

So we pay the "keep it sorted" cost now, incrementally on each insert, instead
of paying a big sort at flush time. Same reason you'd keep a mailing list
alphabetized as you add names rather than sorting the whole thing before every
mailing.

The WAL from Phase 1 is unchanged and still the source of truth on disk — the
memtable is just a *better-shaped* reconstruction of the same data. WAL first,
memtable second, exactly as before.

---

## 2. Algorithm options — the in-memory structure

This is the real decision of the phase. All four keep keys sorted; they differ
in *how* and in what they cost.

| Option | Insert / lookup | Concurrency story | Notes |
|---|---|---|---|
| **`std::collections::BTreeMap`** | O(log n), cache-friendly B-tree | Single-writer; needs an outer lock for concurrent access | Zero deps, in the stdlib, dead simple, already sorted, has range iterators. The obvious start |
| Skip list (e.g. `crossbeam-skiplist`) | O(log n) probabilistic | **Lock-free concurrent** reads *and* writes | What RocksDB/LevelDB actually use — because they need concurrent writers. More complex; a dependency |
| Hand-rolled skip list | O(log n) | Whatever you build | Educational but a rabbit hole; easy to get the concurrency subtly wrong. Not worth it here |
| Balanced BST (AVL/red-black by hand) | O(log n) | Manual | BTreeMap *is* essentially this, done for you. No reason to hand-roll |

### Decision: `crossbeam-skiplist` — LOCKED

We're going with **`crossbeam-skiplist`** (a lock-free concurrent ordered map),
the same class of structure RocksDB/LevelDB use. This is a deliberate choice to
support **concurrent writers into the memtable** from the start, rather than a
single-threaded `BTreeMap` we'd later have to replace.

Memtable type: `SkipMap<Vec<u8>, Value>`.

**Consequence to accept with eyes open:** concurrency doesn't stop at the
memtable. The WAL from Phase 1 is a single append-only file with fsync-per-write.
The moment multiple threads can write, they can also race on the WAL, so WAL
`append` must be serialized (a mutex around append+fsync). That serialized fsync
then becomes the write bottleneck — which is *exactly* the place group-commit
(deferred in Phase 1) will eventually earn its keep. We don't build group-commit
now; we just note that skip-list concurrency and WAL serialization are linked,
and the fsync bottleneck is the signal to revisit it later (Phase 8/10, with a
benchmark).

> The tradeoff we're accepting: a dependency + epoch-based memory reclamation
> complexity, in exchange for not rewriting the memtable when concurrency
> arrives. (The alternative, `BTreeMap` + outer lock, is simpler but single-
> writer; we chose the concurrent structure up front.)

---

## 3. Algorithm options — how deletes live in memory

This is the subtler, more interesting decision, and it sets up a concept
(tombstones) that recurs all the way through compaction.

The naive move for `DELETE k` is "remove k from the map." That's actually
**correct in Phase 2** — but it's a trap, because it won't survive contact with
Phase 3. Here's why, and the two ways to handle it:

| Option | `DELETE k` does… | Works in Phase 2? | Survives into Phase 3+? |
|---|---|---|---|
| **Real removal** | `map.remove(k)` | ✅ yes | ❌ **no** — see below |
| **Tombstone** | `map.insert(k, Tombstone)` — a marker meaning "deleted" | ✅ yes | ✅ yes — this is the LSM way |

### Why real removal breaks later

Once we have SSTables on disk (Phase 3), a key can live in an *old* SSTable.
Deleting it from the in-memory map does nothing to that old on-disk copy — a
read would fall through the empty memtable, find the key in the old SSTable, and
resurrect it. The delete would appear to "un-happen."

The LSM answer is a **tombstone**: `DELETE k` writes a marker that says "k is
deleted as of now." On read, if the newest thing you find for `k` is a
tombstone, you return "not found" — the tombstone *shadows* the old value. The
old value gets physically dropped later, during compaction (Phase 5).

**Recommendation: use tombstones from Phase 2 — LOCKED.** Even though real
removal would pass this phase's test, introducing the tombstone concept now
(when the memtable is the only layer) is far cheaper than retrofitting it in
Phase 3. Model each value as an enum:

```
enum Value {
    Put(Vec<u8>),   // a real value
    Delete,         // a tombstone
}
```

The memtable is then `SkipMap<Vec<u8>, Value>`. A `GET` that lands on
`Value::Delete` returns "not found." This one enum is the seed of the entire
LSM read model — it reappears in SSTables, in the read merge order, and in
compaction's "can I finally drop this tombstone?" logic.

---

## 4. Implementation approach

The memtable is its own struct that bundles the map with its size counter —
this bundling is what makes the freeze race-free (see below):

```rust
struct Memtable {
    map:  SkipMap<Vec<u8>, Value>,
    size: AtomicUsize,   // approximate bytes, owned by THIS memtable
}
```

The Phase 1 `Db` wrapper holds one active `Memtable` and does:

- **`put(k, v)`:** `wal.append(Put, k, v)` (under WAL lock) → on Ok,
  `map.insert(k, Value::Put(v))` and `size.fetch_add(k.len()+v.len()+OVERHEAD)`.
- **`delete(k)`:** `wal.append(Delete, k, _)` → on Ok,
  `map.insert(k, Value::Delete)` and `size.fetch_add(k.len()+1+OVERHEAD)`.
  Note: **insert a tombstone, don't remove.** A tombstone still occupies a slot,
  so it still counts toward size.
- **`get(k)`:** `match map.get(k)` → `Some(Put(v))` → found; `Some(Delete)` or
  `None` → not found.
- **`replay()`:** read WAL records, apply each into the `SkipMap` as a
  `Put`/`Delete` marker. A `Delete` record replays as inserting a tombstone.

The WAL record format from Phase 1 already distinguishes `op = PUT/DELETE`, so
**no on-disk format change is needed** — we're only changing how a replayed
DELETE is represented in memory (tombstone vs removal). Confirms the Phase 1
format was right.

### Size accounting — the counter, done right

The memtable must know how big it is so Phase 3 can flush at a threshold. The
counter looks trivial but has three real subtleties that fall out of the
concurrent skip list. The rules:

1. **Owned, not shared/reset.** The `AtomicUsize` lives *inside* the `Memtable`
   struct. We never reset a counter — freezing swaps in a *fresh* `Memtable`
   whose counter is already 0. (Resetting a shared atomic while other threads
   still add to it is a race; owning it sidesteps that entirely.)
2. **Count bytes *written*, monotonic — not bytes *resident*.** Overwriting the
   same key adds to the counter again even though the skip list holds one entry.
   We do **not** try to decrement on overwrite — that adds races for no benefit,
   and over-counting only makes us flush slightly *early*, which is harmless.
   The threshold is approximate by design.
3. **Include per-entry overhead.** `key.len()+value.len()` undercounts real RAM
   — a skip-list node also has pointer towers, the enum tag, and allocation
   overhead (tens of bytes). Add a fixed `OVERHEAD` constant per entry (say 64)
   so "64MB counted" is a safe over-estimate of real memory, not an under-estimate.
   For a DELETE, value is 0 bytes but still count `key.len()+1+OVERHEAD`.

**Threshold is configurable, default 64MB.** Tests set it tiny (e.g. 1KB) to
trigger a flush without writing 64MB. Phase 2 only exposes `approx_size()` and a
`should_flush()` predicate (`size >= threshold`); the actual freeze happens in
Phase 3.

### The freeze — described here, implemented in Phase 3

When a write pushes `size` past the threshold, the active memtable must become
immutable and a fresh one take over. With concurrent writers this needs **exactly
one winner**, or two threads both freeze and in-flight writes are lost:

- Whoever's write crosses the threshold attempts to seal the active memtable —
  via a `compare_exchange` on a "sealed" flag (or swapping the active-memtable
  pointer under a short lock).
- The single winner moves active → immutable list and installs a fresh
  `Memtable` (counter 0). Every other thread sees "already sealed" and simply
  writes into the new active memtable.

Phase 2 builds the counter + predicate; Phase 3 builds this swap + the flush to
SSTable. Keeping the boundary clean means Phase 2 stays purely in-memory.

---

## 5. Edge cases

- **Overwrite** — `PUT k v1; PUT k v2` → `BTreeMap::insert` replaces; newest
  wins in memory. (On disk both are still in the WAL; replay applies them in
  order, so the map ends at v2. Correct.)
- **Delete then re-put** — `DELETE k; PUT k v` → map ends with `Put(v)`. The
  tombstone was overwritten. Fine.
- **Get on tombstone** — returns not-found, the headline correctness case.
- **Empty value vs deleted** — `PUT k ""` (empty value) is `Value::Put(vec![])`,
  which is *found* with an empty value — distinct from `Value::Delete`. The enum
  keeps these unambiguous; a "value is empty means deleted" hack would not.
- **Replay ordering** — WAL is append-order = write-order, so replaying
  top-to-bottom into the BTreeMap naturally lands on the latest value per key.

---

## 6. Test plan (expands the done-when)

1. **Sorted iteration** — insert keys out of order (`c, a, b`), iterate the
   memtable, assert you get `a, b, c`. Proves the "sorted" property Phase 3 needs.
2. **PUT/GET/DELETE in memory** — basic round-trip; deleted key reads not-found.
3. **Tombstone shadows, not removes** — DELETE a key, then iterate the memtable
   and assert the key is *still present as a tombstone* (not gone). This is the
   test that distinguishes tombstone-model from removal-model, and it's the one
   that matters for Phase 3.
4. **Crash rebuild** — PUT/DELETE a mix, `kill -9`, restart, replay WAL, assert
   the rebuilt BTreeMap matches (including deleted keys reading not-found).
5. **Empty value ≠ deleted** — `PUT k ""` then GET → found, empty. DELETE k
   then GET → not found. Assert the two are distinguishable.
6. **Overwrite / delete-then-reput** — the two edge cases above.

7. **Counter counts writes** — PUT the same key 3 times; assert the size
   counter grew ~3× the entry size (monotonic, not deduplicated). Proves the
   "bytes written, not resident" rule.
8. **`should_flush()` fires** — set threshold tiny (e.g. 1KB), write until
   `should_flush()` returns true, assert it flips at roughly the right byte count.
9. **Concurrent writers, no lost updates** *(the skip-list payoff test)* —
   spawn N threads each doing M distinct PUTs; join; assert all N×M keys are
   present and the counter equals the expected total. Proves the concurrent
   structure + atomic counter actually hold under contention.

Test 3 sets up Phase 3 correctness; test 9 is the one that justifies choosing a
concurrent skip list over a `BTreeMap` in the first place.

---

## 7. Decisions to lock before coding

| Decision | Choice | Status |
|---|---|---|
| Memtable structure | `crossbeam-skiplist` `SkipMap` (concurrent from the start) | LOCKED |
| Delete representation | Tombstone (`Value::Delete`), never real removal | LOCKED |
| Value model | `enum Value { Put(Vec<u8>), Delete }` | LOCKED |
| Size counter | `AtomicUsize` **owned by** the `Memtable` struct | LOCKED |
| Counting rule | bytes written (monotonic) + fixed per-entry `OVERHEAD`; tombstone counts `key+1+OVERHEAD` | LOCKED |
| Flush threshold | configurable, default 64MB; tiny in tests | LOCKED |
| Freeze safety | single winner via compare_exchange / pointer swap (impl in Phase 3) | LOCKED |

Consequence carried forward: concurrent writers mean the WAL append+fsync must
be serialized (mutex), and that serialized fsync is the future group-commit
trigger point (Phase 8/10). Noted, not built now.
