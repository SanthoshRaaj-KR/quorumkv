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

### Why not skip list first (even though "real" engines use it)

The design doc name-drops skip list, and it's tempting to reach for the
"production" answer. But the *only* reason production engines pick a skip list
over a B-tree is **lock-free concurrent access** — many threads writing the
memtable at once without a global lock. In Track A we are single-threaded. We
have no concurrent writers to serve. So the skip list would buy us nothing we
can use, while costing a dependency and more code to reason about.

**Recommendation: `BTreeMap`, LOCKED for Phase 2.** Swap to `crossbeam-skiplist`
*only if and when* a later phase introduces concurrent writers to the memtable
(realistically not until you're optimizing, post-Phase 10). Record it as a
known knob, don't pre-pay for it.

> The general principle worth internalizing: pick the data structure your
> *actual* access pattern needs, not the one the famous system uses under its
> *different* access pattern.

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

The memtable is then `BTreeMap<Vec<u8>, Value>`. A `GET` that lands on
`Value::Delete` returns "not found." This one enum is the seed of the entire
LSM read model — it reappears in SSTables, in the read merge order, and in
compaction's "can I finally drop this tombstone?" logic.

---

## 4. Implementation approach

The Phase 1 `Db` wrapper barely changes shape — we're swapping what it holds:

- **State:** `BTreeMap<Vec<u8>, Value>` instead of `HashMap<Vec<u8>, Vec<u8>>`.
- **`put(k, v)`:** `wal.append(Put, k, v)` → on Ok, `map.insert(k, Value::Put(v))`.
- **`delete(k)`:** `wal.append(Delete, k, _)` → on Ok, `map.insert(k, Value::Delete)`.
  Note: **insert a tombstone, don't remove.**
- **`get(k)`:** `match map.get(k)` → `Some(Put(v))` → found; `Some(Delete)` or
  `None` → not found.
- **`replay()`:** unchanged from Phase 1 in spirit — read WAL records, but now
  apply each into the `BTreeMap` as a `Put`/`Delete` marker instead of into a
  HashMap. A `Delete` record replays as inserting a tombstone.

The WAL record format from Phase 1 already distinguishes `op = PUT/DELETE`, so
**no on-disk format change is needed** — we're only changing how a replayed
DELETE is represented in memory (tombstone vs removal). That's a nice
confirmation the Phase 1 format was right.

### Size accounting (a small thing to add now)

The memtable needs to know *how big it is*, because Phase 3 flushes it when it
crosses a size threshold. Start tracking approximate bytes now: on each insert,
add `key.len() + value_size`; it's a running counter. Approximate is fine — it
only triggers a flush, it's not a correctness number. Doing it now means Phase 3
just reads `memtable.size()` instead of bolting on accounting later.

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

Test 3 is the one worth the most — it's the difference between "passes Phase 2"
and "sets up Phase 3 correctly."

---

## 7. Decisions to lock before coding

| Decision | Recommendation | Status |
|---|---|---|
| Memtable structure | `BTreeMap` (skip list only when concurrent writers exist) | recommend LOCK |
| Delete representation | Tombstone (`Value::Delete`), never real removal | recommend LOCK |
| Value model | `enum Value { Put(Vec<u8>), Delete }` | recommend LOCK |
| Size accounting | Approximate running byte counter, added now | recommend LOCK |

Nothing here changes the WAL or its on-disk format — Phase 2 is a pure
in-memory reshaping plus the tombstone concept. The tombstone decision is the
one I'd most want your explicit buy-in on, because it's a small cost now that
prevents a real headache in Phase 3.
