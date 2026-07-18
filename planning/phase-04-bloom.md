# Phase 4 — Bloom filter

> **Concept:** skipping SSTable files you don't need to read.
> **Done when:** a `GET` for a key present only in the newest SSTable skips the
> older ones instead of scanning them; a `GET` for a totally absent key touches
> *no* SSTable data blocks.

Phase 3 gave us a read path that, worst case, checks every SSTable for a key —
and that gets slower as data grows. A Bloom filter is the probabilistic
"definitely not here / maybe here" test that lets a read skip most files with
**zero disk I/O**. This is the single trick that keeps LSM reads fast.

---

## 1. The one rule that makes Bloom filters safe

A Bloom filter answers "is key `k` in this set?" with two possible outcomes:

- **"No"** — *always correct.* The key is definitely not in this SSTable; skip it.
- **"Maybe"** — might be a **false positive.** Read the block; the key may or may
  not actually be there.

The asymmetry is everything:

- A **false positive** costs one wasted block read. **Harmless.**
- A **false negative** — "no" when the key *is* present — would make a read skip
  a file that has the key, and the data would appear lost. **Catastrophic.**
  Bloom filters *never* produce false negatives **by construction — as long as
  every key in the SSTable was inserted into its filter.**

### Corollary: tombstone keys MUST go into the filter

This ties straight back to the Phase 2/3 tombstone thread. If a DELETE tombstone
is written to an SSTable but its key is *not* added to that file's Bloom filter,
then a later `GET` will bloom-skip the file, miss the tombstone, fall through to
an **older** SSTable, and **resurrect the deleted value.**

So the rule is absolute: **the filter is built over *keys*, and every key that is
written to the SSTable is inserted — `Put` and `Delete` alike.** The filter does
not care about the value type; it only ever answers questions about keys.

---

## 2. Algorithm — Blocked Bloom filter (locked)

We use a **Blocked Bloom filter**, not a classic one. Reasoning:

- **Cache locality.** A classic Bloom filter sets/checks `k` bits at `k` random
  positions across the whole bit array — up to `k` cache misses per lookup. A
  blocked filter confines *all* of a key's bits to a single **64-byte block =
  one CPU cache line**. One cache-line fetch, check all bits, done. For a
  read-amplified workload (traversing millions of connected keys) this is the
  difference between the filter being free and the filter being a CPU bottleneck.
- **Right cost/reward.** Classic is simpler but architecturally outdated for a
  high-read engine. Ribbon filters are more memory-efficient but the math and
  implementation are brutal — not worth it unless we're at strict RocksDB-scale
  RAM limits, which we aren't. Blocked gives ~95% of the cutting-edge benefit
  for a fraction of the effort. (Ribbon stays on the shelf as a future option if
  filter RAM ever becomes the binding constraint.)

### How it works, concretely

- The bit array is a sequence of **512-bit (64-byte) blocks**.
- For a key: hash it once (a fast non-cryptographic hash — **xxh3**). Use the
  high bits of the hash to pick *which block*, then derive `k` bit positions
  *within that one block* and set them.
- **Deriving `k` positions from one hash** (Kirsch–Mitzenmacher double hashing):
  split the 64-bit hash into two 32-bit halves `h1, h2`; bit position `i` is
  `(h1 + i*h2) mod 512`. Avoids computing `k` independent hashes.
- xxh3 is the right hash *here* (fast, good distribution) precisely because this
  is a lookup-distribution problem, not a durability problem — unlike the WAL,
  where we chose CRC32C for the "point at etcd/RocksDB" reason. Different job,
  different tool.

### Tuning — bits per key and `k`

| Parameter | Value | Why |
|---|---|---|
| bits per key | **~10–12** | ~1% false-positive rate; blocked filters lose a little accuracy to the blocking, so bias to the higher end vs a classic filter |
| `k` (bits set) | **~6–7** | optimal `k ≈ 0.7 × bits_per_key`; keep it small so all `k` bits fit comfortably in the 64-byte block |
| block size | **64 bytes** | exactly one cache line — the whole point |

Make bits-per-key a **config knob** (default ~10). It trades RAM for
false-positive rate; a read-heavy deployment can spend more bits to skip more
files. Sized per SSTable from its known key count at flush time.

---

## 3. Where the filter lives — RAM-resident (locked)

**A Bloom filter is loaded into RAM when its SSTable becomes live — at boot for
existing files, at flush-completion for new ones — and stays resident until
compaction retires the SSTable.**

Why not "read from the footer on disk when a query needs it": the filter's whole
job is to *avoid* a disk read. Reading it from disk per query means doing a disk
I/O to save a disk I/O — self-defeating. The check must be in RAM.

Why we can keep them all resident: they're **tiny** — ~10 bits/key ≈ 1.25
bytes/key, so a million keys ≈ 1.25 MB. The RAM budget we manage in this project
is the **64 MB memtable**, not these. Eager residency costs almost nothing and
buys the guarantee that *a Bloom check is never a disk read.*

**Scale-out escape hatch (noted, not built):** RocksDB keeps Bloom blocks in a
bounded, evictable block cache for when there are so many SSTables that their
filters won't all fit in RAM. That's the millions-of-edges future scenario. It
only matters once filter RAM exceeds budget — which at our scale it won't. Add
the evictable cache when a measurement demands it, not before.

---

## 4. SSTable format change (extends Phase 3)

Phase 4 modifies the file we designed in Phase 3. The filter is built *during*
the flush — we're already walking every key in sorted order, so each key is
inserted into the filter as it's written — then serialized into the file:

```
[ Data Block 0 ]
[ … ]
[ Data Block N ]
[ Index Block  ]
[ Bloom Block  ]   ← NEW: the serialized blocked-bloom bit array + its params
[ Footer       ]   ← NEW fields: bloom_offset: u64 | bloom_len: u32
```

- The **Bloom block** stores: the bit array, `k`, block count, bits-per-key, and
  a **CRC32C** over itself (same corruption discipline as the WAL).
- The **footer** grows `bloom_offset`/`bloom_len` so a reader can locate it.
- **On open:** load the bloom block, verify its CRC. If the CRC fails, the filter
  is *derived* data — rebuild it by scanning the SSTable's keys. A Bloom
  corruption is therefore **never a data-loss event**, just a rebuild. (Cheap
  insurance; the filter is fully reconstructable from the data.)

One file-level filter per SSTable — matches the Phase 3 read path. (Partitioned
per-block filters are a RocksDB optimization; not needed here.)

---

## 5. Read path change

Insert one check into the Phase 3 read path, right before doing any per-file work
on an SSTable:

```
get(k):
  active memtable → immutable memtables → for each SSTable newest→oldest:
      if bloom.maybe_contains(k) == NO:  skip file entirely (zero disk I/O)  ← NEW
      else: footer → index → binary-search block → read block → scan for k
```

The Bloom check happens *before* the sparse-index/block read from Phase 3. Bloom
says "skip the file," sparse index says "within a file we couldn't skip, read
this one block." They compose exactly.

---

## 6. Edge cases

- **False positive** — bloom says maybe, block read finds nothing. Correct
  behaviour; just a wasted read. Measurable as the false-positive rate.
- **Tombstone key** — inserted into the filter like any key (§1 corollary). A
  GET for a deleted key must bloom-*hit* the file holding its tombstone.
- **Empty SSTable** — guarded against in Phase 3 (we skip flushing empty
  memtables), so no zero-key filter arises.
- **Corrupt bloom block** — CRC mismatch on open → rebuild from the data blocks;
  never fail the open, never lose data.
- **Hash-seed stability** — the hash seed / algorithm must be fixed and stored
  with the filter, so a filter written today is checked with the identical hash
  on read. Version it in the bloom block params.
- **Very high false-positive rate observed** — signals bits-per-key set too low
  for the key count; it's a performance knob, not a correctness bug.

---

## 7. Test plan (expands the done-when)

1. **Skip older files** — key present only in the newest SSTable; instrument
   block reads; assert older SSTables are bloom-skipped (their filters say no)
   and only the newest file's block is read.
2. **Absent key touches no data blocks** — GET a key never written; assert *zero*
   SSTable data-block reads (all filters say no, modulo the occasional false
   positive which the test tolerates statistically).
3. **No false negatives (the safety test)** — write N keys across several
   SSTables; for every one of the N, assert `maybe_contains` returns true. Not a
   single false negative is acceptable.
4. **Tombstone is bloom-hit** — PUT k (flush), DELETE k (flush), GET k → must
   bloom-*hit* the tombstone file and return not-found. Proves the §1 corollary;
   a regression here silently resurrects deleted data.
5. **False-positive rate in range** — insert N keys, query M absent keys, measure
   the false-positive rate; assert it's near the configured target (~1% at 10
   bits/key). Sanity-checks the tuning and the double-hashing distribution.
6. **Corrupt-then-rebuild** — flip a byte in a bloom block on disk; reopen;
   assert the CRC catches it, the filter rebuilds, and reads still work.
7. **Cache-locality smoke (optional)** — micro-benchmark blocked vs a naive
   classic filter on the same keys to confirm the blocked layout is actually
   faster; documents *why* we chose it.

Tests 3 and 4 are the ones that matter most — they guard the "never a false
negative, tombstones included" invariant that keeps deletes correct.

---

## 8. Decisions locked

| Decision | Choice |
|---|---|
| Filter type | **Blocked Bloom** (64-byte / one-cache-line blocks) |
| Hash | xxh3, split into two 32-bit halves; Kirsch–Mitzenmacher double hashing for `k` positions |
| Tuning | ~10–12 bits/key (config knob, default 10), `k ≈ 6–7` |
| Residency | **RAM-resident, loaded when the SSTable becomes live**; never read from disk per query |
| Granularity | one file-level filter per SSTable |
| Keys inserted | **every key, `Put` and `Delete` alike** (no false negatives, tombstones included) |
| File format | new Bloom block + footer `bloom_offset/len`; built during flush |
| Integrity | CRC32C over the bloom block; corrupt → rebuild from data (never data loss) |
| Read order | Bloom check *before* Phase 3's sparse-index/block read |

Deferred (all consistent with correct-first): **Ribbon filter** (only if filter
RAM ever becomes the binding constraint), **evictable Bloom block cache** (only
once filters exceed the RAM budget), **partitioned per-block filters** (RocksDB
optimization we don't need at this granularity).

**Milestone after this phase:** reads are now fast and scale-stable — most
irrelevant files are skipped with zero disk I/O. What's left in Track A is Phase
5 (compaction) to reclaim the space that overwrites and tombstones leave behind.
