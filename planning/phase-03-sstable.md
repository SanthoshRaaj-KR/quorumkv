# Phase 3 — Flush to SSTable

> **Concept:** getting data out of RAM onto disk, immutably.
> **Done when:** write enough keys to trigger 2–3 flushes, restart, and every
> key reads back correctly from disk — including an overwritten key (newest
> SSTable wins) and a deleted key (tombstone wins).

This is the first phase with a real **on-disk file format we design ourselves**.
It has three moving parts: (1) the freeze+swap machinery we deferred from Phase
2, (2) the SSTable writer that serializes a frozen memtable to disk, and (3) a
read path that now spans memory *and* disk. The through-line is **immutability**:
an SSTable is written once and never edited, which is exactly what will let reads
be lock-free even while compaction rewrites files underneath them.

---

## 1. What we're building, in three parts

1. **Freeze + swap** — when `should_flush()` fires (Phase 2), seal the active
   memtable, hand it to an immutable list, install a fresh active memtable, and
   roll the WAL to a new segment. Single winner, as designed in Phase 2.
2. **Flush** — serialize a sealed memtable's sorted entries into one SSTable
   file, durably, then discard the WAL segment it came from.
3. **Read path** — `get(k)` now checks, newest→oldest: active memtable →
   immutable memtables → SSTables. First hit wins (a `Put` returns the value; a
   `Delete` tombstone returns not-found).

Because the memtable is already sorted (Phase 2), the flush is a straight
in-order walk — no sort needed. That's the payoff we pre-paid for.

---

## 2. Algorithm options

### 2a. On-disk layout — how the file is structured

An SSTable isn't just "keys dumped in order." A reader must be able to find a
key without scanning the whole file. The standard shape (LevelDB/RocksDB):

```
┌─────────────────────────────────────────┐
│ Data Block 0   (sorted entries, ~4 KB)   │
│ Data Block 1                             │
│ …                                        │
│ Data Block N                             │
├─────────────────────────────────────────┤
│ Index Block    (one entry per data block)│
├─────────────────────────────────────────┤
│ Footer         (fixed size, at the end)  │
└─────────────────────────────────────────┘
```

- **Data blocks** — the actual key/value entries, grouped into ~4 KB chunks.
  Blocking (vs one entry at a time) means a read pulls a whole block in one I/O
  and the block is the unit the index points at.
- **Index block** — the map from "key range" to "block location." This is the
  decision below (sparse vs dense).
- **Footer** — fixed-size trailer a reader loads *first* to bootstrap: it says
  where the index block is. Put at the end because you don't know the index's
  offset until all data blocks are written.

### 2b. Index density — sparse vs dense

This is the real read-speed-vs-size fork of the phase.

| Option | Index holds | Read cost | Index size |
|---|---|---|---|
| **Sparse** | one entry per *block* (first key + block offset) | binary-search index → read *one* block → scan within it | tiny (1 entry per ~4KB) |
| Dense | one entry per *key* (key + exact offset) | binary-search index → seek straight to key | huge (1 entry per key — rivals the data) |

**Sparse index — LOCKED.** Dense indexing defeats the purpose: the index would
be nearly as big as the data and blow the memory budget. Sparse is the whole
reason blocks exist — you binary-search a small index to find the *one* block a
key could be in, read that 4 KB block, and linearly scan its handful of entries.
This is also what pairs with the Bloom filter in Phase 4 (Bloom says "skip this
file entirely," sparse index says "within this file, read this one block").

### 2c. On-disk entry / tombstone encoding

Reuse the Phase 1 discipline: hand-rolled, length-prefixed, self-describing. One
entry inside a data block:

```
[ klen: u32 ][ key ][ vtype: u8 ][ vlen: u32 ][ value ]
   vtype: 0x01 = Put (value present)   0x02 = Delete (vlen = 0, no value)
```

**Tombstones are written to the SSTable, not dropped at flush.** A `Value::Delete`
in the memtable becomes a `vtype=Delete` entry on disk. It has to persist —
without it, a read would fall through to an *older* SSTable and resurrect the
key. Tombstones only get physically dropped during compaction (Phase 5), and
only when it's provably safe. This is the on-disk continuation of the Phase 2
tombstone model.

### 2d. Prefix compression inside a block — deferred

Sorted keys in a block often share prefixes (`user:1`, `user:2`…), so real
engines store `shared_len + suffix` with periodic "restart points." It shrinks
files meaningfully. **Deferred** — it complicates the block reader and buys size,
not correctness. Full keys for now; note it as a Phase-3.5 optimization. (Same
philosophy as deferring group-commit: correct and legible first.)

### 2e. Which SSTables exist, and their order — file numbering

The read path needs SSTables ordered newest→oldest. Give each a **monotonically
increasing file number** (`000001.sst`, `000002.sst`, …); higher = newer. On
startup, list the directory, sort by number descending, and that's the read
order. A proper atomic **MANIFEST** file (tracking the live set) is deferred to
**Phase 5**, where compaction adds and removes files and directory-listing stops
being safe. For Phase 3 (flush only ever *adds* one file at a time),
list-and-sort is sufficient — LOCKED.

---

## 3. Recommended SSTable file format (locked)

```
Data Block:   [entry][entry]…            entries sorted, ~4 KB target per block
Index Block:  [ blk: first_key(len-pref) | block_offset: u64 | block_len: u32 ]…
Footer (fixed): [ index_offset: u64 | index_len: u32 | magic: u32 | version: u8 ]
```

- Blocks are cut when the running block size crosses the ~4 KB target (a block
  may exceed it slightly to avoid splitting a single large entry — never split
  an entry across blocks).
- The index has one entry per data block: the block's **first key** plus its
  offset+length. Binary search finds the last block whose first key `<=` target.
- The footer is a fixed byte width at EOF, ending with a `magic` constant. A
  reader seeks to `EOF - footer_size`, checks `magic` (detects a truncated /
  non-SSTable file), reads `index_offset/len`, loads the index, and is ready.

---

## 4. Implementation approach

### 4a. Freeze + swap (the Phase 2 deferral, now built)

- `should_flush()` true → the winning thread (compare_exchange on the sealed
  flag) moves the active `Memtable` into an `immutable: Vec<Arc<Memtable>>`
  list, installs a fresh active `Memtable` (counter 0), and starts a new WAL
  segment for it.
- Reads immediately include the immutable memtables (they're still in RAM and
  hold the freshest flushed-pending data), so no read is ever "lost" during the
  window between freeze and flush completing.
- Flush can be synchronous (simplest) or on a background thread. **Start
  synchronous** — flush the sealed memtable inline before returning from the
  write that triggered it. Background flush is a latency optimization for later;
  note it, don't build it (consistent with our correct-first rule).

### 4b. The flush writer

Walk the sealed memtable in sorted order:
1. Open a **temp file** (`000002.sst.tmp`).
2. Accumulate entries into a block buffer; when it crosses ~4 KB, write the
   block, record `(first_key, offset, len)` into the in-memory index, start a
   new block.
3. After the last block, write the index block, then the fixed footer.
4. `fsync` the file, **atomically rename** `.tmp` → `.sst`, `fsync` the directory.
5. Only *after* the rename+dir-fsync succeeds, delete that memtable's WAL
   segment and drop it from the immutable list.

Order in step 4–5 is the crash-safety contract: the SSTable is fully durable
before the WAL that backs it is discarded. Crash at any point = on restart you
either have the WAL (data recoverable by replay) or the finished SSTable, never
neither. This is the same write-new-then-swap discipline compaction will reuse
in Phase 5.

### 4c. WAL segmentation (a change to Phase 1's single WAL)

Phase 1 had one WAL file. Now the WAL is a sequence of **segments**, one per
memtable generation. On freeze, the sealed memtable keeps its segment; the new
active memtable writes to a new segment. When a memtable's SSTable is durable,
its segment is deleted. On restart: load all SSTables (already durable), then
replay any *surviving* WAL segments into memtables — those represent writes that
were acked but not yet flushed. This is a small extension of Phase 1's replay,
not a redesign.

### 4d. Read path

`get(k)`, first hit wins:
1. active memtable — `SkipMap::get`
2. each immutable memtable, newest→oldest
3. each SSTable, newest→oldest (by file number): load footer → index →
   binary-search index for the candidate block → read that block → scan for `k`.
4. A found `Put` returns its value; a found `Delete` returns not-found; falling
   off the end returns not-found.

(Phase 4 inserts a Bloom-filter check before step 3's per-file work so most
SSTables are skipped without touching a block.)

---

## 5. Edge cases

- **Overwritten key across layers** — newer layer is checked first, so the newer
  value/tombstone shadows the older SSTable copy. The old copy lingers on disk
  until compaction (Phase 5) — correct, just not yet space-optimal.
- **Deleted key in an old SSTable** — the tombstone in a newer layer wins;
  reads return not-found even though the value still physically exists below.
- **Crash mid-flush** — `.tmp` file is orphaned; on restart, ignore/delete any
  `.tmp` files, and the WAL segment still exists so the data replays. No loss.
- **Crash after rename, before WAL delete** — both SSTable and WAL segment
  exist; replay re-inserts already-flushed data into a memtable, which is
  harmless (it'll just be re-flushed or read-shadowed). Idempotent.
- **Empty memtable flush** — guard against writing a 0-entry SSTable; skip the
  flush if the sealed memtable is empty.
- **Single entry larger than the block target** — allowed; the block holds that
  one oversized entry rather than splitting it.

---

## 6. Test plan (expands the done-when)

1. **Trigger flushes** — tiny threshold, write enough to force 2–3 SSTables;
   assert 2–3 `.sst` files exist and no `.tmp` remains.
2. **Read back after restart** — restart the process (fresh memtable), read
   every key; all present, read from disk via footer→index→block.
3. **Newest-wins across SSTables** — PUT k=v1 (flush), PUT k=v2 (flush); read k
   → v2. Proves file-number ordering + read merge order.
4. **Tombstone persists to disk** — PUT k (flush), DELETE k (flush), restart,
   read k → not-found. Proves the tombstone was *written*, not dropped at flush.
5. **Sparse index correctness** — write keys spanning many blocks; read a key
   known to live in a middle block; assert only that block is read (instrument
   block reads), not the whole file.
6. **Crash-safety of flush** — kill the process mid-flush (before rename);
   restart; assert data recovers from the WAL segment and the orphan `.tmp` is
   cleaned up.

Tests 4 and 6 are the ones that matter most — 4 proves the delete semantics
survive the memory→disk boundary, 6 proves the immutable/temp-then-rename
discipline actually holds under a crash.

---

## 7. Decisions locked

| Decision | Choice |
|---|---|
| File layout | data blocks → sparse index block → fixed footer |
| Block target | ~4 KB, never split a single entry across blocks |
| Index | **sparse** — one entry (first key + offset) per block |
| Entry format | `klen|key|vtype|vlen|value`, hand-rolled, length-prefixed |
| Tombstones | **written to the SSTable**, dropped only in compaction (Phase 5) |
| File set / ordering | monotonic file numbers, list-and-sort; MANIFEST deferred to Phase 5 |
| WAL | segmented per memtable generation; segment deleted after its SSTable is durable |
| Flush timing | synchronous inline (background flush deferred) |
| Durability | write `.tmp` → fsync → atomic rename → fsync dir → then delete WAL segment |
| Prefix compression | deferred (Phase 3.5 optimization) |

Deferred and why (all consistent with correct-first): **background flush**
(latency, not correctness), **prefix compression** (size, not correctness),
**MANIFEST** (only needed once compaction mutates the file set concurrently).

**Milestone after this phase:** data now survives leaving RAM. Combined with
Phases 1–2 you have durable writes, a sorted in-memory tier, and immutable
on-disk tiers with a working read path across both. Phase 4 (Bloom) makes reads
fast; Phase 5 (compaction) reclaims the space overwrites/tombstones leave behind.
