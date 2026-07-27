# Phase 5 — Compaction

> **Concept:** garbage collection for the storage engine.
> **Done when:** write the same 10 keys 1000× each, run compaction, and verify
> (a) total disk size drops dramatically, (b) every key still returns its latest
> value, and (c) a deleted key stays deleted.

Every overwrite and every delete leaves garbage on disk: old values shadowed by
newer ones, tombstones shadowing values below them. Reads stay correct (newest
layer wins) but files pile up and space + read cost creep. Compaction merges
SSTables into fewer, cleaner files — dropping dead data — **without ever mutating
a file in place.** It's the largest phase in Track A because it touches three
hard things at once: strategy, tombstone-drop safety, and atomic file-set
changes under concurrency.

---

## 1. Algorithm — strategy: size-tiered vs leveled

This is the phase's headline decision, and for a **read-heavy** workload it is
*not* the automatic "size-tiered because simpler."

| | Size-tiered | Leveled |
|---|---|---|
| Organization | merge SSTables of *similar size* into a bigger one | levels L0…Ln; each level (except L0) has **non-overlapping** key ranges |
| A key can live in | *many* overlapping SSTables at once | at most **one** SSTable per level |
| Read amplification | **high** — a read may check many files | **low** — at most one file per level |
| Write amplification | **low** — data rewritten rarely | **high** — data rewritten as it descends levels |
| Space amplification | high — old versions linger longer | low — compaction is aggressive |
| Used by | Cassandra | RocksDB, CockroachDB/Pebble, LevelDB |

### The call: leveled is the target, size-tiered is the first implementation

For CodeXyro's dependency traversal — millions of connected keys, heavy read
amplification — **read cost is the thing to minimize**, and leveled's "at most
one SSTable per level" is exactly that guarantee. We already pay for expensive
writes (fsync-per-commit), so trading *more* write amplification for *less* read
amplification is the right side of the tradeoff. That's also why every
read-optimized production engine (RocksDB, Pebble) defaults to leveled.

**But** the risky, correctness-critical machinery of compaction — tombstone-drop
safety, the MANIFEST, concurrent atomic swap — is **identical** regardless of
which SSTables you pick to merge. So:

- **Build size-tiered first** to get that machinery correct with the simplest
  possible file-picker (merge N similar-sized files → one).
- **Then switch the picker to leveled** as the target strategy for the workload.
- Make the strategy a **pluggable `CompactionStrategy`** (picks *which* files to
  merge); the merge/drop/swap engine underneath is shared and unchanged.

LOCKED: size-tiered as the first correct implementation, **leveled as the target**
for the read-heavy profile, behind a swappable strategy interface.

---

## 2. Algorithm — the tombstone-drop safety problem

The subtle correctness core of the phase. Compaction drops two kinds of dead
data, and they have *different* safety rules:

- **Overwritten values — always safe to drop.** When merging, if two SSTables
  both have key `k`, keep the newest, discard the older. Trivially correct.
- **Tombstones — droppable only under a strict condition.** A tombstone for `k`
  exists to shadow *older* values of `k` in *older* files. If you physically
  drop the tombstone while any older file still holds a live value for `k`, that
  value becomes visible again — **the delete un-happens.**

**The rule:** a tombstone may only be dropped when the compaction includes the
**bottom-most data** — i.e., there is provably nothing older beneath the merge
that could contain `k`. Until then, the tombstone must be **carried forward**
into the merged output, still doing its shadowing job. In leveled terms:
tombstones are only purged during a compaction that reaches the last level.

(There's a second guard in the full system: an in-flight read or a Raft snapshot
may still need an old version. For Track A standalone the bottom-level rule is
sufficient; we revisit it when snapshots arrive in Phase 9.)

This single rule is the difference between a working GC and one that silently
resurrects deleted data — it's the most important thing in the phase.

---

## 3. Algorithm — the MANIFEST (deferred here from Phase 3)

Phase 3 tracked the live SSTable set by listing the directory and sorting by file
number — fine when flush only ever *adds* one file. Compaction breaks that: it
**adds outputs and removes inputs together**, and a directory listing can't
reflect that multi-file swap atomically. So we introduce the **MANIFEST**:

- An **append-only log of version edits**: `AddFile(n, level, key-range)` and
  `DeleteFile(m)`. The current live set = replay the MANIFEST from the start.
- A compaction commits by appending **one** version edit — "delete inputs
  {a,b,c}, add outputs {x,y}" — as a single record, then fsync. That one atomic
  append is the linearization point: before it, the old set is live; after it,
  the new set is. A crash mid-compaction replays the MANIFEST and gets a
  consistent set either way; orphaned temp/unreferenced files are swept on
  startup.

This is the LevelDB `VersionSet` model, kept minimal. LOCKED.

---

## 4. Implementation approach

### 4a. The merge engine (shared across strategies)

1. Strategy picks a set of input SSTables to merge.
2. **k-way merge** their sorted entries (a min-heap over the input iterators) —
   because each input is already sorted, the output streams out sorted in one
   pass, O(total entries).
3. For each key, keep only the newest version; drop older overwrites; drop a
   tombstone **only if** this compaction includes the bottom-most data (§2),
   else carry it forward.
4. Write output entries into new SSTable(s) using the **Phase 3 writer** (blocks
   + sparse index + **Phase 4 Bloom filter** rebuilt over the surviving keys) —
   to a **temp path**, fsync, atomic rename. Same write-new-then-swap discipline
   as flush.
5. Commit the swap via one MANIFEST version edit + fsync.
6. Delete input files **only after** no live reader still references them (§4c).

### 4b. Crash safety (mirrors flush, extends it)

Order is the contract: outputs fully durable (fsync+rename) → MANIFEST edit
appended+fsync'd → inputs deleted. A crash at any point leaves a MANIFEST that
names a consistent set; the loser (temp outputs, or now-unreferenced inputs) is
garbage-collected on the next startup scan. This is also the **disk-full safety**
property from DESIGN §8: compaction writes to *new* files, so a failure never
corrupts an existing SSTable — it just abandons the half-written temp file.

### 4c. Concurrency — compaction runs while reads/writes continue

Immutability is what makes this safe. Compaction reads immutable inputs and
writes new outputs; the only synchronization point is the MANIFEST swap.

- Reads operate against a **`Version`** — an `Arc`'d snapshot of the current live
  SSTable set. When compaction commits, it installs a new `Version`; new reads
  see it, in-flight reads keep their old `Arc` until done.
- Input files are **not deleted** until the last `Version` referencing them is
  dropped (refcount / epoch). This prevents deleting a file out from under a
  running read. (LevelDB `Version`/`VersionSet` model.)
- Compaction runs on a **background thread**, triggered when a strategy's
  condition trips (e.g. too many similar-sized files, or a level over its size
  budget). Writes and flushes continue concurrently.

---

## 5. Edge cases

- **Deleted key must stay deleted** — the headline. Guaranteed by the §2
  bottom-level tombstone rule; a tombstone dropped too early resurrects data.
- **Overwritten key returns latest** — k-way merge keeps newest per key.
- **Crash mid-compaction** — MANIFEST replay yields a consistent set; temp
  outputs and unreferenced inputs swept on startup. No corruption, no loss.
- **Disk full mid-compaction** — temp write fails, compaction aborts, existing
  SSTables untouched; retry later. Never mutate in place.
- **Read during compaction** — served from an `Arc`'d `Version`; inputs not
  freed until that version is released.
- **Two compactions overlapping on the same file** — the strategy/scheduler must
  not select a file already an input to a running compaction (mark files "being
  compacted"). Avoids double-merge races.

---

## 6. Test plan (expands the done-when)

1. **Space drops** — write 10 keys 1000× each, compact, assert total on-disk
   bytes drop dramatically (≈1000× redundancy collapses to ≈10 live entries).
2. **Latest value survives** — after compaction every key returns its final value.
3. **Delete stays deleted** — delete some keys, compact through the bottom level,
   restart, assert they read not-found — and that the tombstone was actually
   *dropped* (not just shadowed) once bottom-level.
4. **Tombstone NOT dropped early** — compact only upper files (older value still
   exists below); assert the tombstone is *carried forward*, not dropped, and the
   key still reads not-found. This is the §2 safety test — the one that matters most.
5. **Crash mid-compaction** — kill during merge; restart; assert MANIFEST replay
   yields a consistent, correct set and temp files are swept.
6. **Concurrent read correctness** — run reads on a background thread throughout a
   compaction; assert every read returns a correct (never torn, never resurrected)
   value.
7. **Leveled invariant (once the picker is switched)** — assert each level ≥ L1
   has non-overlapping key ranges after compaction.

Test 4 guards the correctness core; test 5 guards durability of the swap.

---

## 7. Decisions locked

| Decision | Choice |
|---|---|
| Strategy | size-tiered first (correct baseline), **leveled as target** for read-heavy workload, behind a pluggable `CompactionStrategy` |
| Merge | k-way merge via min-heap over sorted inputs, single pass |
| Overwrite GC | always drop older version of a key |
| Tombstone GC | drop **only** when the compaction includes bottom-most data; else carry forward |
| File-set tracking | **MANIFEST** (append-only version-edit log); replaces Phase 3 list-and-sort |
| Output files | rebuilt with Phase 3 writer + Phase 4 Bloom; temp → fsync → rename |
| Crash safety | outputs durable → MANIFEST edit fsync'd → inputs deleted (last) |
| Concurrency | background thread; `Arc`'d `Version` for reads; inputs freed only when unreferenced; files-being-compacted marked |

**Milestone: Track A is a complete standalone LSM key-value engine.** Durable
(Ph1), sorted in-memory (Ph2), immutable on-disk with a working read path (Ph3),
fast skips (Ph4), and self-compacting (Ph5). It could ship as a library on its
own. Track B (Raft) now builds independently until they meet at Phase 10.

---

## 8. Status — what shipped, and what is carried forward

Audited 2026-07-23, before starting Phase 6; A1+A2 closed 2026-07-27 (128 lib
tests green plus all integration suites). The done-when (§6.1–6.3) **passes**.
The MANIFEST, the atomic swap, the orphan sweep, the Bloom rebuild,
reader-cache eviction, and reads-during-compaction are all real and tested.
What follows is the honest gap list, so Phase 6 doesn't bury it.

### Locked decisions not yet implemented

| # | Decision (§7) | Actual | Impact |
|---|---|---|---|
| A1 | **Leveled** picker (the stated *target* for the read-heavy workload) | **done (2026-07-27):** `compaction::Leveled` — L0 drains into L1 once `l0_trigger` files accumulate; a level ≥1 over `level_file_trigger` promotes one file (plus overlapping next-level neighbors) a level deeper. Wired in as `Db`'s actual default strategy (`db.rs`), replacing `SizeTiered`. `is_bottom_most` computed exactly from key ranges (no file outside the merge, at a deeper level, overlaps the merged range) rather than the old "only if literally everything is included" rule | resolved — §6.7 now runs (see below) |
| A2 | `FileMeta` carries a **key range** (§3: `AddFile(n, level, key-range)`) | **done (2026-07-27):** `FileMeta.min_key`/`max_key`, populated for free by `SstWriter` (keys already arrive strictly increasing, so first/last added = min/max) and by a one-time `SstReader::key_range()` scan for pre-MANIFEST adoption. MANIFEST wire format grew two length-prefixed fields per added file | resolved, unblocked A1 |
| A3 | Compaction on a **background thread** | **done (2026-07-27):** an automatic (threshold-triggered) flush hands compaction to a real `std::thread::spawn` (`Db::maybe_compact`), so the `put`/`delete` that triggered it returns immediately. `Db::compact` itself no longer takes the write lock at all — a direct call runs concurrently with writes too. `Db::open`/`open_with_threshold`/`restore` now return `Arc<Db>` (via `Arc::new_cyclic`) so a spawned thread can hold a real owning handle | resolved — §4c's "writes and flushes continue concurrently" is now true; proven in `concurrent_writer_survives_compaction` |
| A4 | Files-being-compacted **marking** | **done (2026-07-27):** `Db.compacting: Mutex<HashSet<u64>>` hides claimed input files from `available_files()`, so a foreground `compact()` and a background one spawned by `maybe_compact` never select overlapping inputs; a `CompactingGuard` un-claims on success or an early `?` failure | resolved — proven under heavy contention in `concurrent_compactions_never_collide_on_the_same_input` |
| A5 | k-way merge via **min-heap** | linear O(k) scan of all sources per entry (`merge.rs:48`) | correct, but 150 inputs = 150 peeks per output entry |

### Behavioral gap — the tombstone safety path is unreachable (resolved 2026-07-27)

Previously, `SizeTiered::pick` always merged **every** live file and always set
`is_bottom_most: true`, so:

- It wasn't really size-tiered — no size bucketing, just "merge everything
  once ≥ `min_run` files exist."
- `output_level` was always 0; `FileMeta::level` was written but never read.
- **`is_bottom_most: false` was unreachable from production code** — the §2
  tombstone carry-forward rule ("the most important thing in the phase") only
  ran against hand-built `Compaction` structs in unit tests.

All three are fixed: `SizeTiered` now buckets by real on-disk file size
(`compaction::tests::size_tiered_excludes_a_dissimilar_outlier`), and `Leveled`
makes `is_bottom_most: false` a normal, reachable outcome — proven end-to-end
through the real picker, not a synthetic struct, in
`compaction::tests::leveled_carries_tombstone_forward_when_a_deeper_file_still_overlaps`.

### Test plan coverage

| §6 test | Status |
|---|---|
| 1 space drops / 2 latest value | ✅ `compaction_donewhen.rs` (uses 150 rounds, not the doc's literal 1000 — runtime tradeoff; property is proven either way) |
| 3 delete stays deleted | ✅ end-to-end incl. reopen |
| **4 tombstone NOT dropped early** | ✅ (2026-07-27) — now reachable end-to-end through `Leveled::pick`, not just a synthetic `Compaction` (see above) |
| **5 crash mid-compaction** | ✅ (resolved by Phase 13, built ahead of this audit) — `storage/tests/faultsim_compaction.rs` induces a real fsync failure mid-compaction over 20 seeds, not a hand-planted orphan file; superseded the `compaction_safety.rs:79` simulation this row originally flagged |
| 6 concurrent reads/writes | ✅ (2026-07-27) — `concurrent_reads_during_compaction_are_correct` (reads) plus the new `concurrent_writer_survives_compaction` (writes) and `concurrent_compactions_never_collide_on_the_same_input` (A4 under contention) |
| 7 leveled invariant | ✅ (2026-07-27) — `leveled_promotes_an_overloaded_level_taking_only_overlapping_neighbors` and `leveled_is_not_bottom_most_when_a_deeper_overlapping_file_survives` exercise the non-overlap/overlap-check machinery directly |

### Scale

`run_compaction` fully materializes the merge in RAM: `SstReader::entries()`
returns a `Vec` per input (`sstable.rs:581`) and the output is
`Merge::new(..).collect()` (`compaction.rs:99`). `merge.rs`'s own header claims
the writer "can consume it entry-by-entry without materializing the whole merge
in RAM" — the *engine* streams, both *ends* don't. At the real 64 MB threshold
with `min_run: 4`, one compaction peaks around half a gigabyte. Tests never see
it because they use tiny thresholds.

### Verdict

Phase 5 is **done-when-complete**; A1+A2 (2026-07-27) closed the headline
decision-incompleteness this section originally flagged. What's left —
A3+A4 (background-thread compaction + files-being-compacted marking) and the
A5/RAM-materialization performance items — is not a correctness gap: nothing
here is unsafe for a single-threaded embedded user, and Track B does not
depend on any of it. A3+A4 next, before real concurrent write traffic
(Phase 10 already wired) exercises the "writes block during compaction"
behavior in anger.
