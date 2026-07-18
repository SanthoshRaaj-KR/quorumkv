# quorumkv — planning notes

The middle layer between `DESIGN.md` (the *why*) and the code. One file per
phase from `ROADMAP.md`. Each file answers: **what are the real algorithm
choices here, which one do we pick, and how do we build it?**

Depth is **medium**: options + tradeoffs + a recommended pick + a prose
implementation walkthrough. Not byte-level spec — we go deeper only where a
phase genuinely needs it (WAL framing, SSTable layout, the Rust/Go seam).

## How to read a phase file

Every phase file has the same shape:

1. **Goal** — the one concept (from ROADMAP).
2. **Algorithm options** — the real forks, as a comparison table.
3. **Recommendation** — what we pick and why.
4. **Implementation approach** — how to actually build it.
5. **Edge cases** — the things that bite.
6. **Test plan** — expanding "done when…" into runnable cases.

## Phases

| # | File | Concept | Status |
|---|---|---|---|
| 1 | [phase-01-wal.md](phase-01-wal.md) | Write-ahead log / durability | decisions locked |
| 2 | [phase-02-memtable.md](phase-02-memtable.md) | In-memory sorted layer | decisions locked |
| 3 | [phase-03-sstable.md](phase-03-sstable.md) | Flush to immutable disk file | decisions locked |
| 4 | [phase-04-bloom.md](phase-04-bloom.md) | Skip files you don't need | decisions locked |
| 5 | [phase-05-compaction.md](phase-05-compaction.md) | Storage-engine GC | decisions locked |
| 6 | phase-06-raft-single.md | Raft state machine, isolated | todo |
| 7 | phase-07-election.md | Leader election over RPC | todo |
| 8 | phase-08-replication.md | Agree on one ordered log | todo |
| 9 | phase-09-snapshot.md | Stop the log growing forever | todo |
| 10 | phase-10-apply-seam.md | Connect Raft `apply` → LSM | todo |
| 11 | phase-11-client.md | Usable from outside | todo |
| 12 | phase-12-chaos.md | Prove it under failure | todo |

## Decision log

Decisions get made *inside* the phase that needs them and recorded here so we
never re-litigate. `DESIGN.md` §7 / `ROADMAP.md` "Open decisions" seed this.

| Decision | Phase | Choice | Status |
|---|---|---|---|
| WAL framing | 1 | Length-prefix | locked |
| WAL checksum | 1 | CRC32C per record | locked |
| WAL durability | 1 | fsync per commit (group-commit later) | locked |
| Memtable structure | 2 | crossbeam-skiplist SkipMap | locked |
| Delete representation | 2 | Tombstone (`Value::Delete`) | locked |
| Size counter | 2 | AtomicUsize owned by Memtable; +OVERHEAD/entry | locked |
| Flush threshold | 2 | 64MB, configurable | locked |
| WAL serialization (consequence) | 2 | mutex around append+fsync; group-commit deferred | noted |
| SSTable layout | 3 | data blocks → sparse index → fixed footer | locked |
| SSTable index density | 3 | sparse (one entry per ~4KB block) | locked |
| Tombstone persistence | 3 | written to SSTable, dropped only in compaction | locked |
| File set tracking | 3 | monotonic file numbers + list-and-sort; MANIFEST → Phase 5 | locked |
| WAL segmentation | 3 | per-memtable segment, deleted after SSTable durable | locked |
| Flush timing | 3 | synchronous inline; background flush deferred | locked |
| Bloom filter variant | 4 | Blocked Bloom (64-byte blocks), xxh3 | locked |
| Bloom residency | 4 | RAM-resident when SSTable is live; never per-query disk read | locked |
| Bloom tuning | 4 | ~10 bits/key (config), k≈6-7 | locked |
| Bloom keys | 4 | every key incl. tombstones (no false negatives) | locked |
| Bloom in file | 4 | new Bloom block + footer offset; CRC32C, rebuildable | locked |
| Compaction strategy | 5 | Size-tiered to start | tentative |
| Rust ↔ Go boundary | 10 | TBD | open |
| Read consistency mode | 11 | Leader-only to start | tentative |
