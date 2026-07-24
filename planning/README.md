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
| 5 | [phase-05-compaction.md](phase-05-compaction.md) | Storage-engine GC | locked; **§8 carry-forward** (leveled + bg thread unshipped) |
| 6 | [phase-06-raft-single.md](phase-06-raft-single.md) | Raft state machine, isolated | **built** ✅ |
| 7 | [phase-07-election.md](phase-07-election.md) | Leader election over RPC | **built** ✅ (wire choice provisional — §2) |
| 8 | [phase-08-replication.md](phase-08-replication.md) | Agree on one ordered log | **built** ✅ |
| 9 | phase-09-snapshot.md | Stop the log growing forever | **built** ✅ |
| 10 | [phase-10-apply-seam.md](phase-10-apply-seam.md) | Connect Raft `apply` → LSM | **built** ✅ |
| 11 | [phase-11-client.md](phase-11-client.md) | Usable from outside | **built** ✅ |
| 12 | [phase-12-chaos.md](phase-12-chaos.md) | Prove it under failure | **planned** — decisions locked, signed off, not yet built |
| 13 | [phase-13-fault-injection.md](phase-13-fault-injection.md) | Deterministic storage-level fault injection | planned (drafted ahead of 11/12) |

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
| Compaction strategy | 5 | size-tiered first, **leveled target** (read-heavy), pluggable | locked |
| Tombstone GC safety | 5 | drop only at bottom-most level, else carry forward | locked |
| File-set tracking | 5 | MANIFEST (append-only version edits) | locked |
| Compaction concurrency | 5 | background thread, Arc'd Version, deferred file delete | locked; **bg thread not yet built** (§8 A3) |
| Raft core style | 6 | deterministic driven core (etcd model) — no timers/goroutines/IO inside | locked |
| Raft driver contract | 6 | persist+fsync → send → apply → Advance | locked |
| Raft log indexing | 6 | dummy sentinel {0,0}; access via `Log` methods only | locked |
| Raft persistence | 6 | own append-only file, Phase-1 framing (len prefix + CRC32C) | locked |
| Raft log truncation | 6 | `TruncateFrom` via index→offset table (built ph6, used ph8) | locked |
| Raft apply seam | 6 | `StateMachine{Apply([]byte)}`; opaque command bytes | locked |
| Real time | 7 | `Server` loop outside `Node`; one goroutine owns the `Driver` | locked |
| Tick granularity | 7 | 10ms tick; election 15 ticks → [150,300)ms; heartbeat 5 ticks | locked |
| **Wire format** | 7 | **own TCP + CRC32C frames; gRPC deferred behind `Transport`** | **PROVISIONAL — deviates from ROADMAP, needs sign-off** |
| Transports | 7 | ship two: deterministic `Loopback`/`Bus` + real `TCPTransport` | locked |
| Message loss | 7 | fire-and-forget; drop on unreachable, lazy redial, no queue | locked |
| Heartbeat semantics | 7 | resets election timer even on log mismatch; commit only on match | locked |
| Seed mixing | 7 | node ID folded into `Config.Seed` so a cluster-wide seed stays replayable *and* independent | locked |
| Divergence repair | 8 | follower-hinted backtracking; linear `nextIndex--` as the correctness fallback | locked |
| Truncation rule | 8 | truncate only at the first genuinely differing term; `AppendEntries` idempotent | locked |
| Follower commit bound | 8 | `min(leaderCommit, last new entry)` — fixes Phase 7's `localLastIndex` | locked |
| `Ready` contract | 8 | gains `TruncateFrom`; order becomes truncate → append → fsync → send → apply | locked |
| Replication batching | 8 | 64 entries / 1 MiB per `AppendEntries`; send on `Propose`, heartbeats repair | locked |
| Rust ↔ Go boundary | 10 | local sidecar process per node, hand-rolled HTTP/1.1 subset (not FFI, not gRPC — both mechanically blocked, no `gcc`/`protoc` on this machine) | locked, built |
| `StateMachine` interface | 10 | fallible: `Apply/Snapshot/Restore` all gain `error` | locked, built |
| Command encoding | 10 | `op(1)｜keyLen(4)｜key｜valueLen(4)｜value`, same shape/op-bytes as WAL | locked, built |
| Client wire protocol | 11 | hand-rolled HTTP/1.1 subset, JSON — same fork as phase 10's sidecar, applied one layer out | locked, signed off |
| Write acknowledgment | 11 | new `Server.ProposeAndWait`, resolves on `LastApplied` reaching `(index,term)`; term mismatch → new `ErrProposalLost` | locked, signed off |
| Read consistency mode | 11 | Leader-only, no confirmation | locked |
| Chaos test level (items 1–3) | 12 | `consensus`-level, real TCP + real `clientrpc.Client`, stub SM | locked |
| Partition primitive | 12 | new `TCPTransport.Isolate()`/`Heal()`, promoted from Phase 11's test-only mechanism | locked, signed off |
| Disk-full fault injection (item 4) | 12 | `FileSink` trait, scoped to `SstWriter` only — a slice of Phase 13's already-planned seam | locked, signed off |
| Item 5 (Jepsen-style linearizability) | 12 | out of scope, per ROADMAP's own stretch qualifier | locked, signed off |
| "Run in CI" | 12 | add `.github/workflows/test.yml` (Go job + Rust job) | locked, signed off |
