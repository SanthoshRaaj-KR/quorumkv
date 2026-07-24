# Phase 13 — Deterministic fault injection

> **Concept:** the missing piece between "we already have deterministic
> Raft-level chaos" and "our storage-level crash tests are either realistic
> but unreproducible, or precise but static."
> **Done when:** each of three storage-level crash scenarios is driven by a
> seed. A failing run prints that seed; rerunning with it reproduces the
> identical fault at the identical operation — the same replay guarantee
> `TestReadySequenceIsDeterministic` already gives the Raft layer, extended
> to file I/O in both tracks.

**Built** ✅ — see §6 for what shipped and where it deviated from this plan
in small, disclosed ways.

---

## 0. The gap, precisely — what already exists and what doesn't

Three crash-testing styles already live in this codebase, and they cover
different ground:

| Style | Where | Deterministic? | What it actually proves |
|---|---|---|---|
| Seeded message-level chaos | `consensus.Bus` (`Reorder`, `OnMessage`, `Isolate`) | **Yes** — same seed replays the identical run | Raft survives partition, delay, reordering, duplicate delivery |
| Static byte-truncation | `wal.rs`/`manifest.rs` torn-tail tests | Yes, but **offline** — the file is corrupted *after* writing, not *during* a live run | Replay correctly stops at a torn/corrupt record and keeps the clean prefix |
| Real process kill | `storage/tests/kill9.rs` | **No** — a real `TerminateProcess` lands wherever the OS schedules it | The *actual* OS/page-cache/fsync contract holds under a genuine crash |

Nobody can currently say "crash exactly between this SSTable's temp-write and
its rename, and do it the same way every time." That's the gap. It is not
a Rust-only gap: `consensus/storage.go`'s `FileStorage` (`raft-log`,
`raft-hardstate`, `raft-snapshot`) calls `os.File` directly with the same
lack of a seam. Same concept, two small language-specific implementations.

**This phase doesn't replace either existing style.** `kill9.rs` stays —
it's the only thing that tests the real OS contract. The static
byte-truncation tests stay — they're cheap and precise for "what does replay
do with this exact corrupt byte." This phase adds the middle ground: precise
*and* live *and* reproducible.

---

## 1. Algorithm — what the seam actually is

### 1a. How much of the I/O path gets wrapped

| | Wrap everything (a virtual filesystem) | **Wrap two calls, at the write site** | OS-level tooling (FUSE/VFS shim) |
|---|---|---|---|
| Invasiveness | rewrites every `fs::` call across `wal.rs`/`sstable.rs`/`manifest.rs` | touches only the handful of places that already do "write then fsync" | none in-process, but a whole external layer to build and keep working on Windows |
| Matches existing precedent | no | **yes — same shape as `Bus.OnMessage`/`Reorder`: a thin hook at the one place that matters, not a rewrite** | no |
| Portability | fine | fine (pure Rust/Go) | painful — this project already hit real Windows toolchain friction (no `gcc`, no `protoc`) building Phase 10; a VFS shim is much worse |

**LOCKED: wrap only the write+durability call pair**, not the whole
filesystem API. Concretely:

```rust
// storage/src/faultsim.rs
pub trait FileSink {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()>;
    fn sync_all(&mut self) -> io::Result<()>;
}
impl FileSink for File { /* passthrough — production path, zero behavior change */ }
```

`WalWriter`, `write_sstable`, and `VersionSet::commit` each already do exactly
"encode → write_all → sync_all" in one place (Phase 1 §4, Phase 3, Phase 5 §3
respectively) — the seam replaces the concrete `File` there with `Box<dyn
FileSink>` (production code passes the real one; tests pass a faulty one).
Nothing about the on-disk format, the WAL's CRC discipline, or the MANIFEST's
linearization point changes — this only affects who ends up calling
`write`/`sync` underneath.

Go's `FileStorage` gets the equivalent: `AppendEntries`/`TruncateFrom`/
`SaveHardState`/`SaveSnapshot`/`CompactLog` already funnel through a small
number of write+sync call sites; the same wrap-not-rewrite treatment applies.

### 1b. What a fault *does* — simulate the result, don't really crash

| | Real fork+kill at a chosen instruction | **Simulate the on-disk result of a crash** |
|---|---|---|
| Reproducibility | fragile — OS scheduling still decides exactly what landed | **fully deterministic — the test decides exactly what landed** |
| Cross-platform cost | high (this project already avoids process-tree games on Windows where it can) | none — pure logic, no process control |
| What it can't catch | — | a bug specific to *real* OS/page-cache behavior — which is exactly what `kill9.rs` is still there for |

**LOCKED: a fault doesn't kill anything.** It changes what the *next* call
returns — a `write_all` that only writes the first *k* of *n* bytes then
returns `Ok` (a torn write that the process itself doesn't even notice,
matching what a real crash leaves behind), or a `sync_all` that returns an
`Err` (an fsync failure), or "let the write happen, but skip the next N
operations after it" (crash between two calls, e.g. between an SSTable's
`write_all` and the following rename). The test then does what a real
restart does: open a **fresh** reader/`Db`/`FileStorage` against the same
directory and check what survived.

### 1c. The fault schedule — how a seed becomes a specific fault

A `FaultSchedule` is built from a seed plus a target: "on the Nth call to
`write_all` on the WAL file, truncate to `k` of its bytes" (`k` itself
seed-derived, e.g. `rng.gen_range(1..full_len)`). Printed on any test failure
(`t.Logf`/`println!` the seed), exactly like the existing
`TestSplitVoteResolvesUnderAClusterWideSeed` pattern already does for Raft.
Rerunning with that seed reproduces the exact same truncation at the exact
same call — nothing about *where* the fault lands is left to chance once the
seed is fixed.

---

## 2. The three scenarios

1. **Crash mid-WAL-append.** Run a normal put loop against a `FaultyFile`-backed
   `WalWriter`; at a seed-chosen append, truncate the write to a random
   partial length. Reopen for real (a fresh `Db::open`) against the same
   directory. Assert: every record before the faulted one survives exactly;
   the faulted (and everything after it, since nothing after it could have
   been written yet) is gone; no panic, no corruption of the *prior* prefix.
   This is `kill9.rs`'s scenario, but the fault lands at a chosen record
   instead of wherever the OS happened to schedule the kill.

2. **Crash between an SSTable's temp-write and its rename.** Fault the
   `sync_all` (or skip the subsequent rename entirely, matching Phase 3 §4c's
   documented crash window) right after `write_sstable`'s temp file is
   written. Reopen. Assert: `remove_orphan_tmp` sweeps the orphaned `.tmp`
   file on startup, the WAL segment it was flushing is *not* deleted (Phase
   3's "delete only after the SSTable is durable" rule), and replaying that
   WAL segment reconstructs the same data the failed flush would have
   produced.

3. **Crash mid-compaction.** Fault the write of a compaction's merged output
   file partway through. Reopen. Assert: the compaction's **input** SSTables
   (already committed to the live version) are untouched and still
   readable — compaction must never modify or remove an input until its
   output is durably committed (Phase 5 §4, and `DESIGN.md` §8's own
   "fill the disk mid-compaction" case, now made precise and reproducible
   instead of only reachable by actually filling a disk).

Each scenario gets a Go-side mirror where it makes sense — scenario 1's shape
(truncate an append) applies just as directly to `consensus.FileStorage`'s
`raft-log`, since it uses the identical length-prefixed, CRC-checked framing.

---

## 3. Test plan (expands the done-when)

1. Each scenario above, parameterized over a range of seeds (e.g. 20 seeds
   run in CI, not just one) — the torn point lands somewhere different each
   time, so the *set* of runs covers a spread of "how far into the operation"
   without any one run being random/unreproducible.
2. **A fixed seed reproduces the identical fault.** Run scenario 1 twice with
   the same seed; assert the truncated length and the surviving record count
   are identical both times — the determinism claim itself, tested directly,
   the same way `TestReadySequenceIsDeterministic` tests Raft's.
3. **The passthrough `FileSink` changes nothing.** Every existing WAL/SSTable/
   MANIFEST test still passes with the real `File` impl — the seam must be
   invisible to production code, not just to reviewers.
4. Go-side equivalent of scenario 1 against `FileStorage`.

---

## 4. Explicitly out of scope

Real process-level (fork+kill) determinism — deliberately simulated instead,
per §1b. Network-level fault injection — already covered by `Bus`. A general
"simulate a full disk" facility beyond what scenario 3 needs (a real
Jepsen-style harness is `DESIGN.md` §8's own noted *optional stretch*, not a
requirement here). Extending the seam to the sidecar's HTTP layer (Phase 10)
— a dropped/corrupted request there is a network fault, not a file-write
fault, and belongs with `Bus`-style tooling if it's ever built.

---

## 5. Decisions locked

| Decision | Choice |
|---|---|
| Seam shape | wrap only the write+sync call pair behind a small trait/interface (`FileSink` in Rust, an equivalent in Go) — not a virtual filesystem |
| What a fault does | simulates the on-disk *result* of a crash (truncated write, failed sync) within the same process — never a real kill |
| Reproducibility | every fault is seed-derived; a failing seed is printed and replays the identical fault at the identical call |
| Scope | both tracks — Rust's WAL/SSTable/MANIFEST and Go's `FileStorage` get the same treatment |
| Relationship to existing tests | additive: `kill9.rs` and the static byte-truncation tests stay exactly as they are |
| Scenarios | mid-WAL-append, SSTable temp-write-before-rename, mid-compaction — the three storage-level crash windows already documented but not yet reproducibly tested |

**Milestone: the crash-safety claims this project has made since Phase 1 —
torn writes are safe, orphaned temp files get swept, compaction never
corrupts existing data — stop resting on "we tested it once and it passed"
and start resting on "here's the seed that proves it, and it'll prove it
again."**

---

## 6. What actually shipped

All three scenarios built and passing (20 seeds each), plus the
determinism test and a passthrough-is-invisible check (the entire
pre-existing `cargo test`/`go test` suite — 130+ Rust tests, all of
`consensus` — still passes unchanged after the seam went in).

| Piece | Where |
|---|---|
| `FileSink` trait + `impl FileSink for File` (passthrough) | `storage/src/faultsim.rs` |
| `FaultSchedule`, `FaultKind::{TornWrite,Fail}`, `CallKind::{Write,Sync}`, `FaultyFile` | `storage/src/faultsim.rs` |
| `WalWriter::open_with_sink` (alongside unchanged `open`) | `storage/src/wal.rs` |
| `SstWriter::create_with_sink`, `write_sstable_with_sink` (alongside unchanged `create`/`write_sstable`) | `storage/src/sstable.rs` |
| `run_compaction_with_sink` (alongside unchanged `run_compaction`) | `storage/src/compaction.rs` |
| Scenario 1 (mid-WAL-append) + determinism test | `storage/tests/faultsim_wal.rs` |
| Scenario 2 (SSTable temp-write before rename) | `storage/tests/faultsim_sstable.rs` |
| Scenario 3 (mid-compaction) | `storage/tests/faultsim_compaction.rs` |
| Go mirror: `appendFault`, `splitMix64` | `consensus/faultsim.go` |
| Go mirror's test (scenario 1 + determinism) | `consensus/faultsim_test.go` |

### 6a. No new dependency — hand-rolled PRNG instead of `rand`

`Cargo.toml`'s own comment explains why there are zero dev-dependencies:
the windows-gnu toolchain on this machine can't link crates that pull in
`windows-sys` (which `tempfile` does, and which `rand`'s `getrandom` backend
also does on Windows) — the same class of problem §1 already ruled FFI/gRPC
out for in Phase 10. Rather than risk it, `faultsim.rs` hand-rolls a
20-line SplitMix64 PRNG, used only to turn a seed into a reproducible torn
length. `consensus/faultsim.go` mirrors the identical algorithm, so a
"same seed, same fault" claim holds using the exact same arithmetic on both
sides, not just the same intent.

### 6b. The Go mirror is narrower than the Rust seam, by necessity

The plan's §1a design (a `FileSink`-shaped wrapper) fits Rust's
`WalWriter`/`SstWriter` cleanly because those are **write-only** structs.
`consensus.FileStorage`'s one log file handle is shared by far more:
`AppendEntries`, `TruncateFrom`, and `replay`'s own `Seek`/`ReadAt` calls all
go through the same `*os.File`. Wrapping the whole handle behind an
interface would mean replicating most of `*os.File`'s surface
(`Write`/`Sync`/`Truncate`/`Seek`/`ReadAt`/`Close`) — a much larger, riskier
change than this scenario needs.

Built instead: a single-purpose `appendFault` hook consulted only inside
`AppendEntries`, added as one new unexported field on `FileStorage` (nil in
production — `OpenFileStorage` never sets it). It reproduces the same
"tripped" semantics as Rust's `FaultyFile` (once a fault fires, every
subsequent call is silently skipped, matching that nothing reaches disk
after a real crash) but doesn't attempt to fault `TruncateFrom` or the
snapshot path — scenario 1 (mid-append) is the only one of the three whose
shape applies to `FileStorage` at all (per §2's own note), so this is the
one and only call site that needed covering.

### 6c. Phase 12 inherits this seam directly, not a slice of it

Phase 12's plan (`planning/phase-12-chaos.md` §2, written and signed off
*before* this phase was built) anticipated needing only a narrow slice of
this — `FileSink` wired into `SstWriter` alone, for the disk-full-
mid-compaction scenario. Since this phase shipped the full thing first
(`write_sstable_with_sink` and `run_compaction_with_sink` both exist now),
Phase 12's item 4 can call `run_compaction_with_sink` directly with a
`FaultKind::Fail`-on-`Sync` schedule — exactly `storage/tests/
faultsim_compaction.rs`'s own test, reusable essentially as-is. Nothing
extra to build there when Phase 12's implementation turn comes.

### 6d. One scope note on scenario 2's test

The plan's scenario 2 describes the crash window in terms of a **flush**
(`"...the WAL segment it was flushing is not deleted..."`). The shipped
test exercises the same crash window through `write_sstable_with_sink`
directly (bypassing `Db::flush`), asserting the property that specifically
needed a live fault to test: the aborted write leaves no visible `.sst`,
doesn't touch any already-committed SSTable, and the orphaned `.tmp` sweeps
cleanly. The WAL-segment-retention half of the same scenario — a flush that
fails must not delete the WAL segment it was covering — was already proven
by the existing (static, hand-authored-orphan) test
`flush.rs::crash_before_rename_recovers_from_wal_and_cleans_tmp`, which
didn't need this seam and wasn't touched. Threading the fault through
`Db::flush()`'s public API to unify both halves under one live test is a
reasonable follow-up, not done here to keep this phase's change to `Db`'s
public surface at zero.
