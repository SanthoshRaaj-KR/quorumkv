# Phase 12 — Chaos test suite

> **Concept:** proving correctness under the failures that matter — the
> phase that makes the project resume-credible (`DESIGN.md` §8).
> **Done when:** all five fault injections run in CI and pass repeatably.

Every prior phase either built a capability or proved one piece of it in
isolation. Phase 11 was the first time a *real client* could observe a
write actually landing — which turns out to matter a lot here: `DESIGN.md`
§8's first line, *"verify no acknowledged write is ever lost,"* was
unanswerable in a rigorous sense before Phase 11 gave "acknowledged" a real,
external, client-observed meaning (`clientrpc.Client.Put` returning `nil`,
backed by `Server.ProposeAndWait` resolving on `LastApplied`). This phase
doesn't invent new mechanisms so much as **compose what already exists**
into DESIGN.md §8's five named scenarios, plus close the one real gap that
isn't covered by anything built so far.

---

## 0. What already exists, precisely

| Piece | Where | Covers |
|---|---|---|
| Seeded partition (`Isolate`/`Heal`) | `consensus.Bus` | in-process, deterministic — `election_test.go` already partitions minorities and heals them at the `Driver` level |
| `Crash`/`Restart` (kill -9) | `consensus.Sandbox` | in-process, deterministic — simulates process death + resurrection from persisted state |
| Real client + real ack semantics | `consensus/clientrpc` (Phase 11) | `Client.Put` only returns success after commit+apply; `ErrProposalLost`/`ErrProposeTimeout` are real, tested retryable outcomes |
| Real leader kill, real re-election | proven by hand in Phase 11 (3 real `quorumkv-node` processes, `kill -9` the leader, client kept working) | not yet an automated test |
| Real OS-level kill, WAL durability | `storage/tests/kill9.rs` (Phase 1) | proves fsync'd writes survive a real `TerminateProcess` — at the storage layer alone, nothing about replication |
| Crash-mid-compaction (no corruption) | `storage/tests/compaction_safety.rs`'s `orphan_compaction_output_is_swept_on_reopen` | proves the MANIFEST-swap discipline, but by **hand-planting** an orphan file, not by injecting a real write failure |
| "Disk full mid-compaction" | `planning/phase-05-compaction.md` §4b/§5 | designed for (write-new-then-swap makes it safe *by construction*) but **never fault-injected or tested** |
| A seam sketched for exactly this gap | `planning/phase-13-fault-injection.md` §1a (`FileSink` trait) | drafted, not built — this phase is the first thing that actually needs it |

Four of DESIGN.md §8's five items are mostly a matter of *composing and
automating* what's already proven true. The fifth (disk-full mid-compaction)
is the one genuine gap: nothing in the codebase can inject a real write
failure at a chosen moment.

---

## 1. Decision: what level do items 1–3 run at

Two proven test styles already coexist in this codebase for Raft-level
behavior:

| | Deterministic (`Bus`/`Sandbox`, in-process) | **Real sockets + real client (`clientrpc`)** |
|---|---|---|
| Precedent | `election_test.go`'s partition tests | Phase 11's `tcp_test.go`-style cluster, now proven manually with real processes |
| What "acknowledged" means | whatever the test SM recorded — no external client in the loop | **exactly what `DESIGN.md` §8 asks for**: a real `Client.Put` returning success |
| Speed / CI friendliness | fast, no real timers | slower (real elections take real milliseconds), but Phase 11's TCP tests already run fine in this repo's existing suite |
| Needs Rust/cargo | no | no — a stub `StateMachine` (the same `memSM` pattern `clientrpc`'s own tests use) is enough; the property under test is **Raft-level durability** (does a committed entry survive on a majority), not storage-engine fsync, which Phase 1's `kill9.rs` already proves separately |

**LOCKED: items 1–3 are `consensus`-level tests, real TCP sockets + a real
`clientrpc.Client`, stub `StateMachine`.** This is the most direct reading
of `DESIGN.md` §8's wording (a *client* observes acknowledgment) and
reuses exactly the harness Phase 11 already built and validated by hand —
it just needs to become automated, assertion-bearing test code instead of a
one-off manual `curl` session. No new dependency, no cargo requirement for
this half of the suite.

A **real-process variant** (actual `quorumkv-node` binaries, actual `kill
-9` on the OS process, mirroring `kill9.rs`'s own philosophy) is worth
keeping too, but as a slower, separately-tagged integration test — not
blocking the fast suite on cargo being installed. See §6.

### 1a. The partition primitive `TCPTransport` doesn't have

`Bus` has first-class `Isolate`/`Heal`. `TCPTransport` doesn't — Phase 11's
own tests reconstructed a partition ad hoc from private methods
(`drop`+`SetPeers`), which was fine for one test file but isn't something
item 2/3's tests (or anyone else) should have to redo.

| | Keep reconstructing it per test (Phase 11's approach) | **Promote to first-class `TCPTransport.Isolate`/`Heal`** |
|---|---|---|
| Reuses private methods across packages | can't — `drop` is unexported, only reachable from `package consensus` tests | n/a, it's the real API now |
| Matches `Bus`'s existing shape | no — different vocabulary, ad hoc | **yes — same two verbs, same meaning, real sockets instead of in-memory queues** |
| Risk | low, but every consumer reinvents it | additive: two new exported methods, zero change to `Send`/`SetPeers`/existing tests |

**LOCKED, signed off:** `TCPTransport` gains `Isolate()`/`Heal()`,
implemented with the same drop-connections + clear-address-book mechanism
Phase 11's test helper already proved works, just promoted from test-only
to first-class.

---

## 2. Decision: item 4 — disk-full mid-compaction

`planning/phase-13-fault-injection.md` already designed the right seam
(`FileSink` trait: `write_all`/`sync_all`, production code gets the real
`File`, tests get a faulty one) — for the *whole* WAL/SSTable/Manifest
surface, seeded and deterministic, reproducible from a printed seed on
failure. That is more machinery than this one scenario needs.

| | Block item 4 on Phase 13 shipping first | **Build only the slice Phase 13 already scoped for `SstWriter`** | A one-off hack (e.g. a size-capped tmpfs) |
|---|---|---|---|
| Duplicates future work | no | no — same trait shape, same file (`storage/src/faultsim.rs`), Phase 13 *extends* this to `wal.rs`/`manifest.rs` later instead of designing it from scratch | yes — a throwaway mechanism Phase 13 would replace anyway |
| Unblocks this phase now | no | **yes** | yes, but low-quality reuse |
| Where it plugs in | — | `SstWriter.file: File` (`storage/src/sstable.rs`) is the **one** field both flush and compaction funnel through (phase-05 §4a: *"same write-new-then-swap discipline as flush"*) — change it to `Box<dyn FileSink>` | — |

**LOCKED, signed off** (this phase reaches into Phase 13's planned scope
early, deliberately): build `storage/src/faultsim.rs` now, but scoped to exactly
`FileSink` + a `FailAfterN` test implementation (fails the write that would
cross a byte budget, simulating `ENOSPC`) — wire it into `SstWriter` only.
Phase 13 inherits this file and extends it to `wal.rs`/`manifest.rs` and a
seeded/reproducible harness; nothing built here needs to be redone, only
extended.

**Test:** start a compaction, budget the `FileSink` to fail partway through
the *second* output block, assert `compact_all()` returns an `io::Error`
(propagated, not swallowed), assert the `.tmp` output is either absent or
ignorable garbage, and — the actual property from `DESIGN.md` §8 — assert
every pre-existing SSTable is still present, still passes its own checksum
reads, and every key still reads back its correct value. This is the same
shape as `compaction_safety.rs`'s existing tests, with a real induced
failure instead of a hand-planted orphan file.

---

## 3. Decision: item 5 (stretch)

`ROADMAP.md` marks this explicitly `*(stretch)*`: a Jepsen-style harness
that injects faults automatically and checks linearizability of the
recorded operation history. Building a real linearizability checker
(Knossos/Porcupine-style history verification) is a substantial project on
its own — well beyond "compose what exists."

**LOCKED, signed off: out of scope for this phase**, same as the roadmap's
own qualifier. Items 1–4 are what "done when" requires; item 5 stays a
named future upgrade.

---

## 4. Decision: "run in CI"

The done-when says *"in CI,"* not just "the test suite passes locally." This
repo has a real GitHub remote (`origin`) but **no CI configuration exists
today** — `ROADMAP.md`'s own phrase has been aspirational until now.

| | Treat "in CI" as figurative (local `go test`/`cargo test` is enough) | **Add a minimal GitHub Actions workflow** |
|---|---|---|
| Matches the done-when literally | no | **yes** |
| Cost | zero | one small YAML file, two jobs (`go test ./consensus/...`, `cargo test` in `storage/`) |
| Precedent for needing both toolchains in one CI run | — | already true regardless — `storage`'s own test suite (including the new disk-full test, §2) needs `cargo` in CI independent of this decision |

**LOCKED, signed off** (first infra outside the two language toolchains):
add `.github/workflows/test.yml` with two jobs (Go test suite, Rust test
suite), triggered on push/PR to `master`. The chaos suite
(this phase's new tests) runs as part of the existing `go test`/`cargo
test` invocations — no separate "chaos" CI job, just correctness tests that
happen to be about fault tolerance.

---

## 5. Layout

```
consensus/
├── tcp.go                      + Isolate()/Heal() on TCPTransport (§1a)
└── chaos/                       (new — external test package: needs
    └── chaos_test.go            clientrpc, which imports consensus, so it
                                  can't live as an internal consensus test)
storage/src/
├── faultsim.rs                  (new) FileSink trait + FailAfterN (§2)
└── sstable.rs                   SstWriter.file: File -> Box<dyn FileSink>
storage/tests/
└── compaction_disk_full.rs      (new) item 4's test
.github/workflows/
└── test.yml                     (new) §4
```

`consensus/chaos` sits beside `consensus/clientrpc`, importing `consensus`
and `consensus/clientrpc` — a DAG, same shape as every other seam in this
project (nothing consensus-side ever imports a package that imports it
back).

---

## 6. The five scenarios, concretely

1. **Kill the leader mid-write.** `consensus/chaos`: real TCP cluster
   (`consensus.Server` + stub SM), real `clientrpc.Client`. Run a
   `Put`-loop; a background goroutine kills the current leader's `Server`
   +`TCPTransport` at a random point. After the loop finishes (client
   survives via its own retry/redirect, per Phase 11), assert **every key
   whose `Put` returned `nil` reads back correctly** from the surviving
   majority. Keys whose `Put` returned an error are explicitly allowed to
   be missing — that's the *unacknowledged* case, correct by definition.
   A slower, separately-tagged variant (`-tags=realchaos` or similar) does
   the same thing with real `quorumkv-node` OS processes and a real `kill
   -9`, mirroring `kill9.rs`'s philosophy at the cluster level — not part
   of the default `go test ./...` run, since it needs `cargo` and takes
   real wall-clock seconds per run.
2. **Partition a minority away.** `TCPTransport.Isolate` one node in a
   5-node cluster (a 2-node minority can't affect a 3-node majority's
   quorum). Assert the majority side keeps accepting `Put`/`Get` through
   `clientrpc.Client` the whole time, with the minority's addresses simply
   absent from the client's address list (or present but always failing
   over past, per Phase 11's retry loop) — either way, correctness, not
   availability of every node, is what's asserted.
3. **Heal the partition.** Continue from (2): `Heal()` the isolated
   minority, wait for it to catch up (log repair for a short absence,
   `InstallSnapshot` if it fell far enough behind — reuse Phase 9's
   existing snapshot-threshold knob to force the snapshot path
   deliberately in one sub-test), and assert its final state is *identical*
   to the majority's — same last-applied entries, no divergence.
4. **Fill the disk mid-compaction.** §2's `FileSink`-based Rust test.
5. *(stretch, out of scope — §3)*.

---

## 7. Edge cases

- **A `Put` that returns `ErrProposeTimeout`, not a clean success/failure.**
  Treated as *not acknowledged* for the item-1 assertion — the same
  "unacknowledged writes are allowed to vanish" rule `ROADMAP.md` Phase 1
  already established for the WAL alone, now extended to the whole cluster.
- **Killing the leader before it ever picks up the client's `Put`.** No
  different from a network hiccup the client's retry loop already handles
  (Phase 11 §5) — not a special case here.
- **Two consecutive leader kills in the same test run.** The client's
  bounded retry (`attemptsPerAddr`, Phase 11 §5) may legitimately give up if
  kills happen faster than elections resolve — the test paces kills to
  leave at least one full election window between them, not a true "kill
  everything constantly" storm (that's closer to item 5's territory).
- **Disk-full test leaving a `.tmp` file behind.** Explicitly allowed —
  `storage/tests/compaction_safety.rs` already proves orphaned temp/`.sst`
  outputs are swept on the next `Db::open`; item 4's test doesn't need to
  duplicate that assertion, only confirm existing files survive intact.
- **CI running the slow real-process variant of item 1 on every push.** It
  doesn't — gated separately (§6.1), run manually or on a slower cadence,
  so the default CI job stays fast.

---

## 8. Explicitly out of scope

Item 5's full Jepsen-style linearizability harness (§3). Any new fault
type beyond DESIGN.md §8's five (e.g. clock skew, corrupt-but-checksum-
passing bytes — not asked for). Extending `faultsim.rs` to `wal.rs`/
`manifest.rs` or adding seeded reproducibility — Phase 13's job, this phase
only builds the `SstWriter` slice it needs. Chaos-testing the sidecar
process itself dying independently of its Raft node (a real scenario, but
Phase 10's own sandbox `Crash`/`Restart` already exercises "sidecar killed,
Raft process still up" as a `Driver.run()`-fatal-error case, covered by
existing tests, not repeated here).

---

## 9. Decisions locked

| Decision | Choice |
|---|---|
| Test level for items 1–3 | `consensus`-level, real TCP sockets + real `clientrpc.Client`, stub `StateMachine` — reuses Phase 11's own proven harness |
| Partition primitive | new `TCPTransport.Isolate()`/`Heal()`, promoted from Phase 11's ad hoc test-only mechanism — **signed off** |
| Disk-full fault injection (item 4) | build only the `FileSink` slice Phase 13 already scoped, wired into `SstWriter` alone — **signed off** |
| Item 5 (Jepsen-style linearizability) | out of scope, per ROADMAP's own "(stretch)" qualifier — **signed off** |
| "Run in CI" | add `.github/workflows/test.yml`, two jobs (Go, Rust) — **signed off** |
| Real-process kill-9 variant of item 1 | built, but separately tagged/slow, not part of the default fast suite |
| Layout | `consensus/chaos/` (external test package), `storage/src/faultsim.rs`, `storage/tests/compaction_disk_full.rs`, `.github/workflows/test.yml` |

**Milestone, once built:** every claim `DESIGN.md` makes about surviving
failure has an automated, repeatable test behind it, running in the same CI
a reviewer would actually look at — the point where "resume-credible" stops
being a design-doc adjective and becomes something a stranger can verify by
reading a green checkmark.
