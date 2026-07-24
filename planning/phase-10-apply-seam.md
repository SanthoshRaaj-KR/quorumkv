# Phase 10 — Connect Raft's `apply` to the LSM engine

> **Concept:** the single seam where the two halves meet.
> **Done when:** `PUT(k,v)` sent to the leader ends up readable via the LSM
> engine on **all three** nodes. Kill and restart a node → its LSM state is
> rebuilt and consistent with the others.

Track A (Rust, phases 1–5) and Track B (Go, phases 6–9) have been built and
tested in complete isolation from each other. Every commit Raft produces has
gone to a `recorder`/`sandboxSM` stub that just remembers strings in RAM. This
phase replaces that stub with the real thing — and, because the stub could
never fail and the real thing can, it's the phase that finally has to answer
questions Phase 6 got to leave open.

This is the **payoff phase**: the first moment quorumkv is genuinely a
*replicated* key-value store rather than two well-tested halves.

---

## 0. What already exists, precisely

Both sides are further along than a fresh reading of `DESIGN.md` suggests —
worth stating exactly, since the plan below is additive to this, not a
rewrite:

| Piece | Where | State |
|---|---|---|
| `consensus.StateMachine` interface | `consensus/state.go` | `Apply(cmd []byte)`, `Snapshot() []byte`, `Restore(data []byte)` — all **infallible by signature** |
| Driver applies committed entries | `consensus/driver.go` `run()` | calls `sm.Apply(e.Cmd)` for every non-no-op committed entry, already durability-ordered (persist → send → apply) |
| Driver takes/installs snapshots | `consensus/driver.go` `maybeSnapshot()` | calls `sm.Snapshot()`/`sm.Restore()`; already ordered (SaveSnapshot → CompactLog → node-side compact) |
| Real LSM engine | `storage/` (Rust) | full WAL + memtable + SSTable + Bloom + compaction + MANIFEST, behind `storage::db::Db::{open, put, get, delete, flush, compact}` |
| A precedent for a Rust helper binary | `storage/src/bin/wal_crash_writer.rs` | std-only, zero extra crates, opens a `Db` directly |
| A live, drivable Go cluster | `consensus/sandbox.go` + `cmd/dashboard-backend` | 3 nodes over an in-memory `Bus`, StateMachine currently `sandboxSM` (a string list) |

Nothing above changes shape. Phase 10 adds one real `StateMachine`
implementation and the plumbing it needs to exist at all.

---

## 1. The decision ROADMAP.md deferred, now decided by the environment

`ROADMAP.md`'s "Open decisions" table lists **FFI / gRPC sidecar / subprocess**
for this exact fork, tentatively recommending gRPC. I checked what this
machine can actually build before proposing anything:

```
$ where gcc / clang     → not found
$ protoc --version      → not found
```

That's not a preference — it rules two of the three out mechanically:

| | FFI (cgo) | gRPC sidecar | **Hand-rolled sidecar** |
|---|---|---|---|
| Needs a C compiler | **yes** (cgo) — absent here | only via a bundled-protoc fallback, which *also* needs a C compiler to build — absent here | **no** |
| Needs `protoc`/codegen | no | **yes** — absent here | **no** |
| Process isolation (a Rust panic can't kill the Go node) | no — same process | yes | yes |
| New dependencies | a C toolchain | protobuf + grpc crates/modules on both sides | **zero** — `std::net` on the Rust side, `net/http` on the Go side |
| Precedent in this repo | none | none | **`wal_crash_writer.rs`**: a plain `std`-only binary against `Db`; and the Raft layer already hand-rolls its own wire framing (`consensus/transport.go`) |

**LOCKED: a local sidecar process, one per node, speaking a hand-rolled
protocol — not FFI, not gRPC.** FFI and gRPC are both mechanically blocked on
this machine right now; even setting that aside, a sidecar process keeps a
storage-engine panic from taking the Raft node down with it, which FFI can't
promise. This is a real, environment-driven deviation from `ROADMAP.md`'s
tentative pick — same category of change as Phase 7's wire-format deviation,
flagged the same way.

### 1a. What the sidecar actually speaks

| | Full HTTP crate (`tiny_http` etc.) | **Hand-rolled HTTP/1.1 subset, JSON bodies** | Hand-rolled binary frames (WAL-style) |
|---|---|---|---|
| New Cargo dependency | yes — unverified against this machine's linker gap (`Cargo.toml` already documents crates failing to link here) | **no** | no |
| Debuggable with `curl` | yes | **yes** | no — needs a client to speak it |
| Matches this project's "hand-roll every byte" ethos | no | partly | fully |
| Implementation cost | low (crate does the work) | **low-medium**: request line + headers-until-blank-line + `Content-Length` body is ~40 lines, no chunked encoding, no keep-alive needed | medium: a whole new framing scheme to design and test |

**LOCKED: hand-roll just enough of HTTP/1.1** — request line, headers until a
blank line, a `Content-Length` body, plain-text status line back. No chunked
transfer, no keep-alive, no TLS: this is `127.0.0.1`-only, one client (the
paired Go node) at a time. JSON for the body — binary values get
base64-encoded, which is wasteful but irrelevant at this layer's scale, and
keeps every request `curl`-able while building it, the same reason Phase 9
sends the whole snapshot as one message instead of chunking. Go's stdlib
`net/http` server/client work with this unmodified; only the Rust side is
hand-rolled.

---

## 2. The interface change this phase forces: `StateMachine` becomes fallible

This is the phase's real headline decision — bigger than the wire protocol,
and the one I'd most want your eyes on before I touch code.

`recorder`/`sandboxSM`/`printer` can never fail, so `Apply(cmd []byte)`,
`Snapshot() []byte`, and `Restore(data []byte)` were built with no error
return four phases ago (Phase 6). A real engine *can* fail — sidecar
unreachable, disk full, a corrupt snapshot blob — and today there is
**nowhere for that error to go**. `Driver.run()` already treats every other
I/O failure as fatal and bubbles it (`SaveHardState`, `AppendEntries`,
`TruncateFrom`, `SaveSnapshot`, `CompactLog` — every one returns `error` and
every one is checked). Apply/Snapshot/Restore are the only three operations
in the whole driven-core loop that *can't* report failure, and that was
always provisional, not a design stance — Phase 6's own comment on
`StateMachine` never claimed otherwise.

| | Leave the interface infallible | **Make it fallible, propagate like everything else** |
|---|---|---|
| A sidecar-down Apply | must retry-forever (blocks the single-threaded driver loop indefinitely) or silently drop the write | `Driver.run()` returns the error, same as a failed `fsync` today |
| Consistency with the rest of `Driver.run()` | breaks it — one seam behaves differently from all the others | uniform: every durable operation in the loop can fail and does so the same way |
| Risk of silently losing a *committed* write | real — dropping on failure is exactly the bug the whole WAL discipline exists to prevent | none — a failure is loud and stops the node rather than pretending success |
| Blast radius of the change | none (already broken) | 3 call sites in `driver.go`, 3 existing implementers (`recorder`, `printer`, `sandboxSM`) — all trivially `return nil` |

**LOCKED (pending your sign-off — this is the one change to a Phase 6
contract this whole plan makes):**

```go
type StateMachine interface {
	Apply(cmd []byte) error
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}
```

`driver.go` changes at exactly three points — the apply loop in `run()`, the
`sm.Snapshot()` call in `maybeSnapshot()`, and the two `sm.Restore()` calls
(`NewDriver`, and the install-snapshot branch of `run()`) — each now checked
and returned like every sibling call already is. `recorder`, `printer`, and
`sandboxSM` each gain a `return nil` and keep working unchanged; no test
behavior changes because none of them can actually fail.

---

## 3. The command encoding — the piece that doesn't exist yet anywhere

`Raft.Propose(cmd []byte)` and `StateMachine.Apply(cmd []byte)` have always
passed *opaque* bytes — by design, so Raft never has to understand them. But
nobody has yet decided **what those bytes are**. Every test today proposes a
literal human string ("PUT foo bar") that nothing parses. Phase 10 has to
invent this, and the natural answer is sitting right next to it:

```
op(1) | keyLen(4) | key | valueLen(4) | value
```

— the *same shape* as `storage/src/wal.rs`'s own record encoding, for the
same reason: hand-rolled, self-describing, no ambiguity, and a reader who
already understands one understands the other for free. `op = 1` is PUT,
`op = 2` is DELETE (`valueLen = 0`), matching `wal.rs`'s `OP_PUT`/`OP_DELETE`
constants by value, not just by shape.

**LOCKED.** Lives in a new package (see §5), not in `consensus` — the whole
point of the opaque-bytes design is that `consensus` never has to change to
gain this; `Propose`'s caller encodes, `Apply`'s implementer decodes.

---

## 4. Reads bypass Raft entirely — confirmed, not new

`DESIGN.md` §5 already settles this: a write goes through the full
propose→replicate→commit→apply pipeline, but a **read hits the local engine
directly** on whichever node answers it — no log entry, no replication, no
commit wait. Phase 11 picks a *consistency mode* for an external client
(leader-only to start, per `ROADMAP.md`); Phase 10 doesn't need to decide
that yet, it just needs *a* local read path to exist, because the phase's own
done-when ("ends up readable... on all three nodes") requires reading from
all three engines independently to check.

**LOCKED: `Get` is not part of `consensus.StateMachine`** (Raft has no
opinion on reads — nothing here changes) but *is* part of the new engine
package's public surface, callable per-node. This is also the detail worth
surfacing in the sandbox: GET a follower right after a write and it can
legitimately lag by one heartbeat — a real, visible consequence of this
design, not a bug.

---

## 5. Layout

A new top-level Go package, `engine/` — deliberately not inside `consensus`
(which must stay ignorant of the LSM) and not inside `storage` (which is
Rust). Mirrors `DESIGN.md` §9's intent (a clean seam package) without being
bound to its exact pre-code file sketch.

```
engine/                        (Go: the Phase 10 seam)
├── command.go                 Command{Op,Key,Value} + Encode/Decode
├── client.go                  HTTP client -> one node's sidecar
└── statemachine.go            engine.StateMachine implementing consensus.StateMachine + Get()

storage/src/
├── snapshot.rs                 (new) pack/unpack the live SSTable set as one blob
└── bin/
    └── sidecar.rs               (new) the hand-rolled HTTP/1.1 server over a Db
```

---

## 6. The sidecar protocol, precisely

Five endpoints, all `POST` except `GET /stats`, all JSON in/out:

| Endpoint | Request | Response | Maps to |
|---|---|---|---|
| `POST /put` | `{"key": base64, "value": base64}` | `{}` or `{"error": "..."}` | `Db::put` |
| `POST /delete` | `{"key": base64}` | `{}` / error | `Db::delete` |
| `POST /get` | `{"key": base64}` | `{"value": base64\|null}` / error | `Db::get` |
| `POST /snapshot` | `{}` | `{"data": base64}` / error | `snapshot.rs::pack` |
| `POST /restore` | `{"data": base64}` | `{}` / error | `snapshot.rs::unpack` + fresh `Db::open` |
| `GET /stats` | — | `{"sstables": N, "approxSize": N}` | `Db::sstable_count`/`approx_size` — for the UI, not Raft |

One connection per request (`Connection: close`), no keep-alive — this is a
control link for one local client, not a service under load. The sidecar
binary takes `<dir> <addr>` on argv, exactly like `wal_crash_writer` takes
`<dir> [threshold]`.

### 6a. What `/snapshot` and `/restore` actually move

`DESIGN.md` §3's "the LSM engine's own current SSTable set, since those are
already an immutable point-in-time view" is the design; here's what it means
as bytes. The live set is whatever `VersionSet::current()` names — already
durable, already immutable, nothing to re-serialize. `snapshot.rs::pack`:

```
crc32c(4) | [ fileNumber(8) | length(8) | raw sstable bytes ]*
```

one CRC over the whole blob (cheap insurance, same "reject a torn/corrupt
blob rather than trust it" rule as everything else in this project —
individual SSTables already have their own internal block checksums, so this
is belt-and-suspenders, not load-bearing). `unpack` on the receiving side:
verify the CRC, **discard whatever the target directory currently holds**
(an installed snapshot is authoritative — the exact rule `Log.
RestoreToSnapshot` already enforces on the Raft side), write each file under
its recorded number, commit a fresh MANIFEST naming exactly that set, drop
any WAL segments (the snapshot supersedes everything up to its index), then
`Db::open` normally.

This inherits Phase 9's own scope cut: no chunking, one message, capped by
the 64 MiB frame limit. A real deployment's SSTables can exceed that; noted,
not solved here, same as Phase 9 flagged it.

---

## 7. Process topology

**Real deployment:** one node = **two processes** — the unchanged Go Raft
process, paired with one Rust sidecar. Six processes for three nodes. Raft
replication is Go↔Go over the network exactly as today; the LSM is purely
local per node, written only through that node's own `apply`. A Go node's
`Config` gains a sidecar address; it never talks to another node's sidecar.

**Sandbox** (`consensus/sandbox.go`): today 3 goroutines share one Go
process. `NewSandbox` spawns 3 sidecar processes too, one per simulated node,
each its own temp directory — `sandboxSM` is replaced by `engine.
StateMachine` pointed at that node's sidecar. The interesting change is
`Crash`/`Restart`: **Crash now also kills that node's sidecar**, and
**Restart starts a fresh sidecar over the same directory** before rebuilding
the Go driver — so a sandbox restart proves both halves survive: Raft's log
(already true today, via `MemStorage` reuse) *and* the engine's WAL/SSTables
(new — this is the actual point of Phase 10's done-when, made visible).

**Out of scope:** a general-purpose CLI for a human to run three real
node+sidecar pairs by hand outside tests/sandbox. The done-when is proven by
an automated test (§8) and demonstrated live in the sandbox; a standalone
operator binary is Phase 11 territory once there's a client to point at it.

---

## 8. Test plan (expands the done-when)

1. **Command codec round-trips** — PUT and DELETE, mirroring `wal.rs`'s own
   `encode_record`/`decode_record` tests almost line for line.
2. **Done-when, for real.** 3 real `Server`s (TCP, like the existing
   `newTCPCluster` tests) each paired with a real sidecar process against its
   own temp dir. `PUT` on the leader; poll until `GET` against **all three**
   sidecars returns the value.
3. **Kill and restart a node → consistent state.** Kill a node's Go process
   *and* its sidecar; restart both against the same directories; assert its
   engine state converges with the others (via normal WAL replay if it
   didn't fall far behind, via `/restore` if Phase 9's `InstallSnapshot`
   fired).
4. **A failed Apply is fatal, not silent.** Kill a node's sidecar mid-run
   without killing the node; the next committed entry's `Apply` must return
   an error that `Driver.run()` propagates — not a dropped write, not a
   retry loop. This is the test that justifies §2's interface change.
5. **Snapshot/Restore round-trip through the real engine.** Build up
   SSTables, `/snapshot`, `/restore` into a *fresh empty* directory, assert
   identical `GET`s — the same shape as Phase 9's `InstallSnapshot` tests,
   now with real bytes instead of the test-SM's string blob.
6. **A follower's read can lag.** `PUT` on the leader, immediately `GET` on
   a follower before it's had a heartbeat — assert it's allowed to be stale
   (not wrong once it does catch up). Documents §4's read-bypasses-Raft
   design as an observable property, not just a sentence in `DESIGN.md`.
7. **Sandbox smoke check** (manual, via the dashboard) — propose through the
   UI, `GET` the same key on all three node cards, crash+restart a node and
   watch its KV table repopulate.

---

## 9. Explicitly out of scope

No read-consistency mode beyond "read the local engine" (Phase 11 picks
one for the client). No snapshot chunking (inherited Phase 9 cut). No
general operator CLI (§7). No membership change, ever (standing project
rule). No TLS/auth on the sidecar link — loopback-only, trusted by
construction.

---

## 10. Decisions locked

| Decision | Choice |
|---|---|
| Rust↔Go boundary | **local sidecar process, one per node** — not FFI, not gRPC (both mechanically blocked on this machine: no `gcc`/`clang`, no `protoc`) |
| Sidecar wire protocol | hand-rolled HTTP/1.1 subset (request line + headers + `Content-Length`), JSON bodies, one request per connection |
| `StateMachine` interface | becomes fallible: `Apply/Snapshot/Restore` all gain `error` — the one change to a Phase 6 contract this phase makes, pending sign-off |
| Command encoding | `op(1)｜keyLen(4)｜key｜valueLen(4)｜value`, same shape and op-byte values as the WAL's own record encoding |
| Reads | bypass Raft, hit the local engine directly; exposed on the new `engine` package, not on `consensus.StateMachine` |
| Snapshot payload | the live SSTable set, framed as `crc32c ｜ [fileNumber｜length｜bytes]*`; install wipes the target directory first (same rule as `RestoreToSnapshot`) |
| Layout | new `engine/` Go package; `storage/src/snapshot.rs` + `storage/src/bin/sidecar.rs` on the Rust side |
| Process topology | one sidecar per node, real deployment and sandbox alike; sandbox `Crash`/`Restart` now manage the sidecar's lifecycle too |

**Milestone, once built:** quorumkv stops being two well-tested halves and
becomes one replicated key-value store — the first phase where a `PUT` on
the leader is actually durable, replicated, *and* readable as real data on
every node.
