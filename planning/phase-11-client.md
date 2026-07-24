# Phase 11 — Client library

> **Concept:** making the cluster usable from outside.
> **Done when:** a client keeps working through a leader election without the
> caller writing any retry logic themselves.

Phase 10 made quorumkv a real replicated store, but only from *inside* one Go
process — `Server.Propose`/`Server.Status` are Go method calls, and the only
thing that crosses a real socket today is node-to-node Raft traffic
(`TCPTransport`/`EncodeMessage`). Nothing external — not even another Go
process — can currently write a key or read one back. This phase closes that
gap and, because it's the first time anything reaches the cluster from
outside a single process, it's also the first phase that has to define a
**standalone node**: one real OS process pairing a Raft `Server` with its
paired engine sidecar, addressable over the network. Everything up to now
either lived in-process (`cmd/demo`) or inside one sandboxed process
impersonating three nodes (`cmd/dashboard-backend`).

---

## 0. What already exists, precisely

| Piece | Where | State |
|---|---|---|
| `Server.Propose(cmd) error` | `consensus/server.go` | appends to the **leader's own log** and returns — does **not** wait for majority commit or apply |
| `Server.Status() (Status, error)` | `consensus/server.go` | in-process only; `Status.LeaderID` is exactly the redirect hint `DESIGN.md` §5 step 2 wants, but nothing exposes it over a wire |
| `ErrNotLeader` | `consensus/raft.go` | already exists, already documented as "Phase 11 turns that error into the client's redirect" (phase-06 §6, §7) |
| Node-to-node wire codec | `consensus/transport.go` (`EncodeMessage`/`ReadMessage`) | CRC32C length-prefixed frames — but the payload shape is Raft-message-specific (term, entries, snapshot data...); not a request/response RPC a client could speak |
| `engine.StateMachine.Get` | `consensus/engine/statemachine.go` | per-node local read, bypasses Raft — but lives in a package `consensus` cannot import (cycle: `engine` imports `consensus`) |
| A real 3-process pattern | `consensus/cmd/dashboard-backend/main.go` | closest precedent: owns sidecar lifecycle *and* imports both `consensus` and `consensus/engine` from one outer binary — but it's one process simulating 3 nodes over an in-memory `Bus`, not 3 real node processes over TCP |
| A real TCP Raft cluster | `consensus/tcp_test.go` (`newTCPCluster`) | proves 3 real processes-worth of `Server`+`TCPTransport` work — but only in tests, wired to `recorder`, never a runnable binary |

Nothing above is wrong or needs to change shape. Phase 11 is additive: a new
RPC layer, a new completion-signal on top of `Propose`, and a new binary that
assembles pieces that already exist.

---

## 1. The three gaps, precisely

1. **No client-facing wire protocol.** The only socket protocol is
   Raft-to-Raft. An external caller needs a way to say "PUT this" or "GET
   that" to *some* node and get a real answer or a redirect.
2. **`Propose` doesn't wait for the write to be real.** `DESIGN.md` §5 write
   path: leader appends → replicates → **majority commits → applies →
   responds to the client**. Today `Server.Propose` returns after the first
   step. A client that returns success the moment `Propose` returns would be
   lying — the write isn't committed, let alone applied, so a `GET` right
   after could legitimately miss it even on the leader itself.
3. **No standalone node binary.** A client needs something real to dial. The
   sandbox is one process pretending to be three; Phase 11 needs three
   processes for real, each pairing one `consensus.Server` with one engine
   sidecar (phase-10 §7 explicitly deferred exactly this: *"a standalone
   operator binary is Phase 11 territory once there's a client to point
   at"*).

---

## 2. Decision: the client-facing wire protocol

| | Extend the existing binary frame codec | **Hand-rolled HTTP/1.1 subset, JSON bodies** | gRPC |
|---|---|---|---|
| New dependency | none | none | needs `protoc` — **already ruled out mechanically in phase-10 §1** (no `protoc`/`gcc` on this machine) |
| Matches an existing precedent in *this* codebase | partly — same framing style as `transport.go`, but `Message` is Raft-shaped; a client RPC would need a second, parallel payload format under the same frame header | **fully — the phase-10 sidecar already made exactly this call for exactly this kind of traffic** (control-plane, infrequent, human-diagnosable > byte-optimal) | no |
| `curl`-able while building/debugging | no | **yes** | no |
| "Usable from outside" (this phase's own concept) | only from something that links this Go module | **yes — any HTTP client in any language can drive it**, matching the phase's stated goal more literally than a Go-only binary protocol would | yes, but blocked |
| Traffic shape | many small messages/sec, latency-sensitive (why the Raft wire is binary) | few requests/sec, one per client operation — the sidecar's exact profile | — |

**LOCKED: hand-rolled HTTP/1.1 subset, JSON bodies — the same pattern
phase-10 §1a already established for the sidecar link**, applied one layer
out. This keeps the codebase's wire formats split along one consistent line:
**binary frames for the high-frequency internal links** (Raft-to-Raft, and
the WAL/log on disk), **HTTP+JSON for the low-frequency control-plane links**
that something outside this process — a human with `curl`, a test, a future
non-Go client — needs to speak (sidecar↔node, and now client↔node). Not a new
fork; the second application of a fork phase-10 already resolved.

### 2a. Endpoints

| Endpoint | Request | Success | Redirect / error |
|---|---|---|---|
| `POST /put` | `{"key": base64, "value": base64}` | `{}` | `{"error": "not leader", "leaderId": N}` (`N=0` if unknown) |
| `POST /delete` | `{"key": base64}` | `{}` | same shape |
| `POST /get` | `{"key": base64}` | `{"value": base64\|null}` | same shape (§4 decides whether GET ever redirects) |
| `GET /status` | — | `{"id": N, "role": "...", "term": N, "leaderId": N}` | — |

Same base64-for-binary-values convention as the sidecar (§6 of phase-10),
for the same reason — a `curl -d '{"key":"..."}'` stays plain JSON, and the
project already accepted the waste at this layer's scale.

---

## 3. Decision: how a write waits for "real"

`Server.Propose` staying fire-and-forget is *correct* for Raft's own internal
uses (nothing internal currently needs to block on commit) — this is an
**addition**, not a change to that method or its existing tests.

| | Poll `Status()` in a loop from the RPC handler | **A completion signal owned by `Server`** |
|---|---|---|
| Extra state | none | one `map[index]chan error`-shaped waiter table inside `Server` |
| Latency | bounded by poll interval (a real, if small, tax on every write) | signalled the instant `Driver.run()` applies the entry — no polling tax |
| Handles the "this index got overwritten by a new leader" case | only by accident (poll would just keep not seeing it, no clear signal) | can be made explicit (below) |
| Where it lives | RPC layer, reaching into `Server` from outside its single-goroutine ownership | **inside `Server.run()`'s existing single-goroutine loop — no new locking, same ownership rule Phase 7 established** |

**LOCKED: add `Server.ProposeAndWait(cmd []byte, timeout time.Duration)
(index uint64, err error)`**, additive to (not replacing) `Propose`:

1. Reuses the existing `proposals` channel path to append the entry, but
   captures `(index, term) = (node.LastIndex(), node.Term())` **inside the
   same `run()` iteration**, right after `driver.Propose` succeeds — the one
   moment those two values are guaranteed to describe the entry just
   appended, before anything else can touch them.
2. Registers a waiter for that `(index, term)` in a new field on `Server`,
   then blocks on a reply channel outside the run loop.
3. `Server.run()`, after every iteration that changes `LastApplied` (i.e.
   after `Tick`/`Step`, not just `Propose` — a write typically finishes
   committing on a *later* iteration, triggered by an `AppendEntries`
   response arriving), checks pending waiters against
   `node.LastApplied()`/`node.Log()`:
   - `LastApplied >= index` **and** the log's entry at `index` still has the
     expected `term` → success, signal the waiter.
   - the log's entry at `index` now has a **different** term (a new leader
     truncated and overwrote it — the divergence-repair machinery from
     Phase 8 §... doing exactly its job) → fail the waiter with a new
     `ErrProposalLost` — the write never committed under this node's
     leadership and the caller must retry, most likely against whoever the
     new leader turns out to be.
   - otherwise → still pending, no signal yet.
4. A `timeout` bounds the wait (context-free, matching this project's
   existing style of plain `time.Duration` params over `context.Context` —
   see `engine.Client`'s `http.Client{Timeout: ...}`); on timeout the waiter
   is dropped and `ProposeAndWait` returns a plain "timed out" error. The
   caller (the RPC layer, then the client library) treats a timeout exactly
   like any other retryable failure — no separate cancellation protocol,
   consistent with Phase 7's "no retry queue, the layer above retries"
   stance on `TCPTransport.Send`.

This is the one change to a Phase 6/7 contract this phase makes, and — same
as phase-10 §2 — it's additive and low-risk (new method, new field, zero
change to `Propose`'s existing behavior or tests), but flagging it the same
way: **pending your sign-off**, since `ErrProposalLost` is a new error
callers now need to know how to handle.

---

## 4. Decision: read consistency mode

`README.md`'s decision log already has this as *tentative*: **leader-only,
no confirmation** (`DESIGN.md` §5's middle option). Locking it here:

| | Linearizable (confirm leadership first) | **Leader-only, no confirmation** | Follower read-index |
|---|---|---|---|
| Extra round trip per read | yes — a heartbeat to a majority | **no** | yes — to the leader |
| Implementation cost this phase | needs a "confirm I'm still leader" primitive that doesn't exist yet | **zero new machinery — `GET` already routes to a node's local engine (phase-10 §4); just always route it to the currently-known leader** | needs the leader to expose a queryable commit index and the follower to wait for its own apply to catch up — real new machinery |
| Consistent with the project's existing stale-read note | — | **phase-10 §4 already calls out "GET a follower right after a write... a real, visible consequence of this design, not a bug" — leader-only just narrows that window, doesn't add a new kind of staleness** | — |

**LOCKED: leader-only.** The client's `Get` is routed through the exact same
leader-discovery/redirect path as `Put`/`Delete` — one code path for all
three operations, not two. The narrow staleness window this leaves (a leader
that has just lost leadership to a silent partition could still answer a
stale read for up to one election timeout) is documented, not solved —
exactly the tradeoff `DESIGN.md` §5 already named and priced. Linearizable
reads are a clearly-labeled future upgrade (swap the routing rule, nothing
else), not attempted here.

---

## 5. The client library itself

```go
type Client struct {
    addrs map[uint64]string // static id -> client-RPC address; no discovery (project-wide rule)
    last  uint64            // last known leader id; 0 = unknown, try in id order
}

func New(addrs map[uint64]string) *Client
func (c *Client) Put(key, value []byte) error
func (c *Client) Delete(key []byte) error
func (c *Client) Get(key []byte) (value []byte, ok bool, err error)
```

Algorithm, identical for all three operations:

1. Try `c.last` if set, else the lowest node id.
2. Send the request. Three outcomes:
   - **Success** → done.
   - **`{"error": "not leader", "leaderId": N}`, `N != 0`** → set
     `c.last = N`, retry against that address immediately (no backoff — this
     is a *known-good* redirect, not a failure).
   - **`leaderId == 0`** (election in progress) **or a network error**
     (node down/unreachable) → the node we asked isn't useful right now; move
     to the next id in `addrs` (wrapping), after a short backoff, and retry.
3. Cap total attempts (e.g. `2 * len(addrs)`, so every node gets a second
   look after a full round in case an election resolved mid-loop) before
   giving up with a "no leader found" error.

This is the whole of `ROADMAP.md`'s done-when: *"a client keeps working
through a leader election without the caller writing any retry logic
themselves"* — the retry/redirect loop lives here, once, so nobody using
`Client` has to write it.

---

## 6. Layout

```
consensus/
├── clientrpc/                  (new — sibling to engine/, same tier)
│   ├── server.go               HTTP handlers over one Server + one engine.StateMachine
│   ├── client.go                Client{addrs, last} — §5's algorithm
│   └── protocol.go             request/response JSON shapes shared by both
cmd/
└── quorumkv-node/               (new — the standalone binary phase-10 §7 deferred)
    └── main.go                  one real node: spawn/attach its sidecar, build
                                  engine.StateMachine, consensus.Server+TCPTransport,
                                  clientrpc.Server; three listen addresses
                                  (Raft TCP, client HTTP, sidecar HTTP-loopback-only)
```

`clientrpc` sits at the same level as `engine` (both import `consensus`;
`clientrpc` also imports `engine` for the `Get` path) — the same
import-cycle constraint dashboard-backend's own doc comment already names:
`consensus` stays ignorant of both. No cycle: `clientrpc → consensus`,
`clientrpc → engine → consensus`, a DAG.

`cmd/quorumkv-node` is deliberately most of what
`cmd/dashboard-backend/main.go`'s `sidecarManager` already does, narrowed
from "N simulated nodes in one process" to "exactly one real node" — same
spawn/respawn logic, real config instead of a sandbox generation.

---

## 7. Edge cases

- **Redirected to a leader that has *also* since stepped down.** The client
  doesn't special-case this — it's just another `not leader` response,
  handled by the same retry loop, possibly bouncing a couple of times during
  a fast re-election. Bounded by the attempt cap in §5.
- **`ProposeAndWait` on a node that loses leadership between accepting the
  proposal and it committing.** This is exactly what `ErrProposalLost`
  (§3) is for — the client sees a retryable error, not a false success and
  not a hang.
- **Two different keys proposed back-to-back on the same node.** Each gets
  its own `(index, term)` waiter; `Server.run()` checks all pending waiters
  on every iteration, so out-of-order completion (unlikely but not
  impossible if applies happen to land oddly) is handled per-index, not as
  a FIFO queue.
- **`GET` for a key on a node that is mid-election (no leader).** Surfaces as
  `leaderId == 0`; client treats it like a downed node — hop to the next
  address, per §5.
- **The RPC server's own process crashes mid-request.** No special handling
  needed — the client's connection fails like any unreachable node, same
  retry path.
- **Very large values over HTTP+JSON.** Same base64 tax phase-10 already
  accepted for the sidecar; not re-litigated here. No chunking, no streaming
  — out of scope, same cut as phase-10's snapshot transfer.

---

## 8. Test plan (expands the done-when)

1. **Redirect round-trip.** 3 real `quorumkv-node` processes (or the
   `newTCPCluster`-style in-test harness extended with `clientrpc`). `PUT`
   against a known follower → gets `leaderId` back, not applied there.
2. **Done-when, literally.** `PUT` via `Client` pointed at all three
   addresses in turn (client shouldn't care which one it's given) → succeeds
   from any starting node.
3. **Election survived transparently.** Start a `Put` loop against a 3-node
   cluster; kill the leader mid-stream (same mechanism `tcp_test.go` already
   uses); assert the loop keeps succeeding with zero caller-visible retry
   logic — this is the ROADMAP done-when, executed for real.
4. **`ProposeAndWait` resolves on apply, not just commit.** Assert a `GET`
   immediately following a successful `Put` (same client, same key) always
   sees the new value — proves the wait is on `LastApplied`, not
   `CommitIndex`.
5. **`ErrProposalLost` is reachable and retried.** Force a leadership change
   between accept and commit (isolate the old leader right after `Propose`,
   let a new leader get elected and overwrite the pending index); assert the
   RPC layer surfaces a retryable error and the client's retry succeeds
   against the new leader.
6. **Leader-only staleness is bounded, not unbounded.** `Put` on the leader,
   immediately isolate it (silent partition) before it notices, `Get`
   against it once more — allowed to still answer (documents §4's
   tradeoff) but a **subsequent** `Get` after the old leader would have
   noticed the partition (past one election timeout) must not still claim
   to be leader.
7. **Client survives a fully unreachable node in its address list.** One of
   three addresses points at nothing; `Client` still succeeds via the other
   two, within the attempt cap.

---

## 9. Explicitly out of scope

Linearizable or follower-read-index consistency modes (§4 names them as
future upgrades). Any form of membership change (standing project rule).
Load balancing or connection pooling in `Client` — one request at a time,
same "not a service under load" scale the sidecar accepted. Auth/TLS on the
client RPC link (same trust boundary question as production auth generally —
flagged, not solved, consistent with every other "loopback/local-network
trusted by construction" cut already made). A non-Go client — the protocol
being HTTP+JSON makes one possible later, but only the Go `Client` ships
here.

---

## 10. Decisions locked

| Decision | Choice |
|---|---|
| Client wire protocol | hand-rolled HTTP/1.1 subset, JSON bodies — same fork phase-10 already resolved for the sidecar, applied one layer out |
| Write acknowledgment | new `Server.ProposeAndWait(cmd, timeout) (index, err)`, additive to `Propose`; resolves on `LastApplied` reaching the proposed `(index, term)`; a term mismatch (leadership lost before commit) surfaces as new `ErrProposalLost` — **pending sign-off** |
| Read consistency mode | leader-only, no confirmation (locks README's prior "tentative" entry) — `Get` routed through the same leader-discovery path as writes |
| Leader discovery / retry | client holds a static `id -> address` map (no discovery, standing project rule); on `not leader` hop straight to the hinted id; on unreachable/unknown-leader hop to the next id after a short backoff; capped attempts |
| New process | `cmd/quorumkv-node` — the standalone one-node-per-process binary phase-10 §7 deferred to this phase |
| Layout | `consensus/clientrpc/` (server + client + protocol), sibling to `consensus/engine/` |

**Milestone, once built:** quorumkv stops being something only its own test
suite can talk to. Phase 12/13's chaos and fault-injection work gets a real
external caller to point at instead of driving `Server`/`Driver` directly —
the same shift from "well-tested in isolation" to "provably usable" that
Phase 10 was for the two halves meeting.
