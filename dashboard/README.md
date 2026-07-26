# quorumkv test dashboard

A small local Flask app that runs the real test suites for both engines and
shows a pass/fail matrix grouped by phase — nothing is mocked, every "Run"
button shells out to `cargo test` or `go test -json` against the actual
source tree.

- **Rust storage engine** (Track A, phases 1-5): each `storage/tests/*.rs`
  integration test file and each `storage/src/*.rs` module's inline unit
  tests is its own suite, run via `cargo test --test <file>` /
  `cargo test --lib <module>::`.
- **Go consensus layer** (Track B, phases 6-9): each `consensus/*_test.go`
  file is its own suite, run via `go test -json -run '^(Name1|Name2|...)$'`
  with the test names discovered by grepping the file.

## Run it

```
cd dashboard
pip install -r requirements.txt
python app.py
```

### Pre-warm (recommended — avoids first-click lag)

The first time you hit **Reset cluster** (or start the dashboard) in a session,
each node's Rust sidecar is launched via `cargo run --bin sidecar`, and the
sandbox backend itself is launched via `go run ./cmd/dashboard-backend` — if
either binary isn't already built, that click pays the full compile cost, and
`reset()` spawns nodes **serially**, so a 3-node cluster pays it up to three
times in a row before the page responds. Once both binaries are built (and
the OS has paged them into its file cache from that first run), everything
after is near-instant — which is why the lag only shows up once per session.

Build both ahead of time so the first click is already fast:

```
cd storage && cargo build --bin sidecar
cd ../consensus && go build ./cmd/dashboard-backend
```

Re-run these after pulling changes that touch `storage/` or
`consensus/cmd/dashboard-backend` / `consensus/engine`, since those
invalidate the cached build.

Then open <http://127.0.0.1:5055/>. Requires `go` and `cargo` on `PATH`.

Each card shows a suite (source file) with a **Run** button; each engine
column has a **Run all** button that runs every suite for that engine in
sequence. Results show per-test pass/fail, timing where available, and the
captured failure output for anything that fails. A suite whose build itself
fails (compile error) shows that instead of a test list.

## Live cluster sandbox

<http://127.0.0.1:5055/sandbox> — a second page for actually *driving* a real
Raft cluster instead of just running its tests. This isn't a simulation: it's
the unmodified `consensus.Node`/`Driver`/`Bus` code from the library itself,
wrapped by `consensus.Sandbox` (`consensus/sandbox.go`) and exposed over HTTP
by a tiny Go server (`consensus/cmd/dashboard-backend`) that Flask launches
automatically and proxies to at `/api/sandbox/*`.

From the page you can, per node:

- **Propose** a command on whichever node is currently leader — watch the
  resulting `AppendEntries` traffic, commit index, and applied state show up
  live in the node cards and the message trace below them.
- **Crash** (kill -9: stops ticking, drops in-flight messages) and
  **Restart** (reloads from that node's own in-memory storage, exactly like
  a real process restart) — the classic "fall behind, catch up" path. Set a
  small **snapshot threshold** at reset time and this same crash/restart
  cycle demonstrates `InstallSnapshot` instead of entry-by-entry replay —
  watch for a `snap` row in the trace.
- **Partition** (isolate: keeps ticking, but no message crosses either way)
  and **Heal** — watch an isolated node campaign on its own, and watch the
  term-jump when it reconnects and the cluster reconverges on one leader.

Every node card shows role, term, leader, commit/applied/last index, the
snapshot boundary (`offset` — entries at or below it are grayed out as
"compacted"), its log entries, and what its state machine has applied. The
message trace is exactly what crossed the bus, in order, with a one-line
summary per message (e.g. `AppendEntries prevLogIndex=4 prevLogTerm=2
entries=2 leaderCommit=4` or `InstallSnapshot snapshotIndex=9 ...`).

## Notes

- Every run is a real subprocess call — a Go suite takes ~2s (mostly Go's own
  toolchain overhead), a Rust suite's first run after a code change pays
  `cargo`'s incremental compile cost.
- Phase → file mapping is in `app.py`'s `_GO_FILE_PHASES` /
  `_RUST_INTEGRATION_PHASES` / `_RUST_UNIT_PHASES` dicts. A couple of Rust
  files (`merge.rs`, `db.rs`) are cross-cutting glue rather than one specific
  phase and are labeled "Phase 0" for that reason.
- The dev server's reloader is disabled (`use_reloader=False`) — it was
  false-triggering restarts on unrelated stdlib file "changes" in this
  environment. Restart the process manually after editing `app.py`.
- The sandbox backend is spawned via `go run ./cmd/dashboard-backend` when
  Flask starts (first launch pays Go's compile cost, ~1-2s) and killed via
  `atexit` when Flask exits. If port 5056 is already in use, kill whatever's
  on it — the dashboard doesn't try to reuse an existing instance.
