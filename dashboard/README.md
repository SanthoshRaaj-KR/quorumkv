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

Then open <http://127.0.0.1:5055/>. Requires `go` and `cargo` on `PATH`.

Each card shows a suite (source file) with a **Run** button; each engine
column has a **Run all** button that runs every suite for that engine in
sequence. Results show per-test pass/fail, timing where available, and the
captured failure output for anything that fails. A suite whose build itself
fails (compile error) shows that instead of a test list.

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
