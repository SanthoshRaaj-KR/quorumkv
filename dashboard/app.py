"""quorumkv test dashboard.

A small local Flask app that runs the *real* test suites for both engines —
the Rust LSM storage engine (Track A, phases 1-5) and the Go Raft consensus
layer (Track B, phases 6-9) — and shows a pass/fail matrix grouped by phase.

Nothing here is a mock: every "Run" button shells out to `cargo test` or
`go test -json` against the actual source tree and parses the real output.

Run with:

    pip install -r requirements.txt
    python app.py

then open http://127.0.0.1:5055/
"""

from __future__ import annotations

import atexit
import json
import re
import subprocess
import time
import urllib.error
import urllib.request
from itertools import groupby
from pathlib import Path

from flask import Flask, jsonify, render_template, request

BASE_DIR = Path(__file__).resolve().parent.parent
STORAGE_DIR = BASE_DIR / "storage"
CONSENSUS_DIR = BASE_DIR / "consensus"

GO_TIMEOUT = 120
RUST_TIMEOUT = 180

PHASE_TITLES = {
    0: "Cross-cutting engine glue",
    1: "Phase 1 — WAL / durability",
    2: "Phase 2 — Memtable",
    3: "Phase 3 — SSTable flush",
    4: "Phase 4 — Bloom filter",
    5: "Phase 5 — Compaction",
    6: "Phase 6 — Raft state machine",
    7: "Phase 7 — Leader election",
    8: "Phase 8 — Log replication",
    9: "Phase 9 — Snapshotting",
}

# ─── discovery ────────────────────────────────────────────────────────────────
#
# Suites are discovered from the source tree itself (test function names
# grepped out of each file) rather than hardcoded, so the dashboard can't drift
# out of sync with what the test suites actually contain.

_GO_TEST_FN_RE = re.compile(r"^func (Test\w+)\(t \*testing\.T\)", re.MULTILINE)

# Which Go _test.go file covers which phase (see planning/README.md's phase
# table — the Go tests already split one-file-per-phase almost exactly).
_GO_FILE_PHASES = {
    "raft_test.go": (6, "Raft state machine (single node, driver contract)"),
    "log_test.go": (6, "Log accessors & the sentinel/boundary"),
    "storage_test.go": (6, "Raft's own persistence (append-only file + hardstate)"),
    "election_test.go": (7, "Leader election"),
    "tcp_test.go": (7, "Wire codec & real TCP transport"),
    "replication_test.go": (8, "Log replication over RPC"),
    "snapshot_test.go": (9, "Snapshotting & InstallSnapshot"),
}


def discover_go_suites() -> list[dict]:
    suites = []
    for filename, (phase, label) in _GO_FILE_PHASES.items():
        path = CONSENSUS_DIR / filename
        if not path.exists():
            continue
        names = _GO_TEST_FN_RE.findall(path.read_text(encoding="utf-8"))
        if not names:
            continue
        suites.append(
            {
                "id": f"go:{filename}",
                "engine": "go",
                "phase": phase,
                "file": filename,
                "label": label,
                "test_names": names,
            }
        )
    suites.sort(key=lambda s: (s["phase"], s["file"]))
    return suites


def _extract_rust_test_fns(text: str) -> list[str]:
    """Fn names immediately (modulo other attributes) preceded by #[test]."""
    names: list[str] = []
    pending = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped == "#[test]":
            pending = True
            continue
        if not pending:
            continue
        if stripped.startswith("#["):
            continue  # e.g. #[should_panic] — keep waiting for the fn
        m = re.match(r"(?:pub\s+)?(?:async\s+)?fn\s+(\w+)\s*\(", stripped)
        pending = False
        if m:
            names.append(m.group(1))
    return names


# tests/*.rs — each file is already its own `cargo test --test <name>` target.
_RUST_INTEGRATION_PHASES = {
    "kill9.rs": (1, "WAL crash durability (acknowledged writes survive kill -9)"),
    "db_recovery.rs": (1, "WAL recovery on reopen (incl. corrupt tail)"),
    "concurrency.rs": (2, "Concurrent memtable writers, no lost updates"),
    "flush.rs": (3, "SSTable flush & restart"),
    "bloom_reads.rs": (4, "Bloom filter skips unnecessary reads"),
    "compaction_donewhen.rs": (5, "Compaction correctness (done-when)"),
    "compaction_safety.rs": (5, "Compaction safety under concurrency/crash"),
}

# src/*.rs inline unit tests — isolated via a `<module>::` substring filter
# against `cargo test --lib`.
_RUST_UNIT_PHASES = {
    "wal.rs": (1, "WAL framing & fsync discipline (unit)"),
    "memtable.rs": (2, "Memtable & tombstones (unit)"),
    "sstable.rs": (3, "SSTable layout & sparse index (unit)"),
    "bloom.rs": (4, "Blocked Bloom filter (unit)"),
    "compaction.rs": (5, "Compaction strategy (unit)"),
    "manifest.rs": (5, "MANIFEST file-set tracking (unit)"),
    "merge.rs": (0, "Read-path merge across memtable/SSTables (unit)"),
    "db.rs": (0, "Db engine integration glue (unit)"),
}


def discover_rust_suites() -> list[dict]:
    suites = []
    tests_dir = STORAGE_DIR / "tests"
    for filename, (phase, label) in _RUST_INTEGRATION_PHASES.items():
        path = tests_dir / filename
        if not path.exists():
            continue
        names = _extract_rust_test_fns(path.read_text(encoding="utf-8"))
        if not names:
            continue
        target = filename[:-3]
        suites.append(
            {
                "id": f"rust:test:{target}",
                "engine": "rust",
                "phase": phase,
                "file": f"tests/{filename}",
                "label": label,
                "kind": "integration",
                "cargo_arg": target,
                "test_names": names,
            }
        )

    src_dir = STORAGE_DIR / "src"
    for filename, (phase, label) in _RUST_UNIT_PHASES.items():
        path = src_dir / filename
        if not path.exists():
            continue
        names = _extract_rust_test_fns(path.read_text(encoding="utf-8"))
        if not names:
            continue
        modname = filename[:-3]
        suites.append(
            {
                "id": f"rust:lib:{modname}",
                "engine": "rust",
                "phase": phase,
                "file": f"src/{filename}",
                "label": label,
                "kind": "lib",
                "cargo_arg": f"{modname}::",
                "test_names": names,
            }
        )

    suites.sort(key=lambda s: (s["phase"], s["file"]))
    return suites


GO_SUITES = discover_go_suites()
RUST_SUITES = discover_rust_suites()
ALL_SUITES = {s["id"]: s for s in GO_SUITES + RUST_SUITES}


def group_by_phase(suites: list[dict]) -> list[tuple[int, str, list[dict]]]:
    out = []
    for phase, items in groupby(suites, key=lambda s: s["phase"]):
        out.append((phase, PHASE_TITLES.get(phase, f"Phase {phase}"), list(items)))
    return out


# ─── running: Go ──────────────────────────────────────────────────────────────


def run_go_suite(test_names: list[str]) -> dict:
    pattern = "^(" + "|".join(re.escape(n) for n in test_names) + ")$"
    cmd = ["go", "test", "-json", "-run", pattern, "-count=1", "./..."]
    start = time.time()
    try:
        proc = subprocess.run(
            cmd, cwd=str(CONSENSUS_DIR), capture_output=True, text=True, timeout=GO_TIMEOUT
        )
    except subprocess.TimeoutExpired:
        return {"error": f"timed out after {GO_TIMEOUT}s", "tests": [], "duration_s": GO_TIMEOUT}
    except FileNotFoundError:
        return {"error": "`go` executable not found on PATH", "tests": [], "duration_s": 0}
    duration = time.time() - start

    statuses: dict[str, dict] = {}
    outputs: dict[str, list[str]] = {}
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        name = ev.get("Test")
        if not name:
            continue  # package-level event, not an individual test
        action = ev.get("Action")
        if action == "output":
            outputs.setdefault(name, []).append(ev.get("Output", ""))
        elif action in ("pass", "fail", "skip"):
            statuses[name] = {"status": action, "elapsed_s": ev.get("Elapsed")}

    tests = []
    for name in test_names:
        st = statuses.get(name)
        if st is None:
            tests.append({"name": name, "status": "missing", "elapsed_s": None, "message": None})
            continue
        message = "".join(outputs.get(name, [])) if st["status"] == "fail" else None
        tests.append(
            {"name": name, "status": st["status"], "elapsed_s": st["elapsed_s"], "message": message}
        )

    build_error = None
    if proc.returncode != 0 and not statuses:
        build_error = (proc.stdout + "\n" + proc.stderr).strip()[-4000:]

    return {"duration_s": duration, "tests": tests, "build_error": build_error}


# ─── running: Rust ────────────────────────────────────────────────────────────

_STATUS_RE = re.compile(r"^test (\S+) \.\.\. (ok|FAILED|ignored)\s*$", re.MULTILINE)
_SECTION_RE = re.compile(r"---- (\S+) stdout ----\n(.*?)(?=\n----|\nfailures:|\Z)", re.DOTALL)


def run_rust_suite(kind: str, cargo_arg: str, test_names: list[str]) -> dict:
    if kind == "integration":
        cmd = ["cargo", "test", "--test", cargo_arg, "--", "--test-threads=1"]
    else:
        cmd = ["cargo", "test", "--lib", cargo_arg, "--", "--test-threads=1"]

    start = time.time()
    try:
        proc = subprocess.run(
            cmd, cwd=str(STORAGE_DIR), capture_output=True, text=True, timeout=RUST_TIMEOUT
        )
    except subprocess.TimeoutExpired:
        return {"error": f"timed out after {RUST_TIMEOUT}s", "tests": [], "duration_s": RUST_TIMEOUT}
    except FileNotFoundError:
        return {"error": "`cargo` executable not found on PATH", "tests": [], "duration_s": 0}
    duration = time.time() - start

    out = proc.stdout
    reported = {m.group(1): m.group(2) for m in _STATUS_RE.finditer(out)}
    messages = {m.group(1): m.group(2).strip() for m in _SECTION_RE.finditer(out)}

    def lookup(table: dict, bare_name: str):
        if bare_name in table:
            return table[bare_name]
        for full, val in table.items():
            if full == bare_name or full.endswith("::" + bare_name):
                return val
        return None

    status_word_to_status = {"ok": "pass", "FAILED": "fail", "ignored": "skip"}

    tests = []
    for name in test_names:
        word = lookup(reported, name)
        if word is None:
            tests.append({"name": name, "status": "missing", "elapsed_s": None, "message": None})
            continue
        status = status_word_to_status[word]
        message = lookup(messages, name) if status == "fail" else None
        tests.append({"name": name, "status": status, "elapsed_s": None, "message": message})

    build_error = None
    if proc.returncode != 0 and not reported:
        build_error = (proc.stdout + "\n" + proc.stderr).strip()[-4000:]

    return {"duration_s": duration, "tests": tests, "build_error": build_error}


def run_suite(suite: dict) -> dict:
    if suite["engine"] == "go":
        result = run_go_suite(suite["test_names"])
    else:
        result = run_rust_suite(suite["kind"], suite["cargo_arg"], suite["test_names"])
    result["suite_id"] = suite["id"]
    return result


# ─── live sandbox backend (Go) ────────────────────────────────────────────────
#
# The "run tests" side above works by shelling out per request. The live
# sandbox is different: it's a real, *stateful* Raft cluster you poke at
# incrementally (propose, tick, crash a node, ...), which needs a long-lived
# process holding that state between requests. That's `consensus.Sandbox`,
# wrapped by a tiny Go HTTP server (consensus/cmd/dashboard-backend) — Flask
# just spawns it once and proxies to it, so the whole dashboard is still one
# command to run.

SANDBOX_BACKEND = "http://127.0.0.1:5056"
_sandbox_proc: subprocess.Popen | None = None


def start_sandbox_backend() -> None:
    global _sandbox_proc
    if _sandbox_proc is not None:
        return
    _sandbox_proc = subprocess.Popen(
        ["go", "run", "./cmd/dashboard-backend"], cwd=str(CONSENSUS_DIR)
    )
    atexit.register(_stop_sandbox_backend)

    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            urllib.request.urlopen(SANDBOX_BACKEND + "/state", timeout=1).close()
            print("[dashboard] sandbox backend is up on", SANDBOX_BACKEND)
            return
        except Exception:
            time.sleep(0.3)
    print("[dashboard] WARNING: sandbox backend did not respond within 20s — "
          "check the console for `go run` errors")


def _stop_sandbox_backend() -> None:
    if _sandbox_proc is not None and _sandbox_proc.poll() is None:
        _sandbox_proc.terminate()


def _sandbox_request(path: str, payload=None):
    """Proxy one call to the Go sandbox backend. Returns (json_body, http_status)."""
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        SANDBOX_BACKEND + path,
        data=data,
        method="POST" if data is not None else "GET",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8")), resp.status
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode("utf-8")), e.code
        except json.JSONDecodeError:
            return {"error": f"sandbox backend returned {e.code}"}, e.code
    except urllib.error.URLError as e:
        return {"error": f"sandbox backend unreachable: {e.reason} — is `go run ./cmd/dashboard-backend` still starting?"}, 503


# ─── Flask app ────────────────────────────────────────────────────────────────

app = Flask(__name__)


@app.route("/")
def index():
    return render_template(
        "index.html",
        rust_phases=group_by_phase(RUST_SUITES),
        go_phases=group_by_phase(GO_SUITES),
    )


@app.route("/sandbox")
def sandbox_page():
    return render_template("sandbox.html")


@app.route("/api/sandbox/state")
def sandbox_state():
    data, status = _sandbox_request("/state")
    return jsonify(data), status


@app.route("/api/sandbox/reset", methods=["POST"])
def sandbox_reset():
    data, status = _sandbox_request("/reset", request.get_json(silent=True) or {})
    return jsonify(data), status


@app.route("/api/sandbox/tick", methods=["POST"])
def sandbox_tick():
    n = request.args.get("n", "1")
    data, status = _sandbox_request(f"/tick?n={n}")
    return jsonify(data), status


@app.route("/api/sandbox/<action>", methods=["POST"])
def sandbox_action(action: str):
    if action not in {"propose", "get", "crash", "restart", "isolate", "heal"}:
        return jsonify({"error": f"unknown sandbox action {action!r}"}), 404
    data, status = _sandbox_request(f"/{action}", request.get_json(silent=True) or {})
    return jsonify(data), status


@app.route("/api/run/<path:suite_id>", methods=["POST"])
def api_run(suite_id: str):
    suite = ALL_SUITES.get(suite_id)
    if suite is None:
        return jsonify({"error": f"unknown suite {suite_id!r}"}), 404
    return jsonify(run_suite(suite))


@app.route("/api/run-all/<engine>", methods=["POST"])
def api_run_all(engine: str):
    suites = GO_SUITES if engine == "go" else RUST_SUITES if engine == "rust" else None
    if suites is None:
        return jsonify({"error": f"unknown engine {engine!r}"}), 404
    return jsonify([run_suite(s) for s in suites])


if __name__ == "__main__":
    start_sandbox_backend()
    # use_reloader=False: the watchdog reloader has been seen false-triggering
    # on unrelated stdlib file "changes" in this environment. Not needed for a
    # local dev tool anyway — just restart it after editing.
    app.run(debug=True, port=5055, use_reloader=False)
