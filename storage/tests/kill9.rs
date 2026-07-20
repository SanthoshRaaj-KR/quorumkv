//! Phase 1, Task 6 — the phase's official done-when (phase-01 §6.4):
//!
//! > Write keys in a loop, printing each *after* the append returns; kill the
//! > process externally; restart; assert every printed (acknowledged) key is
//! > still there.
//!
//! We spawn `wal_crash_writer`, read the keys it acknowledges on stdout, then
//! `kill` it — on Windows this is `TerminateProcess`, the immediate, uncatchable
//! equivalent of `kill -9`: no destructors, no flush, no cleanup. Whatever the
//! child had *acknowledged* before the kill must survive, because a printed key
//! means its WAL append already fsynced.

mod common;

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

use common::TempDir;
use storage::db::Db;

#[test]
fn acknowledged_writes_survive_kill9() {
    let dir = TempDir::new();
    let wal = dir.path("wal.log");

    // Spawn the writer, piping its stdout so we can observe acknowledged keys.
    let mut child = Command::new(env!("CARGO_BIN_EXE_wal_crash_writer"))
        .arg(&wal)
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn wal_crash_writer");

    let mut reader = BufReader::new(child.stdout.take().expect("child stdout"));

    // Collect a batch of acknowledged keys.
    const ACKED: usize = 50;
    let mut acked = Vec::with_capacity(ACKED);
    for _ in 0..ACKED {
        let mut line = String::new();
        let n = reader.read_line(&mut line).expect("read child stdout");
        assert!(n > 0, "writer exited before acknowledging {ACKED} writes");
        acked.push(line.trim_end().to_string());
    }

    // kill -9 equivalent: terminate mid-run, possibly mid-write (leaving a torn
    // tail — exactly the crash recovery must tolerate).
    child.kill().expect("kill writer");
    let _ = child.wait();
    drop(reader); // release the pipe / the child's file handle path

    // Restart: reopen the store and verify every acknowledged write survived.
    let db = Db::open(&wal).expect("reopen after crash");
    for key in &acked {
        assert_eq!(
            db.get(key.as_bytes()),
            Some(key.as_bytes()),
            "acknowledged key {key} was lost across the crash",
        );
    }
    assert_eq!(acked.len(), ACKED);
}
