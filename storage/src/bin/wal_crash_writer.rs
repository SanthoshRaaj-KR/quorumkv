//! Crash-test writer (Phase 1, Task 6). Not part of the library API — it exists
//! only to be spawned and `kill`ed by `tests/kill9.rs`.
//!
//! Usage: `wal_crash_writer <wal-path>`
//!
//! It opens a `Db` and PUTs keys forever. Crucially, it prints each key to
//! stdout **only after `put` returns `Ok`** — i.e. after the WAL append fsynced.
//! A printed key is therefore an *acknowledged* write and MUST survive a crash.
//! The harness reads those printed keys, kills this process, reopens the store,
//! and asserts every one is still there.

use std::io::Write;

use storage::db::Db;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: wal_crash_writer <wal-path>");

    let mut db = Db::open(&path).expect("open db");

    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    let mut i: u64 = 0;
    loop {
        // Value == key: enough to prove the (key, value) pair round-tripped.
        let key = format!("key-{i:08}");
        db.put(key.as_bytes(), key.as_bytes()).expect("put");

        // Ack signal: emitted only after the durable append returned. Flush so
        // the harness sees it immediately (a piped stdout is block-buffered).
        writeln!(out, "{key}").expect("write stdout");
        out.flush().expect("flush stdout");

        i += 1;
    }
}
