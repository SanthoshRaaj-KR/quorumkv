//! Phase 2 §6.9 — the skip-list payoff, end-to-end through `Db`.
//!
//! Many threads share one `Arc<Db>` and write distinct keys concurrently. All
//! writes must survive (no lost updates), all must be durable (survive a reopen),
//! and reads must be able to run while writes are in flight.

mod common;

use std::sync::Arc;
use std::thread;

use common::TempDir;
use storage::db::Db;

#[test]
fn concurrent_writers_no_lost_updates_and_durable() {
    const THREADS: usize = 8;
    const PER: usize = 250;

    let dir = TempDir::new();
    let wal = dir.path("wal.log");

    let db = Arc::new(Db::open(&wal).unwrap());

    // Writers: each thread owns a disjoint key range.
    let mut handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for i in 0..PER {
                    let k = format!("t{t:02}-k{i:04}");
                    db.put(k.as_bytes(), k.as_bytes()).unwrap();
                }
            })
        })
        .collect();

    // A concurrent reader, just to exercise lock-free reads during writes. It
    // asserts nothing about *which* keys are present (writes are in flight) —
    // only that reads don't block, corrupt, or panic.
    {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for _ in 0..2_000 {
                let _ = db.get(b"t00-k0000");
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Every acknowledged write is present in memory...
    assert_eq!(db.len(), THREADS * PER);
    for t in 0..THREADS {
        for i in 0..PER {
            let k = format!("t{t:02}-k{i:04}");
            assert_eq!(db.get(k.as_bytes()), Some(k.as_bytes().to_vec()));
        }
    }

    // ...and durable: a reopen replays the WAL to the identical state.
    drop(db);
    let reopened = Db::open(&wal).unwrap();
    assert_eq!(reopened.len(), THREADS * PER);
    for t in 0..THREADS {
        for i in 0..PER {
            let k = format!("t{t:02}-k{i:04}");
            assert_eq!(reopened.get(k.as_bytes()), Some(k.as_bytes().to_vec()));
        }
    }
}
