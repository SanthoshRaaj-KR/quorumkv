//! The skip-list payoff, end-to-end through `Db`, now with flushes happening
//! concurrently with writes and reads (Phase 3).
//!
//! Many threads share one `Arc<Db>` and write distinct keys concurrently while
//! the memtable freezes and flushes to SSTables underneath them. All writes must
//! survive (no lost updates), all must be durable (survive a reopen), and reads
//! must run while writes and flushes are in flight.

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
    // Small threshold: forces many flushes while the writers are running.
    let db = Arc::new(Db::open_with_threshold(&dir.0, 8 * 1024).unwrap());

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

    // A concurrent reader exercising the cross-tier read path during flushes.
    {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for _ in 0..2_000 {
                let _ = db.get(b"t00-k0000").unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Every acknowledged write is present (across memtable + SSTables)...
    assert_eq!(db.len().unwrap(), THREADS * PER);
    for t in 0..THREADS {
        for i in 0..PER {
            let k = format!("t{t:02}-k{i:04}");
            assert_eq!(db.get(k.as_bytes()).unwrap(), Some(k.as_bytes().to_vec()));
        }
    }

    // ...and durable: a reopen (reload SSTables + replay WAL) is identical.
    drop(db);
    let reopened = Db::open(&dir.0).unwrap();
    assert_eq!(reopened.len().unwrap(), THREADS * PER);
    for t in 0..THREADS {
        for i in 0..PER {
            let k = format!("t{t:02}-k{i:04}");
            assert_eq!(reopened.get(k.as_bytes()).unwrap(), Some(k.as_bytes().to_vec()));
        }
    }
}
