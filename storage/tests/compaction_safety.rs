//! Phase 5 §6.5–6.6 — compaction concurrency and crash safety through the public
//! `Db` API.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use common::TempDir;
use storage::db::Db;
use storage::sstable::sst_filename;

/// §6.6 — reads running on other threads throughout a compaction must always see
/// a correct value: never torn, never a resurrected/overwritten version.
#[test]
fn concurrent_reads_during_compaction_are_correct() {
    let dir = TempDir::new();
    let db = Arc::new(Db::open(&dir.0).unwrap());

    // Seed several generations of 100 keys, each flushed to its own SSTable, so
    // the final value of key i is "final-i".
    for gen in 0..4u32 {
        for i in 0..100u32 {
            let v = if gen == 3 { format!("final-{i}") } else { format!("g{gen}-{i}") };
            db.put(format!("k{i:03}").as_bytes(), v.as_bytes()).unwrap();
        }
        db.flush().unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let mut readers = Vec::new();
    for _ in 0..4 {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        readers.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                for i in 0..100u32 {
                    let got = db.get(format!("k{i:03}").as_bytes()).unwrap();
                    // The only committed value for each key is its final one.
                    assert_eq!(got, Some(format!("final-{i}").into_bytes()));
                }
            }
        }));
    }

    // Compact while the readers hammer the store.
    db.compact_all().unwrap();

    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }

    // After compaction: fewer files, values intact.
    assert!(db.sstable_count() < 4);
    for i in 0..100u32 {
        assert_eq!(
            db.get(format!("k{i:03}").as_bytes()).unwrap(),
            Some(format!("final-{i}").into_bytes()),
        );
    }
}

/// §6.5 — a compaction that crashed before its MANIFEST commit leaves an orphan
/// output SSTable (never referenced). On reopen the MANIFEST names the old,
/// consistent set and the orphan is swept — no corruption, no loss.
#[test]
fn orphan_compaction_output_is_swept_on_reopen() {
    let dir = TempDir::new();
    {
        let db = Db::open(&dir.0).unwrap();
        db.put(b"a", b"1").unwrap();
        db.flush().unwrap(); // committed SSTable
        db.put(b"b", b"2").unwrap();
        db.flush().unwrap(); // committed SSTable
    }
    // Simulate a compaction output whose commit never landed: a stray .sst with a
    // number the MANIFEST doesn't reference.
    std::fs::write(dir.0.join(sst_filename(999)), b"orphan compaction output").unwrap();

    let db = Db::open(&dir.0).unwrap();
    assert!(!dir.0.join(sst_filename(999)).exists(), "orphan output must be swept");
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.len().unwrap(), 2);
}

/// A real compaction survives a reopen: the MANIFEST replay yields the compacted
/// set, values are correct, and old input files are gone.
#[test]
fn compacted_state_is_correct_after_reopen() {
    let dir = TempDir::new();
    {
        let db = Db::open(&dir.0).unwrap();
        for gen in 0..5u32 {
            db.put(b"k", format!("v{gen}").as_bytes()).unwrap();
            db.put(b"doomed", b"x").unwrap();
            db.flush().unwrap();
        }
        db.delete(b"doomed").unwrap();
        db.flush().unwrap();
        db.compact_all().unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"v4".to_vec()));
        assert_eq!(db.get(b"doomed").unwrap(), None);
    }
    // Reopen: MANIFEST replay reconstructs the compacted, consistent set.
    let db = Db::open(&dir.0).unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v4".to_vec()));
    assert_eq!(db.get(b"doomed").unwrap(), None);
    assert_eq!(db.len().unwrap(), 1);
}
