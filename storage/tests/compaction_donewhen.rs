//! Phase 5 done-when (phase-05 §6.1–6.3) through the public `Db` API.

mod common;

use common::TempDir;
use storage::db::Db;

fn sst_bytes(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".sst"))
        .map(|e| e.metadata().unwrap().len())
        .sum()
}

/// §6.1 + §6.2 — writing the same 10 keys many times leaves a pile of redundant
/// SSTables; compaction collapses them and every key still returns its latest value.
#[test]
fn redundancy_collapses_and_latest_value_survives() {
    let dir = TempDir::new();
    let db = Db::open(&dir.0).unwrap(); // default (large) threshold: manual flushes only

    const ROUNDS: u32 = 150;
    for round in 0..ROUNDS {
        for k in 0..10u32 {
            db.put(format!("k{k}").as_bytes(), format!("v{round}").as_bytes()).unwrap();
        }
        db.flush().unwrap(); // one SSTable per round (manual flush doesn't compact)
    }
    assert_eq!(db.sstable_count() as u32, ROUNDS, "expected one SSTable per round");
    let before = sst_bytes(&dir.0);

    db.compact_all().unwrap();

    let after = sst_bytes(&dir.0);
    assert!(db.sstable_count() <= 2, "compaction should collapse to a couple of files");
    assert!(after * 5 < before, "disk should drop dramatically: {after} vs {before}");

    // Every key holds its final value; exactly 10 live keys remain.
    for k in 0..10u32 {
        assert_eq!(
            db.get(format!("k{k}").as_bytes()).unwrap(),
            Some(format!("v{}", ROUNDS - 1).into_bytes()),
        );
    }
    assert_eq!(db.len().unwrap(), 10);
}

/// §6.3 — a deleted key stays deleted after a bottom-most compaction and a
/// restart; the tombstone is actually dropped, not merely shadowed.
#[test]
fn deleted_keys_stay_deleted_after_compaction_and_reopen() {
    let dir = TempDir::new();
    {
        let db = Db::open(&dir.0).unwrap();
        // A few generations of live data, then delete half of it.
        for gen in 0..4u32 {
            for k in 0..10u32 {
                db.put(format!("k{k}").as_bytes(), format!("v{gen}").as_bytes()).unwrap();
            }
            db.flush().unwrap();
        }
        for k in 0..5u32 {
            db.delete(format!("k{k}").as_bytes()).unwrap();
        }
        db.flush().unwrap();

        // compact_all merges everything (bottom-most) → tombstones dropped entirely.
        db.compact_all().unwrap();
        assert_eq!(db.sstable_count(), 1);
        for k in 0..5u32 {
            assert_eq!(db.get(format!("k{k}").as_bytes()).unwrap(), None);
        }
        for k in 5..10u32 {
            assert_eq!(db.get(format!("k{k}").as_bytes()).unwrap(), Some(b"v3".to_vec()));
        }
    }

    // Restart: the deleted keys are still gone (tombstones were physically dropped,
    // and there is nothing older to resurrect).
    let db = Db::open(&dir.0).unwrap();
    for k in 0..5u32 {
        assert_eq!(db.get(format!("k{k}").as_bytes()).unwrap(), None);
    }
    for k in 5..10u32 {
        assert_eq!(db.get(format!("k{k}").as_bytes()).unwrap(), Some(b"v3".to_vec()));
    }
    assert_eq!(db.len().unwrap(), 5);
}
