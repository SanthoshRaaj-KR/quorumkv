//! Phase 3 done-when + crash-safety of flush (phase-03 §6).

mod common;

use common::TempDir;
use storage::db::Db;

/// Count `*.sst` and `*.sst.tmp` files in a directory.
fn count_sst_files(dir: &std::path::Path) -> (usize, usize) {
    let mut sst = 0;
    let mut tmp = 0;
    for entry in std::fs::read_dir(dir).unwrap() {
        let name = entry.unwrap().file_name().into_string().unwrap();
        if name.ends_with(".sst.tmp") {
            tmp += 1;
        } else if name.ends_with(".sst") {
            sst += 1;
        }
    }
    (sst, tmp)
}

/// §6.1 + §6.2 — a tiny threshold forces several SSTables; every key still reads
/// back (from whichever tier holds it), and no orphan `.tmp` is left behind.
#[test]
fn many_writes_produce_multiple_sstables_and_read_back() {
    let dir = TempDir::new();
    let db = Db::open_with_threshold(&dir.0, 4096).unwrap();

    for i in 0..600u32 {
        db.put(format!("k{i:05}").as_bytes(), &[b'x'; 40]).unwrap();
    }

    let (ssts, tmps) = count_sst_files(&dir.0);
    assert!(ssts >= 2, "expected 2+ SSTables from repeated flushes, got {ssts}");
    assert_eq!(tmps, 0, "no orphan .tmp should remain");

    for i in 0..600u32 {
        assert_eq!(
            db.get(format!("k{i:05}").as_bytes()).unwrap(),
            Some(vec![b'x'; 40]),
            "key k{i:05} missing",
        );
    }
    assert_eq!(db.len().unwrap(), 600);
}

/// §6.2 — everything reads back after a full restart (fresh process state).
#[test]
fn all_keys_read_back_after_restart() {
    let dir = TempDir::new();
    {
        let db = Db::open_with_threshold(&dir.0, 4096).unwrap();
        for i in 0..600u32 {
            db.put(format!("k{i:05}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
        }
        // A small threshold means auto-compaction (phase-05 §8 A3) likely
        // triggered in the background; it holds its own `Arc<Db>` clone, so
        // this scope ending alone wouldn't stop it before the reopen below
        // touches the same directory.
        db.wait_for_compactions();
    }
    let db = Db::open(&dir.0).unwrap();
    assert_eq!(db.len().unwrap(), 600);
    assert_eq!(db.get(b"k00000").unwrap(), Some(b"v0".to_vec()));
    assert_eq!(db.get(b"k00300").unwrap(), Some(b"v300".to_vec()));
    assert_eq!(db.get(b"k00599").unwrap(), Some(b"v599".to_vec()));
}

/// §6.6 — crash mid-flush *before the rename*: an orphan `.sst.tmp` exists and the
/// WAL segment still holds the data. On reopen the temp is cleaned and the data
/// recovers from the WAL — no loss, no partial SSTable adopted.
#[test]
fn crash_before_rename_recovers_from_wal_and_cleans_tmp() {
    let dir = TempDir::new();
    {
        let db = Db::open(&dir.0).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        // No flush: a,b live only in the WAL segment.
    }
    // Simulate a flush that crashed before its atomic rename.
    std::fs::write(dir.0.join("000001.sst.tmp"), b"half-written garbage").unwrap();

    let db = Db::open(&dir.0).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert!(!dir.0.join("000001.sst.tmp").exists(), "orphan .tmp must be cleaned");
    assert!(!dir.0.join("000001.sst").exists(), "the incomplete flush must not appear as an SSTable");
}

/// §6.4 style at the boundary — crash *after the rename, before the WAL delete*:
/// both the SSTable and its WAL segment exist. Reopen must be idempotent (the WAL
/// re-inserts already-flushed data harmlessly), not lose or duplicate anything.
#[test]
fn crash_after_rename_before_wal_delete_is_idempotent() {
    let dir = TempDir::new();
    {
        let db = Db::open(&dir.0).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.flush().unwrap(); // a,b -> 000001.sst; wal-000001 deleted, active on wal-000002
    }
    // Recreate the sealed segment as if its post-flush deletion never happened.
    {
        use storage::wal::{segment_filename, WalWriter, Record};
        let mut w = WalWriter::open(dir.0.join(segment_filename(1))).unwrap();
        w.append(&Record::Put { key: b"a".to_vec(), value: b"1".to_vec() }).unwrap();
        w.append(&Record::Put { key: b"b".to_vec(), value: b"2".to_vec() }).unwrap();
    }

    let db = Db::open(&dir.0).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.len().unwrap(), 2); // no duplicates in the live view
}
