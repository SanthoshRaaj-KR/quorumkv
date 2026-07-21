//! Recovery matrix exercised through the *public* `Db` API (a real consumer of
//! the crate). Phase 3: the store is now a directory of WAL segments + SSTables.

mod common;

use common::TempDir;
use storage::db::Db;
use storage::wal::segment_filename;

/// Happy path — many keys survive a clean reopen (memtable/WAL only).
#[test]
fn hundred_keys_survive_reopen() {
    let dir = TempDir::new();
    {
        let db = Db::open(&dir.0).unwrap();
        for i in 0..100u32 {
            db.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
        }
    }
    let db = Db::open(&dir.0).unwrap();
    assert_eq!(db.len().unwrap(), 100);
    assert_eq!(db.get(b"k0").unwrap(), Some(b"v0".to_vec()));
    assert_eq!(db.get(b"k99").unwrap(), Some(b"v99".to_vec()));
}

/// Overwrite + delete both resolve correctly after replay (delete → tombstone).
#[test]
fn overwrite_and_delete_survive_reopen() {
    let dir = TempDir::new();
    {
        let db = Db::open(&dir.0).unwrap();
        db.put(b"k", b"first").unwrap();
        db.put(b"k", b"second").unwrap(); // overwrite
        db.put(b"gone", b"x").unwrap();
        db.delete(b"gone").unwrap(); // tombstone
    }
    let db = Db::open(&dir.0).unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"second".to_vec()));
    assert_eq!(db.get(b"gone").unwrap(), None);
    assert_eq!(db.len().unwrap(), 1);
}

/// Corrupt CRC in the WAL segment — the last record fails checksum; on reopen it
/// is dropped (tail truncated) and earlier records survive.
#[test]
fn corrupt_tail_record_is_dropped_on_reopen() {
    let dir = TempDir::new();
    {
        let db = Db::open(&dir.0).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();
    }

    // Flip the final byte of the active WAL segment (inside record "c").
    let seg = dir.0.join(segment_filename(1));
    let mut bytes = std::fs::read(&seg).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    std::fs::write(&seg, &bytes).unwrap();

    let db = Db::open(&dir.0).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), None); // corrupt record dropped
    assert_eq!(db.len().unwrap(), 2);
}

/// Data flushed to an SSTable survives a reopen and reads from disk.
#[test]
fn flushed_data_survives_reopen() {
    let dir = TempDir::new();
    {
        let db = Db::open(&dir.0).unwrap();
        for i in 0..40u32 {
            db.put(format!("k{i:03}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
        }
        db.flush().unwrap(); // force everything to an SSTable
        assert!(db.get(b"k000").unwrap().is_some());
    }
    let db = Db::open(&dir.0).unwrap();
    assert_eq!(db.len().unwrap(), 40);
    assert_eq!(db.get(b"k020").unwrap(), Some(b"v20".to_vec()));
}
