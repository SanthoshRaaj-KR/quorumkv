//! Phase 1, Task 5 — the phase-01 §6 test matrix exercised through the *public*
//! `Db` API only (a real consumer of the crate), complementing the white-box
//! unit tests inside `src/`.

mod common;

use common::TempDir;
use storage::db::Db;

/// §6.1 happy path — many keys survive a clean reopen.
#[test]
fn hundred_keys_survive_reopen() {
    let dir = TempDir::new();
    let wal = dir.path("wal.log");
    {
        let mut db = Db::open(&wal).unwrap();
        for i in 0..100u32 {
            db.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
        }
    }
    let db = Db::open(&wal).unwrap();
    assert_eq!(db.len(), 100);
    assert_eq!(db.get(b"k0"), Some(b"v0".as_slice()));
    assert_eq!(db.get(b"k99"), Some(b"v99".as_slice()));
}

/// §6.2 overwrite + §6.3 delete — both resolve correctly after replay.
#[test]
fn overwrite_and_delete_survive_reopen() {
    let dir = TempDir::new();
    let wal = dir.path("wal.log");
    {
        let mut db = Db::open(&wal).unwrap();
        db.put(b"k", b"first").unwrap();
        db.put(b"k", b"second").unwrap(); // overwrite
        db.put(b"gone", b"x").unwrap();
        db.delete(b"gone").unwrap(); // tombstone
    }
    let db = Db::open(&wal).unwrap();
    assert_eq!(db.get(b"k"), Some(b"second".as_slice()));
    assert_eq!(db.get(b"gone"), None);
    assert_eq!(db.len(), 1);
}

/// §6.6 corrupt CRC — a byte flipped in the last record makes it fail checksum;
/// on reopen that record is dropped (and the tail truncated), earlier ones survive.
#[test]
fn corrupt_tail_record_is_dropped_on_reopen() {
    let dir = TempDir::new();
    let wal = dir.path("wal.log");
    {
        let mut db = Db::open(&wal).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();
    }

    // Flip the final byte — it lives inside record "c"'s value.
    let mut bytes = std::fs::read(&wal).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    std::fs::write(&wal, &bytes).unwrap();

    let db = Db::open(&wal).unwrap();
    assert_eq!(db.get(b"a"), Some(b"1".as_slice()));
    assert_eq!(db.get(b"b"), Some(b"2".as_slice()));
    assert_eq!(db.get(b"c"), None); // corrupt record dropped
    assert_eq!(db.len(), 2);
}
