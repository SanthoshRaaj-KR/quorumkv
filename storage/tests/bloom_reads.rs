//! Phase 4 done-when (phase-04 §7): the Bloom filter actually skips SSTables,
//! measured through the public `Db` API's data-block-read counter.

mod common;

use common::TempDir;
use storage::db::Db;

/// §7.2 — a GET for a never-written key touches (essentially) zero data blocks:
/// every SSTable's filter says "no". A handful of reads from the ~1% false-
/// positive rate is tolerated.
#[test]
fn absent_key_touches_almost_no_data_blocks() {
    let dir = TempDir::new();
    let db = Db::open(&dir.0).unwrap();
    for i in 0..200u32 {
        db.put(format!("present-{i:04}").as_bytes(), b"v").unwrap();
    }
    db.flush().unwrap(); // one SSTable with a filter

    let before = db.sstable_block_reads();
    for i in 0..500u32 {
        assert_eq!(db.get(format!("absent-{i}").as_bytes()).unwrap(), None);
    }
    let reads = db.sstable_block_reads() - before;
    assert!(reads < 25, "absent queries read {reads} data blocks — bloom not filtering?");
}

/// §7.1 — a key living only in the *oldest* SSTable is found while every newer
/// SSTable is bloom-skipped: exactly one data block is read (the one file that
/// has it), not one per file.
#[test]
fn newer_sstables_are_bloom_skipped() {
    let dir = TempDir::new();
    let db = Db::open(&dir.0).unwrap();

    db.put(b"target", b"here").unwrap();
    db.flush().unwrap(); // gen 1: holds "target"
    for i in 0..50u32 {
        db.put(format!("filler-a-{i:03}").as_bytes(), b"v").unwrap();
    }
    db.flush().unwrap(); // gen 2
    for i in 0..50u32 {
        db.put(format!("filler-b-{i:03}").as_bytes(), b"v").unwrap();
    }
    db.flush().unwrap(); // gen 3 (newest)

    let before = db.sstable_block_reads();
    assert_eq!(db.get(b"target").unwrap(), Some(b"here".to_vec()));
    let reads = db.sstable_block_reads() - before;
    assert_eq!(reads, 1, "expected only the one file holding the key to be read");
}

/// §7.4 — the safety corollary end-to-end: a deleted key's tombstone file MUST be
/// bloom-*hit*, or the read falls through to the older file and resurrects the
/// value. GET must return not-found, and the tombstone file must be read.
#[test]
fn tombstone_file_is_bloom_hit_not_skipped() {
    let dir = TempDir::new();
    let db = Db::open(&dir.0).unwrap();

    db.put(b"k", b"value").unwrap();
    db.flush().unwrap(); // gen 1: k = value
    db.delete(b"k").unwrap();
    db.flush().unwrap(); // gen 2: k = tombstone (newest)

    let before = db.sstable_block_reads();
    assert_eq!(db.get(b"k").unwrap(), None, "deleted key resurrected — tombstone file was bloom-skipped!");
    let reads = db.sstable_block_reads() - before;
    assert!(reads >= 1, "the tombstone file must actually be read (bloom-hit)");
}
