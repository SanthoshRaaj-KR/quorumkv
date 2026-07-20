//! The `Db` wrapper (Phase 1, Task 4) — see `planning/phase-01-wal.md` §4.
//!
//! A thin key-value store: an in-memory `HashMap` fronted by the WAL. It exists
//! to enforce the one invariant the whole engine leans on:
//!
//! > **The WAL is made durable *before* in-memory state changes.** `put`/`delete`
//! > call `WalWriter::append` first and mutate the map only after it returns
//! > `Ok`. So a crash can lose an unacknowledged write, but never leave the map
//! > showing a write the log doesn't have.
//!
//! `Db` is deliberately the seam the memtable replaces in Phase 2: the map is
//! the "live layer", and everything routes through `open`/`put`/`get`/`delete`.

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;

use crate::wal::{recover, Record, WalWriter};

/// A durable key-value store backed by a single write-ahead log.
pub struct Db {
    /// The live in-memory state. Rebuilt from the WAL on `open`.
    map: HashMap<Vec<u8>, Vec<u8>>,
    /// The append-only, fsync-per-write log in front of `map`.
    wal: WalWriter,
}

impl Db {
    /// Open the store at `path`, rebuilding in-memory state by replaying the WAL.
    ///
    /// Recovery is three steps:
    /// 1. `recover` the durable records and the offset where the valid log ends.
    /// 2. Fold the records into the map (last write wins; DELETE removes).
    /// 3. **Truncate any torn/corrupt tail** back to that valid offset before we
    ///    start appending. Without step 3, a crash-torn tail would sit in the
    ///    middle of the file; new appends would land *after* it, and the next
    ///    replay would stop at the tail and silently lose them.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();

        let (records, valid_len) = recover(path)?;

        let mut map = HashMap::new();
        for rec in records {
            match rec {
                Record::Put { key, value } => {
                    map.insert(key, value);
                }
                Record::Delete { key } => {
                    map.remove(&key);
                }
            }
        }

        // Heal a torn/corrupt tail so the append point is right after the last
        // durable record. Only touches the file when there is trailing garbage.
        if let Ok(meta) = std::fs::metadata(path) {
            if meta.len() > valid_len {
                let f = OpenOptions::new().write(true).open(path)?;
                f.set_len(valid_len)?;
                f.sync_all()?;
            }
        }

        let wal = WalWriter::open(path)?;
        Ok(Db { map, wal })
    }

    /// Durably record `key -> value`, then update memory. WAL first, always.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let rec = Record::Put { key: key.to_vec(), value: value.to_vec() };
        self.wal.append(&rec)?; // durable first
        if let Record::Put { key, value } = rec {
            // Reuse the buffers we just built rather than cloning again.
            self.map.insert(key, value);
        }
        Ok(())
    }

    /// Durably record a delete, then remove from memory. WAL first, always.
    ///
    /// Deleting an absent key is legal — it is still logged (a no-op on the map).
    pub fn delete(&mut self, key: &[u8]) -> io::Result<()> {
        let rec = Record::Delete { key: key.to_vec() };
        self.wal.append(&rec)?; // durable first
        if let Record::Delete { key } = rec {
            self.map.remove(&key);
        }
        Ok(())
    }

    /// Look up a key. Reads never touch the WAL.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.map.get(key).map(Vec::as_slice)
    }

    /// Number of live keys.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the store holds no live keys.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{append_raw, TempDir};
    use crate::wal::encode_record;

    #[test]
    fn put_then_get() {
        let dir = TempDir::new();
        let mut db = Db::open(dir.path("wal.log")).unwrap();
        db.put(b"k", b"v").unwrap();
        assert_eq!(db.get(b"k"), Some(b"v".as_slice()));
    }

    #[test]
    fn get_absent_is_none() {
        let dir = TempDir::new();
        let db = Db::open(dir.path("wal.log")).unwrap();
        assert_eq!(db.get(b"missing"), None);
    }

    #[test]
    fn overwrite_last_write_wins() {
        let dir = TempDir::new();
        let mut db = Db::open(dir.path("wal.log")).unwrap();
        db.put(b"k", b"1").unwrap();
        db.put(b"k", b"2").unwrap();
        assert_eq!(db.get(b"k"), Some(b"2".as_slice()));
        assert_eq!(db.len(), 1);
    }

    #[test]
    fn delete_removes_key() {
        let dir = TempDir::new();
        let mut db = Db::open(dir.path("wal.log")).unwrap();
        db.put(b"k", b"v").unwrap();
        db.delete(b"k").unwrap();
        assert_eq!(db.get(b"k"), None);
    }

    #[test]
    fn delete_of_absent_key_is_ok() {
        let dir = TempDir::new();
        let mut db = Db::open(dir.path("wal.log")).unwrap();
        db.delete(b"never-existed").unwrap();
        assert_eq!(db.get(b"never-existed"), None);
    }

    #[test]
    fn empty_value_is_distinct_from_absent() {
        let dir = TempDir::new();
        let mut db = Db::open(dir.path("wal.log")).unwrap();
        db.put(b"k", b"").unwrap();
        assert_eq!(db.get(b"k"), Some(b"".as_slice())); // present, empty
        assert_eq!(db.get(b"other"), None); // absent
    }

    #[test]
    fn state_survives_reopen() {
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        {
            let mut db = Db::open(&path).unwrap();
            for i in 0..100u32 {
                db.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
            }
        } // drop -> close
        let db = Db::open(&path).unwrap();
        assert_eq!(db.len(), 100);
        assert_eq!(db.get(b"k42"), Some(b"v42".as_slice()));
    }

    #[test]
    fn overwrite_and_delete_survive_reopen() {
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        {
            let mut db = Db::open(&path).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"a", b"2").unwrap(); // overwrite
            db.put(b"b", b"1").unwrap();
            db.delete(b"b").unwrap(); // tombstone
        }
        let db = Db::open(&path).unwrap();
        assert_eq!(db.get(b"a"), Some(b"2".as_slice())); // last write won
        assert_eq!(db.get(b"b"), None); // delete replayed
        assert_eq!(db.len(), 1);
    }

    #[test]
    fn torn_tail_is_healed_so_later_writes_are_not_shadowed() {
        // The headline correctness case for `open`'s tail-healing: a crash left a
        // torn record; on reopen we must truncate it, so a write made *after*
        // recovery still survives the *next* reopen.
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        {
            let mut db = Db::open(&path).unwrap();
            db.put(b"before", b"crash").unwrap();
        }
        // Inject a half-written record, as a crash mid-append would.
        let partial = encode_record(&Record::Put { key: b"torn".to_vec(), value: b"x".to_vec() });
        append_raw(&path, &partial[..partial.len() - 2]);

        // Reopen (heals the tail), then append a new record.
        {
            let mut db = Db::open(&path).unwrap();
            assert_eq!(db.get(b"before"), Some(b"crash".as_slice()));
            assert_eq!(db.get(b"torn"), None); // torn write dropped
            db.put(b"after", b"recovery").unwrap();
        }

        // The post-recovery write must survive because the tail was truncated,
        // not left to stop the next replay early.
        let db = Db::open(&path).unwrap();
        assert_eq!(db.get(b"before"), Some(b"crash".as_slice()));
        assert_eq!(db.get(b"after"), Some(b"recovery".as_slice()));
        assert_eq!(db.get(b"torn"), None);
    }
}
