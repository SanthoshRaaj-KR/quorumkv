//! The `Db` wrapper — the storage engine's public API.
//!
//! Phase 1 backed this with a `HashMap`; Phase 2 swaps in the sorted, concurrent
//! [`Memtable`] and represents deletes as tombstones. The durability invariant is
//! unchanged: **the WAL is made durable before in-memory state changes.**
//!
//! ## Concurrency (Phase 2)
//!
//! `put`/`delete`/`get` all take `&self`, so a `Db` can be shared across threads
//! (e.g. `Arc<Db>`). Reads are lock-free (the skip list). Writes take a `Mutex`
//! around the WAL so append+fsync is serialized — the single choke point the
//! planning doc flags as the future group-commit site.
//!
//! We hold that WAL lock **across the memtable insert**, not just the append.
//! That keeps the memtable's per-key order identical to the WAL's, so replaying
//! the WAL reconstructs exactly the live memtable — even when two threads write
//! the same key. (Readers still run lock-free throughout.)

use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::sync::Mutex;

use crate::memtable::{Memtable, Value, DEFAULT_THRESHOLD};
use crate::wal::{recover, Record, WalWriter};

/// A durable, concurrent key-value store backed by a single write-ahead log.
pub struct Db {
    mem: Memtable,
    wal: Mutex<WalWriter>,
}

impl Db {
    /// Open the store at `path` with the default flush threshold (64 MB).
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_threshold(path, DEFAULT_THRESHOLD)
    }

    /// Open the store with an explicit memtable flush threshold (tests use a tiny
    /// value; the flush itself is Phase 3).
    ///
    /// Recovery: [`recover`] the durable records and the valid-log offset, fold
    /// the records into the memtable (DELETE → tombstone), truncate any torn tail
    /// back to the valid offset, then open the WAL for appending.
    pub fn open_with_threshold(path: impl AsRef<Path>, threshold: usize) -> io::Result<Self> {
        let path = path.as_ref();
        log::info!(target: "db", "opening {}", path.display());

        let (records, valid_len) = recover(path)?;
        let replayed = records.len();

        let mem = Memtable::with_threshold(threshold);
        for rec in records {
            match rec {
                Record::Put { key, value } => mem.put(&key, &value),
                Record::Delete { key } => mem.delete(&key),
            }
        }

        // Heal a torn/corrupt tail so the append point is right after the last
        // durable record (otherwise later appends would be shadowed on the next
        // replay). Only touches the file when there is trailing garbage.
        if let Ok(meta) = std::fs::metadata(path) {
            if meta.len() > valid_len {
                log::warn!(
                    target: "db",
                    "healing torn tail: truncating {} from {} to {} bytes",
                    path.display(),
                    meta.len(),
                    valid_len,
                );
                let f = OpenOptions::new().write(true).open(path)?;
                f.set_len(valid_len)?;
                f.sync_all()?;
            }
        }

        let wal = WalWriter::open(path)?;
        log::info!(
            target: "db",
            "open complete: replayed {replayed} record(s) -> {} memtable entr(y|ies)",
            mem.len(),
        );
        Ok(Db { mem, wal: Mutex::new(wal) })
    }

    /// Durably record `key -> value`, then update memory. WAL first, always.
    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let rec = Record::Put { key: key.to_vec(), value: value.to_vec() };
        // Hold the WAL lock across the memtable insert (see module docs).
        let mut wal = self.wal.lock().expect("WAL mutex poisoned");
        wal.append(&rec)?; // durable first
        self.mem.put(key, value);
        log::trace!(
            target: "db",
            "put {:?} ({} value byte(s))",
            String::from_utf8_lossy(key),
            value.len(),
        );
        Ok(())
    }

    /// Durably record a delete (as a tombstone), then apply it to memory.
    ///
    /// Deleting an absent key is legal — it is still logged and inserts a
    /// tombstone (a no-op on the live view, but it shadows any older on-disk copy
    /// once SSTables exist in Phase 3).
    pub fn delete(&self, key: &[u8]) -> io::Result<()> {
        let rec = Record::Delete { key: key.to_vec() };
        let mut wal = self.wal.lock().expect("WAL mutex poisoned");
        wal.append(&rec)?; // durable first
        self.mem.delete(key);
        log::trace!(target: "db", "delete {:?}", String::from_utf8_lossy(key));
        Ok(())
    }

    /// Look up a key's live value. A deleted key (tombstone) reads as not-found.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.mem.get(key)
    }

    /// Number of **live** keys (tombstones excluded). O(n) introspection helper.
    pub fn len(&self) -> usize {
        self.mem.iter().filter(|(_, v)| matches!(v, Value::Put(_))).count()
    }

    /// Whether the store holds no live keys.
    pub fn is_empty(&self) -> bool {
        !self.mem.iter().any(|(_, v)| matches!(v, Value::Put(_)))
    }

    /// Approximate bytes written into the active memtable (monotonic).
    pub fn approx_size(&self) -> usize {
        self.mem.approx_size()
    }

    /// Whether the memtable has grown past its flush threshold (Phase 3 acts on it).
    pub fn should_flush(&self) -> bool {
        self.mem.should_flush()
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
        let db = Db::open(dir.path("wal.log")).unwrap();
        db.put(b"k", b"v").unwrap();
        assert_eq!(db.get(b"k"), Some(b"v".to_vec()));
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
        let db = Db::open(dir.path("wal.log")).unwrap();
        db.put(b"k", b"1").unwrap();
        db.put(b"k", b"2").unwrap();
        assert_eq!(db.get(b"k"), Some(b"2".to_vec()));
        assert_eq!(db.len(), 1);
    }

    #[test]
    fn delete_removes_key_from_live_view() {
        let dir = TempDir::new();
        let db = Db::open(dir.path("wal.log")).unwrap();
        db.put(b"k", b"v").unwrap();
        db.delete(b"k").unwrap();
        assert_eq!(db.get(b"k"), None);
        assert_eq!(db.len(), 0); // no live keys...
        assert!(!db.mem.is_empty()); // ...but the tombstone is still resident
    }

    #[test]
    fn delete_of_absent_key_is_ok() {
        let dir = TempDir::new();
        let db = Db::open(dir.path("wal.log")).unwrap();
        db.delete(b"never-existed").unwrap();
        assert_eq!(db.get(b"never-existed"), None);
    }

    #[test]
    fn empty_value_is_distinct_from_absent() {
        let dir = TempDir::new();
        let db = Db::open(dir.path("wal.log")).unwrap();
        db.put(b"k", b"").unwrap();
        assert_eq!(db.get(b"k"), Some(Vec::new())); // present, empty
        assert_eq!(db.get(b"other"), None); // absent
    }

    #[test]
    fn state_survives_reopen() {
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        {
            let db = Db::open(&path).unwrap();
            for i in 0..100u32 {
                db.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
            }
        }
        let db = Db::open(&path).unwrap();
        assert_eq!(db.len(), 100);
        assert_eq!(db.get(b"k42"), Some(b"v42".to_vec()));
    }

    #[test]
    fn tombstone_survives_reopen() {
        // Crash-rebuild with a delete: after replay, the deleted key must still
        // read not-found (the DELETE record replays as a tombstone).
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        {
            let db = Db::open(&path).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"a", b"2").unwrap(); // overwrite
            db.put(b"b", b"1").unwrap();
            db.delete(b"b").unwrap(); // tombstone
        }
        let db = Db::open(&path).unwrap();
        assert_eq!(db.get(b"a"), Some(b"2".to_vec())); // last write won
        assert_eq!(db.get(b"b"), None); // delete replayed
        assert_eq!(db.len(), 1);
    }

    #[test]
    fn delete_then_reput_survives_reopen() {
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        {
            let db = Db::open(&path).unwrap();
            db.delete(b"k").unwrap();
            db.put(b"k", b"back").unwrap(); // resurrect after tombstone
        }
        let db = Db::open(&path).unwrap();
        assert_eq!(db.get(b"k"), Some(b"back".to_vec()));
        assert_eq!(db.len(), 1);
    }

    #[test]
    fn torn_tail_is_healed_so_later_writes_are_not_shadowed() {
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        {
            let db = Db::open(&path).unwrap();
            db.put(b"before", b"crash").unwrap();
        }
        // Inject a half-written record, as a crash mid-append would.
        let partial = encode_record(&Record::Put { key: b"torn".to_vec(), value: b"x".to_vec() });
        append_raw(&path, &partial[..partial.len() - 2]);

        {
            let db = Db::open(&path).unwrap(); // heals the tail
            assert_eq!(db.get(b"before"), Some(b"crash".to_vec()));
            assert_eq!(db.get(b"torn"), None);
            db.put(b"after", b"recovery").unwrap();
        }

        let db = Db::open(&path).unwrap();
        assert_eq!(db.get(b"before"), Some(b"crash".to_vec()));
        assert_eq!(db.get(b"after"), Some(b"recovery".to_vec()));
        assert_eq!(db.get(b"torn"), None);
    }

    #[test]
    fn should_flush_passthrough() {
        let dir = TempDir::new();
        let db = Db::open_with_threshold(dir.path("wal.log"), 512).unwrap();
        assert!(!db.should_flush());
        let mut i = 0;
        while !db.should_flush() {
            db.put(format!("k{i:06}").as_bytes(), b"value").unwrap();
            i += 1;
        }
        assert!(db.approx_size() >= 512);
    }
}
