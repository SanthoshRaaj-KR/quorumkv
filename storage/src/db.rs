//! The `Db` wrapper — the storage engine's public API.
//!
//! Phases 1–2 backed this with a single WAL file and one in-memory memtable.
//! Phase 3 makes it a **directory-backed LSM store** with three tiers a read
//! searches newest→oldest:
//!
//! 1. the **active** memtable (live writes, `SkipMap`),
//! 2. any **immutable** memtables (sealed, mid-flush),
//! 3. the **SSTables** on disk (immutable files, newest file number first).
//!
//! When the active memtable crosses its flush threshold it is *frozen* (sealed,
//! a fresh active installed, a new WAL segment started) and *flushed* to a new
//! SSTable, after which its WAL segment is deleted. Durability is unchanged:
//! WAL-before-memory on writes, and flush is durable-then-visible (temp → fsync →
//! rename → fsync dir → delete WAL segment).
//!
//! ## Concurrency
//!
//! `put`/`delete`/`get` take `&self`. Writes serialize on a `Mutex<WriteState>`
//! (the WAL is one file); the *read view* lives behind a separate
//! `RwLock<Layers>` that is write-locked only for the brief tier swaps at freeze
//! time — so reads never block on a write's fsync or on a flush's disk I/O.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use crate::bloom::DEFAULT_BITS_PER_KEY;
use crate::memtable::{Memtable, Value, DEFAULT_THRESHOLD};
use crate::sstable::{list_sstables, remove_orphan_tmp, sst_filename, write_sstable, SstReader};
use crate::wal::{
    list_segments, recover_segments, remove_segment, segment_filename, Record, WalWriter,
};

/// A durable, concurrent, directory-backed LSM key-value store.
pub struct Db {
    dir: PathBuf,
    threshold: usize,
    /// Bloom bits-per-key for SSTables this store flushes (phase-04 knob).
    bits_per_key: u32,
    write: Mutex<WriteState>,
    layers: RwLock<Layers>,
}

/// State only writers touch: the active WAL segment and its generation number.
struct WriteState {
    wal: WalWriter,
    /// Generation of the active memtable; its WAL segment is `wal-{active_gen}.log`
    /// and, when flushed, its SSTable is `{active_gen}.sst`.
    active_gen: u64,
}

/// The read view: the three tiers, snapshot-cloned by readers under a read lock.
struct Layers {
    active: Arc<Memtable>,
    /// Sealed memtables awaiting flush; newest last. (At most one at a time with
    /// synchronous flush, but a reader may observe it mid-freeze.)
    immutable: Vec<Arc<Memtable>>,
    /// On-disk SSTables, **newest first** (highest generation at index 0).
    sstables: Vec<Arc<SstReader>>,
}

impl Db {
    /// Open (creating if absent) the store rooted at directory `dir`.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_threshold(dir, DEFAULT_THRESHOLD)
    }

    /// Open with an explicit memtable flush threshold (tests use a tiny value to
    /// force flushes without writing 64 MB).
    pub fn open_with_threshold(dir: impl AsRef<Path>, threshold: usize) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        log::info!(target: "db", "opening {}", dir.display());

        // Clean up any half-written SSTable from a crashed flush.
        remove_orphan_tmp(&dir)?;

        // Load SSTables, newest file number first.
        let ssts = list_sstables(&dir)?;
        let max_sst = ssts.last().map(|(n, _)| *n).unwrap_or(0);
        let mut sstables = Vec::new();
        for (_, path) in ssts.iter().rev() {
            sstables.push(Arc::new(SstReader::open(path)?));
        }

        // Replay surviving WAL segments (acked-but-unflushed writes) into a fresh
        // active memtable. These are newer than every SSTable.
        let seg_rec = recover_segments(&dir)?;
        let active = Arc::new(Memtable::with_threshold(threshold));
        for rec in &seg_rec.records {
            match rec {
                Record::Put { key, value } => active.put(key, value),
                Record::Delete { key } => active.delete(key),
            }
        }

        // Choose the active generation and its WAL writer. If segments survived,
        // continue appending to the newest one (healing its torn tail first);
        // otherwise start a fresh generation above the highest SSTable.
        let (active_gen, wal) = match seg_rec.segments.last() {
            Some((max_seg, seg_path)) => {
                heal_segment_tail(seg_path, seg_rec.last_valid_len)?;
                (*max_seg, WalWriter::open(seg_path)?)
            }
            None => {
                let gen = max_sst + 1;
                (gen, WalWriter::open(dir.join(segment_filename(gen)))?)
            }
        };

        log::info!(
            target: "db",
            "open complete: {} sstable(s), replayed {} record(s), active gen {}",
            sstables.len(),
            seg_rec.records.len(),
            active_gen,
        );

        Ok(Db {
            dir,
            threshold,
            bits_per_key: DEFAULT_BITS_PER_KEY,
            write: Mutex::new(WriteState { wal, active_gen }),
            layers: RwLock::new(Layers { active, immutable: Vec::new(), sstables }),
        })
    }

    /// Durably record `key -> value`, update the active memtable, and flush if the
    /// memtable has grown past its threshold. WAL first, always.
    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let mut w = self.write.lock().expect("write mutex poisoned");
        w.wal.append(&Record::Put { key: key.to_vec(), value: value.to_vec() })?;
        let active = self.active();
        active.put(key, value);
        log::trace!(target: "db", "put {:?}", String::from_utf8_lossy(key));
        if active.should_flush() {
            self.freeze_and_flush(&mut w, &active)?;
        }
        Ok(())
    }

    /// Durably record a delete (a tombstone), update memory, and flush if needed.
    pub fn delete(&self, key: &[u8]) -> io::Result<()> {
        let mut w = self.write.lock().expect("write mutex poisoned");
        w.wal.append(&Record::Delete { key: key.to_vec() })?;
        let active = self.active();
        active.delete(key);
        log::trace!(target: "db", "delete {:?}", String::from_utf8_lossy(key));
        if active.should_flush() {
            self.freeze_and_flush(&mut w, &active)?;
        }
        Ok(())
    }

    /// Look up a key across all tiers, newest→oldest. First tier that holds a
    /// marker wins: a `Put` returns its value; a `Delete` tombstone returns
    /// not-found (and stops the search — older tiers are shadowed).
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let (active, immutable, sstables) = self.snapshot();

        if let Some(v) = active.get_marker(key) {
            return Ok(marker_to_value(v));
        }
        for m in immutable.iter().rev() {
            if let Some(v) = m.get_marker(key) {
                return Ok(marker_to_value(v));
            }
        }
        for sst in &sstables {
            if let Some(v) = sst.get(key)? {
                return Ok(marker_to_value(v));
            }
        }
        Ok(None)
    }

    /// Force the active memtable to flush now (no-op if empty). Deterministic
    /// flushing for tests; production flushes are threshold-driven.
    pub fn flush(&self) -> io::Result<()> {
        let mut w = self.write.lock().expect("write mutex poisoned");
        let active = self.active();
        self.freeze_and_flush(&mut w, &active)
    }

    /// Number of live keys across all tiers (tombstones excluded). O(total
    /// entries) — an introspection helper, not a hot path.
    pub fn len(&self) -> io::Result<usize> {
        Ok(self.live_keys()?.len())
    }

    /// Whether the store holds no live keys.
    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.live_keys()?.is_empty())
    }

    // ── internals ────────────────────────────────────────────────────────────

    /// Clone the active memtable handle (cheap `Arc`). Only changes under the
    /// write mutex, which the write path already holds.
    fn active(&self) -> Arc<Memtable> {
        self.layers.read().expect("layers lock poisoned").active.clone()
    }

    /// Snapshot all three tiers under a brief read lock, then search lock-free.
    #[allow(clippy::type_complexity)]
    fn snapshot(&self) -> (Arc<Memtable>, Vec<Arc<Memtable>>, Vec<Arc<SstReader>>) {
        let l = self.layers.read().expect("layers lock poisoned");
        (l.active.clone(), l.immutable.clone(), l.sstables.clone())
    }

    /// Seal `sealed`, install a fresh active + new WAL segment, flush the sealed
    /// memtable to an SSTable, then delete the WAL segment(s) it covered.
    ///
    /// Called while holding the write mutex, so it is the single freeze winner.
    /// The slow disk flush runs with no layers lock held — reads never block on it.
    fn freeze_and_flush(&self, w: &mut WriteState, sealed: &Arc<Memtable>) -> io::Result<()> {
        if sealed.is_empty() {
            return Ok(());
        }
        let sealed_gen = w.active_gen;
        let new_gen = sealed_gen + 1;
        log::info!(
            target: "db",
            "freeze: sealing gen {sealed_gen} (~{} bytes) and flushing",
            sealed.approx_size(),
        );

        // Fresh active + new segment, ready before we swap so no write is dropped.
        let new_active = Arc::new(Memtable::with_threshold(self.threshold));
        let new_wal = WalWriter::open(self.dir.join(segment_filename(new_gen)))?;

        // Swap: sealed → immutable, fresh active in. Brief layers write lock.
        {
            let mut l = self.layers.write().expect("layers lock poisoned");
            l.immutable.push(Arc::clone(sealed));
            l.active = new_active;
        }
        w.wal = new_wal;
        w.active_gen = new_gen;

        // Flush the sealed memtable to disk (no layers lock — this is the slow part).
        let path = write_sstable(&self.dir, sealed_gen, sealed.iter(), sealed.len(), self.bits_per_key)?
            .expect("a non-empty sealed memtable always produces an SSTable");
        let reader = Arc::new(SstReader::open(&path)?);

        // Publish the SSTable and drop the sealed memtable — one atomic swap so no
        // read can ever see the data in neither tier.
        {
            let mut l = self.layers.write().expect("layers lock poisoned");
            l.sstables.insert(0, reader); // newest first
            l.immutable.retain(|m| !Arc::ptr_eq(m, sealed));
        }

        // The data is now durable in the SSTable; discard the WAL segment(s) it
        // backed (all generations <= sealed_gen; usually just one).
        for (n, seg_path) in list_segments(&self.dir)? {
            if n <= sealed_gen {
                remove_segment(&seg_path)?;
            }
        }
        log::info!(target: "db", "flush complete: gen {sealed_gen} -> {}", sst_filename(sealed_gen));
        Ok(())
    }

    /// Merge all tiers newest→oldest into the current live key/value set.
    fn live_keys(&self) -> io::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        let (active, immutable, sstables) = self.snapshot();

        // Newest marker per key wins: insert only if the key is unseen.
        let mut seen: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
        for (k, v) in active.iter() {
            seen.entry(k).or_insert(v);
        }
        for m in immutable.iter().rev() {
            for (k, v) in m.iter() {
                seen.entry(k).or_insert(v);
            }
        }
        for sst in &sstables {
            for (k, v) in sst.entries()? {
                seen.entry(k).or_insert(v);
            }
        }

        Ok(seen
            .into_iter()
            .filter_map(|(k, v)| match v {
                Value::Put(val) => Some((k, val)),
                Value::Delete => None,
            })
            .collect())
    }
}

/// Flatten an on-disk/in-memory marker into a read result.
fn marker_to_value(v: Value) -> Option<Vec<u8>> {
    match v {
        Value::Put(val) => Some(val),
        Value::Delete => None,
    }
}

/// Truncate a WAL segment's torn tail back to its last valid record before we
/// append to it again (same discipline as Phase 1/2 recovery).
fn heal_segment_tail(path: &Path, valid_len: u64) -> io::Result<()> {
    if let Ok(meta) = fs::metadata(path) {
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TempDir;

    // Each test's store lives in its own temp dir.
    fn open(dir: &TempDir) -> Db {
        Db::open(&dir.0).unwrap()
    }
    fn open_small(dir: &TempDir, threshold: usize) -> Db {
        Db::open_with_threshold(&dir.0, threshold).unwrap()
    }

    #[test]
    fn put_then_get() {
        let dir = TempDir::new();
        let db = open(&dir);
        db.put(b"k", b"v").unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn get_absent_is_none() {
        let dir = TempDir::new();
        let db = open(&dir);
        assert_eq!(db.get(b"missing").unwrap(), None);
    }

    #[test]
    fn overwrite_last_write_wins() {
        let dir = TempDir::new();
        let db = open(&dir);
        db.put(b"k", b"1").unwrap();
        db.put(b"k", b"2").unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"2".to_vec()));
        assert_eq!(db.len().unwrap(), 1);
    }

    #[test]
    fn delete_reads_not_found() {
        let dir = TempDir::new();
        let db = open(&dir);
        db.put(b"k", b"v").unwrap();
        db.delete(b"k").unwrap();
        assert_eq!(db.get(b"k").unwrap(), None);
        assert_eq!(db.len().unwrap(), 0);
    }

    #[test]
    fn delete_of_absent_key_is_ok() {
        let dir = TempDir::new();
        let db = open(&dir);
        db.delete(b"never").unwrap();
        assert_eq!(db.get(b"never").unwrap(), None);
    }

    #[test]
    fn empty_value_is_distinct_from_absent() {
        let dir = TempDir::new();
        let db = open(&dir);
        db.put(b"k", b"").unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(Vec::new()));
        assert_eq!(db.get(b"other").unwrap(), None);
    }

    #[test]
    fn state_survives_reopen_memtable_only() {
        let dir = TempDir::new();
        {
            let db = open(&dir);
            for i in 0..50u32 {
                db.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
            }
        }
        let db = open(&dir);
        assert_eq!(db.len().unwrap(), 50);
        assert_eq!(db.get(b"k42").unwrap(), Some(b"v42".to_vec()));
    }

    #[test]
    fn manual_flush_moves_data_to_sstable_and_reads_still_work() {
        let dir = TempDir::new();
        let db = open(&dir);
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.flush().unwrap();

        // Data now lives in an SSTable; the active memtable is empty.
        assert!(dir.0.join(sst_filename(1)).exists());
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(db.len().unwrap(), 2);
    }

    #[test]
    fn newest_sstable_wins_across_flushes() {
        let dir = TempDir::new();
        let db = open(&dir);
        db.put(b"k", b"old").unwrap();
        db.flush().unwrap(); // gen 1
        db.put(b"k", b"new").unwrap();
        db.flush().unwrap(); // gen 2
        assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec()));
        assert_eq!(db.len().unwrap(), 1);
    }

    #[test]
    fn tombstone_persists_across_flush() {
        let dir = TempDir::new();
        let db = open(&dir);
        db.put(b"k", b"v").unwrap();
        db.flush().unwrap(); // k lives in sstable 1
        db.delete(b"k").unwrap();
        db.flush().unwrap(); // tombstone in sstable 2, shadows sstable 1
        assert_eq!(db.get(b"k").unwrap(), None);
        assert_eq!(db.len().unwrap(), 0);
    }

    #[test]
    fn everything_survives_reopen_with_sstables() {
        let dir = TempDir::new();
        {
            let db = open(&dir);
            db.put(b"a", b"1").unwrap();
            db.put(b"b", b"2").unwrap();
            db.flush().unwrap(); // a,b -> sstable
            db.put(b"c", b"3").unwrap(); // c stays in memtable/WAL
            db.delete(b"a").unwrap();
        }
        let db = open(&dir); // reload sstables + replay WAL segment
        assert_eq!(db.get(b"a").unwrap(), None); // deleted
        assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec())); // from sstable
        assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec())); // from WAL replay
        assert_eq!(db.len().unwrap(), 2);
    }

    #[test]
    fn threshold_triggers_automatic_flush() {
        let dir = TempDir::new();
        let db = open_small(&dir, 512);
        for i in 0..200u32 {
            db.put(format!("key{i:05}").as_bytes(), b"value").unwrap();
        }
        // At least one flush happened automatically.
        assert!(dir.0.join(sst_filename(1)).exists());
        // All keys still readable across the tiers.
        for i in 0..200u32 {
            assert_eq!(db.get(format!("key{i:05}").as_bytes()).unwrap(), Some(b"value".to_vec()));
        }
        assert_eq!(db.len().unwrap(), 200);
    }
}
