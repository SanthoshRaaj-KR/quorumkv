//! The `Db` wrapper — the storage engine's public API.
//!
//! Phases 1–4 built a directory-backed LSM store (WAL + memtable + immutable
//! SSTables + Bloom filters). Phase 5 makes the live SSTable set **MANIFEST-backed**
//! (a [`VersionSet`]) instead of directory list-and-sort, and adds **compaction**
//! to reclaim the space overwrites and tombstones leave behind.
//!
//! Read tiers, newest→oldest: active memtable → immutable memtables → the
//! SSTables named by the current `Version` (newest file number first).
//!
//! ## Concurrency
//!
//! `put`/`delete`/`get` take `&self`. Writes and version-changing operations
//! (flush, compaction) serialize on `Mutex<WriteState>`; the read view is a
//! separate `RwLock<Layers>` write-locked only for brief tier swaps — reads never
//! block on an fsync, a flush, or a compaction's merge.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use crate::bloom::DEFAULT_BITS_PER_KEY;
use crate::compaction::{run_compaction, CompactionStrategy, SizeTiered};
use crate::manifest::{FileMeta, VersionEdit, VersionSet, MANIFEST_NAME};
use crate::memtable::{Memtable, Value, DEFAULT_THRESHOLD};
use crate::sstable::{list_sstables, remove_orphan_tmp, sst_filename, write_sstable, SstReader};
use crate::wal::{
    list_segments, recover_segments, remove_segment, segment_filename, Record, WalWriter,
};

/// A durable, concurrent, self-compacting LSM key-value store.
pub struct Db {
    dir: PathBuf,
    threshold: usize,
    bits_per_key: u32,
    /// MANIFEST-backed live set + file-number allocator.
    versions: VersionSet,
    strategy: Box<dyn CompactionStrategy>,
    /// Open SSTable readers, keyed by file number (reused across version changes).
    reader_cache: Mutex<HashMap<u64, Arc<SstReader>>>,
    write: Mutex<WriteState>,
    layers: RwLock<Layers>,
}

/// State only writers touch: the active WAL segment and its generation number.
struct WriteState {
    wal: WalWriter,
    active_gen: u64,
}

/// The read view: memtable tiers + the SSTable readers for the current version.
struct Layers {
    active: Arc<Memtable>,
    immutable: Vec<Arc<Memtable>>,
    /// Readers for the current `Version`, **newest first**.
    sstables: Vec<Arc<SstReader>>,
}

impl Db {
    /// Open (creating if absent) the store rooted at directory `dir`.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_threshold(dir, DEFAULT_THRESHOLD)
    }

    /// Open with an explicit memtable flush threshold (tests use a tiny value).
    pub fn open_with_threshold(dir: impl AsRef<Path>, threshold: usize) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        log::info!(target: "db", "opening {}", dir.display());

        remove_orphan_tmp(&dir)?;

        // MANIFEST-backed live set. A pre-Phase-5 store (SSTables but no MANIFEST)
        // has its files adopted so the sweep below doesn't discard real data.
        let had_manifest = dir.join(MANIFEST_NAME).exists();
        let versions = VersionSet::open(&dir)?;
        if !had_manifest {
            let existing = list_sstables(&dir)?;
            if !existing.is_empty() {
                // No SstWriter was around for these files to capture their
                // range for free (phase-05 §8 A2) — read each one's own
                // sparse index to recover it, a one-time cost paid only when
                // adopting a pre-MANIFEST directory.
                let mut added = Vec::with_capacity(existing.len());
                for (n, path) in &existing {
                    let (min_key, max_key) = SstReader::open(path)?.key_range()?;
                    added.push(FileMeta { number: *n, level: 0, min_key, max_key });
                }
                versions.commit(&VersionEdit { added, deleted: Vec::new() })?;
            }
        }

        // Sweep SSTables not referenced by the current version (e.g. a compaction
        // output whose commit never landed).
        let live: HashSet<u64> = versions.current().file_numbers().into_iter().collect();
        for (num, path) in list_sstables(&dir)? {
            if !live.contains(&num) {
                let _ = fs::remove_file(&path);
                log::debug!(target: "db", "swept orphan sstable {}", path.display());
            }
        }

        // Replay surviving WAL segments into a fresh active memtable.
        let seg_rec = recover_segments(&dir)?;
        let active = Arc::new(Memtable::with_threshold(threshold));
        for rec in &seg_rec.records {
            match rec {
                Record::Put { key, value } => active.put(key, value),
                Record::Delete { key } => active.delete(key),
            }
        }
        let (active_gen, wal) = match seg_rec.segments.last() {
            Some((max_seg, seg_path)) => {
                heal_segment_tail(seg_path, seg_rec.last_valid_len)?;
                (*max_seg, WalWriter::open(seg_path)?)
            }
            None => (1, WalWriter::open(dir.join(segment_filename(1)))?),
        };

        log::info!(
            target: "db",
            "open complete: {} sstable(s), replayed {} record(s), active gen {}",
            live.len(),
            seg_rec.records.len(),
            active_gen,
        );

        let db = Db {
            dir,
            threshold,
            bits_per_key: DEFAULT_BITS_PER_KEY,
            versions,
            strategy: Box::new(SizeTiered::default()),
            reader_cache: Mutex::new(HashMap::new()),
            write: Mutex::new(WriteState { wal, active_gen }),
            layers: RwLock::new(Layers { active, immutable: Vec::new(), sstables: Vec::new() }),
        };
        db.refresh_sstable_view()?;
        Ok(db)
    }

    /// Durably record `key -> value`, update memory, flush if the memtable is full
    /// (and compact if that flush trips the strategy's condition).
    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let flushed = {
            let mut w = self.write.lock().expect("write mutex poisoned");
            w.wal.append(&Record::Put { key: key.to_vec(), value: value.to_vec() })?;
            let active = self.active();
            active.put(key, value);
            log::trace!(target: "db", "put {:?}", String::from_utf8_lossy(key));
            active.should_flush() && {
                self.freeze_and_flush(&mut w, &active)?;
                true
            }
        };
        if flushed {
            self.maybe_compact()?; // note: write lock released above
        }
        Ok(())
    }

    /// Durably record a delete (a tombstone), update memory, flush if needed.
    pub fn delete(&self, key: &[u8]) -> io::Result<()> {
        let flushed = {
            let mut w = self.write.lock().expect("write mutex poisoned");
            w.wal.append(&Record::Delete { key: key.to_vec() })?;
            let active = self.active();
            active.delete(key);
            log::trace!(target: "db", "delete {:?}", String::from_utf8_lossy(key));
            active.should_flush() && {
                self.freeze_and_flush(&mut w, &active)?;
                true
            }
        };
        if flushed {
            self.maybe_compact()?;
        }
        Ok(())
    }

    /// Look up a key across all tiers, newest→oldest. A `Put` returns its value; a
    /// `Delete` tombstone returns not-found (and stops the search).
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
            if !sst.maybe_contains(key) {
                continue; // bloom: definitely not here → skip, zero disk I/O
            }
            if let Some(v) = sst.get(key)? {
                return Ok(marker_to_value(v));
            }
        }
        Ok(None)
    }

    /// Force the active memtable to flush now (no-op if empty).
    ///
    /// Unlike an automatic (threshold-driven) flush, a manual `flush` does **not**
    /// trigger compaction — it gives callers/tests explicit control. Use
    /// [`compact`](Db::compact) / [`compact_all`](Db::compact_all) to compact.
    pub fn flush(&self) -> io::Result<()> {
        let mut w = self.write.lock().expect("write mutex poisoned");
        let active = self.active();
        self.freeze_and_flush(&mut w, &active)
    }

    /// If the strategy selects work, compact fully. Called after an automatic
    /// flush (the write lock must already be released — `compact` reacquires it).
    fn maybe_compact(&self) -> io::Result<()> {
        if self.strategy.pick(&self.versions.current().files).is_some() {
            self.compact_all()?;
        }
        Ok(())
    }

    /// Run one compaction if the strategy selects work; returns whether it did.
    ///
    /// Synchronous: holds the write lock for the duration (so it serializes with
    /// flushes). Reads run throughout against their `Arc` snapshot.
    pub fn compact(&self) -> io::Result<bool> {
        let _w = self.write.lock().expect("write mutex poisoned");
        let files = self.versions.current().files.clone();
        let Some(compaction) = self.strategy.pick(&files) else {
            return Ok(false);
        };
        run_compaction(&self.dir, &self.versions, &compaction, self.bits_per_key)?;
        self.refresh_sstable_view()?;
        Ok(true)
    }

    /// Compact repeatedly until the strategy is idle.
    pub fn compact_all(&self) -> io::Result<()> {
        while self.compact()? {}
        Ok(())
    }

    /// Number of live keys across all tiers (tombstones excluded). O(total entries).
    pub fn len(&self) -> io::Result<usize> {
        Ok(self.live_keys()?.len())
    }

    /// Whether the store holds no live keys.
    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.live_keys()?.is_empty())
    }

    /// Approximate bytes written into the active memtable (monotonic).
    pub fn approx_size(&self) -> usize {
        self.snapshot().0.approx_size()
    }

    /// Whether the active memtable is past its flush threshold.
    pub fn should_flush(&self) -> bool {
        self.snapshot().0.should_flush()
    }

    /// Number of live SSTables in the current version.
    pub fn sstable_count(&self) -> usize {
        self.versions.current().files.len()
    }

    /// Package the current live SSTable set as one transferable blob
    /// (phase-10 §6a) — the Raft snapshot payload. Nothing is re-serialized:
    /// these files are already immutable and already on disk. Named
    /// `snapshot_blob` rather than `snapshot` — that name is already the
    /// private read-tier-snapshot helper below.
    ///
    /// Flushes first. A snapshot must capture *everything applied*, not just
    /// what happened to already be flushed — the live SSTable set alone would
    /// silently miss whatever sits in the active memtable at the moment this
    /// is called, which is exactly the kind of gap that stays invisible until
    /// the one restore that needed it comes up short.
    pub fn snapshot_blob(&self) -> io::Result<Vec<u8>> {
        self.flush()?;
        crate::snapshot::pack(&self.dir, &self.versions.current().file_numbers())
    }

    /// Rebuild a store at `dir` from a blob produced by [`Db::snapshot`].
    ///
    /// An installed snapshot is authoritative over anything already at `dir`
    /// — the same "always discard, never merge" rule Raft's own
    /// `Log.RestoreToSnapshot` enforces — so this wipes `dir` first. It then
    /// writes the packed SSTables and opens normally: a fresh directory with
    /// SSTables but no MANIFEST is exactly the pre-Phase-5 shape [`Db::open`]
    /// already knows how to adopt (as one level-0 version), so restore reuses
    /// that path instead of duplicating MANIFEST-writing logic.
    pub fn restore(dir: impl AsRef<Path>, blob: &[u8]) -> io::Result<Db> {
        let dir = dir.as_ref();
        if dir.exists() {
            fs::remove_dir_all(dir)?;
        }
        fs::create_dir_all(dir)?;
        crate::snapshot::unpack(dir, blob)?;
        Db::open(dir)
    }

    /// Total data-block reads across all live SSTables (metrics/tests).
    pub fn sstable_block_reads(&self) -> u64 {
        self.snapshot().2.iter().map(|s| s.block_reads()).sum()
    }

    // ── internals ────────────────────────────────────────────────────────────

    fn active(&self) -> Arc<Memtable> {
        self.layers.read().expect("layers lock poisoned").active.clone()
    }

    #[allow(clippy::type_complexity)]
    fn snapshot(&self) -> (Arc<Memtable>, Vec<Arc<Memtable>>, Vec<Arc<SstReader>>) {
        let l = self.layers.read().expect("layers lock poisoned");
        (l.active.clone(), l.immutable.clone(), l.sstables.clone())
    }

    /// Rebuild `layers.sstables` from the current version, reusing cached readers
    /// and opening any new files. Newest file number first (size-tiered order).
    fn refresh_sstable_view(&self) -> io::Result<()> {
        let version = self.versions.current();
        let mut files = version.files.clone();
        files.sort_by_key(|f| std::cmp::Reverse(f.number)); // newest first

        let mut cache = self.reader_cache.lock().expect("reader cache poisoned");
        let mut readers = Vec::with_capacity(files.len());
        for f in &files {
            let reader = if let Some(r) = cache.get(&f.number) {
                Arc::clone(r)
            } else {
                let r = Arc::new(SstReader::open(self.dir.join(sst_filename(f.number)))?);
                cache.insert(f.number, Arc::clone(&r));
                r
            };
            readers.push(reader);
        }
        let live: HashSet<u64> = files.iter().map(|f| f.number).collect();
        cache.retain(|k, _| live.contains(k));
        drop(cache);

        self.layers.write().expect("layers lock poisoned").sstables = readers;
        Ok(())
    }

    /// Seal the active memtable, install a fresh one + new WAL segment, flush the
    /// sealed memtable to a new SSTable, commit the MANIFEST add, and delete the
    /// WAL segment(s) it covered.
    fn freeze_and_flush(&self, w: &mut WriteState, sealed: &Arc<Memtable>) -> io::Result<()> {
        if sealed.is_empty() {
            return Ok(());
        }
        let sealed_gen = w.active_gen;
        let new_gen = sealed_gen + 1;
        log::info!(target: "db", "freeze: sealing gen {sealed_gen} (~{} bytes)", sealed.approx_size());

        let new_active = Arc::new(Memtable::with_threshold(self.threshold));
        let new_wal = WalWriter::open(self.dir.join(segment_filename(new_gen)))?;
        {
            let mut l = self.layers.write().expect("layers lock poisoned");
            l.immutable.push(Arc::clone(sealed));
            l.active = new_active;
        }
        w.wal = new_wal;
        w.active_gen = new_gen;

        // Flush to a new SSTable (VersionSet-allocated number), then commit the add.
        let sst_number = self.versions.next_file_number();
        let output = write_sstable(&self.dir, sst_number, sealed.iter(), sealed.len(), self.bits_per_key)?;
        if let Some((_, min_key, max_key)) = output {
            self.versions.commit(&VersionEdit::add(sst_number, 0, min_key, max_key))?;
        }

        // Publish the SSTable in the read view, then drop the sealed immutable.
        self.refresh_sstable_view()?;
        {
            let mut l = self.layers.write().expect("layers lock poisoned");
            l.immutable.retain(|m| !Arc::ptr_eq(m, sealed));
        }

        // Data is durable in the SSTable; discard the WAL segment(s) it backed.
        for (n, seg_path) in list_segments(&self.dir)? {
            if n <= sealed_gen {
                remove_segment(&seg_path)?;
            }
        }
        log::info!(target: "db", "flush complete: gen {sealed_gen} -> {}", sst_filename(sst_number));
        Ok(())
    }

    /// Merge all tiers newest→oldest into the current live key/value set.
    fn live_keys(&self) -> io::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        let (active, immutable, sstables) = self.snapshot();

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
        db.flush().unwrap();
        db.put(b"k", b"new").unwrap();
        db.flush().unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec()));
        assert_eq!(db.len().unwrap(), 1);
    }

    #[test]
    fn tombstone_persists_across_flush() {
        let dir = TempDir::new();
        let db = open(&dir);
        db.put(b"k", b"v").unwrap();
        db.flush().unwrap();
        db.delete(b"k").unwrap();
        db.flush().unwrap();
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
            db.flush().unwrap();
            db.put(b"c", b"3").unwrap();
            db.delete(b"a").unwrap();
        }
        let db = open(&dir);
        assert_eq!(db.get(b"a").unwrap(), None);
        assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
        assert_eq!(db.len().unwrap(), 2);
    }

    #[test]
    fn threshold_triggers_automatic_flush_and_compaction() {
        let dir = TempDir::new();
        let db = open_small(&dir, 512);
        for i in 0..200u32 {
            db.put(format!("key{i:05}").as_bytes(), b"value").unwrap();
        }
        // Flushes happened (SSTables exist) and auto-compaction kept the count low.
        assert!(db.sstable_count() >= 1);
        for i in 0..200u32 {
            assert_eq!(db.get(format!("key{i:05}").as_bytes()).unwrap(), Some(b"value".to_vec()));
        }
        assert_eq!(db.len().unwrap(), 200);
    }

    #[test]
    fn compaction_reduces_sstable_count_and_preserves_reads() {
        let dir = TempDir::new();
        let db = open(&dir);
        // Five flushes of the same key -> five SSTables.
        for i in 0..5u32 {
            db.put(b"k", format!("v{i}").as_bytes()).unwrap();
            db.flush().unwrap();
        }
        assert_eq!(db.sstable_count(), 5);

        db.compact_all().unwrap();

        assert!(db.sstable_count() < 5, "compaction should reduce the file count");
        assert_eq!(db.get(b"k").unwrap(), Some(b"v4".to_vec())); // newest survives
        assert_eq!(db.len().unwrap(), 1);
    }

    // ── Phase 10 §6a: snapshot_blob / restore ────────────────────────────────

    #[test]
    fn snapshot_blob_restores_into_a_fresh_directory_with_identical_reads() {
        let src_dir = TempDir::new();
        let dst_dir = TempDir::new();
        let blob = {
            let db = open(&src_dir);
            for i in 0..50u32 {
                db.put(format!("k{i:03}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
            }
            db.delete(b"k010").unwrap();
            db.flush().unwrap(); // force real SSTables, not just an in-memory memtable
            db.snapshot_blob().unwrap()
        };

        let restored = Db::restore(&dst_dir.0, &blob).unwrap();
        assert_eq!(restored.len().unwrap(), 49);
        assert_eq!(restored.get(b"k000").unwrap(), Some(b"v0".to_vec()));
        assert_eq!(restored.get(b"k049").unwrap(), Some(b"v49".to_vec()));
        assert_eq!(restored.get(b"k010").unwrap(), None); // the delete survived the snapshot too
    }

    #[test]
    fn restore_wipes_whatever_the_target_directory_already_held() {
        // An installed snapshot is authoritative — stale local state must not
        // survive alongside it (phase-10 §6a, mirroring Log.RestoreToSnapshot).
        let dir = TempDir::new();
        {
            let stale = open(&dir);
            stale.put(b"stale-key", b"stale-value").unwrap();
            stale.flush().unwrap();
        }

        let src_dir = TempDir::new();
        let blob = {
            let db = open(&src_dir);
            db.put(b"fresh-key", b"fresh-value").unwrap();
            db.flush().unwrap();
            db.snapshot_blob().unwrap()
        };

        let restored = Db::restore(&dir.0, &blob).unwrap();
        assert_eq!(restored.get(b"fresh-key").unwrap(), Some(b"fresh-value".to_vec()));
        assert_eq!(restored.get(b"stale-key").unwrap(), None, "stale pre-restore state must not survive");
    }

    #[test]
    fn snapshot_blob_captures_unflushed_memtable_data() {
        // No explicit flush() here — this is exactly the gap live-testing
        // caught: snapshot_blob() must not silently miss whatever is still
        // sitting in the active memtable.
        let src_dir = TempDir::new();
        let blob = {
            let db = open(&src_dir);
            db.put(b"never-flushed", b"but-still-here").unwrap();
            db.snapshot_blob().unwrap()
        };

        let dst_dir = TempDir::new();
        let restored = Db::restore(&dst_dir.0, &blob).unwrap();
        assert_eq!(restored.get(b"never-flushed").unwrap(), Some(b"but-still-here".to_vec()));
    }

    #[test]
    fn restored_store_survives_its_own_further_reopen() {
        let src_dir = TempDir::new();
        let blob = {
            let db = open(&src_dir);
            db.put(b"a", b"1").unwrap();
            db.flush().unwrap();
            db.snapshot_blob().unwrap()
        };

        let dst_dir = TempDir::new();
        Db::restore(&dst_dir.0, &blob).unwrap();
        // A fresh Db::open over the restored directory — proving restore left
        // behind a normal, durable store, not a one-shot in-memory view.
        let reopened = open(&dst_dir);
        assert_eq!(reopened.get(b"a").unwrap(), Some(b"1".to_vec()));
    }
}
