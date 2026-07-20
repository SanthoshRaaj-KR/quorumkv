//! The memtable (Phase 2) — see `planning/phase-02-memtable.md`.
//!
//! The in-memory **sorted** layer that serves live reads and writes, sitting in
//! front of the WAL. Two locked ideas drive its shape:
//!
//! 1. **Sorted** — it's a `crossbeam_skiplist::SkipMap` (a lock-free concurrent
//!    ordered map, the class RocksDB/LevelDB use). Keeping keys sorted as we
//!    insert makes the Phase 3 flush to an SSTable a straight sequential copy.
//! 2. **Tombstones, not removal** — `DELETE k` inserts [`Value::Delete`] rather
//!    than removing the key. A real removal would let an older on-disk copy of
//!    the key resurrect in Phase 3; a tombstone *shadows* it until compaction
//!    (Phase 5) physically drops it.
//!
//! The structure is concurrent from the start: `put`/`delete`/`get` all take
//! `&self`, so many threads can write at once. The size counter is an owned
//! `AtomicUsize` (never shared, never reset — a freeze installs a *fresh*
//! memtable, in Phase 3).

use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_skiplist::SkipMap;

/// Fixed per-entry overhead added to the size counter so "N bytes counted" is a
/// safe *over*-estimate of real RAM (skip-list tower pointers, the enum tag,
/// allocator headers) rather than an under-estimate (phase-02 §4).
const OVERHEAD: usize = 64;

/// Default flush threshold: 64 MB (phase-02 §4/§7). Tests use a tiny value.
pub const DEFAULT_THRESHOLD: usize = 64 * 1024 * 1024;

/// A value stored in the memtable: a real value, or a tombstone marking a delete.
///
/// This one enum is the seed of the whole LSM read model — it reappears in
/// SSTables, the read merge order, and compaction's "can I drop this tombstone?"
/// logic. An empty `Put(vec![])` is a *present, empty* value — distinct from
/// `Delete`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Put(Vec<u8>),
    Delete,
}

/// A sorted, concurrent in-memory table bundling the map with its size counter.
pub struct Memtable {
    map: SkipMap<Vec<u8>, Value>,
    /// Approximate bytes *written* (monotonic), owned by this memtable.
    size: AtomicUsize,
    /// Flush trigger: `should_flush()` is true once `size >= threshold`.
    threshold: usize,
}

impl Memtable {
    /// A new, empty memtable with the default 64 MB flush threshold.
    pub fn new() -> Self {
        Self::with_threshold(DEFAULT_THRESHOLD)
    }

    /// A new, empty memtable with an explicit flush threshold (tests use a tiny
    /// value to trigger a flush without writing 64 MB).
    pub fn with_threshold(threshold: usize) -> Self {
        Memtable { map: SkipMap::new(), size: AtomicUsize::new(0), threshold }
    }

    /// Record `key -> value`. Newest write wins; overwriting is allowed and
    /// counts toward size again (bytes written, not resident).
    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.map.insert(key.to_vec(), Value::Put(value.to_vec()));
        self.size.fetch_add(key.len() + value.len() + OVERHEAD, Ordering::Relaxed);
    }

    /// Record a delete as a **tombstone** (an inserted marker, not a removal).
    /// A tombstone still occupies a slot, so it still counts toward size — value
    /// is 0 bytes, counted as `key + 1 + OVERHEAD`.
    pub fn delete(&self, key: &[u8]) {
        self.map.insert(key.to_vec(), Value::Delete);
        self.size.fetch_add(key.len() + 1 + OVERHEAD, Ordering::Relaxed);
    }

    /// Look up a key's live value. A tombstone (or an absent key) reads as
    /// not-found — the tombstone shadows any older value.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.map.get(key) {
            Some(entry) => match entry.value() {
                Value::Put(v) => Some(v.clone()),
                Value::Delete => None,
            },
            None => None,
        }
    }

    /// The raw marker for a key, tombstones included — for callers that need to
    /// *see* deletes (iteration/merge in later phases; the tombstone tests here).
    /// `None` means the key is absent from this memtable entirely.
    pub fn get_marker(&self, key: &[u8]) -> Option<Value> {
        self.map.get(key).map(|e| e.value().clone())
    }

    /// Iterate all entries (tombstones included) in **sorted key order** — the
    /// property the Phase 3 flush depends on.
    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, Value)> + '_ {
        self.map.iter().map(|e| (e.key().clone(), e.value().clone()))
    }

    /// Approximate bytes written into this memtable (monotonic).
    pub fn approx_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Whether this memtable has grown past its flush threshold. The freeze/flush
    /// itself is Phase 3; Phase 2 only exposes the predicate.
    pub fn should_flush(&self) -> bool {
        self.approx_size() >= self.threshold
    }

    /// Number of entries (live values *and* tombstones).
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the memtable holds no entries at all.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get() {
        let m = Memtable::new();
        m.put(b"k", b"v");
        assert_eq!(m.get(b"k"), Some(b"v".to_vec()));
    }

    #[test]
    fn get_absent_is_none() {
        let m = Memtable::new();
        assert_eq!(m.get(b"missing"), None);
    }

    #[test]
    fn deleted_key_reads_not_found() {
        let m = Memtable::new();
        m.put(b"k", b"v");
        m.delete(b"k");
        assert_eq!(m.get(b"k"), None);
    }

    #[test]
    fn tombstone_shadows_but_is_still_present() {
        // The test that distinguishes tombstone-model from removal-model, and the
        // one that matters for Phase 3: a deleted key is NOT gone from the map.
        let m = Memtable::new();
        m.put(b"k", b"v");
        m.delete(b"k");
        assert_eq!(m.get(b"k"), None); // reads not-found
        assert_eq!(m.get_marker(b"k"), Some(Value::Delete)); // but still present
        assert_eq!(m.len(), 1);
        let entries: Vec<_> = m.iter().collect();
        assert_eq!(entries, vec![(b"k".to_vec(), Value::Delete)]);
    }

    #[test]
    fn iteration_is_sorted() {
        let m = Memtable::new();
        m.put(b"c", b"3");
        m.put(b"a", b"1");
        m.put(b"b", b"2");
        let keys: Vec<Vec<u8>> = m.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn empty_value_is_distinct_from_deleted() {
        let m = Memtable::new();
        m.put(b"empty", b""); // present, empty value
        m.delete(b"gone"); // tombstone
        assert_eq!(m.get(b"empty"), Some(Vec::new()));
        assert_eq!(m.get_marker(b"empty"), Some(Value::Put(Vec::new())));
        assert_eq!(m.get(b"gone"), None);
        assert_eq!(m.get_marker(b"gone"), Some(Value::Delete));
    }

    #[test]
    fn overwrite_newest_wins() {
        let m = Memtable::new();
        m.put(b"k", b"v1");
        m.put(b"k", b"v2");
        assert_eq!(m.get(b"k"), Some(b"v2".to_vec()));
        assert_eq!(m.len(), 1); // one resident entry
    }

    #[test]
    fn delete_then_reput_restores_value() {
        let m = Memtable::new();
        m.delete(b"k");
        m.put(b"k", b"back");
        assert_eq!(m.get(b"k"), Some(b"back".to_vec()));
        assert_eq!(m.get_marker(b"k"), Some(Value::Put(b"back".to_vec())));
    }

    #[test]
    fn counter_counts_bytes_written_not_resident() {
        // Same key written 3 times: the map holds one entry, but the counter
        // reflects all three writes (monotonic, no dedup) + per-entry OVERHEAD.
        let m = Memtable::new();
        for _ in 0..3 {
            m.put(b"k", b"val"); // 1 + 3 + OVERHEAD each
        }
        assert_eq!(m.len(), 1);
        assert_eq!(m.approx_size(), 3 * (1 + 3 + OVERHEAD));
    }

    #[test]
    fn delete_counter_accounting() {
        let m = Memtable::new();
        m.delete(b"key"); // 3 + 1 + OVERHEAD
        assert_eq!(m.approx_size(), 3 + 1 + OVERHEAD);
    }

    #[test]
    fn should_flush_fires_past_threshold() {
        let m = Memtable::with_threshold(1024);
        assert!(!m.should_flush());
        let mut i = 0;
        while !m.should_flush() {
            m.put(format!("key{i:06}").as_bytes(), b"value");
            i += 1;
        }
        assert!(m.should_flush());
        assert!(m.approx_size() >= 1024);
    }

    #[test]
    fn concurrent_writers_no_lost_updates() {
        // The skip-list payoff: many threads write distinct keys through &self;
        // all survive and the counter equals the exact expected total.
        use std::sync::Arc;
        use std::thread;

        const THREADS: usize = 8;
        const PER: usize = 200;
        let m = Arc::new(Memtable::new());

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let m = Arc::clone(&m);
                thread::spawn(move || {
                    for i in 0..PER {
                        // Each key is a fixed 9 bytes: "t00-k0000".
                        let k = format!("t{t:02}-k{i:04}");
                        m.put(k.as_bytes(), b"v");
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(m.len(), THREADS * PER);
        for t in 0..THREADS {
            for i in 0..PER {
                let k = format!("t{t:02}-k{i:04}");
                assert_eq!(m.get(k.as_bytes()), Some(b"v".to_vec()));
            }
        }
        // 9-byte key + 1-byte value + OVERHEAD per distinct put.
        let expected = THREADS * PER * (9 + 1 + OVERHEAD);
        assert_eq!(m.approx_size(), expected);
    }
}
