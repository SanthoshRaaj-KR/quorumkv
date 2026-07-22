//! k-way merge for compaction (Phase 5) — see `planning/phase-05-compaction.md`.
//!
//! Compaction takes several SSTables (each already sorted) and produces one
//! merged, sorted, de-duplicated stream in a single pass. Two rules (§2, §4a):
//!
//! - **Newest version of a key wins.** Sources are ordered *newest first*; for a
//!   key present in several, the lowest-index source's value is kept and the
//!   older duplicates are discarded. (An overwritten value is always safe to drop.)
//! - **Tombstones drop only at the bottom.** A `Value::Delete` shadows older
//!   values in older files. It may be physically dropped **only** when the merge
//!   includes the bottom-most data (`drop_tombstones = true`); otherwise it is
//!   *carried forward* into the output, still doing its shadowing job. Dropping a
//!   tombstone while an older live value survives beneath would resurrect it.
//!
//! The engine is a streaming [`Iterator`], so the compaction writer can consume
//! it entry-by-entry without materializing the whole merge in RAM.

use crate::memtable::Value;

/// One merged entry.
pub type Entry = (Vec<u8>, Value);

/// A boxed sorted source of entries (an SSTable's entries, a memtable's, …).
pub type Source = Box<dyn Iterator<Item = Entry>>;

/// Streaming k-way merge over sorted sources ordered **newest first**.
pub struct Merge {
    sources: Vec<std::iter::Peekable<Source>>,
    drop_tombstones: bool,
}

impl Merge {
    /// Build a merge over `sources` (index 0 = newest). Each source must yield
    /// entries in strictly ascending key order. `drop_tombstones` should be true
    /// only when the merge includes the bottom-most data (§2).
    pub fn new(sources: Vec<Source>, drop_tombstones: bool) -> Self {
        Merge {
            sources: sources.into_iter().map(Iterator::peekable).collect(),
            drop_tombstones,
        }
    }
}

impl Iterator for Merge {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        loop {
            // Smallest key currently at the head of any source.
            let mut min_key: Option<Vec<u8>> = None;
            for src in &mut self.sources {
                if let Some((k, _)) = src.peek() {
                    if min_key.as_ref().is_none_or(|mk| k < mk) {
                        min_key = Some(k.clone());
                    }
                }
            }
            let key = min_key?; // every source exhausted → done

            // The newest source (lowest index) holding this key wins; advance
            // every source whose head is this key, discarding older duplicates.
            let mut winner: Option<Value> = None;
            for src in &mut self.sources {
                if src.peek().is_some_and(|(k, _)| *k == key) {
                    let (_, v) = src.next().unwrap();
                    if winner.is_none() {
                        winner = Some(v);
                    }
                }
            }
            let value = winner.expect("the min key came from some source");

            match value {
                // A dead tombstone at the bottom level: physically drop it.
                Value::Delete if self.drop_tombstones => continue,
                v => return Some((key, v)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src(entries: Vec<(&[u8], Value)>) -> Source {
        Box::new(entries.into_iter().map(|(k, v)| (k.to_vec(), v)).collect::<Vec<_>>().into_iter())
    }
    fn put(v: &[u8]) -> Value {
        Value::Put(v.to_vec())
    }
    fn collect(sources: Vec<Source>, drop: bool) -> Vec<Entry> {
        Merge::new(sources, drop).collect()
    }

    #[test]
    fn overwrite_keeps_newest() {
        // newest first: source 0 has the new value.
        let out = collect(
            vec![src(vec![(b"k", put(b"new"))]), src(vec![(b"k", put(b"old"))])],
            true,
        );
        assert_eq!(out, vec![(b"k".to_vec(), put(b"new"))]);
    }

    #[test]
    fn output_is_sorted_across_sources() {
        let out = collect(
            vec![
                src(vec![(b"b", put(b"2")), (b"d", put(b"4"))]),
                src(vec![(b"a", put(b"1")), (b"c", put(b"3"))]),
            ],
            true,
        );
        let keys: Vec<_> = out.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);
    }

    #[test]
    fn tombstone_is_dropped_at_bottom() {
        // drop=true (bottom-most): the tombstone AND the older value both vanish.
        let out = collect(
            vec![src(vec![(b"k", Value::Delete)]), src(vec![(b"k", put(b"old"))])],
            true,
        );
        assert!(out.is_empty(), "at the bottom level a tombstone should be dropped");
    }

    #[test]
    fn tombstone_is_carried_when_not_bottom() {
        // drop=false: the tombstone survives into the output (still shadowing an
        // older value that may exist beneath this merge). The older value merged
        // here is discarded, but the tombstone remains.
        let out = collect(
            vec![src(vec![(b"k", Value::Delete)]), src(vec![(b"k", put(b"old"))])],
            false,
        );
        assert_eq!(out, vec![(b"k".to_vec(), Value::Delete)]);
    }

    #[test]
    fn delete_then_reput_resolves_to_the_put() {
        // Newest source has the re-put; it wins over the older tombstone.
        let out = collect(
            vec![src(vec![(b"k", put(b"back"))]), src(vec![(b"k", Value::Delete)])],
            true,
        );
        assert_eq!(out, vec![(b"k".to_vec(), put(b"back"))]);
    }

    #[test]
    fn three_way_merge_with_mixed_keys() {
        let out = collect(
            vec![
                src(vec![(b"a", put(b"a2")), (b"m", put(b"m1"))]), // newest
                src(vec![(b"a", put(b"a1")), (b"z", Value::Delete)]),
                src(vec![(b"b", put(b"b0")), (b"z", put(b"z0"))]), // oldest
            ],
            true, // bottom-most
        );
        assert_eq!(
            out,
            vec![
                (b"a".to_vec(), put(b"a2")), // newest a wins
                (b"b".to_vec(), put(b"b0")),
                (b"m".to_vec(), put(b"m1")),
                // z: tombstone (newer) shadows z0, dropped at bottom → gone
            ]
        );
    }

    #[test]
    fn empty_sources_merge_to_nothing() {
        assert!(collect(vec![], true).is_empty());
        assert!(collect(vec![src(vec![])], true).is_empty());
    }
}
