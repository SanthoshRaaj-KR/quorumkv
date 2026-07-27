//! Compaction execution (Phase 5) — see `planning/phase-05-compaction.md` §4.
//!
//! Ties together the [`crate::merge`] engine, the Phase 3/4 SSTable writer, and
//! the [`crate::manifest`] version set into one atomic, crash-safe compaction:
//!
//! 1. A [`CompactionStrategy`] picks input files to merge.
//! 2. k-way merge their sorted entries, keeping the newest per key and dropping
//!    tombstones **only** when the merge reaches the bottom-most data (§2).
//! 3. Write the survivors to a new SSTable (writer + Bloom) via temp → rename.
//! 4. Commit **one** MANIFEST edit: delete the inputs, add the output. That
//!    single fsync'd append is the linearization point.
//! 5. Delete the input files (after the commit).
//!
//! Crash safety (§4b): outputs durable → MANIFEST edit fsync'd → inputs deleted.
//! A crash before the commit leaves an orphan temp/output (swept on startup) and
//! the inputs intact; a crash after leaves unreferenced inputs (also swept). The
//! MANIFEST always names a consistent set.

use std::fs::{self, File};
use std::path::Path;

use crate::faultsim::FileSink;
use crate::manifest::{FileMeta, VersionEdit, VersionSet};
use crate::merge::{Merge, Source};
use crate::sstable::{sst_filename, write_sstable_with_sink, SstReader};

// `write_sstable_with_sink(..., |f| Box::new(f))` is `write_sstable`'s own
// definition (sstable.rs) — reused directly here rather than re-importing
// the plain name, so `run_compaction`'s only difference from
// `run_compaction_with_sink` is which closure it passes.

/// A chosen unit of compaction work: which files to merge, where the output goes,
/// and whether tombstones may be dropped (true only when the bottom-most data is
/// included).
#[derive(Debug, Clone)]
pub struct Compaction {
    /// Input file numbers, **newest first** (so the merge keeps the newest value).
    pub inputs: Vec<u64>,
    /// Level the merged output is placed at.
    pub output_level: u32,
    /// Drop tombstones? Only safe when nothing older can exist beneath the merge.
    pub is_bottom_most: bool,
}

/// Picks which files to compact. The merge/commit engine underneath is identical
/// regardless of the picker (size-tiered, or the leveled target — planning/
/// phase-05-compaction.md §1). `dir` is passed alongside the live file set
/// because a picker's trigger heuristic may need to look at real on-disk file
/// sizes (`SizeTiered` does); nothing about `Compaction` or the merge engine
/// depends on it.
pub trait CompactionStrategy: Send + Sync {
    /// Choose a compaction given `dir` and the current live files, or `None`
    /// if idle.
    fn pick(&self, dir: &Path, files: &[FileMeta]) -> Option<Compaction>;
}

/// Size-tiered: bucket files by **similar on-disk size**, and once a bucket
/// reaches `min_run` files, merge that bucket (Cassandra's STCS). This is
/// deliberately not "merge every live file" (the phase-05 §8 A1 audit's own
/// "isn't really size-tiered" gap) — a real bucket is usually a proper
/// *subset* of the live set, which is what makes `is_bottom_most: false`
/// reachable at all: a tombstone only needs to be carried forward, rather
/// than dropped, exactly when the merge does *not* cover everything.
pub struct SizeTiered {
    /// Compact once a similar-size bucket reaches this many files.
    pub min_run: usize,
    /// How far a file's size may drift from a bucket's running average and
    /// still count as "similar" — `1.5` (Cassandra's own default region) means
    /// within [avg/1.5, avg*1.5].
    pub similarity_ratio: f64,
}

impl Default for SizeTiered {
    fn default() -> Self {
        SizeTiered { min_run: 4, similarity_ratio: 1.5 }
    }
}

impl CompactionStrategy for SizeTiered {
    fn pick(&self, dir: &Path, files: &[FileMeta]) -> Option<Compaction> {
        if files.len() < self.min_run {
            return None;
        }

        // Stat every file's real on-disk size — the "similar size" input a
        // real size-tiered picker buckets on. A stat failure (should not
        // happen for a live file) is treated as size 0 rather than aborting
        // the whole pick — the file still participates, just outside any
        // similarity comparison that would otherwise divide by its size.
        let mut sized: Vec<(&FileMeta, u64)> = files
            .iter()
            .map(|f| (f, fs::metadata(dir.join(sst_filename(f.number))).map(|m| m.len()).unwrap_or(0)))
            .collect();
        sized.sort_by_key(|(_, size)| *size);

        // Sliding window over sizes: grow a bucket while the next file's
        // size stays within `similarity_ratio` of the bucket's running
        // average; a bucket that reaches `min_run` is a candidate. Only the
        // first qualifying bucket is returned — good enough, not "optimal."
        let mut bucket: Vec<&FileMeta> = Vec::new();
        let mut bucket_total: u64 = 0;
        for (f, size) in &sized {
            let avg = if bucket.is_empty() { *size } else { bucket_total / bucket.len() as u64 };
            let similar = avg == 0
                || ((*size as f64) <= (avg as f64) * self.similarity_ratio
                    && (*size as f64) >= (avg as f64) / self.similarity_ratio);
            if !bucket.is_empty() && !similar {
                if bucket.len() >= self.min_run {
                    break; // already have a qualifying bucket
                }
                bucket.clear();
                bucket_total = 0;
            }
            bucket.push(f);
            bucket_total += size;
        }
        if bucket.len() < self.min_run {
            return None;
        }

        let mut inputs: Vec<u64> = bucket.iter().map(|f| f.number).collect();
        inputs.sort_unstable_by(|a, b| b.cmp(a)); // newest (highest number) first

        // Bottom-most (tombstones safe to drop) only when the bucket is
        // literally every live file: size-tiered has no per-level
        // non-overlap guarantee, so "nothing older exists elsewhere" can
        // only be proven that conservatively (phase-05 §2).
        let is_bottom_most = bucket.len() == files.len();
        Some(Compaction { inputs, output_level: 0, is_bottom_most })
    }
}

/// Leveled: L0 holds flush outputs (which may overlap each other); once
/// enough accumulate, drain all of L0 into L1 in one merge. Each level ≥ L1
/// holds **non-overlapping** files; once a level exceeds its file budget,
/// one of its files (plus whichever next-level file(s) its range overlaps)
/// gets promoted a level deeper. This is the phase's stated target for a
/// read-heavy workload (planning/phase-05-compaction.md §1): at most one
/// file per level (≥ L1) can hold any given key, instead of size-tiered's
/// "check every file."
pub struct Leveled {
    /// L0 → L1 triggers once this many L0 files have accumulated.
    pub l0_trigger: usize,
    /// Level n ≥ 1 → n+1 triggers once level n holds more than this many
    /// files. Real LevelDB/RocksDB trigger on level *byte size*; this crate's
    /// MANIFEST doesn't carry per-file size (phase-05 §8 A2 added only the
    /// key range), so file count is the trigger here — simpler, and the
    /// non-overlap invariant it proves is identical either way.
    pub level_file_trigger: usize,
}

impl Default for Leveled {
    fn default() -> Self {
        Leveled { l0_trigger: 4, level_file_trigger: 4 }
    }
}

/// Byte-string range overlap: `true` iff `[a_min,a_max]` and `[b_min,b_max]`
/// share at least one key.
fn ranges_overlap(a_min: &[u8], a_max: &[u8], b_min: &[u8], b_max: &[u8]) -> bool {
    a_min <= b_max && b_min <= a_max
}

/// The `(min_key, max_key)` spanning every file in `files` — panics on an
/// empty slice, since every caller here only ever passes a non-empty pick.
fn combined_range<'a>(files: impl Iterator<Item = &'a FileMeta>) -> (Vec<u8>, Vec<u8>) {
    let mut it = files;
    let first = it.next().expect("combined_range needs at least one file");
    let mut min = first.min_key.clone();
    let mut max = first.max_key.clone();
    for f in it {
        if f.min_key < min {
            min = f.min_key.clone();
        }
        if f.max_key > max {
            max = f.max_key.clone();
        }
    }
    (min, max)
}

impl Leveled {
    /// Build a `Compaction` merging `primary` (newer) into `output_level`,
    /// plus whichever existing `output_level` files `primary`'s combined
    /// range overlaps (older — level ≥ 1 is non-overlapping, so at most a
    /// handful can match).
    fn build(&self, files: &[FileMeta], mut primary: Vec<&FileMeta>, output_level: u32) -> Compaction {
        primary.sort_unstable_by(|a, b| b.number.cmp(&a.number)); // newest first among primary
        let (pmin, pmax) = combined_range(primary.iter().copied());

        let overlapping: Vec<&FileMeta> = files
            .iter()
            .filter(|f| f.level == output_level && ranges_overlap(&f.min_key, &f.max_key, &pmin, &pmax))
            .collect();

        let mut inputs: Vec<u64> = primary.iter().map(|f| f.number).collect();
        inputs.extend(overlapping.iter().map(|f| f.number)); // older, so after primary

        let (min, max) = combined_range(primary.iter().copied().chain(overlapping.iter().copied()));

        // Bottom-most: no file *outside this merge*, at any level deeper
        // than output_level, has a range overlapping the merge's combined
        // range — i.e. provably nothing older survives for these keys
        // (phase-05 §2), computed exactly rather than the conservative
        // "only if everything is included" rule size-tiered is stuck with.
        let merged_numbers: std::collections::HashSet<u64> = inputs.iter().copied().collect();
        let is_bottom_most = !files.iter().any(|f| {
            !merged_numbers.contains(&f.number)
                && f.level > output_level
                && ranges_overlap(&f.min_key, &f.max_key, &min, &max)
        });

        Compaction { inputs, output_level, is_bottom_most }
    }
}

impl CompactionStrategy for Leveled {
    fn pick(&self, _dir: &Path, files: &[FileMeta]) -> Option<Compaction> {
        // L0 first: its files may overlap each other (they're independent
        // flush outputs), so once enough accumulate, drain all of L0 into L1
        // in one merge rather than picking a subset.
        let l0: Vec<&FileMeta> = files.iter().filter(|f| f.level == 0).collect();
        if l0.len() >= self.l0_trigger {
            return Some(self.build(files, l0, 1));
        }

        // Otherwise, the first level ≥ 1 over its file budget promotes one
        // file a level deeper. "Oldest" is well-defined by min_key since a
        // level's files are non-overlapping — smallest min_key first.
        let max_level = files.iter().map(|f| f.level).max().unwrap_or(0);
        for n in 1..=max_level {
            let at_n: Vec<&FileMeta> = files.iter().filter(|f| f.level == n).collect();
            if at_n.len() > self.level_file_trigger {
                let victim = *at_n.iter().min_by(|a, b| a.min_key.cmp(&b.min_key)).unwrap();
                return Some(self.build(files, vec![victim], n + 1));
            }
        }
        None
    }
}

/// Execute one compaction: merge the inputs, write the output, commit the atomic
/// MANIFEST swap, and delete the input files.
pub fn run_compaction(
    dir: &Path,
    versions: &VersionSet,
    compaction: &Compaction,
    bits_per_key: u32,
) -> std::io::Result<()> {
    run_compaction_with_sink(dir, versions, compaction, bits_per_key, |f| Box::new(f))
}

/// Same as [`run_compaction`], but lets a test substitute the [`FileSink`]
/// behind the merged output's write — a [`crate::faultsim::FaultyFile`]
/// instead of the real one, for phase-13-fault-injection.md's "crash
/// mid-compaction" scenario. Production code always uses [`run_compaction`].
pub fn run_compaction_with_sink(
    dir: &Path,
    versions: &VersionSet,
    compaction: &Compaction,
    bits_per_key: u32,
    make_sink: impl FnOnce(File) -> Box<dyn FileSink>,
) -> std::io::Result<()> {
    log::info!(
        target: "compaction",
        "compacting {} file(s) -> level {} (drop_tombstones={})",
        compaction.inputs.len(),
        compaction.output_level,
        compaction.is_bottom_most,
    );

    // Open inputs (newest first) and turn each into a sorted entry source.
    let mut sources: Vec<Source> = Vec::with_capacity(compaction.inputs.len());
    for &num in &compaction.inputs {
        let reader = SstReader::open(dir.join(sst_filename(num)))?;
        sources.push(Box::new(reader.entries()?.into_iter()));
    }

    // Merge → the surviving entries.
    let merged: Vec<_> = Merge::new(sources, compaction.is_bottom_most).collect();
    let survivor_count = merged.len();

    // Write the output (skipped entirely if the merge produced nothing).
    let out_number = versions.next_file_number();
    let output = write_sstable_with_sink(dir, out_number, merged, survivor_count, bits_per_key, make_sink)?;

    // One atomic edit: remove inputs, add the output (if any).
    let mut edit = VersionEdit { added: Vec::new(), deleted: compaction.inputs.clone() };
    if let Some((_, min_key, max_key)) = output {
        edit.added.push(FileMeta { number: out_number, level: compaction.output_level, min_key, max_key });
    }
    versions.commit(&edit)?;

    // Inputs are now unreferenced; delete their files. Any open reader on an old
    // Version keeps working (share-delete / unlink-with-open-handle).
    for &num in &compaction.inputs {
        let _ = fs::remove_file(dir.join(sst_filename(num)));
    }

    log::info!(
        target: "compaction",
        "compaction done: {} -> {}",
        compaction.inputs.len(),
        if edit.added.is_empty() { "0 files (all dropped)" } else { "1 file" },
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Value;
    use crate::sstable::write_sstable;
    use crate::testutil::TempDir;
    use crate::manifest::VersionEdit as VE;

    const BPK: u32 = 10;

    fn put(v: &[u8]) -> Value {
        Value::Put(v.to_vec())
    }

    /// Write an SSTable via the allocator + commit it into the version set.
    fn add_sst(dir: &TempDir, vs: &VersionSet, entries: Vec<(Vec<u8>, Value)>) -> u64 {
        let num = vs.next_file_number();
        let n = entries.len();
        let (_, min_key, max_key) = write_sstable(&dir.0, num, entries, n, BPK).unwrap().unwrap();
        vs.commit(&VE::add(num, 0, min_key, max_key)).unwrap();
        num
    }

    fn dir_sst_bytes(dir: &TempDir) -> u64 {
        crate::sstable::list_sstables(&dir.0)
            .unwrap()
            .iter()
            .map(|(_, p)| std::fs::metadata(p).unwrap().len())
            .sum()
    }

    /// A `FileMeta` with a synthetic single-point range, for tests that only
    /// care about which files got picked, not real overlap behavior.
    fn meta(number: u64, level: u32) -> FileMeta {
        let k = format!("k{number:04}").into_bytes();
        FileMeta { number, level, min_key: k.clone(), max_key: k }
    }

    #[test]
    fn size_tiered_picks_when_enough_files() {
        let dir = TempDir::new(); // no backing files: every stat -> size 0 -> "similar"
        let s = SizeTiered { min_run: 3, ..Default::default() };
        let few = vec![meta(1, 0), meta(2, 0)];
        assert!(s.pick(&dir.0, &few).is_none());
        let many: Vec<_> = (1..=3).map(|n| meta(n, 0)).collect();
        let c = s.pick(&dir.0, &many).unwrap();
        assert_eq!(c.inputs, vec![3, 2, 1]); // newest first
        assert!(c.is_bottom_most);
    }

    #[test]
    fn size_tiered_excludes_a_dissimilar_outlier() {
        let dir = TempDir::new();
        let vs = VersionSet::open(&dir.0).unwrap();
        // Three small, similarly-sized files...
        let small: Vec<u64> = (0..3)
            .map(|i| add_sst(&dir, &vs, vec![(format!("k{i}").into_bytes(), put(b"v"))]))
            .collect();
        // ...and one file an order of magnitude bigger.
        let big_entries: Vec<_> =
            (0..200u32).map(|i| (format!("big{i:04}").into_bytes(), put(&vec![b'x'; 200]))).collect();
        add_sst(&dir, &vs, big_entries);

        let s = SizeTiered { min_run: 3, ..Default::default() };
        let c = s.pick(&dir.0, &vs.current().files).unwrap();

        let mut picked = c.inputs.clone();
        picked.sort_unstable();
        assert_eq!(picked, small, "the outlier must not join the small files' bucket");
        assert!(!c.is_bottom_most, "the big file is still live and unmerged, so this isn't bottom-most");
    }

    #[test]
    fn compaction_collapses_redundancy_and_keeps_latest() {
        let dir = TempDir::new();
        let vs = VersionSet::open(&dir.0).unwrap();
        // Three generations of the same 10 keys — newest last.
        for gen in 0..3u32 {
            let entries: Vec<_> = (0..10u32)
                .map(|k| (format!("k{k}").into_bytes(), put(format!("v{k}-gen{gen}").as_bytes())))
                .collect();
            add_sst(&dir, &vs, entries);
        }
        let before = dir_sst_bytes(&dir);

        let strat = SizeTiered { min_run: 3, ..Default::default() };
        let compaction = strat.pick(&dir.0, &vs.current().files).unwrap();
        run_compaction(&dir.0, &vs, &compaction, BPK).unwrap();

        // One file left; disk shrank; every key holds its newest (gen2) value.
        assert_eq!(vs.current().files.len(), 1);
        let after = dir_sst_bytes(&dir);
        assert!(after < before, "compaction should shrink disk: {after} !< {before}");

        let cur = vs.current();
        let out = &cur.files[0];
        let reader = SstReader::open(dir.0.join(sst_filename(out.number))).unwrap();
        for k in 0..10u32 {
            assert_eq!(
                reader.get(format!("k{k}").as_bytes()).unwrap(),
                Some(put(format!("v{k}-gen2").as_bytes())),
            );
        }
    }

    #[test]
    fn tombstone_dropped_at_bottom_most() {
        let dir = TempDir::new();
        let vs = VersionSet::open(&dir.0).unwrap();
        add_sst(&dir, &vs, vec![(b"k".to_vec(), put(b"v"))]); // older: value
        add_sst(&dir, &vs, vec![(b"k".to_vec(), Value::Delete)]); // newer: tombstone

        let compaction = Compaction { inputs: vec![2, 1], output_level: 0, is_bottom_most: true };
        run_compaction(&dir.0, &vs, &compaction, BPK).unwrap();

        // Bottom-most: both the tombstone and the value are gone → empty output,
        // no file added, just the inputs removed.
        assert!(vs.current().files.is_empty(), "empty merge writes no file");
        assert!(!dir.0.join(sst_filename(1)).exists());
        assert!(!dir.0.join(sst_filename(2)).exists());
    }

    #[test]
    fn tombstone_carried_when_not_bottom_most() {
        let dir = TempDir::new();
        let vs = VersionSet::open(&dir.0).unwrap();
        add_sst(&dir, &vs, vec![(b"k".to_vec(), put(b"v"))]); // file 1 (older, value)
        add_sst(&dir, &vs, vec![(b"k".to_vec(), Value::Delete)]); // file 2 (tombstone)
        add_sst(&dir, &vs, vec![(b"other".to_vec(), put(b"x"))]); // file 3 (unrelated)

        // Compact only files {2,1} (NOT the whole set) → not bottom-most.
        let compaction = Compaction { inputs: vec![2, 1], output_level: 0, is_bottom_most: false };
        run_compaction(&dir.0, &vs, &compaction, BPK).unwrap();

        // The tombstone must be CARRIED into the output, not dropped.
        let cur = vs.current();
        let out = cur.files.iter().find(|f| f.number == 4).unwrap();
        let reader = SstReader::open(dir.0.join(sst_filename(out.number))).unwrap();
        assert_eq!(reader.get(b"k").unwrap(), Some(Value::Delete), "tombstone must be carried forward");
    }

    // ── Leveled ──────────────────────────────────────────────────────────────

    #[test]
    fn leveled_drains_l0_into_l1_once_triggered() {
        let dir = TempDir::new();
        let strat = Leveled { l0_trigger: 4, level_file_trigger: 4 };
        let few = vec![meta(1, 0), meta(2, 0), meta(3, 0)];
        assert!(strat.pick(&dir.0, &few).is_none(), "below the L0 trigger");

        let files = vec![meta(1, 0), meta(2, 0), meta(3, 0), meta(4, 0)];
        let c = strat.pick(&dir.0, &files).unwrap();
        let mut inputs = c.inputs.clone();
        inputs.sort_unstable();
        assert_eq!(inputs, vec![1, 2, 3, 4]);
        assert_eq!(c.output_level, 1);
        assert!(c.is_bottom_most, "L1 is empty, so nothing older can exist");
    }

    #[test]
    fn leveled_promotes_an_overloaded_level_taking_only_overlapping_neighbors() {
        let dir = TempDir::new();
        let strat = Leveled { l0_trigger: 100, level_file_trigger: 2 };

        // L1: 3 non-overlapping files (over the budget of 2), plus one
        // unrelated L2 file that overlaps only the first of them.
        let l1_a = FileMeta { number: 10, level: 1, min_key: b"a".to_vec(), max_key: b"c".to_vec() };
        let l1_b = FileMeta { number: 11, level: 1, min_key: b"d".to_vec(), max_key: b"f".to_vec() };
        let l1_c = FileMeta { number: 12, level: 1, min_key: b"g".to_vec(), max_key: b"i".to_vec() };
        let l2_overlap = FileMeta { number: 20, level: 2, min_key: b"b".to_vec(), max_key: b"b".to_vec() };
        let l2_unrelated = FileMeta { number: 21, level: 2, min_key: b"z".to_vec(), max_key: b"z".to_vec() };
        let files = vec![l1_a.clone(), l1_b, l1_c, l2_overlap.clone(), l2_unrelated];

        let c = strat.pick(&dir.0, &files).unwrap();
        // l1_a has the smallest min_key ("a"), so it's the victim; it only
        // overlaps l2_overlap, not l2_unrelated.
        assert_eq!(c.inputs, vec![10, 20]);
        assert_eq!(c.output_level, 2);
        // l2_unrelated is still live, outside this merge, but at the SAME
        // level as the output (not deeper) — it can't hold older data for
        // this key range, so this is still bottom-most.
        assert!(c.is_bottom_most);
    }

    #[test]
    fn leveled_is_not_bottom_most_when_a_deeper_overlapping_file_survives() {
        let dir = TempDir::new();
        let strat = Leveled { l0_trigger: 100, level_file_trigger: 1 };
        let l1_a = FileMeta { number: 10, level: 1, min_key: b"a".to_vec(), max_key: b"c".to_vec() };
        let l1_b = FileMeta { number: 11, level: 1, min_key: b"d".to_vec(), max_key: b"f".to_vec() };
        // A deeper file whose range overlaps l1_a's — genuinely older data
        // this merge (into L2) would NOT reach, since l1_a's own promotion
        // target (L2) only picks L2 overlaps, and this file lives at L3.
        let l3_deeper = FileMeta { number: 30, level: 3, min_key: b"b".to_vec(), max_key: b"b".to_vec() };
        let files = vec![l1_a, l1_b, l3_deeper];

        let c = strat.pick(&dir.0, &files).unwrap();
        assert_eq!(c.output_level, 2);
        assert!(!c.is_bottom_most, "an L3 file still overlaps this merge's range");
    }

    /// End-to-end through the real picker (not a hand-built `Compaction`):
    /// this is phase-05 §8's flagged gap — before leveled compaction existed,
    /// `is_bottom_most: false` was unreachable from any real strategy, so the
    /// tombstone-carry-forward path only ran against synthetic structs.
    #[test]
    fn leveled_carries_tombstone_forward_when_a_deeper_file_still_overlaps() {
        let dir = TempDir::new();
        let vs = VersionSet::open(&dir.0).unwrap();

        // An "older" value already promoted to L2 — deeper than where this
        // L0->L1 drain will output, so the picker must leave it behind
        // rather than sweep it into the merge (only existing *output_level*
        // files get absorbed; L2 is one level past that).
        let older = add_sst(&dir, &vs, vec![(b"k".to_vec(), put(b"v"))]);
        let cur = vs.current();
        let older_meta = cur.files.iter().find(|f| f.number == older).unwrap().clone();
        vs.commit(&VersionEdit {
            added: vec![FileMeta { level: 2, ..older_meta.clone() }],
            deleted: vec![older],
        })
        .unwrap();

        // ...and a tombstone for the same key still sitting in L0.
        add_sst(&dir, &vs, vec![(b"k".to_vec(), Value::Delete)]);

        let strat = Leveled { l0_trigger: 1, level_file_trigger: 100 };
        let compaction = strat.pick(&dir.0, &vs.current().files).unwrap();
        assert_eq!(compaction.output_level, 1);
        assert!(!compaction.is_bottom_most, "the L2 value is still live and overlapping");

        run_compaction(&dir.0, &vs, &compaction, BPK).unwrap();

        // The tombstone must survive the merge (carried forward), not be
        // dropped — dropping it here would resurrect the L1 value.
        let cur = vs.current();
        let out = cur.files.iter().find(|f| f.level == compaction.output_level).unwrap();
        let reader = SstReader::open(dir.0.join(sst_filename(out.number))).unwrap();
        assert_eq!(reader.get(b"k").unwrap(), Some(Value::Delete));
    }
}
