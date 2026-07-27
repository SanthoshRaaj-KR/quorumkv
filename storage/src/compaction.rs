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
/// regardless of the picker (size-tiered now, leveled later).
pub trait CompactionStrategy: Send + Sync {
    /// Choose a compaction given the current live files, or `None` if idle.
    fn pick(&self, files: &[FileMeta]) -> Option<Compaction>;
}

/// Size-tiered: when enough SSTables have accumulated, merge them into one.
///
/// (Flush outputs are all ~the memtable-threshold size, so "enough files" is a
/// reasonable size-tier trigger. Bucketing by exact size is a refinement; the
/// merge/commit machinery it drives is unchanged.)
pub struct SizeTiered {
    /// Compact once at least this many files exist.
    pub min_run: usize,
}

impl Default for SizeTiered {
    fn default() -> Self {
        SizeTiered { min_run: 4 }
    }
}

impl CompactionStrategy for SizeTiered {
    fn pick(&self, files: &[FileMeta]) -> Option<Compaction> {
        if files.len() < self.min_run {
            return None;
        }
        // Merge everything into one file. Because all live data is included, this
        // reaches the bottom-most data — tombstones are safe to drop.
        let mut inputs: Vec<u64> = files.iter().map(|f| f.number).collect();
        inputs.sort_unstable_by(|a, b| b.cmp(a)); // newest (highest number) first
        Some(Compaction { inputs, output_level: 0, is_bottom_most: true })
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
        let s = SizeTiered { min_run: 3 };
        let few = vec![meta(1, 0), meta(2, 0)];
        assert!(s.pick(&few).is_none());
        let many: Vec<_> = (1..=3).map(|n| meta(n, 0)).collect();
        let c = s.pick(&many).unwrap();
        assert_eq!(c.inputs, vec![3, 2, 1]); // newest first
        assert!(c.is_bottom_most);
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

        let strat = SizeTiered { min_run: 3 };
        let compaction = strat.pick(&vs.current().files).unwrap();
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
}
