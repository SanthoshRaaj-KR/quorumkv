//! Phase 13 scenario 3 — crash mid-compaction, live and reproducible
//! (planning/phase-13-fault-injection.md §2.3). `compaction_safety.rs`'s
//! `orphan_compaction_output_is_swept_on_reopen` already proves a stray
//! output file is swept on reopen using a *hand-planted* orphan; this
//! proves the property `DESIGN.md` §8 actually asks for — a real induced
//! failure never corrupts or removes a compaction's **inputs** before its
//! output is durably committed — using a real fault instead of a disk
//! genuinely filling up.

mod common;

use common::TempDir;
use storage::compaction::{run_compaction_with_sink, Compaction};
use storage::faultsim::{CallKind, FaultKind, FaultSchedule, FaultyFile};
use storage::manifest::{VersionEdit, VersionSet};
use storage::memtable::Value;
use storage::sstable::{sst_filename, write_sstable, SstReader};

const BPK: u32 = 10;

fn add_sst(dir: &TempDir, vs: &VersionSet, entries: Vec<(Vec<u8>, Value)>) -> u64 {
    let num = vs.next_file_number();
    let n = entries.len();
    write_sstable(&dir.0, num, entries, n, BPK).unwrap().unwrap();
    vs.commit(&VersionEdit::add(num, 0)).unwrap();
    num
}

#[test]
fn failed_output_write_leaves_every_input_untouched_and_readable() {
    for seed in 0..20u64 {
        let dir = TempDir::new();
        let vs = VersionSet::open(&dir.0).unwrap();

        // Three input SSTables, three generations of the same 10 keys —
        // exactly compaction_safety.rs's own shape, so the "final" value is
        // gen 2's, but that's incidental here: what matters is these three
        // files' bytes must survive completely unchanged.
        let mut inputs = Vec::new();
        for gen in 0..3u32 {
            let entries: Vec<_> = (0..10u32)
                .map(|k| {
                    (format!("k{k}").into_bytes(), Value::Put(format!("v{k}-gen{gen}").into_bytes()))
                })
                .collect();
            inputs.push(add_sst(&dir, &vs, entries));
        }
        let before: Vec<Vec<u8>> =
            inputs.iter().map(|&n| std::fs::read(dir.0.join(sst_filename(n))).unwrap()).collect();

        // Fault the merged output's one fsync call — same mechanism as
        // scenario 2, now reached through the real compaction entry point.
        let schedule = FaultSchedule::new(seed, CallKind::Sync, 1, FaultKind::Fail);
        let compaction = Compaction { inputs: inputs.clone(), output_level: 0, is_bottom_most: true };
        let result = run_compaction_with_sink(&dir.0, &vs, &compaction, BPK, |f| {
            Box::new(FaultyFile::wrap(f, schedule))
        });

        assert!(result.is_err(), "seed {seed}: a failed compaction output write must propagate");

        // Every input: same bytes on disk, still a valid, readable SSTable —
        // compaction must never touch an input before its output commits.
        for (i, &num) in inputs.iter().enumerate() {
            let path = dir.0.join(sst_filename(num));
            assert!(path.exists(), "seed {seed}: input {num} was deleted by a failed compaction");
            let after = std::fs::read(&path).unwrap();
            assert_eq!(after, before[i], "seed {seed}: input {num}'s bytes changed");

            let reader = SstReader::open(&path).unwrap();
            for k in 0..10u32 {
                assert_eq!(
                    reader.get(format!("k{k}").as_bytes()).unwrap(),
                    Some(Value::Put(format!("v{k}-gen{i}").into_bytes())),
                    "seed {seed}: input {num} unreadable or wrong after a failed compaction",
                );
            }
        }

        // The MANIFEST was never touched either: the version set still
        // names exactly the three original inputs, not some half-applied
        // edit (added output without deleting inputs, or vice versa).
        let cur = vs.current();
        let mut live: Vec<u64> = cur.files.iter().map(|f| f.number).collect();
        live.sort_unstable();
        let mut want = inputs.clone();
        want.sort_unstable();
        assert_eq!(live, want, "seed {seed}: version set diverged from a failed compaction");
    }
}
