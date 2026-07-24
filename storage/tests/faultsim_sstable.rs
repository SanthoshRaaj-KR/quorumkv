//! Phase 13 scenario 2 — crash between an SSTable's temp-write and its
//! rename, live and reproducible (planning/phase-13-fault-injection.md
//! §2.2). `flush.rs`'s `crash_before_rename_recovers_from_wal_and_cleans_tmp`
//! already proves this window is safe using a *hand-authored* orphan
//! `.tmp` (garbage bytes written directly); this is the same crash window
//! reached by a real, seed-chosen fault instead.
//!
//! The fault targets `SstWriter::finish`'s `sync_all` call: failing it means
//! `finish`'s own `?` propagation stops **before** the `rename` that follows
//! (`storage::faultsim::FileSink` only wraps the write+sync pair, never the
//! rename — it doesn't need to, because Rust's own `?` already skips
//! whatever comes next once one step fails).

mod common;

use common::TempDir;
use storage::faultsim::{CallKind, FaultKind, FaultSchedule, FaultyFile};
use storage::memtable::Value;
use storage::sstable::{
    list_sstables, remove_orphan_tmp, sst_filename, write_sstable, write_sstable_with_sink,
    SstReader,
};

const BPK: u32 = 10;

fn entries(prefix: &str, n: u32) -> Vec<(Vec<u8>, Value)> {
    (0..n)
        .map(|i| (format!("{prefix}{i:03}").into_bytes(), Value::Put(format!("v{i}").into_bytes())))
        .collect()
}

#[test]
fn failed_sync_aborts_before_rename_leaves_prior_state_untouched_and_sweeps_clean() {
    for seed in 0..20u64 {
        let dir = TempDir::new();

        // A pre-existing, already-committed SSTable — the "prior state" the
        // scenario cares about not corrupting.
        let prior_path =
            write_sstable(&dir.0, 1, entries("prior", 10), 10, BPK).unwrap().unwrap();

        // The faulted attempt: SstWriter::finish makes exactly one sync_all
        // call, so target=1 always lands on it.
        let schedule = FaultSchedule::new(seed, CallKind::Sync, 1, FaultKind::Fail);
        let result = write_sstable_with_sink(&dir.0, 2, entries("new", 10), 10, BPK, |f| {
            Box::new(FaultyFile::wrap(f, schedule))
        });

        assert!(result.is_err(), "seed {seed}: a failed fsync must propagate, not be swallowed");

        // The rename never ran: no live .sst for file 2.
        assert!(
            !dir.0.join(sst_filename(2)).exists(),
            "seed {seed}: an aborted write must never produce a visible .sst"
        );

        // The prior, already-committed SSTable is untouched and still reads
        // correctly — compaction/flush writes to a *new* file; a failure
        // here has no way to reach back and corrupt it.
        let reader = SstReader::open(&prior_path).unwrap();
        for i in 0..10u32 {
            assert_eq!(
                reader.get(format!("prior{i:03}").as_bytes()).unwrap(),
                Some(Value::Put(format!("v{i}").into_bytes())),
                "seed {seed}: prior SSTable corrupted by an unrelated failed write",
            );
        }

        // The orphaned .tmp (if the write got far enough to create one) is
        // swept cleanly, and only the prior file remains live.
        remove_orphan_tmp(&dir.0).unwrap();
        let live = list_sstables(&dir.0).unwrap();
        assert_eq!(live.len(), 1, "seed {seed}: only the untouched prior SSTable should remain");
        assert_eq!(live[0].0, 1);
        for entry in std::fs::read_dir(&dir.0).unwrap() {
            let name = entry.unwrap().file_name().into_string().unwrap();
            assert!(!name.ends_with(".tmp"), "seed {seed}: orphan {name} was not swept");
        }
    }
}
