//! Phase 13 scenario 1 — crash mid-WAL-append, live and reproducible
//! (planning/phase-13-fault-injection.md §2.1). `db_recovery.rs`'s
//! `corrupt_tail_record_is_dropped_on_reopen` already proves replay handles
//! a torn tail *offline* (hand-flip a byte after the fact); this is the
//! same property proven *live*, at a call chosen by a seed instead of
//! wherever a human happened to reach for a hex editor.

mod common;

use common::TempDir;
use storage::faultsim::{CallKind, FaultKind, FaultSchedule, FaultyFile};
use storage::wal::{replay, Record, WalWriter};

fn put(i: u32) -> Record {
    Record::Put { key: format!("k{i:03}").into_bytes(), value: format!("v{i}").into_bytes() }
}

/// Writes `count` records through a `FaultyFile`-backed `WalWriter`, the
/// `target`-th `write_all` call torn to a seed-derived partial length.
/// Returns the records that must survive: everything strictly before the
/// torn call (record index `target - 1`, 1-based call ordinal). The torn
/// record itself, and everything the writer *thinks* it wrote afterward,
/// never actually reach disk (`FaultyFile` discards every call once
/// tripped) — the same "nothing after a real crash lands" property.
fn run_torn_append(seed: u64, dir: &TempDir, count: u32, target: u64) -> Vec<Record> {
    let path = dir.path("wal-segment");
    let schedule = FaultSchedule::new(seed, CallKind::Write, target, FaultKind::TornWrite);
    let mut w =
        WalWriter::open_with_sink(&path, |f| Box::new(FaultyFile::wrap(f, schedule))).unwrap();

    let mut survivors = Vec::new();
    for i in 0..count {
        let rec = put(i);
        let _ = w.append(&rec); // return value ignored: §1b, recovery is self-certifying via CRC
        if (i as u64) < target - 1 {
            survivors.push(rec);
        }
    }
    drop(w);
    survivors
}

#[test]
fn torn_append_drops_exactly_the_faulted_record_and_everything_after() {
    const COUNT: u32 = 30;
    for seed in 0..20u64 {
        let dir = TempDir::new();
        let target = 1 + (seed % u64::from(COUNT)); // which write_all call is torn
        let want = run_torn_append(seed, &dir, COUNT, target);

        let got = replay(dir.path("wal-segment")).unwrap();
        assert_eq!(
            got, want,
            "seed {seed} target {target}: replay after a torn write didn't match the expected prefix"
        );
    }
}

/// The determinism claim itself, tested directly (phase-13 §3.2): the same
/// seed against the same workload produces the identical torn length and
/// therefore the identical surviving record set, both times.
#[test]
fn a_fixed_seed_reproduces_the_identical_fault() {
    const COUNT: u32 = 30;
    const SEED: u64 = 12345;
    const TARGET: u64 = 17;

    let dir_a = TempDir::new();
    let want_a = run_torn_append(SEED, &dir_a, COUNT, TARGET);
    let got_a = replay(dir_a.path("wal-segment")).unwrap();

    let dir_b = TempDir::new();
    let want_b = run_torn_append(SEED, &dir_b, COUNT, TARGET);
    let got_b = replay(dir_b.path("wal-segment")).unwrap();

    assert_eq!(want_a, want_b, "same seed produced a different expected survivor set");
    assert_eq!(got_a, got_b, "same seed produced a different replayed result");
    assert_eq!(got_a, want_a);
}

/// A schedule that never fires (target beyond the call count this test
/// makes) leaves the file exactly as an unfaulted `WalWriter` would —
/// the passthrough must be inert when it never triggers, not just when
/// [`storage::faultsim::FileSink`] is the plain `File` impl.
#[test]
fn a_schedule_that_never_fires_changes_nothing() {
    const COUNT: u32 = 10;
    let dir = TempDir::new();
    // target is comfortably beyond COUNT, so no write_all call ever matches it.
    let want = run_torn_append(1, &dir, COUNT, COUNT as u64 + 1000);
    assert_eq!(want.len() as u32, COUNT, "no fault should have fired");

    let got = replay(dir.path("wal-segment")).unwrap();
    assert_eq!(got, want);
}
