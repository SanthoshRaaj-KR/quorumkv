//! Deterministic, seed-reproducible storage-level fault injection
//! (planning/phase-13-fault-injection.md).
//!
//! Wraps exactly the write+durability call pair every durable writer in
//! this crate already funnels through — [`crate::wal::WalWriter::append`]
//! and [`crate::sstable::SstWriter`]'s block/index/footer writes plus its
//! `finish`'s fsync — behind a small [`FileSink`] seam (§1a: "wrap two
//! calls, at the write site," not a virtual filesystem). Production code
//! gets the real [`File`] (the passthrough `impl` below is a no-op, zero
//! behavior change); tests substitute a [`FaultyFile`] that can simulate a
//! torn write or a failed fsync at one seed-chosen call.
//!
//! No new crate dependency: this file hand-rolls a tiny SplitMix64 PRNG
//! rather than pulling in `rand`. `Cargo.toml`'s own comment documents why
//! there are no dev-dependencies at all — the windows-gnu toolchain on this
//! machine can't link crates that pull in `windows-sys` (as `rand`'s
//! `getrandom` backend does on Windows), the same class of problem that
//! ruled out gRPC/FFI in Phase 10. A dozen lines of arithmetic sidesteps it
//! entirely and keeps every byte of "how a seed becomes a fault" auditable,
//! matching this project's existing hand-rolled-format ethos.

use std::fs::File;
use std::io::{self, Write};

/// The write+durability seam. [`crate::wal::WalWriter`] and
/// [`crate::sstable::SstWriter`] hold a `Box<dyn FileSink>` instead of a
/// concrete [`File`] so a test can substitute [`FaultyFile`] without either
/// type knowing it happened.
pub trait FileSink: Send {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()>;
    fn sync_all(&mut self) -> io::Result<()>;
}

/// The production path: a real file, unmodified behavior.
impl FileSink for File {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        Write::write_all(self, buf)
    }
    fn sync_all(&mut self) -> io::Result<()> {
        File::sync_all(self)
    }
}

/// Which call kind a [`FaultSchedule`] is watching. `write_all` and
/// `sync_all` are counted independently — a scenario cares about one
/// specifically (a torn *write*, or a failed *sync*), not "the Nth call of
/// either."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallKind {
    Write,
    Sync,
}

/// What the targeted call does instead of succeeding normally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    /// Write only a prefix of the buffer, then return `Ok` — a torn write
    /// the caller doesn't even notice, matching what a real crash leaves
    /// behind (§1b). The exact prefix length is derived from the
    /// schedule's seed at the moment the fault fires, from the *real*
    /// buffer length of that specific call — the schedule never needs to
    /// know call sizes in advance.
    TornWrite,
    /// Return an `io::Error` instead of performing the call. Used directly
    /// for an fsync failure, and indirectly for "crash between this call
    /// and the next durable step": the caller's own `?`-propagation stops
    /// before reaching it — e.g. `SstWriter::finish`'s `sync_all()?` short-
    /// circuits before the `rename` that follows it, so failing the sync is
    /// how a scenario simulates a crash landing in that exact window
    /// without `FileSink` needing to know anything about renames at all.
    Fail,
}

/// A deterministic, seed-reproducible plan for exactly one fault (§1c): on
/// the `target`-th call of `kind` this schedule sees, apply `fault_kind`.
/// Every other call — before, and any that would follow (there rarely are
/// any: the fault's own `?` propagation usually ends the operation) —
/// passes through untouched.
///
/// Printed via [`FaultSchedule::seed`] on any test failure; rerunning with
/// that seed reproduces the identical fault at the identical call, because
/// `target` and the torn length are both pure functions of `seed`.
pub struct FaultSchedule {
    seed: u64,
    kind: CallKind,
    target: u64,
    fault_kind: FaultKind,
    seen: u64,
    rng: SplitMix64,
}

impl FaultSchedule {
    /// Build a schedule targeting the `target`-th call of `kind` (1-based —
    /// `target == 1` fires on the very first such call).
    pub fn new(seed: u64, kind: CallKind, target: u64, fault_kind: FaultKind) -> Self {
        Self { seed, kind, target, fault_kind, seen: 0, rng: SplitMix64::new(seed) }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Called by [`FaultyFile`] for every call of `kind` it sees.
    /// Returns `Some` exactly once, on the target call.
    fn poll(&mut self, kind: CallKind, buf_len: usize) -> Option<Fired> {
        if kind != self.kind {
            return None;
        }
        self.seen += 1;
        if self.seen != self.target {
            return None;
        }
        Some(match self.fault_kind {
            FaultKind::TornWrite => Fired::TornWrite(self.rng.gen_below(buf_len)),
            FaultKind::Fail => Fired::Fail,
        })
    }
}

enum Fired {
    TornWrite(usize),
    Fail,
}

/// A [`FileSink`] that behaves exactly like the real file until its
/// [`FaultSchedule`] fires, and afterward silently accepts and discards
/// every further call — a real crash stops the process, so nothing after
/// that instant reaches disk either, even though the (unaware) caller keeps
/// calling as if nothing happened.
pub struct FaultyFile {
    inner: File,
    schedule: FaultSchedule,
    tripped: bool,
}

impl FaultyFile {
    pub fn wrap(inner: File, schedule: FaultSchedule) -> Self {
        Self { inner, schedule, tripped: false }
    }

    /// The schedule's seed, for a test to log before asserting — so a
    /// failure's `t.Logf`-equivalent output already has what's needed to
    /// reproduce it (§1c).
    pub fn seed(&self) -> u64 {
        self.schedule.seed()
    }
}

impl FileSink for FaultyFile {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        if self.tripped {
            return Ok(());
        }
        match self.schedule.poll(CallKind::Write, buf.len()) {
            Some(Fired::TornWrite(len)) => {
                self.tripped = true;
                Write::write_all(&mut self.inner, &buf[..len])
            }
            Some(Fired::Fail) => {
                self.tripped = true;
                Err(injected_error("write"))
            }
            None => Write::write_all(&mut self.inner, buf),
        }
    }

    fn sync_all(&mut self) -> io::Result<()> {
        if self.tripped {
            return Ok(());
        }
        match self.schedule.poll(CallKind::Sync, 0) {
            Some(Fired::Fail) => {
                self.tripped = true;
                Err(injected_error("sync"))
            }
            Some(Fired::TornWrite(_)) => {
                unreachable!("a Sync-kind FaultSchedule never produces TornWrite")
            }
            None => self.inner.sync_all(),
        }
    }
}

fn injected_error(op: &str) -> io::Error {
    io::Error::other(format!("faultsim: injected {op} failure"))
}

/// A minimal deterministic PRNG (SplitMix64) — not cryptographic, not
/// general-purpose, just enough to turn one `u64` seed into a reproducible
/// stream of "which byte offset does this torn write land at." See the
/// module doc for why this exists instead of the `rand` crate.
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `[0, bound)`. `bound == 0` always yields `0`. Not
    /// perfectly uniform (a classic modulo-bias sliver near `u64::MAX`),
    /// which does not matter here — this only ever picks a test's fault
    /// location, not anything security- or correctness-sensitive.
    fn gen_below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            return 0;
        }
        (self.next_u64() % bound as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_fires_only_on_the_target_call_of_its_kind() {
        let mut s = FaultSchedule::new(1, CallKind::Write, 2, FaultKind::Fail);
        assert!(matches!(s.poll(CallKind::Write, 10), None)); // call 1
        assert!(matches!(s.poll(CallKind::Sync, 10), None)); // wrong kind, doesn't count
        assert!(matches!(s.poll(CallKind::Write, 10), Some(Fired::Fail))); // call 2: fires
        assert!(matches!(s.poll(CallKind::Write, 10), None)); // call 3: already fired
    }

    #[test]
    fn same_seed_yields_the_same_torn_length() {
        let a = SplitMix64::new(42).gen_below(1000);
        let b = SplitMix64::new(42).gen_below(1000);
        assert_eq!(a, b);
    }

    #[test]
    fn torn_write_length_is_never_out_of_range() {
        for seed in 0..1000u64 {
            let mut s = FaultSchedule::new(seed, CallKind::Write, 1, FaultKind::TornWrite);
            match s.poll(CallKind::Write, 37) {
                Some(Fired::TornWrite(len)) => assert!(len < 37, "seed {seed}: len {len} >= 37"),
                _ => panic!("seed {seed}: expected TornWrite to fire on the first call"),
            }
        }
    }
}
