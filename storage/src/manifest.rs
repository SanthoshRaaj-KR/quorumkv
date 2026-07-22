//! MANIFEST / Version / VersionSet (Phase 5) — see `planning/phase-05-compaction.md` §3.
//!
//! Phase 3 tracked the live SSTable set by listing the directory and sorting by
//! file number — fine when flush only ever *adds* one file. Compaction breaks
//! that: it **adds outputs and removes inputs together**, and a directory listing
//! can't reflect that multi-file swap atomically.
//!
//! The MANIFEST is an **append-only log of version edits** (`AddFile`/`DeleteFile`).
//! The current live set is the replay of the log. A compaction (or a flush)
//! commits by appending **one** edit and fsyncing — that single append is the
//! linearization point: before it the old set is live, after it the new set is. A
//! crash mid-operation replays to a consistent set either way (a torn final edit
//! is ignored, exactly like the WAL's torn tail).
//!
//! This is the LevelDB `VersionSet` model, kept minimal.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::wal::{fsync_dir, parent_dir};

/// The MANIFEST filename within a store directory.
pub const MANIFEST_NAME: &str = "MANIFEST";

/// Metadata for one live SSTable: its file number and its level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileMeta {
    pub number: u64,
    pub level: u32,
}

/// One atomic change to the live set: files to add and files to remove.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionEdit {
    pub added: Vec<FileMeta>,
    pub deleted: Vec<u64>,
}

impl VersionEdit {
    /// A single-file add (a flush output).
    pub fn add(number: u64, level: u32) -> Self {
        VersionEdit { added: vec![FileMeta { number, level }], deleted: Vec::new() }
    }
}

/// An immutable snapshot of the live SSTable set. Reads hold an `Arc<Version>`;
/// a commit installs a fresh one.
#[derive(Debug, Clone, Default)]
pub struct Version {
    pub files: Vec<FileMeta>,
}

impl Version {
    fn apply(&mut self, edit: &VersionEdit) {
        if !edit.deleted.is_empty() {
            self.files.retain(|f| !edit.deleted.contains(&f.number));
        }
        self.files.extend_from_slice(&edit.added);
    }

    /// Live file numbers, for orphan-sweeping on startup.
    pub fn file_numbers(&self) -> Vec<u64> {
        self.files.iter().map(|f| f.number).collect()
    }
}

/// Why a MANIFEST record could not be decoded (torn tail / corruption → stop).
#[derive(Debug, Clone, PartialEq, Eq)]
enum ManifestError {
    Incomplete,
    CrcMismatch,
    Malformed,
}

/// Owns the MANIFEST, the current `Version`, and the file-number allocator.
pub struct VersionSet {
    manifest: Mutex<File>,
    current: RwLock<Arc<Version>>,
    next_number: AtomicU64,
}

impl VersionSet {
    /// Open (creating if absent) the version set rooted at `dir`, replaying the
    /// MANIFEST into the current version and healing any torn final edit.
    pub fn open(dir: &Path) -> io::Result<Self> {
        let path = dir.join(MANIFEST_NAME);

        let mut version = Version::default();
        let mut max_number = 0u64;
        let existed = path.exists();

        if existed {
            let bytes = fs::read(&path)?;
            let mut pos = 0usize;
            while pos < bytes.len() {
                match decode_edit(&bytes[pos..]) {
                    Ok((edit, consumed)) => {
                        for f in &edit.added {
                            max_number = max_number.max(f.number);
                        }
                        version.apply(&edit);
                        pos += consumed;
                    }
                    Err(_) => break, // torn/corrupt final edit — ignore it
                }
            }
            // Heal a torn tail so future appends aren't shadowed on the next replay.
            if (pos as u64) < bytes.len() as u64 {
                let f = OpenOptions::new().write(true).open(&path)?;
                f.set_len(pos as u64)?;
                f.sync_all()?;
            }
        }

        let manifest = OpenOptions::new().create(true).append(true).open(&path)?;
        if !existed {
            fsync_dir(parent_dir(&path))?;
        }

        log::info!(
            target: "manifest",
            "opened: {} live file(s), next file number {}",
            version.files.len(),
            max_number + 1,
        );

        Ok(VersionSet {
            manifest: Mutex::new(manifest),
            current: RwLock::new(Arc::new(version)),
            next_number: AtomicU64::new(max_number + 1),
        })
    }

    /// The current live set (cheap `Arc` clone).
    pub fn current(&self) -> Arc<Version> {
        Arc::clone(&self.current.read().expect("version lock poisoned"))
    }

    /// Durably commit one edit — append it, fsync (the linearization point), then
    /// install the new version — and return the new live set.
    pub fn commit(&self, edit: &VersionEdit) -> io::Result<Arc<Version>> {
        let record = encode_edit(edit);
        {
            let mut m = self.manifest.lock().expect("manifest mutex poisoned");
            m.write_all(&record)?;
            m.sync_all()?;
        }
        // Keep the allocator ahead of any number this edit introduces.
        if let Some(max_added) = edit.added.iter().map(|f| f.number).max() {
            self.next_number.fetch_max(max_added + 1, Ordering::SeqCst);
        }

        let mut cur = self.current.write().expect("version lock poisoned");
        let mut next = (**cur).clone();
        next.apply(edit);
        let arc = Arc::new(next);
        *cur = Arc::clone(&arc);
        log::debug!(
            target: "manifest",
            "commit: +{} file(s), -{} file(s) -> {} live",
            edit.added.len(),
            edit.deleted.len(),
            arc.files.len(),
        );
        Ok(arc)
    }

    /// Allocate the next monotonic file number.
    pub fn next_file_number(&self) -> u64 {
        self.next_number.fetch_add(1, Ordering::SeqCst)
    }

    /// Peek the next file number without allocating (for logging/tests).
    pub fn peek_next_file_number(&self) -> u64 {
        self.next_number.load(Ordering::SeqCst)
    }
}

// ── record framing: [ crc32c:u32 | len:u32 | payload ] (mirrors the WAL) ──────

fn encode_edit(edit: &VersionEdit) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(edit.added.len() as u32).to_le_bytes());
    for f in &edit.added {
        payload.extend_from_slice(&f.number.to_le_bytes());
        payload.extend_from_slice(&f.level.to_le_bytes());
    }
    payload.extend_from_slice(&(edit.deleted.len() as u32).to_le_bytes());
    for n in &edit.deleted {
        payload.extend_from_slice(&n.to_le_bytes());
    }

    let mut rec = Vec::with_capacity(8 + payload.len());
    rec.extend_from_slice(&[0u8; 4]); // crc placeholder
    rec.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    rec.extend_from_slice(&payload);
    let crc = crc32c::crc32c(&rec[4..]);
    rec[0..4].copy_from_slice(&crc.to_le_bytes());
    rec
}

fn decode_edit(buf: &[u8]) -> Result<(VersionEdit, usize), ManifestError> {
    if buf.len() < 8 {
        return Err(ManifestError::Incomplete);
    }
    let stored_crc = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let len = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
    let total = 8 + len;
    if buf.len() < total {
        return Err(ManifestError::Incomplete);
    }
    if crc32c::crc32c(&buf[4..total]) != stored_crc {
        return Err(ManifestError::CrcMismatch);
    }

    let p = &buf[8..total];
    let mut off = 0usize;
    let added_count = read_u32(p, &mut off)?;
    let mut added = Vec::with_capacity(added_count as usize);
    for _ in 0..added_count {
        let number = read_u64(p, &mut off)?;
        let level = read_u32(p, &mut off)?;
        added.push(FileMeta { number, level });
    }
    let deleted_count = read_u32(p, &mut off)?;
    let mut deleted = Vec::with_capacity(deleted_count as usize);
    for _ in 0..deleted_count {
        deleted.push(read_u64(p, &mut off)?);
    }
    Ok((VersionEdit { added, deleted }, total))
}

fn read_u32(buf: &[u8], off: &mut usize) -> Result<u32, ManifestError> {
    let end = off.checked_add(4).ok_or(ManifestError::Malformed)?;
    let slice = buf.get(*off..end).ok_or(ManifestError::Malformed)?;
    *off = end;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

fn read_u64(buf: &[u8], off: &mut usize) -> Result<u64, ManifestError> {
    let end = off.checked_add(8).ok_or(ManifestError::Malformed)?;
    let slice = buf.get(*off..end).ok_or(ManifestError::Malformed)?;
    *off = end;
    Ok(u64::from_le_bytes(slice.try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TempDir;

    fn nums(v: &Version) -> Vec<u64> {
        let mut n = v.file_numbers();
        n.sort_unstable();
        n
    }

    #[test]
    fn empty_open_has_no_files_and_starts_at_one() {
        let dir = TempDir::new();
        let vs = VersionSet::open(&dir.0).unwrap();
        assert!(vs.current().files.is_empty());
        assert_eq!(vs.next_file_number(), 1);
        assert_eq!(vs.next_file_number(), 2);
    }

    #[test]
    fn commit_adds_files_to_the_current_version() {
        let dir = TempDir::new();
        let vs = VersionSet::open(&dir.0).unwrap();
        vs.commit(&VersionEdit::add(1, 0)).unwrap();
        vs.commit(&VersionEdit::add(2, 0)).unwrap();
        assert_eq!(nums(&vs.current()), vec![1, 2]);
    }

    #[test]
    fn multi_file_edit_is_atomic() {
        let dir = TempDir::new();
        let vs = VersionSet::open(&dir.0).unwrap();
        vs.commit(&VersionEdit::add(1, 0)).unwrap();
        vs.commit(&VersionEdit::add(2, 0)).unwrap();
        // A compaction: delete inputs {1,2}, add output {3}, as one edit.
        vs.commit(&VersionEdit {
            added: vec![FileMeta { number: 3, level: 1 }],
            deleted: vec![1, 2],
        })
        .unwrap();
        assert_eq!(nums(&vs.current()), vec![3]);
    }

    #[test]
    fn state_recovers_across_reopen() {
        let dir = TempDir::new();
        {
            let vs = VersionSet::open(&dir.0).unwrap();
            vs.commit(&VersionEdit::add(1, 0)).unwrap();
            vs.commit(&VersionEdit::add(2, 0)).unwrap();
            vs.commit(&VersionEdit { added: vec![], deleted: vec![1] }).unwrap();
        }
        let vs = VersionSet::open(&dir.0).unwrap();
        assert_eq!(nums(&vs.current()), vec![2]);
        // Deleted file 1's number is not reused (max seen was 2).
        assert_eq!(vs.next_file_number(), 3);
    }

    #[test]
    fn torn_final_edit_is_ignored_on_reopen() {
        let dir = TempDir::new();
        {
            let vs = VersionSet::open(&dir.0).unwrap();
            vs.commit(&VersionEdit::add(1, 0)).unwrap();
            vs.commit(&VersionEdit::add(2, 0)).unwrap();
        }
        // Append a half-written edit record, as a crash mid-commit would.
        let partial = encode_edit(&VersionEdit::add(3, 0));
        crate::testutil::append_raw(&dir.0.join(MANIFEST_NAME), &partial[..partial.len() - 2]);

        let vs = VersionSet::open(&dir.0).unwrap();
        assert_eq!(nums(&vs.current()), vec![1, 2]); // torn edit dropped
        // And the tail was healed: a new commit survives a further reopen.
        vs.commit(&VersionEdit::add(4, 0)).unwrap();
        drop(vs);
        let vs = VersionSet::open(&dir.0).unwrap();
        assert_eq!(nums(&vs.current()), vec![1, 2, 4]);
    }

    #[test]
    fn edit_round_trips_through_codec() {
        let edit = VersionEdit {
            added: vec![FileMeta { number: 7, level: 2 }, FileMeta { number: 9, level: 0 }],
            deleted: vec![3, 4, 5],
        };
        let bytes = encode_edit(&edit);
        let (decoded, consumed) = decode_edit(&bytes).unwrap();
        assert_eq!(decoded, edit);
        assert_eq!(consumed, bytes.len());
    }
}
