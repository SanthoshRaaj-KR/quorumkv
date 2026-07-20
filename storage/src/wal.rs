//! Write-ahead log (Phase 1) — see `planning/phase-01-wal.md`.
//!
//! Task 1 (this file, so far): the on-disk **record codec** — how one PUT or
//! DELETE is framed as bytes, and how those bytes are parsed back. No file I/O
//! yet; that arrives with `WalWriter` (Task 2) and `replay` (Task 3).
//!
//! ## Record layout (phase-01 §3)
//!
//! ```text
//! ┌──────────┬──────────┬─────────┬──────────┬─────────┬──────────┐
//! │ crc32c   │ length   │ op      │ key       │ vlen     │ value    │
//! │ 4 bytes  │ 4 bytes  │ 1 byte  │ klen+key  │ 4 bytes  │ vlen     │
//! └──────────┴──────────┴─────────┴──────────┴─────────┴──────────┘
//!   └── crc covers everything to its right: [length .. end] ──┘
//! ```
//!
//! - `crc32c` — CRC32C over `length || payload`. Checked first on replay; a
//!   mismatch means "torn or corrupt, stop here" (this is what makes an
//!   unacknowledged write safe to lose rather than corrupting).
//! - `length` — byte count of the payload that follows (everything after the
//!   length field). Lets replay know exactly how far this record extends.
//! - `op` — `0x01 = PUT`, `0x02 = DELETE`.
//! - key is length-prefixed (`klen`); value uses `vlen`. A DELETE is just a
//!   record with `vlen == 0`.
//!
//! All integers are little-endian. `klen`/`vlen`/`length` are `u32`, so a
//! single key or value is capped at 4 GiB (phase-01 §5 — documented, not a
//! concern for Track A).

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const OP_PUT: u8 = 0x01;
const OP_DELETE: u8 = 0x02;

/// Bytes before the payload: `crc32c` (4) + `length` (4).
const HEADER_LEN: usize = 8;

/// One logical WAL operation — the unit the codec encodes and decodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Why a buffer could not be decoded into a whole record.
///
/// The streaming reader in Task 3 maps these onto replay decisions: `Incomplete`
/// and `CrcMismatch` both mean "the log ends cleanly here" (a torn tail is not
/// an error); `InvalidOp`/`Malformed` indicate genuinely broken bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// Fewer bytes present than this record needs — a clean end-of-log boundary
    /// or a torn tail write. Not corruption.
    Incomplete,
    /// A full record's worth of bytes is present but the checksum disagrees —
    /// a torn/corrupt write. Replay stops here.
    CrcMismatch,
    /// The `op` byte is neither PUT nor DELETE.
    InvalidOp(u8),
    /// CRC passed but the inner length prefixes don't fit the payload. Effectively
    /// impossible after a valid CRC; handled defensively so decode never panics.
    Malformed,
}

/// Encode one record into its full framed on-disk bytes (`crc || length || payload`).
pub fn encode_record(rec: &Record) -> Vec<u8> {
    let (op, key, value): (u8, &[u8], &[u8]) = match rec {
        Record::Put { key, value } => (OP_PUT, key, value),
        Record::Delete { key } => (OP_DELETE, key, &[]),
    };

    debug_assert!(key.len() <= u32::MAX as usize, "key exceeds 4 GiB u32 cap");
    debug_assert!(value.len() <= u32::MAX as usize, "value exceeds 4 GiB u32 cap");

    // payload = op(1) | klen(4) | key | vlen(4) | value
    let payload_len = 1 + 4 + key.len() + 4 + value.len();
    let mut buf = Vec::with_capacity(HEADER_LEN + payload_len);

    // Reserve the 4-byte CRC slot; we backfill it once the rest is written.
    buf.extend_from_slice(&[0u8; 4]);
    buf.extend_from_slice(&(payload_len as u32).to_le_bytes());
    buf.push(op);
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(value);

    // CRC covers [length .. end] — i.e. everything after the CRC field itself.
    let crc = crc32c::crc32c(&buf[4..]);
    buf[0..4].copy_from_slice(&crc.to_le_bytes());
    buf
}

/// Decode the single record at the front of `buf`.
///
/// On success returns the record and the number of bytes it occupied, so a caller
/// can advance and decode the next one. Trailing bytes beyond that first record
/// are ignored here (the streaming reader loops).
pub fn decode_record(buf: &[u8]) -> Result<(Record, usize), DecodeError> {
    // Need at least the header before we can trust a length.
    if buf.len() < HEADER_LEN {
        return Err(DecodeError::Incomplete);
    }
    let stored_crc = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let length = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;

    let total = HEADER_LEN + length;
    if buf.len() < total {
        // The declared payload runs past what we have — torn tail (or a corrupt
        // length that happens to point past EOF). Either way: stop cleanly.
        return Err(DecodeError::Incomplete);
    }

    // Recompute over [length .. end of this record] and compare before trusting
    // any inner field.
    let computed = crc32c::crc32c(&buf[4..total]);
    if computed != stored_crc {
        return Err(DecodeError::CrcMismatch);
    }

    // Payload is now trusted. Parse defensively anyway so a (near-impossible)
    // CRC-passing-but-inconsistent payload can't panic on a slice.
    let payload = &buf[HEADER_LEN..total];
    let mut off = 0usize;

    let op = *payload.get(off).ok_or(DecodeError::Malformed)?;
    off += 1;

    let klen = read_u32(payload, &mut off)? as usize;
    let key = read_bytes(payload, &mut off, klen)?.to_vec();

    let vlen = read_u32(payload, &mut off)? as usize;
    let value = read_bytes(payload, &mut off, vlen)?.to_vec();

    let record = match op {
        OP_PUT => Record::Put { key, value },
        OP_DELETE => {
            // A DELETE must carry no value (vlen == 0 by construction).
            if !value.is_empty() {
                return Err(DecodeError::Malformed);
            }
            Record::Delete { key }
        }
        other => return Err(DecodeError::InvalidOp(other)),
    };
    Ok((record, total))
}

/// Read a little-endian u32 at `*off`, advancing it. `Malformed` if it doesn't fit.
fn read_u32(buf: &[u8], off: &mut usize) -> Result<u32, DecodeError> {
    let end = off.checked_add(4).ok_or(DecodeError::Malformed)?;
    let slice = buf.get(*off..end).ok_or(DecodeError::Malformed)?;
    *off = end;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

/// Borrow `n` bytes at `*off`, advancing it. `Malformed` if fewer remain.
fn read_bytes<'a>(buf: &'a [u8], off: &mut usize, n: usize) -> Result<&'a [u8], DecodeError> {
    let end = off.checked_add(n).ok_or(DecodeError::Malformed)?;
    let slice = buf.get(*off..end).ok_or(DecodeError::Malformed)?;
    *off = end;
    Ok(slice)
}

// ────────────────────────────────────────────────────────────────────────────
// Task 2 — the writer: framed bytes -> durable file.
// ────────────────────────────────────────────────────────────────────────────

/// Append-only, `fsync`-per-write handle onto a WAL file.
///
/// This struct is where the whole engine's core promise is created: **when
/// `append` returns `Ok`, the record has been flushed to stable storage.** The
/// caller (the `Db` wrapper in Task 4) must update in-memory state *only after*
/// `append` returns `Ok` — never before. If `append` returns `Err`, the write
/// was not durable and must not be treated as acknowledged (phase-01 §4).
pub struct WalWriter {
    file: File,
    #[allow(dead_code)] // used by later phases (segment discard); kept for clarity now.
    path: PathBuf,
}

impl WalWriter {
    /// Open (creating if absent) `path` for appending.
    ///
    /// On first creation we also `fsync` the containing directory so the file's
    /// *existence* is durable, not just its contents (phase-01 §2c). Reopening an
    /// existing WAL appends to it — it is never truncated.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let existed = path.exists();

        // `append(true)` makes every write go to the current end of file, so a
        // reopen continues the log rather than overwriting it. `File` is
        // unbuffered — `write_all` issues the `write` syscall directly, so there
        // is no in-process buffer to flush before `fsync`.
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        if !existed {
            fsync_dir(parent_dir(&path))?;
        }
        Ok(Self { file, path })
    }

    /// Encode `rec`, append it, and make it durable before returning.
    ///
    /// Order (phase-01 §4): encode → `write_all` (into the OS page cache) →
    /// `fsync` (page cache → stable storage). Any error is propagated so the
    /// caller does not acknowledge a non-durable write.
    pub fn append(&mut self, rec: &Record) -> io::Result<()> {
        let bytes = encode_record(rec);
        self.file.write_all(&bytes)?;
        // `sync_all` == `fsync`: flushes data *and* file metadata. Phase-01 §2c
        // locks fsync-per-append for now; `sync_data` (fdatasync) is the noted
        // future optimization once the file size is stable.
        self.file.sync_all()?;
        Ok(())
    }
}

/// The directory to fsync for a WAL path, treating a bare filename as `.`.
fn parent_dir(path: &Path) -> &Path {
    match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    }
}

/// `fsync` a directory so a newly-created file's directory entry is durable.
///
/// This is a Unix concern; on Unix we open the directory and `sync_all` its
/// handle. Windows' std cannot fsync a directory handle, and NTFS makes the
/// directory entry durable through different means, so it is a no-op there
/// (phase-01 §2c). Note this only affects *power-loss* durability of the file's
/// existence — it is irrelevant to the `kill -9` done-when, which the OS page
/// cache already survives.
#[cfg(unix)]
fn fsync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn fsync_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}

// ────────────────────────────────────────────────────────────────────────────
// Task 3 — replay: durable file -> ordered records.
// ────────────────────────────────────────────────────────────────────────────

/// Crash recovery: read the WAL at `path`, returning every complete record in
/// write order **and the byte offset where the valid log ends**.
///
/// It walks the file record by record (using the `consumed` count from
/// `decode_record`) and **stops cleanly at the first byte it cannot parse into a
/// whole record** — a torn tail (`Incomplete`), a checksum failure
/// (`CrcMismatch`), or mid-file corruption. Every such stop is expected, not an
/// error: it marks where the log actually ended. This is exactly what makes "an
/// unacknowledged write may be missing" *safe* — the half-written tail is
/// dropped, and everything acknowledged before it survives (phase-01 §4, §5).
///
/// The returned offset is what `Db::open` truncates the file to before it starts
/// appending again, so a crash-torn tail can't shadow later writes.
///
/// A missing file yields an empty log at offset 0 (a first run). Genuine I/O
/// errors while reading an existing file are propagated.
pub fn recover(path: impl AsRef<Path>) -> io::Result<(Vec<Record>, u64)> {
    let bytes = match std::fs::read(path.as_ref()) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((Vec::new(), 0)),
        Err(e) => return Err(e),
    };

    let mut records = Vec::new();
    let mut pos = 0usize;
    while pos < bytes.len() {
        match decode_record(&bytes[pos..]) {
            Ok((rec, consumed)) => {
                records.push(rec);
                pos += consumed;
            }
            // Torn tail or corruption: the durable log ends here. Stop cleanly;
            // `pos` now marks the end of the valid prefix.
            Err(_) => break,
        }
    }
    Ok((records, pos as u64))
}

/// Convenience over [`recover`] for callers that only need the records (e.g.
/// tests). Folding these into current state is the `Db` wrapper's job.
pub fn replay(path: impl AsRef<Path>) -> io::Result<Vec<Record>> {
    Ok(recover(path)?.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(k: &[u8], v: &[u8]) -> Record {
        Record::Put { key: k.to_vec(), value: v.to_vec() }
    }
    fn del(k: &[u8]) -> Record {
        Record::Delete { key: k.to_vec() }
    }

    use crate::testutil::{append_raw, TempDir};

    /// Decode every record in a raw byte buffer by walking `consumed`, stopping
    /// at the first non-record. Lets a test assert what actually landed on disk.
    fn decode_all(mut buf: &[u8]) -> Vec<Record> {
        let mut out = Vec::new();
        while let Ok((rec, n)) = decode_record(buf) {
            out.push(rec);
            buf = &buf[n..];
        }
        out
    }

    #[test]
    fn put_round_trips() {
        let rec = put(b"alpha", b"one");
        let bytes = encode_record(&rec);
        let (decoded, consumed) = decode_record(&bytes).unwrap();
        assert_eq!(decoded, rec);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn delete_round_trips() {
        let rec = del(b"alpha");
        let bytes = encode_record(&rec);
        let (decoded, consumed) = decode_record(&bytes).unwrap();
        assert_eq!(decoded, rec);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn empty_value_put_is_distinct_from_delete() {
        // Both have vlen == 0 on the wire; only the op byte separates them.
        let p = put(b"k", b"");
        let d = del(b"k");
        let (dp, _) = decode_record(&encode_record(&p)).unwrap();
        let (dd, _) = decode_record(&encode_record(&d)).unwrap();
        assert_eq!(dp, p);
        assert_eq!(dd, d);
        assert_ne!(dp, dd);
    }

    #[test]
    fn empty_key_round_trips() {
        let rec = put(b"", b"v");
        let (decoded, _) = decode_record(&encode_record(&rec)).unwrap();
        assert_eq!(decoded, rec);
    }

    #[test]
    fn large_value_round_trips() {
        let rec = put(b"big", &vec![0xABu8; 100_000]);
        let (decoded, consumed) = decode_record(&encode_record(&rec)).unwrap();
        assert_eq!(decoded, rec);
        assert_eq!(consumed, HEADER_LEN + 1 + 4 + 3 + 4 + 100_000);
    }

    #[test]
    fn short_header_is_incomplete() {
        let bytes = encode_record(&put(b"k", b"v"));
        assert_eq!(decode_record(&bytes[..3]), Err(DecodeError::Incomplete));
        assert_eq!(decode_record(&[]), Err(DecodeError::Incomplete));
    }

    #[test]
    fn truncated_payload_is_incomplete() {
        // Header (with a valid length) present, but the payload is cut short —
        // exactly the torn-tail write shape.
        let bytes = encode_record(&put(b"key", b"value"));
        let torn = &bytes[..bytes.len() - 1];
        assert_eq!(decode_record(torn), Err(DecodeError::Incomplete));
    }

    #[test]
    fn flipped_payload_byte_is_crc_mismatch() {
        let mut bytes = encode_record(&put(b"key", b"value"));
        let last = bytes.len() - 1; // inside the value
        bytes[last] ^= 0xFF;
        assert_eq!(decode_record(&bytes), Err(DecodeError::CrcMismatch));
    }

    #[test]
    fn flipped_length_byte_is_caught() {
        // Corrupting the length field is caught either as CrcMismatch (length is
        // inside the CRC region) or Incomplete (if it now points past the buffer).
        let mut bytes = encode_record(&put(b"key", b"value"));
        bytes[4] ^= 0x01;
        let err = decode_record(&bytes).unwrap_err();
        assert!(matches!(err, DecodeError::CrcMismatch | DecodeError::Incomplete));
    }

    #[test]
    fn invalid_op_is_reported() {
        // Corrupt the op byte, then re-checksum so it passes CRC and reaches the
        // op check — proving InvalidOp is surfaced rather than mis-parsed.
        let mut bytes = encode_record(&put(b"k", b"v"));
        bytes[HEADER_LEN] = 0x09; // op byte
        let crc = crc32c::crc32c(&bytes[4..]);
        bytes[0..4].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(decode_record(&bytes), Err(DecodeError::InvalidOp(0x09)));
    }

    #[test]
    fn decodes_first_of_two_concatenated_records() {
        // Proves `consumed` lets a caller walk to the next record — the exact
        // primitive the Task 3 streaming reader relies on.
        let a = put(b"a", b"1");
        let b = del(b"b");
        let mut stream = encode_record(&a);
        let first_len = stream.len();
        stream.extend_from_slice(&encode_record(&b));

        let (ra, consumed) = decode_record(&stream).unwrap();
        assert_eq!(ra, a);
        assert_eq!(consumed, first_len);

        let (rb, _) = decode_record(&stream[consumed..]).unwrap();
        assert_eq!(rb, b);
    }

    #[test]
    fn trailing_bytes_after_a_record_are_ignored() {
        let mut bytes = encode_record(&put(b"k", b"v"));
        let real_len = bytes.len();
        bytes.extend_from_slice(b"garbage-tail");
        let (decoded, consumed) = decode_record(&bytes).unwrap();
        assert_eq!(decoded, put(b"k", b"v"));
        assert_eq!(consumed, real_len);
    }

    // ── Task 2: WalWriter ────────────────────────────────────────────────────

    #[test]
    fn open_creates_the_file() {
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        assert!(!path.exists());
        let _w = WalWriter::open(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn appended_records_land_on_disk_in_order() {
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        let mut w = WalWriter::open(&path).unwrap();
        w.append(&put(b"a", b"1")).unwrap();
        w.append(&put(b"b", b"2")).unwrap();
        w.append(&del(b"a")).unwrap();

        // Read the raw file back (the writer is still open) and decode it.
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(decode_all(&bytes), vec![put(b"a", b"1"), put(b"b", b"2"), del(b"a")]);
    }

    #[test]
    fn append_is_synchronously_visible() {
        // After `append` returns Ok, the bytes are readable by a fresh reader
        // without closing the writer — the write reached (at least) the OS, which
        // is what durability-on-return requires.
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        let mut w = WalWriter::open(&path).unwrap();
        w.append(&put(b"k", b"v")).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(decode_all(&bytes), vec![put(b"k", b"v")]);
    }

    #[test]
    fn reopen_appends_rather_than_truncates() {
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        {
            let mut w = WalWriter::open(&path).unwrap();
            w.append(&put(b"first", b"1")).unwrap();
        } // writer dropped -> file closed
        {
            let mut w = WalWriter::open(&path).unwrap();
            w.append(&put(b"second", b"2")).unwrap();
        }
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(decode_all(&bytes), vec![put(b"first", b"1"), put(b"second", b"2")]);
    }

    #[test]
    fn fresh_wal_file_is_empty() {
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        let _w = WalWriter::open(&path).unwrap();
        assert_eq!(std::fs::read(&path).unwrap().len(), 0);
    }

    #[test]
    fn parent_dir_falls_back_to_current_dir() {
        // A bare filename has no directory component; we fsync "." instead.
        assert_eq!(parent_dir(Path::new("bare.log")), Path::new("."));
        assert_eq!(parent_dir(Path::new("sub/wal.log")), Path::new("sub"));
    }

    // ── Task 3: replay ───────────────────────────────────────────────────────

    #[test]
    fn replay_of_absent_file_is_empty() {
        let dir = TempDir::new();
        // Never created.
        let recs = replay(dir.path("nope.log")).unwrap();
        assert!(recs.is_empty());
    }

    #[test]
    fn replay_of_empty_file_is_empty() {
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        let _w = WalWriter::open(&path).unwrap(); // creates a 0-byte file
        assert!(replay(&path).unwrap().is_empty());
    }

    #[test]
    fn replay_recovers_all_records_in_order() {
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        let written = vec![put(b"a", b"1"), put(b"b", b"2"), del(b"a"), put(b"c", b"3")];
        {
            let mut w = WalWriter::open(&path).unwrap();
            for r in &written {
                w.append(r).unwrap();
            }
        }
        assert_eq!(replay(&path).unwrap(), written);
    }

    #[test]
    fn replay_recovers_100_keys() {
        // The headline done-when, at the record-sequence level (the crash is
        // simulated for real in Task 6).
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        {
            let mut w = WalWriter::open(&path).unwrap();
            for i in 0..100u32 {
                w.append(&put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())).unwrap();
            }
        }
        let recs = replay(&path).unwrap();
        assert_eq!(recs.len(), 100);
        assert_eq!(recs[42], put(b"k42", b"v42"));
    }

    #[test]
    fn replay_stops_at_a_torn_tail() {
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        {
            let mut w = WalWriter::open(&path).unwrap();
            w.append(&put(b"one", b"1")).unwrap();
            w.append(&put(b"two", b"2")).unwrap();
        }
        // Simulate a crash mid-write: a header plus only half a payload.
        let partial = encode_record(&put(b"three", b"3"));
        let half = partial.len() - 2;
        append_raw(&path, &partial[..half]);

        // Exactly the two acknowledged records come back; no panic on the tail.
        assert_eq!(replay(&path).unwrap(), vec![put(b"one", b"1"), put(b"two", b"2")]);
    }

    #[test]
    fn replay_stops_at_a_stray_partial_header() {
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        {
            let mut w = WalWriter::open(&path).unwrap();
            w.append(&put(b"one", b"1")).unwrap();
        }
        append_raw(&path, &[0xDE, 0xAD, 0xBE]); // 3 bytes, less than a header
        assert_eq!(replay(&path).unwrap(), vec![put(b"one", b"1")]);
    }

    #[test]
    fn replay_keeps_prefix_before_mid_file_corruption() {
        let dir = TempDir::new();
        let path = dir.path("wal.log");
        {
            let mut w = WalWriter::open(&path).unwrap();
            w.append(&put(b"one", b"1")).unwrap();
            w.append(&put(b"two", b"2")).unwrap();
            w.append(&put(b"three", b"3")).unwrap();
        }
        // Corrupt one byte inside the *second* record, then rewrite the file.
        let len0 = encode_record(&put(b"one", b"1")).len();
        let len1 = encode_record(&put(b"two", b"2")).len();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[len0 + len1 - 1] ^= 0xFF; // last byte of record #2
        std::fs::write(&path, &bytes).unwrap();

        // Only the clean prefix (record #1) survives; #2 fails CRC and #3 is
        // unreachable behind it.
        assert_eq!(replay(&path).unwrap(), vec![put(b"one", b"1")]);
    }
}
