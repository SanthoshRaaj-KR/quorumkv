//! Packing/unpacking the live SSTable set as one transferable blob — the
//! Rust side of the Phase 10 seam (`planning/phase-10-apply-seam.md` §6a).
//!
//! `DESIGN.md` §3 calls for the snapshot payload to be "the LSM engine's own
//! current SSTable set, since those are already an immutable point-in-time
//! view" — nothing to re-serialize, just bytes already sitting on disk. This
//! module is exactly that: `pack` reads the current live files and frames
//! them into one blob; `unpack` writes them back out under a fresh directory
//! so [`crate::db::Db::open`] can adopt them.
//!
//! ## Blob layout
//!
//! ```text
//! crc32c(4) | [ fileNumber(8) | length(8) | raw sstable bytes ]*
//! ```
//!
//! One CRC over the whole blob — belt-and-suspenders, since each SSTable
//! already carries its own internal block checksums; this just rejects a
//! torn/corrupt *transfer* before any byte of it touches disk, the same
//! "reject rather than trust" rule as every other frame in this project.

use std::fs;
use std::io;
use std::path::Path;

use crate::sstable::sst_filename;

/// Read every file in `numbers` from `dir` and frame them into one blob
/// (phase-10 §6a). `numbers` is normally `VersionSet::current().file_numbers()`.
pub fn pack(dir: &Path, numbers: &[u64]) -> io::Result<Vec<u8>> {
    let mut payload = Vec::new();
    for &num in numbers {
        let bytes = fs::read(dir.join(sst_filename(num)))?;
        payload.extend_from_slice(&num.to_le_bytes());
        payload.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        payload.extend_from_slice(&bytes);
    }

    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&[0u8; 4]); // crc placeholder, backfilled below
    out.extend_from_slice(&payload);
    let crc = crc32c::crc32c(&out[4..]);
    out[0..4].copy_from_slice(&crc.to_le_bytes());
    Ok(out)
}

/// Verify and unpack `blob` into `dir`, writing each SSTable under its
/// recorded file number. Does **not** touch `dir`'s MANIFEST or any other
/// state — that's [`crate::db::Db::restore`]'s job, which wipes `dir` first
/// (an installed snapshot is authoritative, the same rule Raft's own
/// `Log.RestoreToSnapshot` enforces) and then calls this.
pub fn unpack(dir: &Path, blob: &[u8]) -> io::Result<()> {
    if blob.len() < 4 {
        return Err(bad_data("snapshot blob shorter than its own header"));
    }
    let stored_crc = u32::from_le_bytes(blob[0..4].try_into().unwrap());
    let computed = crc32c::crc32c(&blob[4..]);
    if computed != stored_crc {
        return Err(bad_data("snapshot blob checksum mismatch"));
    }

    let payload = &blob[4..];
    let mut off = 0usize;
    while off < payload.len() {
        if off + 16 > payload.len() {
            return Err(bad_data("snapshot blob truncated (frame header)"));
        }
        let number = u64::from_le_bytes(payload[off..off + 8].try_into().unwrap());
        off += 8;
        let length = u64::from_le_bytes(payload[off..off + 8].try_into().unwrap()) as usize;
        off += 8;
        if off + length > payload.len() {
            return Err(bad_data("snapshot blob truncated (file body)"));
        }
        fs::write(dir.join(sst_filename(number)), &payload[off..off + length])?;
        off += length;
    }
    Ok(())
}

fn bad_data(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TempDir;

    #[test]
    fn pack_unpack_round_trips_file_bytes() {
        let src = TempDir::new();
        fs::write(src.path("000001.sst"), b"first-file-bytes").unwrap();
        fs::write(src.path("000002.sst"), b"second, a bit longer").unwrap();

        let blob = pack(&src.0, &[1, 2]).unwrap();

        let dst = TempDir::new();
        unpack(&dst.0, &blob).unwrap();

        assert_eq!(fs::read(dst.path("000001.sst")).unwrap(), b"first-file-bytes");
        assert_eq!(fs::read(dst.path("000002.sst")).unwrap(), b"second, a bit longer");
    }

    #[test]
    fn empty_file_set_packs_and_unpacks_to_nothing() {
        let src = TempDir::new();
        let blob = pack(&src.0, &[]).unwrap();

        let dst = TempDir::new();
        unpack(&dst.0, &blob).unwrap();
        assert_eq!(fs::read_dir(&dst.0).unwrap().count(), 0);
    }

    #[test]
    fn corrupt_blob_is_rejected_before_touching_disk() {
        let src = TempDir::new();
        fs::write(src.path("000001.sst"), b"data").unwrap();
        let mut blob = pack(&src.0, &[1]).unwrap();
        let last = blob.len() - 1;
        blob[last] ^= 0xFF; // corrupt a byte inside the packed file body

        let dst = TempDir::new();
        assert!(unpack(&dst.0, &blob).is_err());
        assert_eq!(fs::read_dir(&dst.0).unwrap().count(), 0, "a rejected blob must write nothing");
    }

    #[test]
    fn truncated_blob_is_rejected() {
        let src = TempDir::new();
        fs::write(src.path("000001.sst"), b"data").unwrap();
        let blob = pack(&src.0, &[1]).unwrap();
        let dst = TempDir::new();
        assert!(unpack(&dst.0, &blob[..blob.len() - 2]).is_err());
    }
}
