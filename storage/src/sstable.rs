//! SSTable (Sorted String Table) — the immutable on-disk form of a memtable.
//! See `planning/phase-03-sstable.md`.
//!
//! Task 1 (this file, so far): the **byte-level format codec** — the three
//! building blocks a full SSTable is assembled from. No file I/O yet; the writer
//! (Task 2) and reader (Task 3) build on these.
//!
//! ## File shape (phase-03 §3)
//!
//! ```text
//! ┌───────────────────────────────────────────┐
//! │ Data Block 0   (sorted entries, ~4 KB)     │
//! │ Data Block 1 … N                           │
//! ├───────────────────────────────────────────┤
//! │ Index Block    (one entry per data block)  │
//! ├───────────────────────────────────────────┤
//! │ Footer         (fixed size, at EOF)        │
//! └───────────────────────────────────────────┘
//! ```
//!
//! ### Data-block entry (phase-03 §2c)
//!
//! ```text
//! [ klen: u32 ][ key ][ vtype: u8 ][ vlen: u32 ][ value ]
//!   vtype 0x01 = Put (value present)   0x02 = Delete (vlen = 0, no value)
//! ```
//!
//! A `Value::Delete` becomes a `vtype=Delete` entry — **tombstones are written to
//! disk**, not dropped at flush, or a read would fall through to an older SSTable
//! and resurrect the key (dropped only in Phase 5 compaction).
//!
//! ### Index entry (one per data block)
//!
//! ```text
//! [ klen: u32 ][ first_key ][ block_offset: u64 ][ block_len: u32 ]
//! ```
//!
//! ### Footer (fixed width, at EOF)
//!
//! ```text
//! [ index_offset: u64 ][ index_len: u32 ][ magic: u32 ][ version: u8 ]
//! ```
//!
//! All integers are little-endian, matching the WAL. There is no per-entry CRC
//! here (unlike the WAL): an SSTable is written whole via temp-file + atomic
//! rename, so a reader never sees a torn tail — a bad `magic` in the footer is
//! how a truncated/foreign file is rejected instead.

use crate::memtable::Value;

/// Value-type tag for a `Put` entry (a real value follows).
const VTYPE_PUT: u8 = 0x01;
/// Value-type tag for a `Delete` entry (a tombstone; `vlen == 0`, no value).
const VTYPE_DELETE: u8 = 0x02;

/// Footer magic: ASCII "QSST", identifies a quorumkv SSTable.
pub const MAGIC: u32 = 0x5153_5354;
/// On-disk format version.
pub const VERSION: u8 = 1;
/// Fixed footer width: `index_offset(8) + index_len(4) + magic(4) + version(1)`.
pub const FOOTER_LEN: usize = 8 + 4 + 4 + 1;

/// Why a byte slice could not be parsed as part of an SSTable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SstFormatError {
    /// Fewer bytes than the field being read requires.
    UnexpectedEof,
    /// A value-type tag that is neither Put nor Delete.
    BadVtype(u8),
    /// The footer magic didn't match — not an SSTable, or truncated.
    BadMagic(u32),
    /// The footer's format version is not one we understand.
    BadVersion(u8),
    /// Structurally inconsistent (e.g. a Delete entry claiming a non-zero vlen).
    Malformed,
}

impl std::fmt::Display for SstFormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SstFormatError::UnexpectedEof => write!(f, "unexpected end of SSTable data"),
            SstFormatError::BadVtype(b) => write!(f, "invalid value-type tag {b:#04x}"),
            SstFormatError::BadMagic(m) => write!(f, "bad SSTable magic {m:#010x}"),
            SstFormatError::BadVersion(v) => write!(f, "unsupported SSTable version {v}"),
            SstFormatError::Malformed => write!(f, "malformed SSTable entry"),
        }
    }
}

impl std::error::Error for SstFormatError {}

// ── Data-block entry ─────────────────────────────────────────────────────────

/// Append one `key -> value` entry to `buf` (used by the block writer).
pub fn encode_entry(key: &[u8], value: &Value, buf: &mut Vec<u8>) {
    debug_assert!(key.len() <= u32::MAX as usize, "key exceeds 4 GiB u32 cap");
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    match value {
        Value::Put(v) => {
            debug_assert!(v.len() <= u32::MAX as usize, "value exceeds 4 GiB u32 cap");
            buf.push(VTYPE_PUT);
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v);
        }
        Value::Delete => {
            buf.push(VTYPE_DELETE);
            buf.extend_from_slice(&0u32.to_le_bytes()); // vlen = 0, no value bytes
        }
    }
}

/// Decode the single entry at the front of `buf`, returning it and the number of
/// bytes it occupied (so a block scan can advance to the next entry).
pub fn decode_entry(buf: &[u8]) -> Result<(Vec<u8>, Value, usize), SstFormatError> {
    let mut off = 0usize;
    let klen = read_u32(buf, &mut off)? as usize;
    let key = read_bytes(buf, &mut off, klen)?.to_vec();

    let vtype = read_u8(buf, &mut off)?;
    let vlen = read_u32(buf, &mut off)? as usize;
    let value = match vtype {
        VTYPE_PUT => Value::Put(read_bytes(buf, &mut off, vlen)?.to_vec()),
        VTYPE_DELETE => {
            if vlen != 0 {
                return Err(SstFormatError::Malformed);
            }
            Value::Delete
        }
        other => return Err(SstFormatError::BadVtype(other)),
    };
    Ok((key, value, off))
}

// ── Index entry ──────────────────────────────────────────────────────────────

/// Append one sparse-index entry — the block's first key plus where it lives.
pub fn encode_index_entry(first_key: &[u8], block_offset: u64, block_len: u32, buf: &mut Vec<u8>) {
    debug_assert!(first_key.len() <= u32::MAX as usize, "key exceeds 4 GiB u32 cap");
    buf.extend_from_slice(&(first_key.len() as u32).to_le_bytes());
    buf.extend_from_slice(first_key);
    buf.extend_from_slice(&block_offset.to_le_bytes());
    buf.extend_from_slice(&block_len.to_le_bytes());
}

/// Decode one index entry, returning `(first_key, block_offset, block_len,
/// bytes_consumed)`.
pub fn decode_index_entry(buf: &[u8]) -> Result<(Vec<u8>, u64, u32, usize), SstFormatError> {
    let mut off = 0usize;
    let klen = read_u32(buf, &mut off)? as usize;
    let first_key = read_bytes(buf, &mut off, klen)?.to_vec();
    let block_offset = read_u64(buf, &mut off)?;
    let block_len = read_u32(buf, &mut off)?;
    Ok((first_key, block_offset, block_len, off))
}

// ── Footer ───────────────────────────────────────────────────────────────────

/// The fixed-width trailer a reader loads first to bootstrap the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    pub index_offset: u64,
    pub index_len: u32,
}

/// Encode the footer to its fixed byte width.
pub fn encode_footer(footer: &Footer) -> [u8; FOOTER_LEN] {
    let mut b = [0u8; FOOTER_LEN];
    b[0..8].copy_from_slice(&footer.index_offset.to_le_bytes());
    b[8..12].copy_from_slice(&footer.index_len.to_le_bytes());
    b[12..16].copy_from_slice(&MAGIC.to_le_bytes());
    b[16] = VERSION;
    b
}

/// Decode a footer from the last [`FOOTER_LEN`] bytes of a file, validating the
/// magic and version (this is how a truncated or non-SSTable file is rejected).
pub fn decode_footer(buf: &[u8]) -> Result<Footer, SstFormatError> {
    if buf.len() < FOOTER_LEN {
        return Err(SstFormatError::UnexpectedEof);
    }
    let magic = u32::from_le_bytes(buf[12..16].try_into().unwrap());
    if magic != MAGIC {
        return Err(SstFormatError::BadMagic(magic));
    }
    let version = buf[16];
    if version != VERSION {
        return Err(SstFormatError::BadVersion(version));
    }
    Ok(Footer {
        index_offset: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
        index_len: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
    })
}

// ── Little-endian read helpers (never panic; short read -> UnexpectedEof) ─────

fn read_u8(buf: &[u8], off: &mut usize) -> Result<u8, SstFormatError> {
    let b = *buf.get(*off).ok_or(SstFormatError::UnexpectedEof)?;
    *off += 1;
    Ok(b)
}

fn read_u32(buf: &[u8], off: &mut usize) -> Result<u32, SstFormatError> {
    let end = off.checked_add(4).ok_or(SstFormatError::UnexpectedEof)?;
    let slice = buf.get(*off..end).ok_or(SstFormatError::UnexpectedEof)?;
    *off = end;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

fn read_u64(buf: &[u8], off: &mut usize) -> Result<u64, SstFormatError> {
    let end = off.checked_add(8).ok_or(SstFormatError::UnexpectedEof)?;
    let slice = buf.get(*off..end).ok_or(SstFormatError::UnexpectedEof)?;
    *off = end;
    Ok(u64::from_le_bytes(slice.try_into().unwrap()))
}

fn read_bytes<'a>(buf: &'a [u8], off: &mut usize, n: usize) -> Result<&'a [u8], SstFormatError> {
    let end = off.checked_add(n).ok_or(SstFormatError::UnexpectedEof)?;
    let slice = buf.get(*off..end).ok_or(SstFormatError::UnexpectedEof)?;
    *off = end;
    Ok(slice)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(key: &[u8], value: &Value) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_entry(key, value, &mut buf);
        buf
    }

    #[test]
    fn put_entry_round_trips() {
        let bytes = enc(b"alpha", &Value::Put(b"one".to_vec()));
        let (key, value, consumed) = decode_entry(&bytes).unwrap();
        assert_eq!(key, b"alpha");
        assert_eq!(value, Value::Put(b"one".to_vec()));
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn delete_entry_round_trips() {
        let bytes = enc(b"alpha", &Value::Delete);
        let (key, value, consumed) = decode_entry(&bytes).unwrap();
        assert_eq!(key, b"alpha");
        assert_eq!(value, Value::Delete);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn empty_value_put_is_distinct_from_delete() {
        // Both encode vlen == 0; only the vtype byte separates them.
        let p = enc(b"k", &Value::Put(Vec::new()));
        let d = enc(b"k", &Value::Delete);
        assert_ne!(p, d);
        assert_eq!(decode_entry(&p).unwrap().1, Value::Put(Vec::new()));
        assert_eq!(decode_entry(&d).unwrap().1, Value::Delete);
    }

    #[test]
    fn entries_walk_by_consumed() {
        // Two entries back-to-back in a block: `consumed` advances to the next.
        let mut block = Vec::new();
        encode_entry(b"a", &Value::Put(b"1".to_vec()), &mut block);
        encode_entry(b"b", &Value::Delete, &mut block);

        let (k1, v1, n1) = decode_entry(&block).unwrap();
        assert_eq!((k1.as_slice(), v1), (b"a".as_slice(), Value::Put(b"1".to_vec())));
        let (k2, v2, _) = decode_entry(&block[n1..]).unwrap();
        assert_eq!((k2.as_slice(), v2), (b"b".as_slice(), Value::Delete));
    }

    #[test]
    fn bad_vtype_is_reported() {
        let mut bytes = enc(b"k", &Value::Put(b"v".to_vec()));
        // vtype sits right after klen(4) + key(1).
        bytes[4 + 1] = 0x09;
        assert_eq!(decode_entry(&bytes), Err(SstFormatError::BadVtype(0x09)));
    }

    #[test]
    fn truncated_entry_is_unexpected_eof() {
        let bytes = enc(b"key", &Value::Put(b"value".to_vec()));
        assert_eq!(decode_entry(&bytes[..bytes.len() - 1]), Err(SstFormatError::UnexpectedEof));
        assert_eq!(decode_entry(&[]), Err(SstFormatError::UnexpectedEof));
    }

    #[test]
    fn index_entry_round_trips() {
        let mut buf = Vec::new();
        encode_index_entry(b"first-key", 4096, 512, &mut buf);
        let (key, offset, len, consumed) = decode_index_entry(&buf).unwrap();
        assert_eq!(key, b"first-key");
        assert_eq!(offset, 4096);
        assert_eq!(len, 512);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn index_entries_walk_by_consumed() {
        let mut buf = Vec::new();
        encode_index_entry(b"aaa", 0, 100, &mut buf);
        encode_index_entry(b"mmm", 100, 200, &mut buf);
        let (_, o1, l1, n1) = decode_index_entry(&buf).unwrap();
        assert_eq!((o1, l1), (0, 100));
        let (k2, o2, l2, _) = decode_index_entry(&buf[n1..]).unwrap();
        assert_eq!((k2.as_slice(), o2, l2), (b"mmm".as_slice(), 100, 200));
    }

    #[test]
    fn footer_round_trips() {
        let f = Footer { index_offset: 123_456, index_len: 789 };
        let bytes = encode_footer(&f);
        assert_eq!(bytes.len(), FOOTER_LEN);
        assert_eq!(decode_footer(&bytes).unwrap(), f);
    }

    #[test]
    fn footer_rejects_bad_magic() {
        let mut bytes = encode_footer(&Footer { index_offset: 1, index_len: 2 });
        bytes[12] ^= 0xFF; // corrupt the magic
        assert!(matches!(decode_footer(&bytes), Err(SstFormatError::BadMagic(_))));
    }

    #[test]
    fn footer_rejects_bad_version() {
        let mut bytes = encode_footer(&Footer { index_offset: 1, index_len: 2 });
        bytes[16] = 99; // bogus version
        assert_eq!(decode_footer(&bytes), Err(SstFormatError::BadVersion(99)));
    }

    #[test]
    fn footer_too_short_is_unexpected_eof() {
        assert_eq!(decode_footer(&[0u8; FOOTER_LEN - 1]), Err(SstFormatError::UnexpectedEof));
    }
}
