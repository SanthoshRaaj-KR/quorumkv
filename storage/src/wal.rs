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

#[cfg(test)]
mod tests {
    use super::*;

    fn put(k: &[u8], v: &[u8]) -> Record {
        Record::Put { key: k.to_vec(), value: v.to_vec() }
    }
    fn del(k: &[u8]) -> Record {
        Record::Delete { key: k.to_vec() }
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
}
