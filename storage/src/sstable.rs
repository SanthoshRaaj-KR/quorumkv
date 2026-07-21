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

use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{self, AtomicU64};

use crate::memtable::Value;
use crate::wal::{fsync_dir, parent_dir};

/// Target size for a data block. A block is cut once it crosses this; a single
/// entry larger than the target gets its own (oversized) block — an entry is
/// never split across blocks (phase-03 §3).
pub const BLOCK_TARGET: usize = 4096;

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

// ────────────────────────────────────────────────────────────────────────────
// Task 2 — the writer: a sorted entry stream -> one durable .sst file.
// ────────────────────────────────────────────────────────────────────────────

/// The `.sst` filename for a file number, e.g. `000002.sst`.
pub fn sst_filename(file_number: u64) -> String {
    format!("{file_number:06}.sst")
}

/// Parse an SSTable file number from `NNNNNN.sst`, or `None` if it isn't one
/// (notably rejects `.sst.tmp` orphans).
pub fn parse_sst_number(name: &str) -> Option<u64> {
    let digits = name.strip_suffix(".sst")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<u64>().ok()
}

/// List SSTables under `dir` as `(file_number, path)`, ascending by number. A
/// missing directory yields an empty list.
pub fn list_sstables(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    match std::fs::read_dir(dir) {
        Ok(rd) => {
            for entry in rd {
                let entry = entry?;
                if let Some(n) = entry.file_name().to_str().and_then(parse_sst_number) {
                    out.push((n, entry.path()));
                }
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    out.sort_by_key(|(n, _)| *n);
    Ok(out)
}

/// Remove any orphan `*.sst.tmp` files under `dir` — leftovers from a flush that
/// crashed before its atomic rename (phase-03 §5). Best-effort per file.
pub fn remove_orphan_tmp(dir: &Path) -> io::Result<()> {
    match std::fs::read_dir(dir) {
        Ok(rd) => {
            for entry in rd {
                let entry = entry?;
                if let Some(name) = entry.file_name().to_str() {
                    if name.ends_with(".sst.tmp") {
                        let _ = std::fs::remove_file(entry.path());
                        log::debug!(target: "sstable", "removed orphan flush temp {name}");
                    }
                }
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    Ok(())
}

/// Streaming writer for one SSTable. Call [`add`](SstWriter::add) with entries in
/// **strictly increasing key order**, then [`finish`](SstWriter::finish).
///
/// Durability (phase-03 §4b): everything is written to `NNNNNN.sst.tmp`, fsynced,
/// then **atomically renamed** to `NNNNNN.sst` and the directory fsynced. A crash
/// before the rename leaves only an orphan `.tmp` (ignored/cleaned on restart)
/// while the backing WAL segment still holds the data — never a half-visible
/// SSTable.
pub struct SstWriter {
    file: File,
    tmp_path: PathBuf,
    final_path: PathBuf,
    /// Running write offset into the file (bytes written so far).
    offset: u64,
    /// The data block currently being accumulated.
    block: Vec<u8>,
    /// First key of the current block (recorded into the index when it's cut).
    block_first_key: Option<Vec<u8>>,
    /// Accumulated sparse index (one entry per completed block).
    index: Vec<u8>,
    entry_count: u64,
    /// Last key added, for the strictly-increasing debug assertion.
    last_key: Option<Vec<u8>>,
}

impl SstWriter {
    /// Create a writer for `file_number` under `dir` (opens the `.tmp` file).
    pub fn create(dir: &Path, file_number: u64) -> io::Result<Self> {
        let final_path = dir.join(sst_filename(file_number));
        let tmp_path = dir.join(format!("{}.tmp", sst_filename(file_number)));
        let file = OpenOptions::new().create(true).write(true).truncate(true).open(&tmp_path)?;
        Ok(SstWriter {
            file,
            tmp_path,
            final_path,
            offset: 0,
            block: Vec::with_capacity(BLOCK_TARGET + 256),
            block_first_key: None,
            index: Vec::new(),
            entry_count: 0,
            last_key: None,
        })
    }

    /// Add one entry. Keys must arrive strictly increasing (the memtable iterates
    /// sorted with unique keys, so this holds).
    pub fn add(&mut self, key: &[u8], value: &Value) -> io::Result<()> {
        debug_assert!(
            self.last_key.as_deref().is_none_or(|lk| lk < key),
            "SSTable entries must be added in strictly increasing key order",
        );

        if self.block.is_empty() {
            self.block_first_key = Some(key.to_vec());
        }
        encode_entry(key, value, &mut self.block);
        self.entry_count += 1;
        self.last_key = Some(key.to_vec());

        // Cut the block once it crosses the target. A single oversized entry
        // trips this too, so it ends up alone in its own block — never split.
        if self.block.len() >= BLOCK_TARGET {
            self.flush_block()?;
        }
        Ok(())
    }

    /// Write the current block out and record its index entry. No-op if empty.
    fn flush_block(&mut self) -> io::Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let block_offset = self.offset;
        let block_len = self.block.len() as u32;
        self.file.write_all(&self.block)?;
        self.offset += u64::from(block_len);

        let first_key = self.block_first_key.take().expect("a non-empty block has a first key");
        encode_index_entry(&first_key, block_offset, block_len, &mut self.index);
        self.block.clear();
        Ok(())
    }

    /// Finish: flush the last block, write the index and footer, fsync, atomically
    /// rename `.tmp` → `.sst`, fsync the directory, and return the final path.
    ///
    /// Panics in debug builds if no entries were added — callers must not flush an
    /// empty memtable (use [`write_sstable`], which skips empties).
    pub fn finish(mut self) -> io::Result<PathBuf> {
        debug_assert!(self.entry_count > 0, "refusing to write a 0-entry SSTable");

        self.flush_block()?;

        let index_offset = self.offset;
        let index_len = self.index.len() as u32;
        self.file.write_all(&self.index)?;
        self.offset += u64::from(index_len);

        let footer = encode_footer(&Footer { index_offset, index_len });
        self.file.write_all(&footer)?;

        // Durable-then-visible: fsync the bytes, atomically rename into place,
        // then fsync the directory so the rename itself survives a crash.
        self.file.sync_all()?;
        std::fs::rename(&self.tmp_path, &self.final_path)?;
        fsync_dir(parent_dir(&self.final_path))?;

        log::debug!(
            target: "sstable",
            "wrote {} ({} entr(y|ies), {} block(s))",
            self.final_path.display(),
            self.entry_count,
            self.index_entry_count(),
        );
        Ok(self.final_path)
    }

    /// Number of blocks written (index entries), for logging.
    fn index_entry_count(&self) -> usize {
        let mut off = 0usize;
        let mut n = 0usize;
        while off < self.index.len() {
            match decode_index_entry(&self.index[off..]) {
                Ok((_, _, _, consumed)) => {
                    off += consumed;
                    n += 1;
                }
                Err(_) => break,
            }
        }
        n
    }
}

/// Flush a sorted entry stream to one SSTable under `dir`, or skip it.
///
/// Returns `Ok(Some(path))` for a written file, or `Ok(None)` if `entries` was
/// empty (we never write a 0-entry SSTable — phase-03 §5).
pub fn write_sstable<I>(dir: &Path, file_number: u64, entries: I) -> io::Result<Option<PathBuf>>
where
    I: IntoIterator<Item = (Vec<u8>, Value)>,
{
    let mut it = entries.into_iter().peekable();
    if it.peek().is_none() {
        log::debug!(target: "sstable", "skipping flush: no entries");
        return Ok(None);
    }
    let mut w = SstWriter::create(dir, file_number)?;
    for (key, value) in it {
        w.add(&key, &value)?;
    }
    Ok(Some(w.finish()?))
}

// ────────────────────────────────────────────────────────────────────────────
// Task 3 — the reader: footer -> in-RAM sparse index -> one-block point reads.
// ────────────────────────────────────────────────────────────────────────────

/// One in-memory sparse-index entry: a block's first key and where it lives.
#[derive(Debug)]
struct IndexEntry {
    first_key: Vec<u8>,
    offset: u64,
    len: u32,
}

/// A read-only handle onto one immutable SSTable.
///
/// On [`open`](SstReader::open) it loads the footer and the (small) sparse index
/// into RAM; the data blocks stay on disk and are read one at a time on demand.
/// All reads are positioned (`&self`, no shared file cursor), so a single reader
/// can serve many threads concurrently — which is exactly what SSTable
/// immutability buys us.
#[derive(Debug)]
pub struct SstReader {
    file: File,
    index: Vec<IndexEntry>,
    path: PathBuf,
    /// Count of data blocks pulled from disk — instrumentation for the
    /// sparse-index test (proves a `get` reads one block, not the whole file).
    block_reads: AtomicU64,
}

impl SstReader {
    /// Open an SSTable: read+validate the footer, load the sparse index into RAM.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let file_len = file.metadata()?.len();
        if file_len < FOOTER_LEN as u64 {
            return Err(to_io(SstFormatError::UnexpectedEof));
        }

        let mut footer_buf = [0u8; FOOTER_LEN];
        read_exact_at(&file, &mut footer_buf, file_len - FOOTER_LEN as u64)?;
        let footer = decode_footer(&footer_buf).map_err(to_io)?;

        let mut index_buf = vec![0u8; footer.index_len as usize];
        read_exact_at(&file, &mut index_buf, footer.index_offset)?;

        let mut index = Vec::new();
        let mut off = 0usize;
        while off < index_buf.len() {
            let (first_key, offset, len, consumed) =
                decode_index_entry(&index_buf[off..]).map_err(to_io)?;
            index.push(IndexEntry { first_key, offset, len });
            off += consumed;
        }

        Ok(SstReader { file, index, path, block_reads: AtomicU64::new(0) })
    }

    /// Look up `key`. Returns the on-disk marker: `Some(Put)`, `Some(Delete)`
    /// (a tombstone — the caller must treat this as "found, not-found" and stop
    /// searching older SSTables), or `None` (key not in this file).
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Value>> {
        // Sparse index → the one block that could contain `key`.
        let block_idx = match self.candidate_block(key) {
            Some(i) => i,
            None => return Ok(None), // key sorts before every block's first key
        };
        let entry = &self.index[block_idx];
        let block = self.read_block(entry.offset, entry.len)?;

        // Scan the block. Entries are sorted, so once we pass `key` it's absent.
        let mut off = 0usize;
        while off < block.len() {
            let (k, v, consumed) = decode_entry(&block[off..]).map_err(to_io)?;
            match k.as_slice().cmp(key) {
                Ordering::Equal => return Ok(Some(v)),
                Ordering::Greater => return Ok(None),
                Ordering::Less => off += consumed,
            }
        }
        Ok(None)
    }

    /// Every entry (tombstones included) in sorted order — for compaction (Phase 5)
    /// and tests. Reads the whole file.
    pub fn entries(&self) -> io::Result<Vec<(Vec<u8>, Value)>> {
        let mut out = Vec::new();
        for e in &self.index {
            let block = self.read_block(e.offset, e.len)?;
            let mut off = 0usize;
            while off < block.len() {
                let (k, v, consumed) = decode_entry(&block[off..]).map_err(to_io)?;
                out.push((k, v));
                off += consumed;
            }
        }
        Ok(out)
    }

    /// The last block whose first key is `<= key`, or `None` if `key` is smaller
    /// than every block's first key (so it can't be in this file).
    fn candidate_block(&self, key: &[u8]) -> Option<usize> {
        let p = self.index.partition_point(|e| e.first_key.as_slice() <= key);
        if p > 0 {
            Some(p - 1)
        } else {
            None
        }
    }

    /// Read one data block from disk (positioned read), counting it.
    fn read_block(&self, offset: u64, len: u32) -> io::Result<Vec<u8>> {
        self.block_reads.fetch_add(1, atomic::Ordering::Relaxed);
        let mut buf = vec![0u8; len as usize];
        read_exact_at(&self.file, &mut buf, offset)?;
        Ok(buf)
    }

    /// Number of data blocks read from disk so far (test instrumentation).
    pub fn block_reads(&self) -> u64 {
        self.block_reads.load(atomic::Ordering::Relaxed)
    }

    /// The file this reader was opened from.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn to_io(e: SstFormatError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

/// Positioned `read_exact` that does not disturb any shared file cursor, so it is
/// safe to call on a `&File` from multiple threads. Uses `pread` (unix) /
/// `seek_read` (windows); a portable clone-and-seek fallback elsewhere.
#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    // `seek_read` (like pread) may return a short read; loop until the buffer fills.
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "SSTable read past EOF"))
            }
            Ok(n) => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = file.try_clone()?;
    f.seek(SeekFrom::Start(offset))?;
    f.read_exact(buf)
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

    // ── Task 2: SstWriter ────────────────────────────────────────────────────

    use crate::testutil::TempDir;

    /// A mini-reader for tests: parse a full SSTable's bytes via footer → index →
    /// blocks, returning all entries in file order plus the block count. Doubles
    /// as an end-to-end check of the on-disk structure (the real reader is Task 3).
    fn parse_sst(bytes: &[u8]) -> (Vec<(Vec<u8>, Value)>, usize) {
        let footer = decode_footer(&bytes[bytes.len() - FOOTER_LEN..]).unwrap();
        let idx_start = footer.index_offset as usize;
        let idx_end = idx_start + footer.index_len as usize;
        let index_bytes = &bytes[idx_start..idx_end];

        let mut blocks = Vec::new();
        let mut off = 0;
        while off < index_bytes.len() {
            let (fk, boff, blen, n) = decode_index_entry(&index_bytes[off..]).unwrap();
            blocks.push((fk, boff, blen));
            off += n;
        }

        let mut entries = Vec::new();
        for (_, boff, blen) in &blocks {
            let block = &bytes[*boff as usize..*boff as usize + *blen as usize];
            let mut o = 0;
            while o < block.len() {
                let (k, v, n) = decode_entry(&block[o..]).unwrap();
                entries.push((k, v));
                o += n;
            }
        }
        (entries, blocks.len())
    }

    fn write_and_read(dir: &TempDir, num: u64, entries: Vec<(Vec<u8>, Value)>) -> (Vec<(Vec<u8>, Value)>, usize, PathBuf) {
        let path = write_sstable(&dir.0, num, entries).unwrap().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let (parsed, blocks) = parse_sst(&bytes);
        (parsed, blocks, path)
    }

    #[test]
    fn writes_a_readable_structure() {
        let dir = TempDir::new();
        let entries = vec![
            (b"a".to_vec(), Value::Put(b"1".to_vec())),
            (b"b".to_vec(), Value::Delete),
            (b"c".to_vec(), Value::Put(b"3".to_vec())),
        ];
        let (parsed, blocks, _) = write_and_read(&dir, 1, entries.clone());
        assert_eq!(parsed, entries); // exact round-trip, sorted order preserved
        assert_eq!(blocks, 1); // small: one block
    }

    #[test]
    fn no_tmp_file_remains_after_write() {
        let dir = TempDir::new();
        let path = write_sstable(&dir.0, 7, vec![(b"k".to_vec(), Value::Put(b"v".to_vec()))])
            .unwrap()
            .unwrap();
        assert!(path.exists());
        assert_eq!(path.file_name().unwrap(), "000007.sst");
        assert!(!dir.0.join("000007.sst.tmp").exists());
    }

    #[test]
    fn empty_stream_writes_no_file() {
        let dir = TempDir::new();
        let entries: Vec<(Vec<u8>, Value)> = Vec::new();
        assert!(write_sstable(&dir.0, 1, entries).unwrap().is_none());
        assert!(!dir.0.join("000001.sst").exists());
        assert!(!dir.0.join("000001.sst.tmp").exists());
    }

    #[test]
    fn many_entries_span_multiple_blocks() {
        let dir = TempDir::new();
        // ~200 bytes/entry * 100 entries ≈ 20 KB → several ~4 KB blocks.
        let entries: Vec<_> = (0..100u32)
            .map(|i| (format!("key{i:05}").into_bytes(), Value::Put(vec![b'x'; 180])))
            .collect();
        let (parsed, blocks, _) = write_and_read(&dir, 2, entries.clone());
        assert_eq!(parsed, entries);
        assert!(blocks >= 2, "expected multiple blocks, got {blocks}");
    }

    #[test]
    fn oversized_entry_gets_its_own_block() {
        let dir = TempDir::new();
        // One entry bigger than the block target, then a small one after it.
        let big = vec![b'z'; BLOCK_TARGET * 2];
        let entries = vec![
            (b"big".to_vec(), Value::Put(big.clone())),
            (b"small".to_vec(), Value::Put(b"s".to_vec())),
        ];
        let (parsed, blocks, _) = write_and_read(&dir, 3, entries.clone());
        assert_eq!(parsed, entries); // oversized entry not split
        assert_eq!(blocks, 2); // big alone, then small
    }

    #[test]
    fn tombstones_are_persisted() {
        let dir = TempDir::new();
        let entries = vec![
            (b"alive".to_vec(), Value::Put(b"v".to_vec())),
            (b"dead".to_vec(), Value::Delete),
        ];
        let (parsed, _, _) = write_and_read(&dir, 4, entries.clone());
        assert_eq!(parsed[1], (b"dead".to_vec(), Value::Delete));
    }

    #[test]
    fn flushing_a_memtable_round_trips() {
        // The real usage: hand the writer a memtable's sorted iterator.
        use crate::memtable::Memtable;
        let mem = Memtable::new();
        mem.put(b"c", b"3");
        mem.put(b"a", b"1");
        mem.delete(b"b");
        let dir = TempDir::new();
        let (parsed, _, _) = write_and_read(&dir, 5, mem.iter().collect());
        assert_eq!(
            parsed,
            vec![
                (b"a".to_vec(), Value::Put(b"1".to_vec())),
                (b"b".to_vec(), Value::Delete),
                (b"c".to_vec(), Value::Put(b"3".to_vec())),
            ]
        );
    }

    // ── Task 3: SstReader ────────────────────────────────────────────────────

    fn write_reader(dir: &TempDir, num: u64, entries: Vec<(Vec<u8>, Value)>) -> SstReader {
        let path = write_sstable(&dir.0, num, entries).unwrap().unwrap();
        SstReader::open(path).unwrap()
    }

    #[test]
    fn get_present_and_absent() {
        let dir = TempDir::new();
        let r = write_reader(
            &dir,
            1,
            vec![
                (b"a".to_vec(), Value::Put(b"1".to_vec())),
                (b"c".to_vec(), Value::Put(b"3".to_vec())),
                (b"e".to_vec(), Value::Put(b"5".to_vec())),
            ],
        );
        assert_eq!(r.get(b"a").unwrap(), Some(Value::Put(b"1".to_vec())));
        assert_eq!(r.get(b"e").unwrap(), Some(Value::Put(b"5".to_vec())));
        assert_eq!(r.get(b"z").unwrap(), None); // past the end
        assert_eq!(r.get(b"b").unwrap(), None); // within range, absent
    }

    #[test]
    fn tombstone_returns_delete_marker() {
        let dir = TempDir::new();
        let r = write_reader(&dir, 2, vec![(b"dead".to_vec(), Value::Delete)]);
        // The reader surfaces the tombstone; the read path treats it as not-found
        // but stops searching older SSTables.
        assert_eq!(r.get(b"dead").unwrap(), Some(Value::Delete));
    }

    #[test]
    fn entries_returns_all_in_sorted_order() {
        let dir = TempDir::new();
        let entries = vec![
            (b"a".to_vec(), Value::Put(b"1".to_vec())),
            (b"b".to_vec(), Value::Delete),
            (b"c".to_vec(), Value::Put(b"3".to_vec())),
        ];
        let r = write_reader(&dir, 3, entries.clone());
        assert_eq!(r.entries().unwrap(), entries);
    }

    #[test]
    fn get_reads_exactly_one_block() {
        let dir = TempDir::new();
        // Force many blocks.
        let entries: Vec<_> = (0..200u32)
            .map(|i| (format!("key{i:05}").into_bytes(), Value::Put(vec![b'x'; 100])))
            .collect();
        let r = write_reader(&dir, 4, entries);
        assert!(r.index.len() >= 3, "test needs multiple blocks, got {}", r.index.len());

        // A key in a middle block: exactly one block is pulled from disk.
        assert_eq!(r.block_reads(), 0);
        let got = r.get(b"key00100").unwrap();
        assert_eq!(got, Some(Value::Put(vec![b'x'; 100])));
        assert_eq!(r.block_reads(), 1, "a get must read exactly one block");

        // A second get reads exactly one more.
        let _ = r.get(b"key00150").unwrap();
        assert_eq!(r.block_reads(), 2);
    }

    #[test]
    fn get_below_first_key_reads_no_block() {
        let dir = TempDir::new();
        let r = write_reader(&dir, 5, vec![(b"m".to_vec(), Value::Put(b"v".to_vec()))]);
        assert_eq!(r.get(b"a").unwrap(), None); // sorts before every block
        assert_eq!(r.block_reads(), 0, "absent-below-range must not touch a block");
    }

    #[test]
    fn open_rejects_a_non_sstable_file() {
        let dir = TempDir::new();
        let path = dir.path("garbage.sst");
        std::fs::write(&path, b"this is definitely not an sstable file").unwrap();
        let err = SstReader::open(&path).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn reader_round_trips_across_blocks() {
        let dir = TempDir::new();
        let entries: Vec<_> = (0..500u32)
            .map(|i| (format!("k{i:06}").into_bytes(), Value::Put(format!("v{i}").into_bytes())))
            .collect();
        let r = write_reader(&dir, 6, entries.clone());
        for (k, v) in &entries {
            assert_eq!(r.get(k).unwrap(), Some(v.clone()));
        }
    }
}
