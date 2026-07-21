//! Blocked Bloom filter (Phase 4) — see `planning/phase-04-bloom.md`.
//!
//! A compact, RAM-resident "is this key **definitely not** here?" test placed in
//! front of each SSTable. Its guarantees are asymmetric:
//!
//! - `maybe_contains` == **false** → the key is *definitely* absent. Skip the
//!   file with zero disk I/O.
//! - `maybe_contains` == **true** → the key *might* be present (or a false
//!   positive). Do the real Phase 3 lookup.
//!
//! It **never** returns a false negative *by construction* — every key inserted
//! sets bits that the same key's query then checks. The single rule the whole
//! design rests on: **every key written to an SSTable is inserted, `Put` and
//! `Delete` alike** (§1), or a bloom-skip would resurrect a deleted key.
//!
//! ## Blocked layout (§2)
//!
//! The bit array is a sequence of **64-byte (512-bit) blocks = one CPU cache
//! line each**. A key's `k` bits all land in *one* block, so a lookup is a single
//! cache-line touch. We hash the key once with **xxh3**, use the high 32 bits to
//! pick the block, and derive the `k` positions within it by Kirsch–Mitzenmacher
//! double hashing (`h1 + i·h2`).
//!
//! The filter is built once during a flush and never mutated after — so it is
//! shared read-only (`Arc<SstReader>`) and queried lock-free.

use xxhash_rust::xxh3::xxh3_64;

/// Bits per block: 512 = one 64-byte cache line.
const BLOCK_BITS: u32 = 512;
/// Bytes per block.
const BLOCK_BYTES: usize = 64;
/// Hash algorithm/seed version, stored with the filter so a filter written today
/// is always checked with the identical hash (§6 hash-seed stability).
const HASH_VERSION: u8 = 1;

/// Default bits-per-key (~1% false-positive target). A config knob (§2).
pub const DEFAULT_BITS_PER_KEY: u32 = 10;

/// Header before the bit array in the serialized form:
/// `k(4) | block_count(8) | bits_per_key(4) | hash_version(1)`.
const SER_HEADER_LEN: usize = 4 + 8 + 4 + 1;

/// A blocked Bloom filter over a set of keys.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// `block_count * 64` bytes.
    bits: Vec<u8>,
    /// Number of 512-bit blocks.
    block_count: u64,
    /// Bits set/checked per key.
    k: u32,
    /// Configured bits-per-key (kept for info / rebuild).
    bits_per_key: u32,
}

/// Why a serialized bloom block could not be decoded. On any of these the caller
/// (the SSTable reader) rebuilds the filter from the data — never a data-loss event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BloomError {
    /// Fewer bytes than a header + CRC needs.
    TooShort,
    /// CRC32C over the block did not match.
    CrcMismatch,
    /// Serialized with a hash version this build doesn't understand.
    BadHashVersion(u8),
    /// The bit-array length disagrees with the stored block count.
    Malformed,
}

impl BloomFilter {
    /// A new, empty filter sized for `num_keys` at `bits_per_key`.
    pub fn new(num_keys: usize, bits_per_key: u32) -> Self {
        let bits_per_key = bits_per_key.max(1);
        let total_bits = (num_keys as u64).saturating_mul(u64::from(bits_per_key)).max(1);
        let block_count = total_bits.div_ceil(u64::from(BLOCK_BITS)).max(1);
        let k = optimal_k(bits_per_key);
        BloomFilter {
            bits: vec![0u8; block_count as usize * BLOCK_BYTES],
            block_count,
            k,
            bits_per_key,
        }
    }

    /// Build a filter over `keys` (which must number `num_keys`).
    pub fn build<'a, I>(keys: I, num_keys: usize, bits_per_key: u32) -> Self
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let mut f = BloomFilter::new(num_keys, bits_per_key);
        for key in keys {
            f.insert(key);
        }
        f
    }

    /// Insert a key (sets its `k` bits within its block).
    pub fn insert(&mut self, key: &[u8]) {
        let (base, h1, h2) = self.locate(key);
        for i in 0..self.k {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2)) % BLOCK_BITS) as usize;
            self.bits[base + bit / 8] |= 1u8 << (bit % 8);
        }
    }

    /// Test a key. `false` = definitely absent; `true` = maybe present.
    pub fn maybe_contains(&self, key: &[u8]) -> bool {
        let (base, h1, h2) = self.locate(key);
        for i in 0..self.k {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2)) % BLOCK_BITS) as usize;
            if self.bits[base + bit / 8] & (1u8 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Block byte-offset plus the two 32-bit hash halves for double hashing.
    /// `h2` is forced odd so the stride is coprime with 512 (better spread).
    fn locate(&self, key: &[u8]) -> (usize, u32, u32) {
        let h = xxh3_64(key);
        // Multiply-shift maps the high 32 bits uniformly onto [0, block_count).
        let block = (((h >> 32) as u128 * self.block_count as u128) >> 32) as usize;
        (block * BLOCK_BYTES, h as u32, ((h >> 32) as u32) | 1)
    }

    /// Serialize to a self-describing, CRC32C-protected byte block (§4).
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(SER_HEADER_LEN + self.bits.len() + 4);
        buf.extend_from_slice(&self.k.to_le_bytes());
        buf.extend_from_slice(&self.block_count.to_le_bytes());
        buf.extend_from_slice(&self.bits_per_key.to_le_bytes());
        buf.push(HASH_VERSION);
        buf.extend_from_slice(&self.bits);
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decode a serialized bloom block, verifying its CRC and structure. On any
    /// error the caller rebuilds from the SSTable's keys instead.
    pub fn deserialize(buf: &[u8]) -> Result<Self, BloomError> {
        if buf.len() < SER_HEADER_LEN + 4 {
            return Err(BloomError::TooShort);
        }
        let crc_pos = buf.len() - 4;
        let stored_crc = u32::from_le_bytes(buf[crc_pos..].try_into().unwrap());
        if crc32c::crc32c(&buf[..crc_pos]) != stored_crc {
            return Err(BloomError::CrcMismatch);
        }

        let k = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let block_count = u64::from_le_bytes(buf[4..12].try_into().unwrap());
        let bits_per_key = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let hash_version = buf[16];
        if hash_version != HASH_VERSION {
            return Err(BloomError::BadHashVersion(hash_version));
        }

        let bits = buf[SER_HEADER_LEN..crc_pos].to_vec();
        if bits.len() as u64 != block_count.saturating_mul(BLOCK_BYTES as u64) {
            return Err(BloomError::Malformed);
        }
        Ok(BloomFilter { bits, block_count, k, bits_per_key })
    }

    /// Bits set per key.
    pub fn k(&self) -> u32 {
        self.k
    }

    /// Configured bits-per-key.
    pub fn bits_per_key(&self) -> u32 {
        self.bits_per_key
    }

    /// Size of the bit array in bytes.
    pub fn size_bytes(&self) -> usize {
        self.bits.len()
    }
}

/// Optimal number of bits to set per key: `k ≈ 0.7 · bits_per_key`, clamped so
/// all `k` bits fit comfortably in a 64-byte block (§2 tuning).
fn optimal_k(bits_per_key: u32) -> u32 {
    ((f64::from(bits_per_key) * 0.7).round() as u32).clamp(1, 8)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(prefix: &str, n: usize) -> Vec<Vec<u8>> {
        (0..n).map(|i| format!("{prefix}{i}").into_bytes()).collect()
    }

    #[test]
    fn no_false_negatives_is_absolute() {
        // THE safety test: every inserted key must test positive. Not one miss.
        let ks = keys("key-", 5_000);
        let f = BloomFilter::build(ks.iter().map(|k| k.as_slice()), ks.len(), 10);
        for k in &ks {
            assert!(f.maybe_contains(k), "false negative for {:?}", String::from_utf8_lossy(k));
        }
    }

    #[test]
    fn absent_keys_mostly_test_negative() {
        // Not a correctness test (false positives are allowed) — a sanity check
        // that the filter actually filters, at a rate near the ~1% target.
        let ks = keys("present-", 2_000);
        let f = BloomFilter::build(ks.iter().map(|k| k.as_slice()), ks.len(), 10);

        let queries = 20_000;
        let positives = (0..queries)
            .filter(|i| f.maybe_contains(format!("absent-{i}").as_bytes()))
            .count();
        let rate = positives as f64 / queries as f64;
        assert!(rate < 0.05, "false-positive rate {rate:.4} too high (bits distribute badly?)");
    }

    #[test]
    fn empty_filter_reports_absent() {
        let f = BloomFilter::new(0, 10);
        assert!(!f.maybe_contains(b"anything"));
    }

    #[test]
    fn k_follows_bits_per_key() {
        assert_eq!(optimal_k(10), 7);
        assert_eq!(optimal_k(1), 1); // clamped floor
        assert_eq!(optimal_k(100), 8); // clamped ceiling
    }

    #[test]
    fn serialize_round_trips_and_preserves_membership() {
        let ks = keys("k", 1_000);
        let f = BloomFilter::build(ks.iter().map(|k| k.as_slice()), ks.len(), 12);
        let bytes = f.serialize();
        let g = BloomFilter::deserialize(&bytes).unwrap();

        assert_eq!(g.k(), f.k());
        assert_eq!(g.bits_per_key(), 12);
        assert_eq!(g.size_bytes(), f.size_bytes());
        // Identical membership answers, present and absent.
        for k in &ks {
            assert!(g.maybe_contains(k));
        }
        for i in 0..1_000 {
            let q = format!("absent-{i}");
            assert_eq!(g.maybe_contains(q.as_bytes()), f.maybe_contains(q.as_bytes()));
        }
    }

    #[test]
    fn corrupt_crc_is_detected() {
        let f = BloomFilter::build([b"a".as_slice(), b"b".as_slice()], 2, 10);
        let mut bytes = f.serialize();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF; // flip a bit-array byte
        assert_eq!(BloomFilter::deserialize(&bytes).unwrap_err(), BloomError::CrcMismatch);
    }

    #[test]
    fn too_short_is_rejected() {
        assert_eq!(BloomFilter::deserialize(&[0u8; 4]).unwrap_err(), BloomError::TooShort);
    }

    #[test]
    fn size_grows_with_bits_per_key() {
        let small = BloomFilter::new(1_000, 10);
        let big = BloomFilter::new(1_000, 20);
        assert!(big.size_bytes() > small.size_bytes());
    }
}
