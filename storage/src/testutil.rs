//! Test-only shared helpers. Std-only (we intentionally carry no `tempfile` dep;
//! see `storage/Cargo.toml` for why the windows-gnu toolchain rules it out).

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique temporary directory, recursively removed when dropped. Enough for
/// tests; not a general-purpose `tempfile` replacement.
pub struct TempDir(pub PathBuf);

impl TempDir {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!("quorumkv-test-{}-{}", std::process::id(), n));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }

    /// A path to `name` inside this temp dir.
    pub fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Append raw bytes to an existing file and fsync — used to inject torn/partial
/// tails a normal `WalWriter` would never produce.
pub fn append_raw(path: &Path, bytes: &[u8]) {
    let mut f = OpenOptions::new().append(true).open(path).unwrap();
    f.write_all(bytes).unwrap();
    f.sync_all().unwrap();
}
