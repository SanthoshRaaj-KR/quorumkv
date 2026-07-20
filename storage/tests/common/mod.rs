//! Shared helpers for the integration tests. (`src/testutil.rs` is `#[cfg(test)]`
//! and only visible to unit tests, so integration tests need their own copy.)
#![allow(dead_code)] // each test binary compiles this; not every item is used by all.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique temporary directory, recursively removed when dropped.
pub struct TempDir(pub PathBuf);

impl TempDir {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!("quorumkv-it-{}-{}", std::process::id(), n));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
