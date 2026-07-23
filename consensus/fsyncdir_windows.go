//go:build windows

package consensus

// fsyncDir is a no-op on Windows: directory handles cannot be flushed, and
// NTFS metadata for a rename is journalled by the filesystem itself. Same
// treatment as the Rust engine's fsync_dir (storage/src/wal.rs).
func fsyncDir(string) error { return nil }
