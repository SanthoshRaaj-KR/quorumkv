//go:build !windows

package consensus

import "os"

// fsyncDir flushes a directory entry so a rename survives a crash. Mirrors the
// Rust engine's fsync_dir (storage/src/wal.rs).
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
