package consensus

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// File names within a node's data directory.
const (
	HardStateName = "raft-hardstate"
	LogName       = "raft-log"
)

// castagnoli is CRC32C — the same polynomial the Rust WAL uses (SSE4.2
// accelerated on amd64), so both halves of quorumkv checksum identically.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Storage is Raft's stable storage. The driver — never [Node] — calls it.
type Storage interface {
	LoadHardState() (HardState, error)
	SaveHardState(hs HardState) error
	LoadEntries() ([]Entry, error)
	AppendEntries(entries []Entry) error
	// TruncateFrom durably discards every entry with index >= i (Phase 8).
	TruncateFrom(i uint64) error
	Close() error
}

// ─── FileStorage ─────────────────────────────────────────────────────────────

// FileStorage keeps Raft's persistent state in two files with two lifetimes (§4):
//
//   - raft-hardstate — currentTerm + votedFor. ~20 bytes, rewritten on every
//     term bump and every vote. Written tmp → fsync → rename → fsync dir, so a
//     crash mid-write leaves the previous state rather than a torn one.
//   - raft-log — the entries, append-only, framed exactly like the Phase 1 WAL:
//     crc32c(4) || length(4) || payload, little-endian, CRC over length||payload.
//     A torn tail is dropped on replay; that entry was never acknowledged.
//
// Raft's log is not the LSM's WAL: Phase 8 overwrites conflicting suffixes, so
// this type also maintains an in-memory index→offset table to make TruncateFrom
// a seek + File.Truncate.
type FileStorage struct {
	dir     string
	logFile *os.File
	// offsets[k] is the byte offset of the record holding log index k+1.
	offsets []int64
	size    int64
}

var _ Storage = (*FileStorage)(nil)

// OpenFileStorage opens (creating if absent) the Raft state under dir, replaying
// raft-log to rebuild the index→offset table and dropping any torn tail.
func OpenFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("consensus: create %s: %w", dir, err)
	}
	f, err := os.OpenFile(filepath.Join(dir, LogName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("consensus: open %s: %w", LogName, err)
	}
	s := &FileStorage{dir: dir, logFile: f}
	if err := s.replay(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

// replay scans raft-log, building the offset table. It stops at the first record
// that is incomplete or fails its CRC and truncates the file there — a torn tail
// is a clean end-of-log, not corruption (the same rule as the Phase 1 WAL).
func (s *FileStorage) replay() error {
	raw, err := io.ReadAll(s.logFile)
	if err != nil {
		return fmt.Errorf("consensus: read %s: %w", LogName, err)
	}
	s.offsets = s.offsets[:0]

	var off int64
	var wantIndex uint64 = 1
	for {
		e, n, derr := decodeEntry(raw[off:])
		if derr != nil {
			break // incomplete or bad CRC → the log ends here
		}
		if e.Index != wantIndex {
			// CRC passed but the sequence is broken: refuse rather than guess.
			return fmt.Errorf("consensus: %s: entry index %d out of sequence (want %d)", LogName, e.Index, wantIndex)
		}
		s.offsets = append(s.offsets, off)
		off += int64(n)
		wantIndex++
	}

	if off != int64(len(raw)) {
		// Drop the torn tail so the next append starts from a clean boundary.
		if err := s.logFile.Truncate(off); err != nil {
			return fmt.Errorf("consensus: truncate torn tail: %w", err)
		}
		if err := s.logFile.Sync(); err != nil {
			return fmt.Errorf("consensus: sync after truncate: %w", err)
		}
	}
	s.size = off
	if _, err := s.logFile.Seek(off, io.SeekStart); err != nil {
		return fmt.Errorf("consensus: seek: %w", err)
	}
	return nil
}

// LoadEntries returns every durable entry, ascending from index 1.
func (s *FileStorage) LoadEntries() ([]Entry, error) {
	if _, err := s.logFile.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(io.LimitReader(s.logFile, s.size))
	if err != nil {
		return nil, err
	}
	if _, err := s.logFile.Seek(s.size, io.SeekStart); err != nil {
		return nil, err
	}
	out := make([]Entry, 0, len(s.offsets))
	var off int64
	for off < int64(len(raw)) {
		e, n, derr := decodeEntry(raw[off:])
		if derr != nil {
			break
		}
		out = append(out, e)
		off += int64(n)
	}
	return out, nil
}

// AppendEntries durably appends entries. One fsync covers the whole batch — the
// call does not return until every entry is on stable storage.
func (s *FileStorage) AppendEntries(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	buf := make([]byte, 0, 64*len(entries))
	offs := make([]int64, 0, len(entries))
	at := s.size
	for _, e := range entries {
		offs = append(offs, at)
		enc := encodeEntry(e)
		buf = append(buf, enc...)
		at += int64(len(enc))
	}
	if _, err := s.logFile.Write(buf); err != nil {
		return fmt.Errorf("consensus: append to %s: %w", LogName, err)
	}
	if err := s.logFile.Sync(); err != nil {
		return fmt.Errorf("consensus: fsync %s: %w", LogName, err)
	}
	s.offsets = append(s.offsets, offs...)
	s.size = at
	return nil
}

// TruncateFrom durably discards every entry with index >= i (Phase 8's log
// repair). Truncating past the end is a no-op.
func (s *FileStorage) TruncateFrom(i uint64) error {
	if i < 1 {
		return errors.New("consensus: TruncateFrom requires index >= 1")
	}
	if i > uint64(len(s.offsets)) {
		return nil
	}
	off := s.offsets[i-1] // offset of the record holding index i
	if err := s.logFile.Truncate(off); err != nil {
		return fmt.Errorf("consensus: truncate %s: %w", LogName, err)
	}
	if err := s.logFile.Sync(); err != nil {
		return fmt.Errorf("consensus: fsync after truncate: %w", err)
	}
	if _, err := s.logFile.Seek(off, io.SeekStart); err != nil {
		return err
	}
	s.offsets = s.offsets[:i-1]
	s.size = off
	return nil
}

// LoadHardState reads the persisted term and vote. A missing file is the zero
// state (term 0, no vote) — a node that has never run.
func (s *FileStorage) LoadHardState() (HardState, error) {
	raw, err := os.ReadFile(filepath.Join(s.dir, HardStateName))
	if errors.Is(err, os.ErrNotExist) {
		return HardState{}, nil
	}
	if err != nil {
		return HardState{}, fmt.Errorf("consensus: read %s: %w", HardStateName, err)
	}
	if len(raw) != 20 {
		return HardState{}, fmt.Errorf("consensus: %s: got %d bytes, want 20", HardStateName, len(raw))
	}
	if crc32.Checksum(raw[:16], castagnoli) != binary.LittleEndian.Uint32(raw[16:20]) {
		return HardState{}, fmt.Errorf("consensus: %s: checksum mismatch", HardStateName)
	}
	return HardState{
		Term:     binary.LittleEndian.Uint64(raw[0:8]),
		VotedFor: binary.LittleEndian.Uint64(raw[8:16]),
	}, nil
}

// SaveHardState durably records the term and vote, via tmp → fsync → rename →
// fsync dir. The payload is smaller than a sector so a torn write is close to
// impossible, but rename costs nothing here and removes the argument entirely.
func (s *FileStorage) SaveHardState(hs HardState) error {
	var buf [20]byte
	binary.LittleEndian.PutUint64(buf[0:8], hs.Term)
	binary.LittleEndian.PutUint64(buf[8:16], hs.VotedFor)
	binary.LittleEndian.PutUint32(buf[16:20], crc32.Checksum(buf[:16], castagnoli))

	final := filepath.Join(s.dir, HardStateName)
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("consensus: create %s: %w", tmp, err)
	}
	if _, err := f.Write(buf[:]); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("consensus: rename %s: %w", HardStateName, err)
	}
	return fsyncDir(s.dir)
}

// Close releases the log file handle.
func (s *FileStorage) Close() error { return s.logFile.Close() }

// ─── record codec ────────────────────────────────────────────────────────────
//
// Frame (Phase 1 §3, mirrored):
//
//	┌──────────┬──────────┬──────────────────────────────────┐
//	│ crc32c   │ length   │ payload                          │
//	│ 4 bytes  │ 4 bytes  │ term(8) index(8) clen(4) cmd     │
//	└──────────┴──────────┴──────────────────────────────────┘
//	  └── crc covers everything to its right: length || payload ──┘
//
// All integers little-endian.

const headerLen = 8

var errIncomplete = errors.New("consensus: incomplete record")

func encodeEntry(e Entry) []byte {
	payload := make([]byte, 20+len(e.Cmd))
	binary.LittleEndian.PutUint64(payload[0:8], e.Term)
	binary.LittleEndian.PutUint64(payload[8:16], e.Index)
	binary.LittleEndian.PutUint32(payload[16:20], uint32(len(e.Cmd)))
	copy(payload[20:], e.Cmd)

	buf := make([]byte, headerLen+len(payload))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(payload)))
	copy(buf[headerLen:], payload)
	binary.LittleEndian.PutUint32(buf[0:4], crc32.Checksum(buf[4:], castagnoli))
	return buf
}

// decodeEntry parses one record from the head of buf, returning it and the
// number of bytes consumed. Any error means "the log ends cleanly here".
func decodeEntry(buf []byte) (Entry, int, error) {
	if len(buf) < headerLen {
		return Entry{}, 0, errIncomplete
	}
	length := int(binary.LittleEndian.Uint32(buf[4:8]))
	total := headerLen + length
	if length < 20 || total > len(buf) {
		return Entry{}, 0, errIncomplete
	}
	if crc32.Checksum(buf[4:total], castagnoli) != binary.LittleEndian.Uint32(buf[0:4]) {
		return Entry{}, 0, errors.New("consensus: crc mismatch")
	}
	p := buf[headerLen:total]
	cmdLen := int(binary.LittleEndian.Uint32(p[16:20]))
	if 20+cmdLen != len(p) {
		return Entry{}, 0, errors.New("consensus: malformed record")
	}
	e := Entry{
		Term:  binary.LittleEndian.Uint64(p[0:8]),
		Index: binary.LittleEndian.Uint64(p[8:16]),
	}
	if cmdLen > 0 {
		e.Cmd = append([]byte(nil), p[20:]...)
	}
	return e, total, nil
}

// ─── MemStorage ──────────────────────────────────────────────────────────────

// MemStorage is a non-durable Storage for tests that are not about durability.
type MemStorage struct {
	hs      HardState
	entries []Entry
}

var _ Storage = (*MemStorage)(nil)

func NewMemStorage() *MemStorage { return &MemStorage{} }

func (m *MemStorage) LoadHardState() (HardState, error) { return m.hs, nil }
func (m *MemStorage) SaveHardState(hs HardState) error  { m.hs = hs; return nil }
func (m *MemStorage) LoadEntries() ([]Entry, error)     { return append([]Entry(nil), m.entries...), nil }
func (m *MemStorage) Close() error                      { return nil }

func (m *MemStorage) AppendEntries(entries []Entry) error {
	m.entries = append(m.entries, entries...)
	return nil
}

func (m *MemStorage) TruncateFrom(i uint64) error {
	if i < 1 {
		return errors.New("consensus: TruncateFrom requires index >= 1")
	}
	if i > uint64(len(m.entries)) {
		return nil
	}
	m.entries = m.entries[:i-1]
	return nil
}
