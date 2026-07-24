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
	SnapshotName  = "raft-snapshot"
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
	// SaveSnapshot durably records a new snapshot boundary and its opaque
	// state-machine bytes, tmp → fsync → rename → fsync dir (Phase 9 §6).
	SaveSnapshot(snap Snapshot) error
	// LoadSnapshot returns the most recent snapshot, or ok=false if none has
	// ever been taken.
	LoadSnapshot() (snap Snapshot, ok bool, err error)
	// CompactLog durably drops every log record at or below index, keeping
	// whatever survives above it (Phase 9 §6). index must be >= the log's
	// current boundary.
	CompactLog(index uint64) error
	Close() error
}

// ─── FileStorage ─────────────────────────────────────────────────────────────

// FileStorage keeps Raft's persistent state in three files with three lifetimes
// (§4, Phase 9 §6):
//
//   - raft-hardstate — currentTerm + votedFor. ~20 bytes, rewritten on every
//     term bump and every vote. Written tmp → fsync → rename → fsync dir, so a
//     crash mid-write leaves the previous state rather than a torn one.
//   - raft-log — the entries, append-only, framed exactly like the Phase 1 WAL:
//     crc32c(4) || length(4) || payload, little-endian, CRC over length||payload.
//     A torn tail is dropped on replay; that entry was never acknowledged.
//   - raft-snapshot — a single blob (same frame shape) holding the last
//     snapshot's lastIncludedIndex, lastIncludedTerm and opaque state bytes.
//     Whole-file replace, same tmp → fsync → rename → fsync dir discipline.
//
// Raft's log is not the LSM's WAL: Phase 8 overwrites conflicting suffixes and
// Phase 9 drops compacted prefixes, so this type also maintains an in-memory
// index→offset table to make TruncateFrom and CompactLog a seek + rewrite
// rather than a full log replay.
type FileStorage struct {
	dir     string
	logFile *os.File
	// firstIndex is the index of the first record actually held in raft-log:
	// 1 until the log is ever compacted, thereafter the snapshot's
	// lastIncludedIndex + 1.
	firstIndex uint64
	// offsets[k] is the byte offset of the record holding log index
	// firstIndex+k.
	offsets []int64
	size    int64

	// The last snapshot, cached in memory (Phase 9 §1 flags this as the thing
	// Phase 10 must change once the blob is the LSM's SSTable set rather than a
	// small slice).
	hasSnapshot bool
	snapshot    Snapshot
}

var _ Storage = (*FileStorage)(nil)

// OpenFileStorage opens (creating if absent) the Raft state under dir, loading
// any snapshot first (so replay knows where the surviving log is expected to
// start) and then replaying raft-log, dropping any torn tail.
func OpenFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("consensus: create %s: %w", dir, err)
	}
	s := &FileStorage{dir: dir, firstIndex: 1}

	snap, ok, err := loadSnapshotFile(dir)
	if err != nil {
		return nil, err
	}
	if ok {
		s.hasSnapshot = true
		s.snapshot = snap
		s.firstIndex = snap.Index + 1
	}

	f, err := os.OpenFile(filepath.Join(dir, LogName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("consensus: open %s: %w", LogName, err)
	}
	s.logFile = f
	if err := s.replay(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

// replay scans raft-log, building the offset table. It stops at the first
// record that is incomplete or fails its CRC and truncates the file there — a
// torn tail is a clean end-of-log, not corruption (the same rule as the
// Phase 1 WAL).
//
// It also tolerates the *first* record not matching firstIndex: a crash
// between SaveSnapshot and the log-file compaction it precedes (Phase 9 §7)
// can leave a snapshot on disk paired with a log that hasn't been trimmed to
// match it yet — including, for an installed snapshot, a divergent tail that
// must never be resurrected. Either way the snapshot is authoritative, so a
// mismatched log is treated as empty rather than trusted; nothing committed is
// lost cluster-wide, since a majority still holds it and the current leader
// will resend whatever this node is now missing.
func (s *FileStorage) replay() error {
	raw, err := io.ReadAll(s.logFile)
	if err != nil {
		return fmt.Errorf("consensus: read %s: %w", LogName, err)
	}
	s.offsets = s.offsets[:0]

	var off int64
	wantIndex := s.firstIndex
	for {
		e, n, derr := decodeEntry(raw[off:])
		if derr != nil {
			break // incomplete or bad CRC → the log ends here
		}
		if e.Index != wantIndex {
			if len(s.offsets) == 0 {
				break // see the doc comment above: treat as empty, not corrupt
			}
			// CRC passed but the sequence is broken past the first record:
			// refuse rather than guess.
			return fmt.Errorf("consensus: %s: entry index %d out of sequence (want %d)", LogName, e.Index, wantIndex)
		}
		s.offsets = append(s.offsets, off)
		off += int64(n)
		wantIndex++
	}

	if off != int64(len(raw)) {
		// Drop the torn tail (or, per the mismatch case above, everything) so
		// the next append starts from a clean boundary.
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

// LoadEntries returns every durable entry, ascending from the log's first
// surviving index (1 if never compacted).
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
// repair; also used by Phase 9 to wipe a superseded tail before installing a
// snapshot). i must be >= the log's first surviving index; truncating past the
// end is a no-op.
func (s *FileStorage) TruncateFrom(i uint64) error {
	if i < s.firstIndex {
		return fmt.Errorf("consensus: TruncateFrom(%d) is below the log's first index (%d)", i, s.firstIndex)
	}
	pos := i - s.firstIndex
	if pos >= uint64(len(s.offsets)) {
		return nil // nothing to drop
	}
	off := s.offsets[pos]
	if err := s.logFile.Truncate(off); err != nil {
		return fmt.Errorf("consensus: truncate %s: %w", LogName, err)
	}
	if err := s.logFile.Sync(); err != nil {
		return fmt.Errorf("consensus: fsync after truncate: %w", err)
	}
	if _, err := s.logFile.Seek(off, io.SeekStart); err != nil {
		return err
	}
	s.offsets = s.offsets[:pos]
	s.size = off
	return nil
}

// CompactLog durably drops every log record at or below index, rewriting the
// file to hold only the surviving tail: copy it to raft-log.tmp, fsync,
// rename, rebuild the offset table (Phase 9 §6). O(surviving log), run rarely.
// A no-op if index is already at or below the current boundary.
func (s *FileStorage) CompactLog(index uint64) error {
	boundary := s.firstIndex - 1
	if index < boundary {
		return fmt.Errorf("consensus: CompactLog(%d) is behind the log's current boundary (%d)", index, boundary)
	}
	if index == boundary {
		return nil
	}
	drop := index - boundary
	if drop > uint64(len(s.offsets)) {
		drop = uint64(len(s.offsets))
	}

	var survivingOff int64
	if drop < uint64(len(s.offsets)) {
		survivingOff = s.offsets[drop]
	} else {
		survivingOff = s.size
	}

	tail := make([]byte, s.size-survivingOff)
	if len(tail) > 0 {
		if _, err := s.logFile.ReadAt(tail, survivingOff); err != nil {
			return fmt.Errorf("consensus: read surviving tail of %s: %w", LogName, err)
		}
	}

	tmp := filepath.Join(s.dir, LogName+".tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("consensus: create %s: %w", tmp, err)
	}
	if _, err := f.Write(tail); err != nil {
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

	// Close the old handle before renaming over it — portable across
	// filesystems that don't like a rename-over-open-file, not just POSIX ones.
	if err := s.logFile.Close(); err != nil {
		return err
	}
	final := filepath.Join(s.dir, LogName)
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("consensus: rename %s: %w", LogName, err)
	}
	if err := fsyncDir(s.dir); err != nil {
		return err
	}
	newFile, err := os.OpenFile(final, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("consensus: reopen %s: %w", LogName, err)
	}
	s.logFile = newFile

	newOffsets := make([]int64, len(s.offsets)-int(drop))
	for i, off := range s.offsets[drop:] {
		newOffsets[i] = off - survivingOff
	}
	s.offsets = newOffsets
	s.size = int64(len(tail))
	s.firstIndex = index + 1
	if _, err := s.logFile.Seek(s.size, io.SeekStart); err != nil {
		return err
	}
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

// LoadSnapshot returns the most recent snapshot, cached in memory since it was
// last loaded or saved.
func (s *FileStorage) LoadSnapshot() (Snapshot, bool, error) {
	return s.snapshot, s.hasSnapshot, nil
}

// SaveSnapshot durably records snap as the new snapshot, tmp → fsync → rename
// → fsync dir — the same whole-file-replace discipline as SaveHardState.
func (s *FileStorage) SaveSnapshot(snap Snapshot) error {
	enc := encodeSnapshot(snap)
	final := filepath.Join(s.dir, SnapshotName)
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("consensus: create %s: %w", tmp, err)
	}
	if _, err := f.Write(enc); err != nil {
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
		return fmt.Errorf("consensus: rename %s: %w", SnapshotName, err)
	}
	if err := fsyncDir(s.dir); err != nil {
		return err
	}
	s.hasSnapshot = true
	s.snapshot = snap
	return nil
}

// loadSnapshotFile reads raft-snapshot from dir, or reports ok=false if it has
// never been written.
func loadSnapshotFile(dir string) (Snapshot, bool, error) {
	raw, err := os.ReadFile(filepath.Join(dir, SnapshotName))
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("consensus: read %s: %w", SnapshotName, err)
	}
	snap, err := decodeSnapshot(raw)
	if err != nil {
		return Snapshot{}, false, err
	}
	return snap, true, nil
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

// encodeSnapshot renders a Snapshot as a single self-delimiting frame, the same
// frame shape as an entry: crc32c(4) || length(4) || index(8) term(8) data.
func encodeSnapshot(s Snapshot) []byte {
	payload := make([]byte, 16+len(s.Data))
	binary.LittleEndian.PutUint64(payload[0:8], s.Index)
	binary.LittleEndian.PutUint64(payload[8:16], s.Term)
	copy(payload[16:], s.Data)

	buf := make([]byte, headerLen+len(payload))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(payload)))
	copy(buf[headerLen:], payload)
	binary.LittleEndian.PutUint32(buf[0:4], crc32.Checksum(buf[4:], castagnoli))
	return buf
}

// decodeSnapshot parses a whole-file snapshot frame. Unlike decodeEntry (which
// tolerates a torn tail as a clean end-of-log), raft-snapshot is written
// whole via tmp → rename, so any mismatch here is real corruption, not a
// benign partial write.
func decodeSnapshot(buf []byte) (Snapshot, error) {
	if len(buf) < headerLen {
		return Snapshot{}, fmt.Errorf("consensus: %s: truncated frame", SnapshotName)
	}
	length := int(binary.LittleEndian.Uint32(buf[4:8]))
	total := headerLen + length
	if length < 16 || total != len(buf) {
		return Snapshot{}, fmt.Errorf("consensus: %s: malformed frame", SnapshotName)
	}
	if crc32.Checksum(buf[4:total], castagnoli) != binary.LittleEndian.Uint32(buf[0:4]) {
		return Snapshot{}, fmt.Errorf("consensus: %s: checksum mismatch", SnapshotName)
	}
	p := buf[headerLen:total]
	s := Snapshot{
		Index: binary.LittleEndian.Uint64(p[0:8]),
		Term:  binary.LittleEndian.Uint64(p[8:16]),
	}
	if len(p) > 16 {
		s.Data = append([]byte(nil), p[16:]...)
	}
	return s, nil
}

// ─── MemStorage ──────────────────────────────────────────────────────────────

// MemStorage is a non-durable Storage for tests that are not about durability.
type MemStorage struct {
	hs      HardState
	entries []Entry
	// firstIndex is the index entries[0] would represent — 1 until CompactLog
	// ever moves it forward.
	firstIndex uint64

	hasSnapshot bool
	snapshot    Snapshot
}

var _ Storage = (*MemStorage)(nil)

func NewMemStorage() *MemStorage { return &MemStorage{firstIndex: 1} }

func (m *MemStorage) LoadHardState() (HardState, error) { return m.hs, nil }
func (m *MemStorage) SaveHardState(hs HardState) error  { m.hs = hs; return nil }
func (m *MemStorage) LoadEntries() ([]Entry, error)     { return append([]Entry(nil), m.entries...), nil }
func (m *MemStorage) Close() error                      { return nil }

func (m *MemStorage) AppendEntries(entries []Entry) error {
	m.entries = append(m.entries, entries...)
	return nil
}

func (m *MemStorage) TruncateFrom(i uint64) error {
	if i < m.firstIndex {
		return fmt.Errorf("consensus: TruncateFrom(%d) is below the log's first index (%d)", i, m.firstIndex)
	}
	pos := i - m.firstIndex
	if pos >= uint64(len(m.entries)) {
		return nil
	}
	m.entries = m.entries[:pos]
	return nil
}

func (m *MemStorage) LoadSnapshot() (Snapshot, bool, error) { return m.snapshot, m.hasSnapshot, nil }

func (m *MemStorage) SaveSnapshot(snap Snapshot) error {
	m.hasSnapshot = true
	m.snapshot = snap
	return nil
}

func (m *MemStorage) CompactLog(index uint64) error {
	boundary := m.firstIndex - 1
	if index < boundary {
		return fmt.Errorf("consensus: CompactLog(%d) is behind the log's current boundary (%d)", index, boundary)
	}
	if index == boundary {
		return nil
	}
	drop := index - boundary
	if drop > uint64(len(m.entries)) {
		drop = uint64(len(m.entries))
	}
	m.entries = append([]Entry(nil), m.entries[drop:]...)
	m.firstIndex = index + 1
	return nil
}
