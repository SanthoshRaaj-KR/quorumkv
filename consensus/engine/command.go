// Package engine is the Phase 10 seam (planning/phase-10-apply-seam.md):
// it connects Raft's opaque commands and Snapshot/Restore callbacks to a
// real Rust LSM engine running as a local sidecar process, one per node.
//
// This package imports [consensus], never the reverse — consensus.Node,
// Driver and Log stay completely ignorant of what a "key" or "value" is
// (consensus/state.go's own package doc says as much), and this is where
// that opacity ends. commands are opaque bytes on the Raft side; on this
// side they're PUT/DELETE.
package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Op is the operation a Command encodes — deliberately the same byte values
// as storage/src/wal.rs's own OP_PUT/OP_DELETE constants (phase-10 §3): two
// halves of the same project, same convention, no coincidence.
type Op byte

const (
	OpPut    Op = 1
	OpDelete Op = 2
)

func (o Op) String() string {
	switch o {
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	default:
		return fmt.Sprintf("op(%d)", byte(o))
	}
}

// Command is one client write — the concrete meaning behind the opaque
// []byte that Raft's Propose/Apply carry. Raft never parses this; only this
// package and the engine on the other end of the sidecar link do.
type Command struct {
	Op    Op
	Key   []byte
	Value []byte // empty for Delete
}

// EncodeCommand renders c as op(1) | keyLen(4) | key | valueLen(4) | value —
// the same hand-rolled shape as storage/src/wal.rs's own record encoding,
// for the same reason: self-describing, no ambiguity, no new dependency.
func EncodeCommand(c Command) []byte {
	buf := make([]byte, 0, 1+4+len(c.Key)+4+len(c.Value))
	buf = append(buf, byte(c.Op))

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(c.Key)))
	buf = append(buf, lenBuf[:]...)
	buf = append(buf, c.Key...)

	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(c.Value)))
	buf = append(buf, lenBuf[:]...)
	buf = append(buf, c.Value...)
	return buf
}

// DecodeCommand parses bytes produced by [EncodeCommand].
func DecodeCommand(b []byte) (Command, error) {
	if len(b) < 1+4 {
		return Command{}, errors.New("engine: command shorter than its own header")
	}
	op := Op(b[0])
	off := 1

	keyLen := binary.LittleEndian.Uint32(b[off : off+4])
	off += 4
	if uint32(len(b)-off) < keyLen {
		return Command{}, errors.New("engine: command key truncated")
	}
	key := b[off : off+int(keyLen)]
	off += int(keyLen)

	if len(b)-off < 4 {
		return Command{}, errors.New("engine: command missing value length")
	}
	valLen := binary.LittleEndian.Uint32(b[off : off+4])
	off += 4
	if uint32(len(b)-off) < valLen {
		return Command{}, errors.New("engine: command value truncated")
	}
	value := b[off : off+int(valLen)]
	off += int(valLen)

	if off != len(b) {
		return Command{}, errors.New("engine: trailing bytes after command")
	}
	if op != OpPut && op != OpDelete {
		return Command{}, fmt.Errorf("engine: unknown op %d", op)
	}
	return Command{
		Op:    op,
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	}, nil
}
