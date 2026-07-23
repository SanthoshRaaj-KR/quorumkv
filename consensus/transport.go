package consensus

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sync"
)

// ─── message codec ───────────────────────────────────────────────────────────
//
// Same frame as the Raft log and the Phase 1 WAL — crc32c(4) || length(4) ||
// payload, little-endian, CRC over length||payload. Three uses of one codec
// beats three codecs (planning/phase-07 §2).
//
// Payload:
//
//	type(1) from(8) to(8) term(8)
//	lastLogIndex(8) lastLogTerm(8)
//	prevLogIndex(8) prevLogTerm(8) leaderCommit(8)
//	granted(1) success(1) matchIndex(8)
//	conflictIndex(8) conflictTerm(8)
//	entryCount(4) [ term(8) index(8) cmdLen(4) cmd ]*

const msgFixedLen = 1 + 8*3 + 8*2 + 8*3 + 1 + 1 + 8 + 8 + 8 + 4

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// EncodeMessage renders m as a self-delimiting frame.
func EncodeMessage(m Message) []byte {
	payload := make([]byte, msgFixedLen, msgFixedLen+64)
	payload[0] = byte(m.Type)
	le := binary.LittleEndian
	le.PutUint64(payload[1:9], m.From)
	le.PutUint64(payload[9:17], m.To)
	le.PutUint64(payload[17:25], m.Term)
	le.PutUint64(payload[25:33], m.LastLogIndex)
	le.PutUint64(payload[33:41], m.LastLogTerm)
	le.PutUint64(payload[41:49], m.PrevLogIndex)
	le.PutUint64(payload[49:57], m.PrevLogTerm)
	le.PutUint64(payload[57:65], m.LeaderCommit)
	payload[65] = boolByte(m.Granted)
	payload[66] = boolByte(m.Success)
	le.PutUint64(payload[67:75], m.MatchIndex)
	le.PutUint64(payload[75:83], m.ConflictIndex)
	le.PutUint64(payload[83:91], m.ConflictTerm)
	le.PutUint32(payload[91:95], uint32(len(m.Entries)))

	for _, e := range m.Entries {
		var hdr [20]byte
		le.PutUint64(hdr[0:8], e.Term)
		le.PutUint64(hdr[8:16], e.Index)
		le.PutUint32(hdr[16:20], uint32(len(e.Cmd)))
		payload = append(payload, hdr[:]...)
		payload = append(payload, e.Cmd...)
	}

	buf := make([]byte, headerLen+len(payload))
	le.PutUint32(buf[4:8], uint32(len(payload)))
	copy(buf[headerLen:], payload)
	le.PutUint32(buf[0:4], crc32.Checksum(buf[4:], castagnoli))
	return buf
}

// DecodeMessage parses one frame from the head of buf, returning the message and
// the bytes consumed.
func DecodeMessage(buf []byte) (Message, int, error) {
	if len(buf) < headerLen {
		return Message{}, 0, errIncomplete
	}
	le := binary.LittleEndian
	length := int(le.Uint32(buf[4:8]))
	total := headerLen + length
	if length < msgFixedLen || total > len(buf) {
		return Message{}, 0, errIncomplete
	}
	if crc32.Checksum(buf[4:total], castagnoli) != le.Uint32(buf[0:4]) {
		return Message{}, 0, errors.New("consensus: message crc mismatch")
	}
	p := buf[headerLen:total]

	m := Message{
		Type:          MessageType(p[0]),
		From:          le.Uint64(p[1:9]),
		To:            le.Uint64(p[9:17]),
		Term:          le.Uint64(p[17:25]),
		LastLogIndex:  le.Uint64(p[25:33]),
		LastLogTerm:   le.Uint64(p[33:41]),
		PrevLogIndex:  le.Uint64(p[41:49]),
		PrevLogTerm:   le.Uint64(p[49:57]),
		LeaderCommit:  le.Uint64(p[57:65]),
		Granted:       p[65] == 1,
		Success:       p[66] == 1,
		MatchIndex:    le.Uint64(p[67:75]),
		ConflictIndex: le.Uint64(p[75:83]),
		ConflictTerm:  le.Uint64(p[83:91]),
	}

	count := int(le.Uint32(p[91:95]))
	off := msgFixedLen
	for i := 0; i < count; i++ {
		if off+20 > len(p) {
			return Message{}, 0, errors.New("consensus: truncated entry header")
		}
		e := Entry{Term: le.Uint64(p[off : off+8]), Index: le.Uint64(p[off+8 : off+16])}
		cmdLen := int(le.Uint32(p[off+16 : off+20]))
		off += 20
		if off+cmdLen > len(p) {
			return Message{}, 0, errors.New("consensus: truncated entry command")
		}
		if cmdLen > 0 {
			e.Cmd = append([]byte(nil), p[off:off+cmdLen]...)
		}
		off += cmdLen
		m.Entries = append(m.Entries, e)
	}
	if off != len(p) {
		return Message{}, 0, errors.New("consensus: trailing bytes in message")
	}
	return m, total, nil
}

// ReadMessage reads exactly one framed message from r.
func ReadMessage(r io.Reader) (Message, error) {
	var hdr [headerLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Message{}, err
	}
	length := int(binary.LittleEndian.Uint32(hdr[4:8]))
	if length < msgFixedLen || length > 64<<20 {
		return Message{}, errors.New("consensus: implausible message length")
	}
	frame := make([]byte, headerLen+length)
	copy(frame, hdr[:])
	if _, err := io.ReadFull(r, frame[headerLen:]); err != nil {
		return Message{}, err
	}
	m, _, err := DecodeMessage(frame)
	return m, err
}

// ─── Loopback ────────────────────────────────────────────────────────────────

// Bus is an in-process message router: no sockets, no goroutines, no clock.
//
// It is not a toy. A whole cluster runs inside one goroutine, a partition is
// "stop routing to these ids", and a failing run replays exactly from a seed —
// which is what makes Phase 12's chaos suite possible at all. The TCP transport
// proves the same code survives a real socket; this one proves it is *correct*.
type Bus struct {
	mu      sync.Mutex
	queues  map[uint64][]Message
	blocked map[uint64]bool
	// Reorder, when set, is applied to each destination's queue on Take —
	// a hook for deliberately delivering out of order.
	Reorder func([]Message) []Message
}

func NewBus() *Bus {
	return &Bus{queues: make(map[uint64][]Message), blocked: make(map[uint64]bool)}
}

// Transport returns the [Transport] that node id sends through.
func (b *Bus) Transport(id uint64) Transport { return &loopback{bus: b, from: id} }

// Isolate cuts a node off in both directions — the partition primitive. Messages
// to and from it are dropped, not queued: Raft tolerates loss, and a replay
// queue would be a second consensus protocol underneath the real one.
func (b *Bus) Isolate(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blocked[id] = true
	delete(b.queues, id)
}

// Heal reconnects a node isolated by Isolate.
func (b *Bus) Heal(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.blocked, id)
}

// IsIsolated reports whether id is currently cut off.
func (b *Bus) IsIsolated(id uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.blocked[id]
}

// Take drains and returns everything queued for id.
func (b *Bus) Take(id uint64) []Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	msgs := b.queues[id]
	delete(b.queues, id)
	if b.Reorder != nil {
		msgs = b.Reorder(msgs)
	}
	return msgs
}

// Pending is the number of messages waiting cluster-wide.
func (b *Bus) Pending() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	total := 0
	for _, q := range b.queues {
		total += len(q)
	}
	return total
}

type loopback struct {
	bus  *Bus
	from uint64
}

func (l *loopback) Send(msgs []Message) {
	l.bus.mu.Lock()
	defer l.bus.mu.Unlock()
	if l.bus.blocked[l.from] {
		return
	}
	for _, m := range msgs {
		if m.To == l.from || l.bus.blocked[m.To] {
			continue
		}
		l.bus.queues[m.To] = append(l.bus.queues[m.To], m)
	}
}
