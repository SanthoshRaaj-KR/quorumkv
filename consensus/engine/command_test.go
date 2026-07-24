package engine

import (
	"bytes"
	"testing"
)

// Mirrors storage/src/wal.rs's own encode_record/decode_record round-trip
// tests almost line for line — same shape, same reasoning (phase-10 §3).

func TestCommandRoundTrips(t *testing.T) {
	cases := []Command{
		{Op: OpPut, Key: []byte("alpha"), Value: []byte("one")},
		{Op: OpDelete, Key: []byte("alpha")},
		{Op: OpPut, Key: []byte(""), Value: []byte("v")},   // empty key
		{Op: OpPut, Key: []byte("k"), Value: []byte("")},   // empty value — distinct from Delete
		{Op: OpPut, Key: []byte("big"), Value: bytes.Repeat([]byte{0xAB}, 10_000)},
	}
	for _, want := range cases {
		enc := EncodeCommand(want)
		got, err := DecodeCommand(enc)
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if got.Op != want.Op || !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) {
			t.Errorf("round-trip = %+v, want %+v", got, want)
		}
	}
}

func TestEmptyValuePutIsDistinctFromDelete(t *testing.T) {
	// Both encode a zero-length value section; only the op byte separates them.
	p := Command{Op: OpPut, Key: []byte("k"), Value: []byte("")}
	d := Command{Op: OpDelete, Key: []byte("k")}

	dp, err := DecodeCommand(EncodeCommand(p))
	if err != nil {
		t.Fatal(err)
	}
	dd, err := DecodeCommand(EncodeCommand(d))
	if err != nil {
		t.Fatal(err)
	}
	if dp.Op != OpPut || dd.Op != OpDelete {
		t.Errorf("ops = %v / %v, want put / delete", dp.Op, dd.Op)
	}
}

func TestDecodeRejectsShortAndTruncatedCommands(t *testing.T) {
	enc := EncodeCommand(Command{Op: OpPut, Key: []byte("key"), Value: []byte("value")})

	if _, err := DecodeCommand(nil); err == nil {
		t.Error("decoding nil should fail")
	}
	if _, err := DecodeCommand(enc[:3]); err == nil {
		t.Error("decoding a too-short header should fail")
	}
	if _, err := DecodeCommand(enc[:len(enc)-1]); err == nil {
		t.Error("decoding a truncated payload should fail")
	}
	if _, err := DecodeCommand(append(enc, 0xFF)); err == nil {
		t.Error("trailing bytes after a valid command should be rejected")
	}
}

func TestDecodeRejectsUnknownOp(t *testing.T) {
	enc := EncodeCommand(Command{Op: OpPut, Key: []byte("k"), Value: []byte("v")})
	enc[0] = 0x09 // corrupt the op byte
	if _, err := DecodeCommand(enc); err == nil {
		t.Error("an unknown op byte should be rejected, not silently misparsed")
	}
}

// The op byte values are load-bearing: they must match storage/src/wal.rs's
// OP_PUT (0x01) / OP_DELETE (0x02) constants exactly (phase-10 §3).
func TestOpValuesMatchTheWALConvention(t *testing.T) {
	if OpPut != 1 {
		t.Errorf("OpPut = %d, want 1 (matching wal.rs's OP_PUT)", OpPut)
	}
	if OpDelete != 2 {
		t.Errorf("OpDelete = %d, want 2 (matching wal.rs's OP_DELETE)", OpDelete)
	}
}
