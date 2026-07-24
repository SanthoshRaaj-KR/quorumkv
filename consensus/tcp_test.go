package consensus

import (
	"bytes"
	"testing"
	"time"
)

// ─── §6.10 message codec ─────────────────────────────────────────────────────

func TestMessageCodecRoundTrips(t *testing.T) {
	cases := []Message{
		{Type: MsgVoteReq, From: 3, To: 1, Term: 9, LastLogIndex: 41, LastLogTerm: 8},
		{Type: MsgVoteResp, From: 1, To: 3, Term: 9, Granted: true},
		{Type: MsgAppReq, From: 2, To: 3, Term: 4, PrevLogIndex: 7, PrevLogTerm: 3, LeaderCommit: 6},
		{Type: MsgAppResp, From: 3, To: 2, Term: 4, Success: true, MatchIndex: 7},
		{
			Type: MsgAppReq, From: 1, To: 2, Term: 5, PrevLogIndex: 2, PrevLogTerm: 4,
			Entries: []Entry{
				{Term: 5, Index: 3, Cmd: []byte("PUT a 1")},
				{Term: 5, Index: 4}, // a no-op, empty command
				{Term: 5, Index: 5, Cmd: bytes.Repeat([]byte{0x7F}, 3000)},
			},
		},
		{
			Type: MsgSnap, From: 1, To: 2, Term: 6,
			SnapshotIndex: 500, SnapshotTerm: 4, SnapshotData: bytes.Repeat([]byte{0xCD}, 4096),
		},
		{Type: MsgAppResp, From: 2, To: 1, Term: 6, Success: true, MatchIndex: 500}, // the InstallSnapshot ack
	}

	for _, want := range cases {
		enc := EncodeMessage(want)
		got, n, err := DecodeMessage(enc)
		if err != nil {
			t.Fatalf("decode %v: %v", want.Type, err)
		}
		if n != len(enc) {
			t.Errorf("%v: consumed %d bytes, want %d", want.Type, n, len(enc))
		}
		if !messagesEqual(got, want) {
			t.Errorf("%v round-trip:\n got %+v\nwant %+v", want.Type, got, want)
		}
	}
}

func messagesEqual(a, b Message) bool {
	if a.Type != b.Type || a.From != b.From || a.To != b.To || a.Term != b.Term ||
		a.LastLogIndex != b.LastLogIndex || a.LastLogTerm != b.LastLogTerm ||
		a.PrevLogIndex != b.PrevLogIndex || a.PrevLogTerm != b.PrevLogTerm ||
		a.LeaderCommit != b.LeaderCommit || a.Granted != b.Granted ||
		a.Success != b.Success || a.MatchIndex != b.MatchIndex ||
		a.ConflictIndex != b.ConflictIndex || a.ConflictTerm != b.ConflictTerm ||
		a.SnapshotIndex != b.SnapshotIndex || a.SnapshotTerm != b.SnapshotTerm ||
		!bytes.Equal(a.SnapshotData, b.SnapshotData) ||
		len(a.Entries) != len(b.Entries) {
		return false
	}
	for i := range a.Entries {
		if a.Entries[i].Term != b.Entries[i].Term || a.Entries[i].Index != b.Entries[i].Index ||
			!bytes.Equal(a.Entries[i].Cmd, b.Entries[i].Cmd) {
			return false
		}
	}
	return true
}

func TestMessageDecodeRejectsShortAndCorrupt(t *testing.T) {
	enc := EncodeMessage(Message{Type: MsgVoteReq, From: 1, To: 2, Term: 3})
	for i := 1; i < len(enc); i++ {
		if _, _, err := DecodeMessage(enc[:i]); err == nil {
			t.Errorf("decode of a %d-byte prefix should fail", i)
		}
	}
	bad := append([]byte(nil), enc...)
	bad[10] ^= 0xFF
	if _, _, err := DecodeMessage(bad); err == nil {
		t.Error("a corrupted frame should fail the CRC")
	}
}

func TestReadMessageStreamsConcatenatedFrames(t *testing.T) {
	want := []Message{
		{Type: MsgVoteReq, From: 1, To: 2, Term: 1},
		{Type: MsgVoteResp, From: 2, To: 1, Term: 1, Granted: true},
		{Type: MsgAppReq, From: 1, To: 2, Term: 1, Entries: []Entry{{Term: 1, Index: 1, Cmd: []byte("x")}}},
	}
	var buf bytes.Buffer
	for _, m := range want {
		buf.Write(EncodeMessage(m))
	}
	for i, w := range want {
		got, err := ReadMessage(&buf)
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if !messagesEqual(got, w) {
			t.Errorf("message %d:\n got %+v\nwant %+v", i, got, w)
		}
	}
}

// ─── §6.9 real sockets ───────────────────────────────────────────────────────

// tcpCluster is three Servers on 127.0.0.1 talking over real TCP. Unlike the
// deterministic Bus cluster, this one has genuine concurrency and a real clock —
// its job is to prove the same Raft code survives a socket, not to test the
// algorithm (the Bus tests do that).
type tcpCluster struct {
	t       *testing.T
	servers map[uint64]*Server
	trs     map[uint64]*TCPTransport
	ids     []uint64
}

func newTCPCluster(t *testing.T, n int) *tcpCluster {
	t.Helper()
	c := &tcpCluster{t: t, servers: make(map[uint64]*Server), trs: make(map[uint64]*TCPTransport)}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, uint64(i))
	}

	// Bind first, so every address is known before anyone tries to dial.
	addrs := make(map[uint64]string, n)
	for _, id := range c.ids {
		tr, err := NewTCPTransport(id, "127.0.0.1:0")
		if err != nil {
			t.Fatalf("transport %d: %v", id, err)
		}
		c.trs[id] = tr
		addrs[id] = tr.Addr()
	}
	for _, id := range c.ids {
		c.trs[id].SetPeers(addrs)
	}

	for _, id := range c.ids {
		// A short tick keeps the test fast: election lands in [50ms, 100ms).
		srv, err := NewServer(Config{
			ID: id, Peers: c.ids,
			ElectionTimeout: 10, HeartbeatTimeout: 3,
			Storage: NewMemStorage(), Seed: int64(id) * 31,
		}, &recorder{}, c.trs[id], c.trs[id].Inbound(), 5*time.Millisecond)
		if err != nil {
			t.Fatalf("server %d: %v", id, err)
		}
		c.servers[id] = srv
	}
	for _, id := range c.ids {
		c.servers[id].Start()
	}
	return c
}

func (c *tcpCluster) stopAll() {
	for _, id := range c.ids {
		if s, ok := c.servers[id]; ok {
			_ = s.Stop()
		}
		if tr, ok := c.trs[id]; ok {
			_ = tr.Close()
		}
	}
}

// waitLeader polls until exactly one node reports itself leader.
func (c *tcpCluster) waitLeader(timeout time.Duration, skip map[uint64]bool) (uint64, uint64) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []uint64
		var term uint64
		for _, id := range c.ids {
			if skip[id] {
				continue
			}
			st, err := c.servers[id].Status()
			if err != nil {
				continue
			}
			if st.Role == Leader {
				leaders = append(leaders, id)
				term = st.Term
			}
		}
		if len(leaders) == 1 {
			return leaders[0], term
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no single leader within %v", timeout)
	return 0, 0
}

func TestTCPClusterElectsAndReElects(t *testing.T) {
	c := newTCPCluster(t, 3)
	defer c.stopAll()

	leader, term := c.waitLeader(10*time.Second, nil)
	t.Logf("elected node %d in term %d over real sockets", leader, term)

	// Everyone else should agree who leads.
	for _, id := range c.ids {
		if id == leader {
			continue
		}
		st, err := c.servers[id].Status()
		if err != nil {
			t.Fatal(err)
		}
		if st.Role != Follower {
			t.Errorf("node %d role = %s, want follower", id, st.Role)
		}
	}

	// Kill the leader for real: stop its loop and close its sockets.
	if err := c.servers[leader].Stop(); err != nil {
		t.Fatalf("stop leader: %v", err)
	}
	if err := c.trs[leader].Close(); err != nil {
		t.Fatalf("close leader transport: %v", err)
	}
	delete(c.servers, leader)
	delete(c.trs, leader)

	fresh, freshTerm := c.waitLeader(10*time.Second, map[uint64]bool{leader: true})
	if fresh == leader {
		t.Fatal("the killed node cannot lead")
	}
	if freshTerm <= term {
		t.Errorf("new term %d, want > %d", freshTerm, term)
	}
	t.Logf("re-elected node %d in term %d after killing node %d", fresh, freshTerm, leader)
}

func TestTCPProposeOnLeaderAndFollower(t *testing.T) {
	c := newTCPCluster(t, 3)
	defer c.stopAll()

	leader, _ := c.waitLeader(10*time.Second, nil)

	if err := c.servers[leader].Propose([]byte("PUT k v")); err != nil {
		t.Errorf("Propose on the leader: %v", err)
	}
	for _, id := range c.ids {
		if id == leader {
			continue
		}
		if err := c.servers[id].Propose([]byte("nope")); err != ErrNotLeader {
			t.Errorf("Propose on follower %d = %v, want ErrNotLeader", id, err)
		}
	}
}

func TestTCPSendToUnreachablePeerIsDropped(t *testing.T) {
	tr, err := NewTCPTransport(1, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	// A peer that is not listening: Send must return, not block or panic.
	tr.SetPeers(map[uint64]string{2: "127.0.0.1:1"})

	done := make(chan struct{})
	go func() {
		tr.Send([]Message{{Type: MsgVoteReq, From: 1, To: 2, Term: 1}})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Send to an unreachable peer blocked — delivery must be fire-and-forget")
	}
}

func TestServerStopIsIdempotent(t *testing.T) {
	tr, err := NewTCPTransport(1, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	tr.SetPeers(map[uint64]string{})

	srv, err := NewServer(Config{
		ID: 1, Peers: []uint64{1},
		ElectionTimeout: 5, HeartbeatTimeout: 2,
		Storage: NewMemStorage(),
	}, &recorder{}, tr, tr.Inbound(), 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()

	// A cluster of one elects itself even over a real transport.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if st, err := srv.Status(); err == nil && st.Role == Leader {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	st, err := srv.Status()
	if err != nil {
		t.Fatal(err)
	}
	if st.Role != Leader {
		t.Fatalf("single node did not elect itself: role=%s", st.Role)
	}

	if err := srv.Stop(); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if err := srv.Stop(); err != nil {
		t.Errorf("second Stop should be a no-op, got %v", err)
	}
}
