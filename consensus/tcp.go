package consensus

import (
	"fmt"
	"net"
	"sync"
)

// TCPTransport carries Raft messages over real sockets.
//
// Delivery is **fire-and-forget** (planning/phase-07 §2): a message to an
// unreachable peer is dropped and the connection redialled on the next send.
// There is no retry queue, deliberately — Raft already tolerates loss,
// duplication and reordering, and a queue underneath it would be a second,
// worse consensus protocol.
//
// The wire format is [EncodeMessage]: the same CRC32C length-prefixed frame the
// WAL and the Raft log use.
type TCPTransport struct {
	id      uint64
	ln      net.Listener
	inbound chan Message

	mu    sync.Mutex
	addrs map[uint64]string
	// conns are outbound, dialled connections, keyed by peer.
	conns map[uint64]net.Conn
	// accepted are inbound connections. They must be tracked separately: a
	// reader goroutine blocked on one is only unblocked by closing *that*
	// connection, so a Close that forgets them hangs forever in wg.Wait.
	accepted map[net.Conn]struct{}
	closed   bool

	// isolated is Bus.Isolate's real-socket equivalent (planning/phase-12
	// §1a): cuts this node off from every peer, bidirectionally. savedAddrs
	// holds the address book Isolate found in place, so Heal can restore it
	// without any external caller having to remember it.
	isolated   bool
	savedAddrs map[uint64]string

	wg sync.WaitGroup
}

var _ Transport = (*TCPTransport)(nil)

// NewTCPTransport binds bind (use "127.0.0.1:0" for an ephemeral port) and
// starts accepting. Peers are set separately with SetPeers, because in tests
// every node's address is only known after all of them have bound.
func NewTCPTransport(id uint64, bind string) (*TCPTransport, error) {
	ln, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, fmt.Errorf("consensus: listen %s: %w", bind, err)
	}
	t := &TCPTransport{
		id:       id,
		ln:       ln,
		inbound:  make(chan Message, 256),
		addrs:    make(map[uint64]string),
		conns:    make(map[uint64]net.Conn),
		accepted: make(map[net.Conn]struct{}),
	}
	t.wg.Add(1)
	go t.accept()
	return t, nil
}

// Addr is the bound address, valid immediately after construction.
func (t *TCPTransport) Addr() string { return t.ln.Addr().String() }

// SetPeers installs the static id → host:port map. No discovery, no membership
// change — both out of scope (DESIGN.md §1).
func (t *TCPTransport) SetPeers(addrs map[uint64]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.addrs = make(map[uint64]string, len(addrs))
	for id, a := range addrs {
		if id != t.id {
			t.addrs[id] = a
		}
	}
}

// Inbound is the stream of decoded messages. [Server] selects on it; nothing
// here ever touches Raft state.
func (t *TCPTransport) Inbound() <-chan Message { return t.inbound }

// Send delivers msgs, dropping any that cannot be written right now.
func (t *TCPTransport) Send(msgs []Message) {
	for _, m := range msgs {
		if m.To == t.id {
			continue
		}
		conn, err := t.conn(m.To)
		if err != nil {
			continue // peer down: drop, retry on the next heartbeat
		}
		if _, err := conn.Write(EncodeMessage(m)); err != nil {
			t.drop(m.To)
		}
	}
}

// conn returns a live connection to peer, dialling lazily.
func (t *TCPTransport) conn(peer uint64) (net.Conn, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, net.ErrClosed
	}
	if c, ok := t.conns[peer]; ok {
		t.mu.Unlock()
		return c, nil
	}
	addr, ok := t.addrs[peer]
	t.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("consensus: no address for peer %d", peer)
	}

	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		c.Close()
		return nil, net.ErrClosed
	}
	// Another goroutine may have won the race; keep one connection per peer.
	if existing, ok := t.conns[peer]; ok {
		c.Close()
		return existing, nil
	}
	t.conns[peer] = c
	return c, nil
}

// Isolate cuts this node off from every peer, bidirectionally: it closes
// every open connection (outbound and inbound), clears its address book so
// Send can't redial, and its accept loop starts refusing new inbound
// connections outright. This is TCPTransport's equivalent of Bus.Isolate,
// promoted (planning/phase-12 §1a) from the ad hoc drop+SetPeers mechanism
// tcpCluster's tests reconstructed by hand.
func (t *TCPTransport) Isolate() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.isolated {
		return
	}
	t.isolated = true
	t.savedAddrs = t.addrs
	t.addrs = make(map[uint64]string)
	for id, c := range t.conns {
		c.Close()
		delete(t.conns, id)
	}
	// Closing each accepted connection is what unblocks its reader
	// goroutine; it removes itself from t.accepted via its own defer.
	for c := range t.accepted {
		c.Close()
	}
}

// Heal reverses Isolate, restoring the address book that was in place when
// Isolate was called. Connections to peers are re-established lazily on the
// next Send, same as any other post-drop redial.
func (t *TCPTransport) Heal() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.isolated {
		return
	}
	t.isolated = false
	t.addrs = t.savedAddrs
	t.savedAddrs = nil
}

// IsIsolated reports whether Isolate has been called without a matching Heal.
func (t *TCPTransport) IsIsolated() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.isolated
}

func (t *TCPTransport) drop(peer uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[peer]; ok {
		c.Close()
		delete(t.conns, peer)
	}
}

func (t *TCPTransport) accept() {
	defer t.wg.Done()
	for {
		c, err := t.ln.Accept()
		if err != nil {
			return // listener closed
		}
		t.mu.Lock()
		if t.closed || t.isolated {
			t.mu.Unlock()
			c.Close()
			continue
		}
		t.accepted[c] = struct{}{}
		t.mu.Unlock()

		t.wg.Add(1)
		go t.read(c)
	}
}

func (t *TCPTransport) read(c net.Conn) {
	defer t.wg.Done()
	defer func() {
		t.mu.Lock()
		delete(t.accepted, c)
		t.mu.Unlock()
		c.Close()
	}()
	for {
		m, err := ReadMessage(c)
		if err != nil {
			return // peer gone or a corrupt frame: drop the connection
		}
		select {
		case t.inbound <- m:
		default:
			// Inbound is full: drop rather than block the reader. Losing a
			// heartbeat is a non-event; stalling the socket is not.
		}
	}
}

// Close stops accepting, closes every connection, and waits for the reader
// goroutines to finish.
func (t *TCPTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	for id, c := range t.conns {
		c.Close()
		delete(t.conns, id)
	}
	// Closing each accepted connection is what unblocks its reader goroutine;
	// without this, wg.Wait below never returns.
	for c := range t.accepted {
		c.Close()
	}
	t.mu.Unlock()

	err := t.ln.Close()
	t.wg.Wait()
	return err
}
