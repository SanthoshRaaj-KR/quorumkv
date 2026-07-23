package consensus

// Driver owns the one thing [Node] deliberately does not: I/O, in the one order
// that keeps Raft safe (§2).
//
//	persist HardState + entries  →  fsync  →  send  →  apply  →  Advance
//
// Keeping that sequence in exactly one place is the point of the driven-core
// design — there is a single function to audit, and Phase 7 changes it only by
// handing Transport a non-nil implementation.
//
// Driver is single-threaded: one goroutine may drive one Driver.
type Driver struct {
	node    *Node
	storage Storage
	sm      StateMachine
	tr      Transport
}

// NewDriver builds a node from cfg and wires it to a state machine and an
// optional transport. A nil Transport drops outbound messages, which is exactly
// right for Phase 6's cluster of one.
func NewDriver(cfg Config, sm StateMachine, tr Transport) (*Driver, error) {
	n, err := NewNode(cfg)
	if err != nil {
		return nil, err
	}
	d := &Driver{node: n, storage: cfg.Storage, sm: sm, tr: tr}
	// A freshly restored node may already owe work (a reloaded HardState is not
	// dirty, but committed entries from a previous life are re-applied).
	if err := d.run(); err != nil {
		return nil, err
	}
	return d, nil
}

// Node exposes the underlying state machine for inspection.
func (d *Driver) Node() *Node { return d.node }

// Tick advances logical time by one and completes any resulting work.
func (d *Driver) Tick() error {
	d.node.Tick()
	return d.run()
}

// Step delivers an inbound message and completes any resulting work.
func (d *Driver) Step(m Message) error {
	if err := d.node.Step(m); err != nil {
		return err
	}
	return d.run()
}

// Propose appends a command (leader only) and completes the resulting work.
// It returns [ErrNotLeader] on a follower or candidate.
func (d *Driver) Propose(cmd []byte) error {
	if err := d.node.Propose(cmd); err != nil {
		return err
	}
	return d.run()
}

// Close releases the storage handles.
func (d *Driver) Close() error { return d.storage.Close() }

// run executes one Ready in contract order. A single pass suffices: applying an
// entry cannot itself produce new Raft work in Phase 6.
func (d *Driver) run() error {
	rd := d.node.Ready()
	if rd.IsEmpty() {
		d.node.Advance()
		return nil
	}

	// 1. Durable first — always, before anything observable leaves this node.
	if rd.HardState != nil {
		if err := d.storage.SaveHardState(*rd.HardState); err != nil {
			return err
		}
	}
	if len(rd.EntriesToPersist) > 0 {
		if err := d.storage.AppendEntries(rd.EntriesToPersist); err != nil {
			return err
		}
	}

	// 2. Only now may peers hear about it. (No-op until Phase 7.)
	if len(rd.Messages) > 0 && d.tr != nil {
		d.tr.Send(rd.Messages)
	}

	// 3. Apply. The election no-op is a real committed entry but carries no
	//    command, so it advances lastApplied without reaching the state machine.
	for _, e := range rd.CommittedEntries {
		if e.IsNoOp() {
			continue
		}
		d.sm.Apply(e.Cmd)
	}

	d.node.Advance()
	return nil
}
