package consensus

// Driver owns the one thing [Node] deliberately does not: I/O, in the one order
// that keeps Raft safe (§2, extended by Phase 8 §3 and Phase 9 §4):
//
//	persist Snapshot → fsync → compact log file → (restore SM)
//	                                             → truncate
//	                                             → persist HardState + entries → fsync
//	                                             → send
//	                                             → apply
//	                                             → Advance()
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

	// Local snapshot policy (Phase 9 §3). Every node snapshots independently
	// on its own applied state — this is driver housekeeping, never part of
	// Ready.
	snapshotThreshold int
	lastSnapshotIndex uint64
}

// NewDriver builds a node from cfg and wires it to a state machine and an
// optional transport. A nil Transport drops outbound messages, which is exactly
// right for Phase 6's cluster of one.
func NewDriver(cfg Config, sm StateMachine, tr Transport) (*Driver, error) {
	n, err := NewNode(cfg)
	if err != nil {
		return nil, err
	}

	threshold := cfg.SnapshotThreshold
	if threshold <= 0 {
		threshold = DefaultSnapshotThreshold
	}
	d := &Driver{node: n, storage: cfg.Storage, sm: sm, tr: tr, snapshotThreshold: threshold}

	// If a snapshot already exists, the state machine must start from it
	// before anything else replays on top (Phase 9 §5) — including the very
	// first d.run() below, which re-applies whatever committed entries
	// survived above the boundary.
	if snap, ok, err := cfg.Storage.LoadSnapshot(); err != nil {
		return nil, err
	} else if ok {
		d.lastSnapshotIndex = snap.Index
		sm.Restore(snap.Data)
	}

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

	// 0. A received snapshot supersedes everything older — persist it and
	//    compact the log file to match before any other durable operation
	//    (Phase 9 §4). RestoreFromSnapshot additionally wipes any on-disk tail
	//    above the boundary: RestoreToSnapshot already discarded it from the
	//    in-memory log (an installed snapshot is always authoritative, §7), so
	//    nothing above the boundary may survive on disk either.
	if rd.Snapshot != nil {
		if err := d.storage.SaveSnapshot(*rd.Snapshot); err != nil {
			return err
		}
		if rd.RestoreFromSnapshot {
			if err := d.storage.TruncateFrom(rd.Snapshot.Index + 1); err != nil {
				return err
			}
		}
		if err := d.storage.CompactLog(rd.Snapshot.Index); err != nil {
			return err
		}
		if rd.RestoreFromSnapshot {
			d.sm.Restore(rd.Snapshot.Data)
		}
		d.lastSnapshotIndex = rd.Snapshot.Index
	}

	// 1. Durable first — always, before anything observable leaves this node.
	//
	//    Truncation precedes the append and is not merely stylistic: appending
	//    first and truncating after leaves a crash window in which the log on
	//    disk carries the wrong suffix.
	if rd.TruncateFrom != nil {
		if err := d.storage.TruncateFrom(*rd.TruncateFrom); err != nil {
			return err
		}
	}
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

	// 4. Local snapshot housekeeping — driver-side, never through Ready
	//    (Phase 9 §3): decided here, after apply, on this node's own applied
	//    state.
	return d.maybeSnapshot()
}

// maybeSnapshot takes a new local snapshot once enough entries have been
// applied beyond the last one (Config.SnapshotThreshold). It persists the
// snapshot, compacts the log file, and only then tells the node to compact its
// in-memory log to match — SaveSnapshot before CompactLog is the same
// durable-before-destructive rule as everywhere else in this package.
func (d *Driver) maybeSnapshot() error {
	applied := d.node.LastApplied()
	if applied == 0 || applied <= d.lastSnapshotIndex {
		return nil
	}
	if applied-d.lastSnapshotIndex < uint64(d.snapshotThreshold) {
		return nil
	}

	snap := Snapshot{
		Index: applied,
		Term:  d.node.Log().Term(applied),
		Data:  d.sm.Snapshot(),
	}
	if err := d.storage.SaveSnapshot(snap); err != nil {
		return err
	}
	if err := d.storage.CompactLog(snap.Index); err != nil {
		return err
	}
	d.node.SnapshotTaken(snap)
	d.lastSnapshotIndex = snap.Index
	return nil
}
