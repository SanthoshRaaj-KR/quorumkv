package consensus

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
)

// ErrNotLeader is returned by Propose on a node that is not the leader. Phase 11
// turns this into the client's redirect.
var ErrNotLeader = errors.New("consensus: not leader")

// Config describes one node's participation in a Raft group.
type Config struct {
	// ID of this node. Must be non-zero (0 is the None sentinel).
	ID uint64
	// Peers is every voter in the group, including ID. A single-element slice is
	// the Phase 6 "cluster of one".
	Peers []uint64
	// ElectionTimeout in ticks. The effective timeout is randomized into
	// [ElectionTimeout, 2*ElectionTimeout) to break vote splits (Phase 7).
	ElectionTimeout int
	// HeartbeatTimeout in ticks; a leader emits heartbeats this often (Phase 7).
	HeartbeatTimeout int
	// Storage supplies the persisted term, vote and log at construction.
	Storage Storage
	// Seed fixes the randomized election timeout, so a whole run replays
	// identically — which is what makes Phase 12's chaos suite worth having.
	// Never defaults to wall-clock time.
	//
	// A cluster-wide seed is the intended usage: the node ID is mixed in, so
	// every node still draws an independent timeout stream (see NewNode).
	Seed int64
}

func (c *Config) validate() error {
	switch {
	case c.ID == None:
		return errors.New("consensus: Config.ID must be non-zero")
	case len(c.Peers) == 0:
		return errors.New("consensus: Config.Peers must include at least this node")
	case c.ElectionTimeout <= 0:
		return errors.New("consensus: Config.ElectionTimeout must be positive")
	case c.HeartbeatTimeout <= 0:
		return errors.New("consensus: Config.HeartbeatTimeout must be positive")
	case c.Storage == nil:
		return errors.New("consensus: Config.Storage must be set")
	}
	found := false
	for _, p := range c.Peers {
		if p == c.ID {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("consensus: Config.Peers must contain ID %d", c.ID)
	}
	return nil
}

// Node is the Raft state machine. It owns no timers, no goroutines and no I/O:
// a driver calls Tick/Step/Propose, then Ready/Advance. Node is not safe for
// concurrent use — the driver is the single owner.
type Node struct {
	id    uint64
	peers []uint64

	// Persistent (via HardState + the log).
	role        Role
	currentTerm uint64
	votedFor    uint64
	log         *Log

	// leaderID is the leader this node currently believes in (None if unknown).
	// Not persisted — it is re-learned from the first heartbeat. Tracked from
	// Phase 7 so Phase 11's client has something to redirect to.
	leaderID uint64

	// Volatile — deliberately not persisted (§6).
	commitIndex uint64
	lastApplied uint64

	// Leader-only.
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	// Candidate-only.
	votes map[uint64]bool

	electionElapsed  int
	electionTimeout  int
	randomizedET     int
	heartbeatElapsed int
	heartbeatTimeout int
	rng              *rand.Rand

	// Accumulated for the next Ready.
	unstable       []Entry
	pendingMsgs    []Message
	hardStateDirty bool
	mark           readyMark
}

// readyMark records exactly what the last Ready reported, so Advance consumes
// that and not whatever has accumulated since.
type readyMark struct {
	active    bool
	hardState bool
	entries   int
	messages  int
	commit    uint64
}

// NewNode builds a node, restoring term, vote and log from Storage.
//
// A restarting node always comes back as a **Follower**, even if it was leader
// (§6). Raft keeps no persistent leader state; restoring as leader is a
// self-inflicted split brain.
func NewNode(cfg Config) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	hs, err := cfg.Storage.LoadHardState()
	if err != nil {
		return nil, err
	}
	persisted, err := cfg.Storage.LoadEntries()
	if err != nil {
		return nil, err
	}
	// Mix the node ID into the seed. Randomized election timeouts only break a
	// vote split if the nodes draw *different* values — and a chaos harness
	// naturally sets one cluster-wide seed for reproducibility. Without this
	// mixing, that seed would give every node an identical timeout stream and
	// the split would never resolve. Mixing keeps runs replayable *and* keeps
	// the streams independent.
	seed := cfg.Seed
	if seed == 0 {
		seed = 1
	}
	seed = seed*2862933555777941757 + int64(cfg.ID)*3037000493
	n := &Node{
		id:               cfg.ID,
		peers:            append([]uint64(nil), cfg.Peers...),
		role:             Follower,
		currentTerm:      hs.Term,
		votedFor:         hs.VotedFor,
		log:              NewLog(persisted),
		nextIndex:        make(map[uint64]uint64),
		matchIndex:       make(map[uint64]uint64),
		votes:            make(map[uint64]bool),
		electionTimeout:  cfg.ElectionTimeout,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		rng:              rand.New(rand.NewSource(seed)),
	}
	n.resetElectionTimer()
	return n, nil
}

// ─── accessors ───────────────────────────────────────────────────────────────

func (n *Node) ID() uint64          { return n.id }
func (n *Node) Role() Role          { return n.role }
func (n *Node) Term() uint64        { return n.currentTerm }
func (n *Node) VotedFor() uint64    { return n.votedFor }
func (n *Node) LeaderID() uint64    { return n.leaderID }
func (n *Node) CommitIndex() uint64 { return n.commitIndex }
func (n *Node) LastApplied() uint64 { return n.lastApplied }
func (n *Node) LastIndex() uint64   { return n.log.LastIndex() }
func (n *Node) Log() *Log           { return n.log }

// Quorum is the number of votes needed for a majority. Written once, generally:
// at N=1 it returns 1, so "a majority of one is trivial" falls out of the rule
// instead of being a special case Phase 7 has to delete (§5b).
func (n *Node) Quorum() int { return len(n.peers)/2 + 1 }

// ─── driving ─────────────────────────────────────────────────────────────────

// Tick advances the logical clock by one. There is no real clock anywhere in
// this package; the driver decides what a tick is worth.
func (n *Node) Tick() {
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTimeout {
			n.heartbeatElapsed = 0
			n.broadcastHeartbeat()
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.randomizedET {
		n.campaign()
	}
}

// Propose appends a command to the log. Leader only.
func (n *Node) Propose(cmd []byte) error {
	if n.role != Leader {
		return ErrNotLeader
	}
	n.appendEntry(Entry{Term: n.currentTerm, Index: n.log.LastIndex() + 1, Cmd: cmd})
	n.maybeAdvanceCommit()
	return nil
}

// Step delivers an inbound message.
func (n *Node) Step(m Message) error {
	// A message from a later term means everything this node believed is stale:
	// defer, adopt the term, and forget the vote (forgetting it is the part
	// that is easy to omit and silently disenfranchises the node for a term).
	if m.Term > n.currentTerm {
		n.becomeFollower(m.Term, None)
	}

	switch m.Type {
	case MsgVoteReq:
		n.handleVoteRequest(m)
	case MsgVoteResp:
		if n.role == Candidate && m.Term == n.currentTerm {
			n.handleVoteResponse(m)
		}
	case MsgAppReq:
		n.handleAppendEntries(m)
	case MsgAppResp:
		n.handleAppendResponse(m)
	default:
		return fmt.Errorf("consensus: unknown message type %d", m.Type)
	}
	return nil
}

// Ready reports what the driver must do. It does not mutate the node; the
// matching Advance consumes exactly what this call reported.
func (n *Node) Ready() Ready {
	rd := Ready{}
	if n.hardStateDirty {
		hs := HardState{Term: n.currentTerm, VotedFor: n.votedFor}
		rd.HardState = &hs
	}
	if len(n.unstable) > 0 {
		rd.EntriesToPersist = append([]Entry(nil), n.unstable...)
	}
	if len(n.pendingMsgs) > 0 {
		rd.Messages = append([]Message(nil), n.pendingMsgs...)
	}
	if n.commitIndex > n.lastApplied {
		rd.CommittedEntries = n.log.Slice(n.lastApplied+1, n.commitIndex)
	}
	n.mark = readyMark{
		active:    true,
		hardState: rd.HardState != nil,
		entries:   len(rd.EntriesToPersist),
		messages:  len(rd.Messages),
		commit:    n.commitIndex,
	}
	return rd
}

// Advance reports that the driver has completed the last Ready: state is
// durable, messages are sent, committed entries are applied.
func (n *Node) Advance() {
	if !n.mark.active {
		return
	}
	if n.mark.hardState {
		n.hardStateDirty = false
	}
	n.unstable = n.unstable[n.mark.entries:]
	n.pendingMsgs = n.pendingMsgs[n.mark.messages:]
	if n.mark.commit > n.lastApplied {
		n.lastApplied = n.mark.commit
	}
	n.mark = readyMark{}
}

// ─── role transitions ────────────────────────────────────────────────────────

// becomeFollower steps down into term, setting the vote to votedFor (None when
// entering a new term — a stale vote must never carry forward).
func (n *Node) becomeFollower(term, votedFor uint64) {
	if term != n.currentTerm || n.votedFor != votedFor {
		n.currentTerm = term
		n.votedFor = votedFor
		n.hardStateDirty = true
	}
	n.role = Follower
	n.leaderID = None
	n.votes = make(map[uint64]bool)
	n.resetElectionTimer()
}

// campaign starts an election. Note there is no N==1 shortcut: the real
// candidate path runs even in a cluster of one, so Phase 7 changes nothing here
// except that votes start arriving as messages (§5b).
func (n *Node) campaign() {
	n.role = Candidate
	n.leaderID = None
	n.currentTerm++
	n.votedFor = n.id
	n.hardStateDirty = true
	n.votes = map[uint64]bool{n.id: true} // vote for self, persisted before it counts
	n.resetElectionTimer()

	if n.countVotes() >= n.Quorum() {
		n.becomeLeader()
		return
	}
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.send(Message{
			Type:         MsgVoteReq,
			From:         n.id,
			To:           p,
			Term:         n.currentTerm,
			LastLogIndex: n.log.LastIndex(),
			LastLogTerm:  n.log.LastTerm(),
		})
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.id
	n.heartbeatElapsed = 0
	n.nextIndex = make(map[uint64]uint64, len(n.peers))
	n.matchIndex = make(map[uint64]uint64, len(n.peers))
	for _, p := range n.peers {
		n.nextIndex[p] = n.log.LastIndex() + 1
		n.matchIndex[p] = 0
	}
	n.matchIndex[n.id] = n.log.LastIndex()

	// The election no-op (§5c, Raft §5.4.2): an entry in the leader's own term,
	// so it can learn its commitIndex without committing a previous term's entry
	// by replica count. Cheap now, painful to retrofit once Phase 8 exists.
	n.appendEntry(Entry{Term: n.currentTerm, Index: n.log.LastIndex() + 1})
	n.maybeAdvanceCommit()

	// Assert authority immediately rather than waiting a heartbeat interval —
	// otherwise followers can time out and start a pointless election against a
	// leader that has already won.
	n.broadcastHeartbeat()
}

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.randomizedET = n.electionTimeout + n.rng.Intn(n.electionTimeout)
}

// ─── vote handling ───────────────────────────────────────────────────────────

func (n *Node) handleVoteRequest(m Message) {
	// Grant when: the candidate is not from a stale term, this node has not
	// already voted for someone else this term, and the candidate's log is at
	// least as up-to-date (§5.4.1 — the check that protects committed data).
	grant := m.Term >= n.currentTerm &&
		(n.votedFor == None || n.votedFor == m.From) &&
		n.log.IsUpToDate(m.LastLogIndex, m.LastLogTerm)

	if grant {
		n.votedFor = m.From
		n.hardStateDirty = true // persisted BEFORE the reply leaves (§2)
		n.resetElectionTimer()
	}
	n.send(Message{
		Type:    MsgVoteResp,
		From:    n.id,
		To:      m.From,
		Term:    n.currentTerm,
		Granted: grant,
	})
}

func (n *Node) handleVoteResponse(m Message) {
	n.votes[m.From] = m.Granted
	if n.countVotes() >= n.Quorum() {
		n.becomeLeader()
	}
}

// ─── AppendEntries (Phase 7: heartbeats only) ────────────────────────────────

// handleAppendEntries processes a leader's AppendEntries. In Phase 7 it never
// carries entries — replication is Phase 8 — but the log-matching check and the
// commit rule are already real.
//
// The subtlety (§3): a heartbeat from a current-or-newer term **always** resets
// the election timer, even when the log-match check fails. Leader liveness and
// log agreement are different questions, and conflating them makes a follower
// with a stale log time out and campaign against a perfectly healthy leader.
func (n *Node) handleAppendEntries(m Message) {
	if m.Term < n.currentTerm {
		// A leader from a dead term. Reply with ours so it steps down.
		n.send(Message{Type: MsgAppResp, From: n.id, To: m.From, Term: n.currentTerm})
		return
	}

	// A legitimate leader for this term: adopt it. A candidate that hears this
	// has lost the election and steps down.
	n.role = Follower
	n.leaderID = m.From
	n.resetElectionTimer()

	match := n.log.Has(m.PrevLogIndex) && n.log.Term(m.PrevLogIndex) == m.PrevLogTerm
	if match {
		// Phase 8 appends m.Entries here and truncates any conflicting suffix.
		if m.LeaderCommit > n.commitIndex {
			// Never commit past what this node can prove it holds.
			n.commitIndex = min(m.LeaderCommit, n.log.LastIndex())
		}
	}

	resp := Message{Type: MsgAppResp, From: n.id, To: m.From, Term: n.currentTerm, Success: match}
	if match {
		resp.MatchIndex = m.PrevLogIndex + uint64(len(m.Entries))
	}
	n.send(resp)
}

// handleAppendResponse folds a follower's reply into the leader's replication
// bookkeeping.
//
// A *failure* is deliberately ignored in Phase 7: walking nextIndex backward to
// find the last point of agreement and shipping the missing entries is Phase 8's
// whole job, and doing half of it here is how the log-matching property gets
// quietly broken.
func (n *Node) handleAppendResponse(m Message) {
	if n.role != Leader || m.Term != n.currentTerm {
		return
	}
	if !m.Success {
		return // Phase 8: nextIndex backoff.
	}
	if m.MatchIndex > n.matchIndex[m.From] {
		n.matchIndex[m.From] = m.MatchIndex
	}
	n.nextIndex[m.From] = n.matchIndex[m.From] + 1
	n.maybeAdvanceCommit()
}

func (n *Node) countVotes() int {
	c := 0
	for _, granted := range n.votes {
		if granted {
			c++
		}
	}
	return c
}

// ─── log and commit ──────────────────────────────────────────────────────────

func (n *Node) appendEntry(e Entry) {
	n.log.Append(e)
	n.unstable = append(n.unstable, e)
	n.matchIndex[n.id] = n.log.LastIndex()
}

// maybeAdvanceCommit implements the general commit rule, written now so Phase 8
// adds no commit logic at all (§5d):
//
//  1. take every peer's matchIndex,
//  2. find the highest index replicated on a majority,
//  3. commit it **only if it belongs to the current term** (Raft §5.4.2) — the
//     rule that stops a leader committing a previous term's entry by counting
//     replicas, which is how committed data gets lost.
//
// At N=1 this reduces to "commit my own last index", but by the general path.
func (n *Node) maybeAdvanceCommit() {
	if n.role != Leader {
		return
	}
	matches := make([]uint64, 0, len(n.peers))
	for _, p := range n.peers {
		if p == n.id {
			matches = append(matches, n.log.LastIndex())
		} else {
			matches = append(matches, n.matchIndex[p])
		}
	}
	// Descending: element [Quorum()-1] is the highest index a majority holds.
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	candidate := matches[n.Quorum()-1]

	if candidate > n.commitIndex && n.log.Has(candidate) && n.log.Term(candidate) == n.currentTerm {
		n.commitIndex = candidate
	}
}

func (n *Node) send(m Message) { n.pendingMsgs = append(n.pendingMsgs, m) }

// broadcastHeartbeat is the leader's periodic empty AppendEntries: the thing
// that keeps followers from timing out. Phase 8 gives it entries to carry.
func (n *Node) broadcastHeartbeat() {
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		// Anchor on nextIndex rather than the leader's own last index, so the
		// probe already means what Phase 8 needs it to mean.
		next := n.nextIndex[p]
		if next < 1 {
			next = 1
		}
		prev := next - 1
		var prevTerm uint64
		if n.log.Has(prev) {
			prevTerm = n.log.Term(prev)
		}
		n.send(Message{
			Type:         MsgAppReq,
			From:         n.id,
			To:           p,
			Term:         n.currentTerm,
			PrevLogIndex: prev,
			PrevLogTerm:  prevTerm,
			LeaderCommit: n.commitIndex,
		})
	}
}
