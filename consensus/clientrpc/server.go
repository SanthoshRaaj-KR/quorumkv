package clientrpc

import (
	"encoding/json"
	"net/http"
	"time"

	"quorumkv/consensus"
	"quorumkv/consensus/engine"
)

// Getter is the local-read half of the seam (phase-10 §4, phase-11 §4): GET
// bypasses Raft entirely and answers from a node's own local engine.
// Declared here as a small interface, the same pattern [consensus.Transport]
// and [consensus.StateMachine] already use, rather than naming
// *engine.StateMachine directly — Server has no reason to know that
// concrete type, only that it can read a key.
type Getter interface {
	Get(key []byte) (value []byte, ok bool, err error)
}

// DefaultProposeTimeout bounds how long a PUT/DELETE waits for its entry to
// commit and apply before telling the caller to retry (phase-11 §3). Kept
// well under a client's own retry patience so a stuck node fails fast
// enough for the client to try somewhere else.
const DefaultProposeTimeout = 3 * time.Second

// Server exposes one node's consensus.Server + local engine over the
// client-facing HTTP protocol (phase-11 §2a). One Server per node — the
// same "never reach past your own node" rule engine.Client already follows
// for the sidecar link.
type Server struct {
	raft    *consensus.Server
	engine  Getter
	timeout time.Duration
}

// NewServer builds a Server. timeout <= 0 uses [DefaultProposeTimeout].
func NewServer(raft *consensus.Server, eng Getter, timeout time.Duration) *Server {
	if timeout <= 0 {
		timeout = DefaultProposeTimeout
	}
	return &Server{raft: raft, engine: eng, timeout: timeout}
}

// Handler returns the client-facing HTTP handler (§2a's four endpoints).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/put", s.handlePut)
	mux.HandleFunc("/delete", s.handleDelete)
	mux.HandleFunc("/get", s.handleGet)
	mux.HandleFunc("/status", s.handleStatus)
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// writeRedirect reports a request this node couldn't serve because it isn't
// leader, carrying the leader hint DESIGN.md §5 step 2 asks for. A 503
// (Service Unavailable) rather than a 4xx: from the client's perspective
// this node is a legitimate address that just can't help right now, not a
// malformed request.
func writeRedirect(w http.ResponseWriter, leaderID uint64, err error) {
	w.WriteHeader(http.StatusServiceUnavailable)
	writeJSON(w, errorResponse{Error: err.Error(), LeaderID: leaderID})
}

func writeBadRequest(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	writeJSON(w, errorResponse{Error: err.Error()})
}

// propose runs cmd through ProposeAndWait and translates the three retryable
// consensus outcomes (§3) into the wire's redirect shape; anything else
// (a request decode failure) is a plain 400 the client should not retry.
func (s *Server) propose(w http.ResponseWriter, cmd []byte) {
	_, err := s.raft.ProposeAndWait(cmd, s.timeout)
	if err == nil {
		writeJSON(w, struct{}{})
		return
	}
	switch err {
	case consensus.ErrNotLeader, consensus.ErrProposalLost, consensus.ErrProposeTimeout:
		writeRedirect(w, s.leaderHint(), err)
	default:
		writeBadRequest(w, err)
	}
}

// leaderHint reads the current LeaderID for a redirect response, treating a
// [consensus.ErrStopped] status read (the node is shutting down) as simply
// "no hint available" rather than failing the whole response.
func (s *Server) leaderHint() uint64 {
	st, err := s.raft.Status()
	if err != nil {
		return 0
	}
	return st.LeaderID
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequest(w, err)
		return
	}
	s.propose(w, engine.EncodeCommand(engine.Command{Op: engine.OpPut, Key: req.Key, Value: req.Value}))
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	var req deleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequest(w, err)
		return
	}
	s.propose(w, engine.EncodeCommand(engine.Command{Op: engine.OpDelete, Key: req.Key}))
}

// handleGet serves a read only when this node believes it's leader —
// phase-11 §4's locked leader-only consistency mode, the same redirect path
// as a write so the client has exactly one retry loop for all three
// operations (§5).
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	var req getRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequest(w, err)
		return
	}
	st, err := s.raft.Status()
	if err != nil {
		writeBadRequest(w, err)
		return
	}
	if st.Role != consensus.Leader {
		writeRedirect(w, st.LeaderID, consensus.ErrNotLeader)
		return
	}
	value, ok, err := s.engine.Get(req.Key)
	if err != nil {
		writeBadRequest(w, err)
		return
	}
	writeJSON(w, getResponse{Value: value, Found: ok})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st, err := s.raft.Status()
	if err != nil {
		writeBadRequest(w, err)
		return
	}
	writeJSON(w, statusResponse{ID: st.ID, Role: st.Role.String(), Term: st.Term, LeaderID: st.LeaderID})
}
