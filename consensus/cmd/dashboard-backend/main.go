// Command dashboard-backend is a tiny local HTTP server exposing a live,
// introspectable Raft cluster (consensus.Sandbox) for the quorumkv test
// dashboard's interactive view. It is not part of the library — a thin JSON
// wrapper so a browser can propose commands, tick the clock, and crash,
// restart, or partition nodes, watching the real Node/Driver/Bus code do its
// thing rather than a simulation of it.
//
//	go run ./cmd/dashboard-backend
//
// Listens on 127.0.0.1:5056. Normally launched automatically by the Flask
// dashboard (dashboard/app.py), which proxies to it.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync"

	"quorumkv/consensus"
)

var (
	mu sync.Mutex
	sb *consensus.Sandbox
)

func current() *consensus.Sandbox {
	mu.Lock()
	defer mu.Unlock()
	return sb
}

func reset(cfg consensus.SandboxConfig) (consensus.SandboxState, error) {
	next, err := consensus.NewSandbox(cfg)
	if err != nil {
		return consensus.SandboxState{}, err
	}
	mu.Lock()
	sb = next
	mu.Unlock()
	return next.State(), nil
}

func withCORS(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			return
		}
		h(w, r)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	writeJSON(w, map[string]string{"error": err.Error()})
}

type resetReq struct {
	Nodes             int   `json:"nodes"`
	ElectionTimeout   int   `json:"election_timeout"`
	HeartbeatTimeout  int   `json:"heartbeat_timeout"`
	SnapshotThreshold int   `json:"snapshot_threshold"`
	Seed              int64 `json:"seed"`
}

func handleReset(w http.ResponseWriter, r *http.Request) {
	var req resetReq
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req) // a missing/empty body just means "use defaults"
	}
	state, err := reset(consensus.SandboxConfig{
		Nodes: req.Nodes, ElectionTimeout: req.ElectionTimeout,
		HeartbeatTimeout: req.HeartbeatTimeout, SnapshotThreshold: req.SnapshotThreshold,
		Seed: req.Seed,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, state)
}

func handleState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, current().State())
}

func handleTick(w http.ResponseWriter, r *http.Request) {
	n := 1
	if v := r.URL.Query().Get("n"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			n = parsed
		}
	}
	current().Tick(n)
	writeJSON(w, current().State())
}

func handlePropose(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Node uint64 `json:"node"`
		Cmd  string `json:"cmd"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, err)
		return
	}
	if err := current().Propose(req.Node, req.Cmd); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, current().State())
}

// nodeAction wires up one of Crash/Restart/Isolate/Heal, all of which take
// {"node": id} and return the resulting state.
func nodeAction(fn func(*consensus.Sandbox, uint64) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Node uint64 `json:"node"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, err)
			return
		}
		if err := fn(current(), req.Node); err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, current().State())
	}
}

func main() {
	if _, err := reset(consensus.SandboxConfig{Nodes: 3}); err != nil {
		log.Fatalf("initial sandbox: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/reset", withCORS(handleReset))
	mux.HandleFunc("/state", withCORS(handleState))
	mux.HandleFunc("/tick", withCORS(handleTick))
	mux.HandleFunc("/propose", withCORS(handlePropose))
	mux.HandleFunc("/crash", withCORS(nodeAction((*consensus.Sandbox).Crash)))
	mux.HandleFunc("/restart", withCORS(nodeAction((*consensus.Sandbox).Restart)))
	mux.HandleFunc("/isolate", withCORS(nodeAction((*consensus.Sandbox).Isolate)))
	mux.HandleFunc("/heal", withCORS(nodeAction((*consensus.Sandbox).Heal)))

	const addr = "127.0.0.1:5056"
	log.Printf("dashboard-backend listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
