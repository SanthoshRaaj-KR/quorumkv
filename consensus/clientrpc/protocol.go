// Package clientrpc is the phase-11 client-facing wire protocol
// (planning/phase-11-client.md): a hand-rolled HTTP/1.1 subset, JSON
// bodies — the same fork phase-10 already resolved for the storage
// sidecar (planning/phase-10-apply-seam.md §1a), applied one layer out.
// It sits beside consensus/engine: both import consensus, neither is
// imported back by it.
package clientrpc

// Binary fields are plain []byte, not manually base64-encoded strings —
// encoding/json already renders a []byte as base64 and decodes it back,
// so this gets the sidecar's own "binary values survive JSON intact"
// convention for free (the same reasoning consensus.SandboxEntry.Cmd
// already documents).

type putRequest struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type deleteRequest struct {
	Key []byte `json:"key"`
}

type getRequest struct {
	Key []byte `json:"key"`
}

type getResponse struct {
	Value []byte `json:"value,omitempty"`
	Found bool   `json:"found"`
}

// errorResponse is the shape of every non-2xx reply. LeaderID is the
// DESIGN.md §5 step 2 redirect hint: non-zero names the node the caller
// should retry against, zero means "unknown — an election is likely in
// progress, try someone else."
type errorResponse struct {
	Error    string `json:"error"`
	LeaderID uint64 `json:"leaderId,omitempty"`
}

type statusResponse struct {
	ID       uint64 `json:"id"`
	Role     string `json:"role"`
	Term     uint64 `json:"term"`
	LeaderID uint64 `json:"leaderId"`
}
