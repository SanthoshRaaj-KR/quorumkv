// quorumkv consensus layer (Track B — Raft).
//
// Phase 6 is stdlib-only by decision (planning/phase-06-raft-single.md §10).
// The first dependency (gRPC) arrives with the transport in Phase 7.
module quorumkv/consensus

go 1.26
