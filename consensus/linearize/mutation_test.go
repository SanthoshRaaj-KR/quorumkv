package linearize

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"quorumkv/consensus"
	"quorumkv/consensus/clientrpc"
)

// The mutation test (planning/phase-14-linearizability.md §5.3): checkpoints
// 1 and 2 only prove the checker agrees with itself — on hand-built
// histories it was designed against, and on a real cluster that (as far as
// we know) has no bugs. Neither rules out the checker being too permissive.
// This test closes that gap: it deliberately wires in a real, plausible bug
// — a node answering GET from a periodically-refreshed local snapshot
// instead of live state, with no leadership check at all (the same failure
// shape as "a stale follower serves a read directly," generalized to cover
// a stale leader too) — and asserts Check() actually catches it.
//
// No change to consensus/clientrpc's production code: the bug is injected
// entirely within this package's own test-only HTTP handler, composed with
// the real handler for every endpoint except /get.

// staleCache is a periodically-refreshed snapshot of a memSM's data,
// independent of when writes actually land — a stand-in for "this node's
// idea of the current state is up to `refresh` behind reality," which is
// exactly what a correct implementation's leader-only check exists to
// prevent a client from ever observing as if authoritative.
type staleCache struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func newStaleCache(sm *memSM, refresh time.Duration, stop <-chan struct{}) *staleCache {
	c := &staleCache{data: make(map[string][]byte)}
	c.refresh(sm)
	go func() {
		ticker := time.NewTicker(refresh)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				c.refresh(sm)
			}
		}
	}()
	return c
}

func (c *staleCache) refresh(sm *memSM) {
	sm.mu.Lock()
	snap := make(map[string][]byte, len(sm.data))
	for k, v := range sm.data {
		snap[k] = append([]byte(nil), v...)
	}
	sm.mu.Unlock()

	c.mu.Lock()
	c.data = snap
	c.mu.Unlock()
}

func (c *staleCache) Get(key []byte) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.data[string(key)]
	return v, ok
}

// buggyGetHandler delegates every endpoint except /get to the real
// clientrpc.Server, and answers /get unconditionally from a staleCache —
// no leadership check, no freshness check, exactly the bug class §5.3
// needs the checker to catch.
func buggyGetHandler(srv *consensus.Server, sm *memSM, cache *staleCache) http.Handler {
	real := clientrpc.NewServer(srv, sm, 2*time.Second).Handler()

	mux := http.NewServeMux()
	mux.Handle("/put", real)
	mux.Handle("/delete", real)
	mux.Handle("/status", real)
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Key []byte `json:"key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		value, found := cache.Get(req.Key)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"value": value, "found": found})
	})
	return mux
}

func TestMutationCheckerCatchesAStaleUncheckedRead(t *testing.T) {
	// Long enough that no refresh tick can possibly land inside this test's
	// own sub-second PUT-then-GET window, however long cluster startup and
	// leader election happened to take — determinism matters more here than
	// modeling a realistic refresh cadence (the staleCache type itself is
	// still a generically reusable "periodically stale" seam).
	const refresh = time.Hour
	stop := make(chan struct{})
	defer close(stop)

	var caches []*staleCache
	var mu sync.Mutex
	c, err := buildCluster(3, 1, func(srv *consensus.Server, sm *memSM) http.Handler {
		cache := newStaleCache(sm, refresh, stop)
		mu.Lock()
		caches = append(caches, cache)
		mu.Unlock()
		return buggyGetHandler(srv, sm, cache)
	})
	if err != nil {
		t.Fatalf("buildCluster: %v", err)
	}
	defer c.stop()

	if _, err := c.waitLeader(10 * time.Second); err != nil {
		t.Fatalf("waitLeader: %v", err)
	}

	cl := clientrpc.New(c.clientAddrs())
	h := NewHistory()

	// A PUT that fully completes (real time) well before the stale cache's
	// next refresh — every node's cache is guaranteed to still hold the
	// pre-write snapshot for the immediate future.
	h.Do("k", OpPut, "op-1", func() (Value, bool) {
		err := cl.Put([]byte("k"), []byte("op-1"))
		return Value{}, err == nil
	})

	// A real gap, not just "the next line of Go" — two operations issued
	// back-to-back in one goroutine can land within the same wall-clock
	// tick (observed in practice: identical timestamps to the nanosecond
	// on some runs), which the checker then — correctly, given the
	// intervals it was actually handed — treats as concurrent rather than
	// ordered. That's not a checker bug; it's the same inherent limit any
	// wall-clock-based linearizability tool has at sub-clock-resolution
	// timescales. A few milliseconds of real separation avoids it here,
	// where the point is a guaranteed catch, not probing that boundary.
	time.Sleep(5 * time.Millisecond)

	// A raw GET straight against the buggy handler — deterministically
	// observes the stale (pre-write) snapshot, since staleCache never
	// refreshes during this test at all (§ above).
	h.Do("k", OpGet, "", func() (Value, bool) {
		var addr string
		for _, a := range c.clientAddrs() {
			addr = a
			break
		}
		val, found, err := rawGet(addr, "k")
		if err != nil {
			return Value{}, false
		}
		return Value{Found: found, Data: val}, true
	})

	ok, v := Check(h)
	if ok {
		t.Fatal("expected the checker to catch a stale, leadership-unchecked read, but it reported linearizable")
	}
	t.Logf("checker correctly caught the injected bug:\n%s", v.Dump())
}

// rawGet bypasses clientrpc.Client's redirect-following entirely — this
// test wants to hit a specific node's handler directly, not whichever one
// the client's retry loop happens to land on. []byte fields round-trip
// through encoding/json's own base64 encoding automatically (both here and
// in buggyGetHandler's response), the same convention clientrpc's real
// protocol types use (planning/phase-11-client.md §2a).
func rawGet(addr, key string) (string, bool, error) {
	reqBody, err := json.Marshal(struct {
		Key []byte `json:"key"`
	}{Key: []byte(key)})
	if err != nil {
		return "", false, err
	}
	resp, err := http.Post("http://"+addr+"/get", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	var out struct {
		Value []byte `json:"value"`
		Found bool   `json:"found"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", false, err
	}
	return string(out.Value), out.Found, nil
}
