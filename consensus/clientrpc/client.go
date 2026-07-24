package clientrpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

// retryBackoff is the pause before trying a *different* node after an
// unreachable node or an unknown-leader response (an election is plausibly
// still in progress). A known-good redirect (a non-zero LeaderID) skips
// this entirely — that hop isn't a failure, it's the protocol working.
const retryBackoff = 25 * time.Millisecond

// attemptsPerAddr bounds retries as a multiple of the address count (§5):
// enough rounds that a full leader election (typically a couple of
// election-timeout windows) resolves within the backoff budget, while still
// giving up on a genuinely leaderless cluster instead of retrying forever.
const attemptsPerAddr = 6

// Client is the phase-11 client library (§5): it holds a static set of node
// addresses (no discovery — the standing project rule every transport in
// this codebase already follows), finds the leader, follows redirects, and
// retries through a leader election, so the caller writes none of that
// retry logic itself (ROADMAP's phase-11 done-when).
type Client struct {
	addrs map[uint64]string
	ids   []uint64 // addrs' keys, fixed order, for round-robin fallback
	http  *http.Client

	mu   sync.Mutex
	last uint64 // last known leader id; 0 = unknown, start from ids[0]
}

// New builds a Client addressed at addrs (node id -> client-RPC "host:port").
func New(addrs map[uint64]string) *Client {
	ids := make([]uint64, 0, len(addrs))
	for id := range addrs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return &Client{
		addrs: addrs,
		ids:   ids,
		http:  &http.Client{Timeout: 10 * time.Second},
	}
}

// Put durably writes key -> value, waiting for it to commit and apply on
// the leader before returning (consensus.Server.ProposeAndWait, phase-11 §3).
func (c *Client) Put(key, value []byte) error {
	_, err := c.do("/put", putRequest{Key: key, Value: value}, nil)
	return err
}

// Delete durably records a tombstone for key.
func (c *Client) Delete(key []byte) error {
	_, err := c.do("/delete", deleteRequest{Key: key}, nil)
	return err
}

// Get reads key from the current leader (leader-only consistency, phase-11
// §4). ok is false if the key is absent.
func (c *Client) Get(key []byte) (value []byte, ok bool, err error) {
	var resp getResponse
	if _, err := c.do("/get", getRequest{Key: key}, &resp); err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

// do implements §5's algorithm: try the last known leader (or the lowest
// id, first call), follow a redirect hint immediately, and on anything
// else — unreachable node, unknown leader, decode failure — hop to the next
// id after a short backoff. Bounded by 2x the address count so every node
// gets a second look in case an election resolved mid-loop.
func (c *Client) do(path string, req any, out any) (uint64, error) {
	if len(c.ids) == 0 {
		return 0, errors.New("clientrpc: no addresses configured")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("clientrpc: %s: encode request: %w", path, err)
	}

	c.mu.Lock()
	target := c.last
	c.mu.Unlock()
	if target == 0 {
		target = c.ids[0]
	}

	attempts := attemptsPerAddr * len(c.ids)
	var lastErr error
	for i := 0; i < attempts; i++ {
		addr, ok := c.addrs[target]
		if !ok {
			target = c.nextID(target)
			continue
		}

		status, respBody, err := c.post(addr, path, body)
		if err != nil {
			lastErr = err
			time.Sleep(retryBackoff)
			target = c.nextID(target)
			continue
		}

		if status == http.StatusOK {
			c.mu.Lock()
			c.last = target
			c.mu.Unlock()
			if out != nil {
				if err := json.Unmarshal(respBody, out); err != nil {
					return target, fmt.Errorf("clientrpc: %s: decode response: %w", path, err)
				}
			}
			return target, nil
		}

		var errResp errorResponse
		_ = json.Unmarshal(respBody, &errResp)
		if errResp.Error == "" {
			errResp.Error = fmt.Sprintf("status %d", status)
		}
		lastErr = errors.New(errResp.Error)
		if errResp.LeaderID != 0 {
			target = errResp.LeaderID // a known-good redirect: no backoff, no wasted hop
			continue
		}
		time.Sleep(retryBackoff)
		target = c.nextID(target)
	}
	return 0, fmt.Errorf("clientrpc: %s: no leader found after %d attempts: %w", path, attempts, lastErr)
}

func (c *Client) nextID(id uint64) uint64 {
	for i, x := range c.ids {
		if x == id {
			return c.ids[(i+1)%len(c.ids)]
		}
	}
	return c.ids[0]
}

func (c *Client) post(addr, path string, body []byte) (status int, respBody []byte, err error) {
	resp, err := c.http.Post("http://"+addr+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, out, nil
}
