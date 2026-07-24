package engine

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client talks to exactly one node's local sidecar (phase-10 §6) over its
// hand-rolled HTTP protocol. One Client per node — it must never cross to
// another node's sidecar, the same "only your own local engine" rule that
// keeps a node's Raft process and its sidecar a matched pair.
type Client struct {
	addr string
	http *http.Client
}

// NewClient builds a client bound to addr (e.g. "127.0.0.1:51285").
func NewClient(addr string) *Client {
	return &Client{addr: addr, http: &http.Client{Timeout: 10 * time.Second}}
}

func (c *Client) post(path string, req any) ([]byte, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Post("http://"+c.addr+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("engine: %s: %w", path, err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("engine: %s: read response: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		var errResp struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(out, &errResp)
		if errResp.Error != "" {
			return nil, fmt.Errorf("engine: %s: %s", path, errResp.Error)
		}
		return nil, fmt.Errorf("engine: %s: status %d", path, resp.StatusCode)
	}
	return out, nil
}

// Put durably writes key -> value on this node's local engine.
func (c *Client) Put(key, value []byte) error {
	_, err := c.post("/put", map[string]string{
		"key":   base64.StdEncoding.EncodeToString(key),
		"value": base64.StdEncoding.EncodeToString(value),
	})
	return err
}

// Delete durably records a tombstone for key.
func (c *Client) Delete(key []byte) error {
	_, err := c.post("/delete", map[string]string{"key": base64.StdEncoding.EncodeToString(key)})
	return err
}

// Get reads key directly from this node's local engine — bypassing Raft
// entirely (DESIGN.md §5, phase-10 §4). ok is false if the key is absent.
func (c *Client) Get(key []byte) (value []byte, ok bool, err error) {
	out, err := c.post("/get", map[string]string{"key": base64.StdEncoding.EncodeToString(key)})
	if err != nil {
		return nil, false, err
	}
	var resp struct {
		Value *string `json:"value"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		return nil, false, fmt.Errorf("engine: get: decode response: %w", err)
	}
	if resp.Value == nil {
		return nil, false, nil
	}
	val, err := base64.StdEncoding.DecodeString(*resp.Value)
	if err != nil {
		return nil, false, fmt.Errorf("engine: get: decode value: %w", err)
	}
	return val, true, nil
}

// Snapshot asks the sidecar to package its current SSTable set as one blob
// (phase-10 §6a).
func (c *Client) Snapshot() ([]byte, error) {
	out, err := c.post("/snapshot", map[string]string{})
	if err != nil {
		return nil, err
	}
	var resp struct {
		Data string `json:"data"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		return nil, fmt.Errorf("engine: snapshot: decode response: %w", err)
	}
	blob, err := base64.StdEncoding.DecodeString(resp.Data)
	if err != nil {
		return nil, fmt.Errorf("engine: snapshot: decode blob: %w", err)
	}
	return blob, nil
}

// Restore replaces this node's entire local engine state with data
// previously returned by Snapshot — this node's own, or one installed from
// a leader. An installed snapshot is authoritative: whatever was here before
// is discarded (phase-10 §6a).
func (c *Client) Restore(data []byte) error {
	_, err := c.post("/restore", map[string]string{"data": base64.StdEncoding.EncodeToString(data)})
	return err
}

// Stats is lightweight engine introspection for tooling (the sandbox UI),
// never consulted by Raft.
type Stats struct {
	SSTables   int `json:"sstables"`
	ApproxSize int `json:"approxSize"`
}

// Stats reports this node's current engine stats.
func (c *Client) Stats() (Stats, error) {
	resp, err := c.http.Get("http://" + c.addr + "/stats")
	if err != nil {
		return Stats{}, fmt.Errorf("engine: stats: %w", err)
	}
	defer resp.Body.Close()
	var s Stats
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return Stats{}, fmt.Errorf("engine: stats: decode response: %w", err)
	}
	return s, nil
}
