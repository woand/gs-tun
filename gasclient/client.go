// Package gasclient speaks the gscript-tunnel GAS doPost protocol.
package gasclient

import (
	"sync"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// CtlStream is the reserved control stream (dir "up").
const CtlStream = "__ctl__"

// Chunk is one ordered payload unit from a poll response.
type Chunk struct {
	Seq  int
	Data []byte
	Fin  bool
}

type Client struct {
	URL, Token, Sess string
	HC               *http.Client

	mu    sync.Mutex
	redir string // cached GAS redirect target (/exec -> googleusercontent)
}

func New(url, token, sess string) *Client {
	return &Client{
		URL: url, Token: token, Sess: sess,
		HC: &http.Client{
			Timeout: 150 * time.Second, // > max long-poll hold (90s)
			// Do NOT auto-follow: Go would turn the GAS 302 into a GET
			// and drop the body. We re-POST manually below.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// target returns the cached redirect URL if known, else the /exec URL.
func (c *Client) target() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.redir != "" {
		return c.redir
	}
	return c.URL
}

func (c *Client) setRedir(u string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.redir = u
}

func (c *Client) doPost(url string, raw []byte) (data []byte, status int, loc string, err error) {
	req, err := http.NewRequest("POST", url, bytes.NewReader(raw))
	if err != nil {
		return nil, 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HC.Do(req)
	if err != nil {
		return nil, 0, "", err
	}
	defer resp.Body.Close()
	data, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, "", err
	}
	return data, resp.StatusCode, resp.Header.Get("Location"), nil
}

// call POSTs one JSON body and returns the decoded JSON response.
// GAS /exec always 302-redirects to googleusercontent.com; the redirect
// is followed with a repeat POST (same body) and the target is cached.
// Retries transient transport failures. Push stays idempotent because the
// server dedups by seq (seq <= tail -> {ok, dup}).
func (c *Client) call(body map[string]any) (map[string]any, error) {
	body["token"] = c.Token
	body["sess"] = c.Sess
	raw, _ := json.Marshal(body)
	var last error
	for attempt := 0; attempt < 6; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
		data, status, loc, err := c.doPost(c.target(), raw)
		if err != nil {
			last = err
			continue
		}
		if status == 301 || status == 302 || status == 303 {
			if loc == "" {
				last = fmt.Errorf("gas redirect without location")
				continue
			}
			c.setRedir(loc)
			data, status, _, err = c.doPost(loc, raw)
			if err != nil {
				last = err
				continue
			}
		}
		if status == 403 {
			return nil, fmt.Errorf("auth rejected (bad token?)")
		}
		if status/100 != 2 {
			last = fmt.Errorf("gas http %d", status)
			continue
		}
		var out map[string]any
		if err := json.Unmarshal(data, &out); err != nil {
			last = err
			continue
		}
		if em, ok := out["error"].(string); ok && em != "" {
			return nil, fmt.Errorf("gas error: %s", em)
		}
		return out, nil
	}
	return nil, fmt.Errorf("gas unreachable: %w", last)
}

// DesyncError reports the seq the server expects. For atomic datagrams
// the writer can adopt it and resend safely.
type DesyncError struct{ Expect int }

func (e *DesyncError) Error() string {
	return fmt.Sprintf("seq desync: server expects %d", e.Expect)
}

// Push appends one chunk to the (dir, stream) queue. Returns the acked seq.
// Retried POSTs are deduped by the server (seq <= tail -> {ok, dup}).
func (c *Client) Push(dir, stream string, seq int, data []byte, fin bool) (int, error) {
	enc := base64.StdEncoding.EncodeToString(data)
	out, err := c.call(map[string]any{
		"op": "push", "dir": dir, "stream": stream,
		"seq": seq, "data": enc, "fin": fin,
	})
	if err != nil {
		return 0, err
	}
	if ok, _ := out["ok"].(bool); ok {
		return int(out["ack"].(float64)), nil
	}
	if expect, ok := out["expect"].(float64); ok {
		return 0, &DesyncError{Expect: int(expect)}
	}
	return 0, fmt.Errorf("push rejected on %s/%s: %v", dir, stream, out)
}

// Poll long-holds (waitSec, capped server-side at 90) for new chunks after `after`.
// Returns chunks in order and the new `after` cursor.
func (c *Client) Poll(dir, stream string, after, waitSec int) ([]Chunk, int, error) {
	out, err := c.call(map[string]any{
		"op": "poll", "dir": dir, "stream": stream,
		"after": after, "wait": waitSec,
	})
	if err != nil {
		return nil, after, err
	}
	var chunks []Chunk
	if arr, ok := out["chunks"].([]any); ok {
		for _, it := range arr {
			m, ok := it.(map[string]any)
			if !ok {
				continue
			}
			var d []byte
			if s, ok := m["data"].(string); ok && s != "" {
				d, _ = base64.StdEncoding.DecodeString(s)
			}
			fin, _ := m["fin"].(bool)
			chunks = append(chunks, Chunk{
				Seq:  int(m["seq"].(float64)),
				Data: d,
				Fin:  fin,
			})
		}
	}
	next := after
	if n, ok := out["next"].(float64); ok {
		next = int(n)
	}
	return chunks, next, nil
}
