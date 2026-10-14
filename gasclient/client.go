// Package gasclient speaks the gscript-tunnel GAS doPost protocol.
package gasclient

import (
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
}

func New(url, token, sess string) *Client {
	return &Client{
		URL: url, Token: token, Sess: sess,
		HC: &http.Client{Timeout: 150 * time.Second}, // > max long-poll hold (90s)
		// NOTE: GAS /exec answers every POST with a 302 to a googleusercontent
		// echo URL. Go's default redirect policy turns the follow-up into a
		// bodyless GET, which is exactly right: GAS replays the original POST
		// body against doPost and returns its output. Do NOT override
		// CheckRedirect here.
	}
}

// call POSTs one JSON body and returns the decoded JSON response.
// The GAS 302 -> GET dance is handled by the http.Client's default redirect
// policy (see New). Retries transient transport failures. Push stays
// idempotent because the server dedups by seq (seq <= tail -> {ok, dup}).
func (c *Client) call(body map[string]any) (map[string]any, error) {
	body["token"] = c.Token
	body["sess"] = c.Sess
	raw, _ := json.Marshal(body)
	var last error
	for attempt := 0; attempt < 6; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
		req, err := http.NewRequest("POST", c.URL, bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.HC.Do(req)
		if err != nil {
			last = err
			continue
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			last = err
			continue
		}
		if resp.StatusCode/100 != 2 {
			last = fmt.Errorf("gas http %d: %.120s", resp.StatusCode, string(data))
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
