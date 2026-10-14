// Package gasclient speaks the lossy gscript-tunnel GAS mailbox protocol.
//
// GAS is deliberately not a reliable transport here. A push only hands a
// batch of opaque packets to the temporary mailbox; packets may be lost,
// duplicated, or delivered out of order. KCP above this package owns
// ordering, acknowledgement, retransmission, and flow control.
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

// Client is a thin HTTP client for the GAS mailbox. Calls are intentionally
// single-attempt: retrying a push at this layer would create duplicates and
// would duplicate the reliability machinery already provided by KCP.
type Client struct {
	URL, Token, Sess string
	HC               *http.Client
}

func New(url, token, sess string) *Client {
	return &Client{
		URL: url, Token: token, Sess: sess,
		HC: &http.Client{Timeout: 150 * time.Second}, // > max long-poll hold (90s)
		// GAS /exec normally answers with a redirect. Keep the default
		// redirect policy because it is part of the deployed web-app path.
	}
}

// call sends one mailbox operation. It has no application-level retry.
// A failed push is intentionally treated as packet loss and KCP will resend
// the packet when its own ACK is not received.
func (c *Client) call(body map[string]any) (map[string]any, error) {
	body["token"] = c.Token
	body["sess"] = c.Sess
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", c.URL, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HC.Do(req)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("gas http %d: %.120s", resp.StatusCode, string(data))
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	if em, ok := out["error"].(string); ok && em != "" {
		return nil, fmt.Errorf("gas error: %s", em)
	}
	return out, nil
}

// Push sends one opaque packet. The response is only an HTTP/application
// status; it is not a packet acknowledgement.
func (c *Client) Push(dir, stream string, data []byte) error {
	return c.PushBatch(dir, stream, [][]byte{data})
}

// PushBatch sends several opaque packets in one GAS request. The order in
// this slice is not a protocol guarantee; KCP supplies ordering above us.
func (c *Client) PushBatch(dir, stream string, packets [][]byte) error {
	encoded := make([]string, 0, len(packets))
	for _, packet := range packets {
		encoded = append(encoded, base64.StdEncoding.EncodeToString(packet))
	}
	_, err := c.call(map[string]any{
		"op": "push", "dir": dir, "stream": stream, "packets": encoded,
	})
	return err
}

// Poll waits for a best-effort batch of opaque packets. The GAS side may
// destructively remove packets before the HTTP response reaches us; that is
// acceptable because KCP retransmits packets whose ACKs do not arrive.
func (c *Client) Poll(dir, stream string, waitSec int) ([][]byte, error) {
	out, err := c.call(map[string]any{
		"op": "poll", "dir": dir, "stream": stream, "wait": waitSec,
	})
	if err != nil {
		return nil, err
	}
	arr, _ := out["packets"].([]any)
	packets := make([][]byte, 0, len(arr))
	for _, item := range arr {
		s, ok := item.(string)
		if !ok || s == "" {
			continue
		}
		packet, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			continue
		}
		packets = append(packets, packet)
	}
	return packets, nil
}
