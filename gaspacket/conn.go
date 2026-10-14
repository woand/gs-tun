// Package gaspacket implements net.PacketConn over the lossy GAS mailbox.
// Each item is one opaque KCP datagram. GAS may lose, duplicate, or reorder
// items; KCP owns reliability and ordering.
package gaspacket

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gscript-tunnel/gasclient"
)

// Stream is the default KCP datagram stream. Run-specific sessions are
// normally used so a restarted KCP conversation cannot consume stale data.
const Stream = "dgram"

const (
	txBatchMax    = 16
	txBatchWait   = 25 * time.Millisecond
	txPushWorkers = 4
)

// SessForToken derives the legacy bus namespace.
func SessForToken(token string) string {
	return SessForTokenAndServer(token, "")
}

// SessForTokenAndServer derives a stable base namespace used for control
// announcements. Data sessions should prefer SessForRun.
func SessForTokenAndServer(token, serverID string) string {
	sum := sha256.Sum256([]byte("gs-tun:" + token + ":sv:" + serverID))
	return "g-" + hex.EncodeToString(sum[:])[:12]
}

// SessForRun creates an isolated mailbox namespace for one KCP conversation.
// This replaces the old startup tail scan now that GAS no longer has seq.
func SessForRun(token, serverID, runID string) string {
	sum := sha256.Sum256([]byte("gs-tun-run:" + token + ":sv:" + serverID + ":run:" + runID))
	return "r-" + hex.EncodeToString(sum[:])[:16]
}

// ServerID generates a stable server identifier from the machine hostname.
func ServerID(token string) string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	sum := sha256.Sum256([]byte("gs-tun-sv:" + token + ":" + host))
	return hex.EncodeToString(sum[:])[:8]
}

type fakeAddr string

func (a fakeAddr) Network() string { return "gas" }
func (a fakeAddr) String() string  { return string(a) }

// Conn is a virtual datagram socket over the GAS mailbox.
type Conn struct {
	gc       *gasclient.Client
	pushDir  string
	pollDir  string
	stream   string
	tx       chan []byte
	txDrop   uint64
	txFail   uint64
	rx       chan []byte
	rmu      sync.Mutex
	deadline time.Time
	die      chan struct{}
	dieOnce  sync.Once
	laddr    net.Addr
	raddr    net.Addr
}

var errClosed = &net.OpError{Op: "read", Err: fmt.Errorf("gaspacket: closed")}

type timeoutError struct{}

func (e timeoutError) Error() string   { return "gaspacket: i/o timeout" }
func (e timeoutError) Timeout() bool   { return true }
func (e timeoutError) Temporary() bool { return true }

// Dial creates a lossy virtual datagram socket. The optional stream argument
// allows a run-specific mailbox; omitting it keeps the legacy stream name.
func Dial(gc *gasclient.Client, pushDir, pollDir string, streams ...string) (*Conn, error) {
	stream := Stream
	if len(streams) > 0 && streams[0] != "" {
		stream = streams[0]
	}
	c := &Conn{
		gc:      gc,
		pushDir: pushDir,
		pollDir: pollDir,
		stream:  stream,
		tx:      make(chan []byte, 4096),
		rx:      make(chan []byte, 2048),
		die:     make(chan struct{}),
		laddr:   fakeAddr("gas://local"),
		raddr:   fakeAddr("gas://peer"),
	}
	go c.pollLoop()
	go c.senderLoop()
	return c, nil
}

func (c *Conn) senderLoop() {
	workers := make(chan struct{}, txPushWorkers)
	for {
		batch, ok := c.nextBatch()
		if !ok {
			return
		}
		select {
		case workers <- struct{}{}:
		case <-c.die:
			return
		}
		go func(packets [][]byte) {
			defer func() { <-workers }()
			// A failed push is deliberately not retried here. It is packet
			// loss at the GAS layer; KCP will retransmit after its ACK timeout.
			if err := c.gc.PushBatch(c.pushDir, c.stream, packets); err != nil {
				atomic.AddUint64(&c.txFail, 1)
			}
		}(batch)
	}
}

func (c *Conn) nextBatch() ([][]byte, bool) {
	var first []byte
	select {
	case first = <-c.tx:
	case <-c.die:
		return nil, false
	}
	batch := make([][]byte, 0, txBatchMax)
	batch = append(batch, first)
	timer := time.NewTimer(txBatchWait)
	defer timer.Stop()
	for len(batch) < txBatchMax {
		select {
		case packet := <-c.tx:
			batch = append(batch, packet)
		case <-timer.C:
			return batch, true
		case <-c.die:
			return batch, true
		}
	}
	return batch, true
}

func (c *Conn) pollLoop() {
	for {
		select {
		case <-c.die:
			return
		default:
		}
		packets, err := c.gc.Poll(c.pollDir, c.stream, 75)
		if err != nil {
			select {
			case <-c.die:
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		for _, packet := range packets {
			if len(packet) == 0 {
				continue
			}
			select {
			case c.rx <- packet:
			case <-c.die:
				return
			default:
				// KCP will recover a dropped datagram through its own
				// retransmission. Never block the mailbox reader.
				atomic.AddUint64(&c.txDrop, 1)
			}
		}
	}
}

func (c *Conn) ReadFrom(p []byte) (int, net.Addr, error) {
	c.rmu.Lock()
	dl := c.deadline
	c.rmu.Unlock()

	var timer <-chan time.Time
	if !dl.IsZero() {
		if d := time.Until(dl); d <= 0 {
			return 0, nil, timeoutError{}
		}
		t := time.NewTimer(time.Until(dl))
		defer t.Stop()
		timer = t.C
	}
	select {
	case b := <-c.rx:
		return copy(p, b), c.raddr, nil
	case <-timer:
		return 0, nil, timeoutError{}
	case <-c.die:
		return 0, nil, errClosed
	}
}

// WriteTo queues a datagram without blocking KCP's update loop. A full queue
// is intentional lossy backpressure; KCP owns retransmission.
func (c *Conn) WriteTo(p []byte, addr net.Addr) (int, error) {
	select {
	case <-c.die:
		return 0, errClosed
	default:
	}
	dgram := make([]byte, len(p))
	copy(dgram, p)
	select {
	case c.tx <- dgram:
		return len(p), nil
	case <-c.die:
		return 0, errClosed
	default:
		atomic.AddUint64(&c.txDrop, 1)
		return len(p), nil
	}
}

func (c *Conn) Close() error {
	c.dieOnce.Do(func() { close(c.die) })
	return nil
}

func (c *Conn) LocalAddr() net.Addr { return c.laddr }

func (c *Conn) SetDeadline(t time.Time) error {
	c.rmu.Lock()
	c.deadline = t
	c.rmu.Unlock()
	return nil
}

func (c *Conn) SetReadDeadline(t time.Time) error  { return c.SetDeadline(t) }
func (c *Conn) SetWriteDeadline(t time.Time) error { return nil }
