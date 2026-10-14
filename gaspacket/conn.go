// Package gaspacket implements net.PacketConn over the GAS doPost bus,
// so kcp-go sessions (client NewConn / server ServeConn) can run without
// any UDP or unix socket. One KCP datagram == one bus chunk, preserving
// packet boundaries exactly like a datagram socket.
//
// Only ONE KCP client session per bus (token) is supported: the bus-level
// sequence numbers assume a single ordered writer per direction.
package gaspacket

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"gscript-tunnel/gasclient"
)

// Stream is the bus stream carrying KCP datagrams.
const Stream = "dgram"

// SessForToken derives the bus namespace from the token so both ends agree
// without a --sess flag. One token == one client<->server pair.
func SessForToken(token string) string {
	sum := sha256.Sum256([]byte("gs-tun:" + token))
	return "g-" + hex.EncodeToString(sum[:])[:12]
}

type fakeAddr string

func (a fakeAddr) Network() string { return "gas" }
func (a fakeAddr) String() string  { return string(a) }

// Conn is a virtual datagram socket over the GAS bus.
type Conn struct {
	gc       *gasclient.Client
	pushDir  string // "up" for client, "down" for server
	pollDir  string // "down" for client, "up" for server
	tx       chan []byte // async sender queue; WriteTo never blocks kcp-go
	txDrop   uint64      // datagrams dropped due to full tx queue
	wseq     int
	after    int
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

// Dial creates the virtual socket. pushDir/pollDir select the bus
// directions: client uses ("up","down"), server uses ("down","up").
// It learns current tails so a restart skips stale queued datagrams and
// resumes pushing right after the last committed seq.
func Dial(gc *gasclient.Client, pushDir, pollDir string) (*Conn, error) {
	c := &Conn{
		gc:      gc,
		pushDir: pushDir,
		pollDir: pollDir,
		tx:      make(chan []byte, 4096),
		rx:      make(chan []byte, 2048),
		die:     make(chan struct{}),
		laddr:   fakeAddr("gas://local"),
		raddr:   fakeAddr("gas://peer"),
	}
	// Learn tails: skip stale datagrams queued by a previous run (600s TTL).
	if _, next, err := gc.Poll(c.pollDir, Stream, 0, 1); err == nil {
		c.after = next
	} else {
		return nil, fmt.Errorf("gaspacket: learn poll tail: %w", err)
	}
	if _, next, err := gc.Poll(c.pushDir, Stream, 0, 1); err == nil {
		c.wseq = next + 1
	} else {
		return nil, fmt.Errorf("gaspacket: learn push tail: %w", err)
	}
	go c.pollLoop()
	go c.senderLoop()
	return c, nil
}

// senderLoop pushes queued datagrams in order. kcp-go calls WriteTo
// synchronously from its global update goroutine, so the actual HTTPS POST
// happens here, never stalling KCP's timing. Transport errors are retried;
// a desync adopts the server's expected seq (datagrams are atomic).
func (c *Conn) senderLoop() {
	for {
		select {
		case <-c.die:
			return
		case dgram := <-c.tx:
			for {
				ack, err := c.gc.Push(c.pushDir, Stream, c.wseq, dgram, false)
				if err == nil {
					c.wseq = ack + 1
					break
				}
				var de *gasclient.DesyncError
				if errors.As(err, &de) {
					c.wseq = de.Expect
					continue
				}
				select {
				case <-c.die:
					return
				case <-time.After(2 * time.Second):
				}
			}
		}
	}
}

// pollLoop feeds received datagrams into rx until Close.
func (c *Conn) pollLoop() {
	after := c.after
	for {
		select {
		case <-c.die:
			return
		default:
		}
		chunks, next, err := c.gc.Poll(c.pollDir, Stream, after, 75)
		if err != nil {
			select {
			case <-c.die:
				return
			case <-time.After(2 * time.Second):
				continue
			}
		}
		after = next
		for _, ch := range chunks {
			if len(ch.Data) == 0 {
				continue
			}
			select {
			case c.rx <- ch.Data:
			case <-c.die:
				return
			default:
				// kcp-go's readLoop drains continuously; overflow means
				// something is very wrong. Drop rather than deadlock.
			}
		}
	}
}

// ReadFrom blocks until a datagram arrives, the read deadline fires, or
// Close is called. The returned addr is always the constant fake peer addr
// (kcp-go keys server sessions and validates client sources by it).
func (c *Conn) ReadFrom(p []byte) (int, net.Addr, error) {
	c.rmu.Lock()
	dl := c.deadline
	c.rmu.Unlock()

	var timer <-chan time.Time
	if !dl.IsZero() {
		if d := time.Until(dl); d <= 0 {
			return 0, nil, timeoutError{}
		} else {
			t := time.NewTimer(d)
			defer t.Stop()
			timer = t.C
		}
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

// WriteTo queues one datagram for the sender loop. It never blocks: kcp-go
// invokes it synchronously from its timing-sensitive update goroutine, and
// a slow bus must not stall KCP. A full queue drops the datagram (KCP
// retransmits); the copy is required because kcp-go reuses the buffer.
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

// Close unblocks any pending ReadFrom and stops the poll loop. Idempotent.
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
