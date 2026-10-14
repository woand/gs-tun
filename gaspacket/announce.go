package gaspacket

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"gscript-tunnel/gasclient"
)

// AnnounceStream carries client run announcements (dir "up").
// The server rebuilds its KCP listener whenever the run ID changes, so a
// client reconnect (new KCP conv) never gets blackholed by a stale session.
const AnnounceStream = "__announce__"

// NewRunID returns a random run identifier.
func NewRunID() string {
	var b [8]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Announcer publishes the client's current run ID. One instance per client
// process; the run ID changes on every (re)connect.
type Announcer struct {
	gc    *gasclient.Client
	mu    sync.Mutex
	seq   int
	runID string
}

// NewAnnouncer learns the announce stream tail so it never collides with
// announcements from previous runs (600s TTL).
func NewAnnouncer(gc *gasclient.Client) (*Announcer, error) {
	a := &Announcer{gc: gc}
	if _, next, err := gc.Poll("up", AnnounceStream, 0, 1); err != nil {
		return nil, err
	} else {
		a.seq = next
	}
	return a, nil
}

func (a *Announcer) SetRunID(id string) {
	a.mu.Lock()
	a.runID = id
	a.mu.Unlock()
}

// Announce publishes the current run ID (retries seq desync automatically).
func (a *Announcer) Announce() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.runID == "" {
		return errors.New("gaspacket: no run ID set")
	}
	body, _ := json.Marshal(map[string]string{"run": a.runID})
	for {
		ack, err := a.gc.Push("up", AnnounceStream, a.seq+1, body, false)
		if err == nil {
			a.seq = ack
			return nil
		}
		var de *gasclient.DesyncError
		if errors.As(err, &de) {
			a.seq = de.Expect - 1
			continue
		}
		return err
	}
}

// WatchRuns polls the announce stream and reports every announced run ID.
// The first poll also reports pre-existing announcements so a restarted
// server picks up a live client. Callers use the stream as a heartbeat:
// a run ID not re-announced within a few minutes is gone.
func WatchRuns(gc *gasclient.Client) <-chan string {
	out := make(chan string, 16)
	go func() {
		after := 0
		first := true
		for {
			wait := 75
			if first {
				wait = 1
				first = false
			}
			chunks, next, err := gc.Poll("up", AnnounceStream, after, wait)
			if err != nil {
				log.Println("gaspacket: announce poll:", err)
				time.Sleep(2 * time.Second)
				continue
			}
			after = next
			for _, ch := range chunks {
				var m map[string]string
				if json.Unmarshal(ch.Data, &m) == nil {
					if r := m["run"]; r != "" {
						out <- r
					}
				}
			}
		}
	}()
	return out
}
