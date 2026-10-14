package gaspacket

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"sync"
	"time"

	"gscript-tunnel/gasclient"
)

// AnnounceStream carries client run announcements in the base control
// namespace. Announcements are best-effort and do not need a bus sequence.
const AnnounceStream = "__announce__"

func NewRunID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

type Announcer struct {
	gc    *gasclient.Client
	mu    sync.Mutex
	runID string
}

func NewAnnouncer(gc *gasclient.Client) (*Announcer, error) {
	return &Announcer{gc: gc}, nil
}

func (a *Announcer) SetRunID(id string) {
	a.mu.Lock()
	a.runID = id
	a.mu.Unlock()
}

func (a *Announcer) Announce() error {
	a.mu.Lock()
	runID := a.runID
	a.mu.Unlock()
	if runID == "" {
		return nil
	}
	body, _ := json.Marshal(map[string]string{"run": runID})
	return a.gc.Push("up", AnnounceStream, body)
}

// WatchRuns waits for announcements and reports only the newest run in each
// mailbox batch. This avoids starting handlers for every stale heartbeat after
// a server restart.
func WatchRuns(gc *gasclient.Client) <-chan string {
	out := make(chan string, 4)
	go func() {
		for {
			packets, err := gc.Poll("up", AnnounceStream, 75)
			if err != nil {
				log.Println("gaspacket: announce poll:", err)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			latest := ""
			for _, packet := range packets {
				var message map[string]string
				if json.Unmarshal(packet, &message) == nil && message["run"] != "" {
					latest = message["run"]
				}
			}
			if latest != "" {
				out <- latest
			}
		}
	}()
	return out
}
