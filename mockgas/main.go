// mockgas: in-memory implementation of the lossy Code.gs mailbox protocol.
// Usage: mockgas --listen 127.0.0.1:18099 [--token secret]
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type store struct {
	mu     sync.Mutex
	queues map[string][]string
}

func key(sess, dir, stream string) string { return sess + "|" + dir + "|" + stream }

func main() {
	listen := flag.String("listen", "127.0.0.1:18099", "listen addr")
	token := flag.String("token", "", "if set, enforce token check like GAS")
	flag.Parse()
	st := &store{queues: map[string][]string{}}

	http.HandleFunc("/exec", func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &b); err != nil {
			write(w, map[string]any{"error": "bad json"})
			return
		}
		if *token != "" && b["token"] != *token {
			w.WriteHeader(403)
			write(w, map[string]any{"error": "bad token"})
			return
		}
		sess, _ := b["sess"].(string)
		dir, _ := b["dir"].(string)
		stream, _ := b["stream"].(string)
		if sess == "" || dir == "" || stream == "" {
			write(w, map[string]any{"error": "sess/stream/dir required"})
			return
		}

		switch b["op"] {
		case "push":
			arr, _ := b["packets"].([]any)
			batch := make([]string, 0, len(arr))
			for _, value := range arr {
				if s, ok := value.(string); ok && s != "" {
					batch = append(batch, s)
				}
			}
			if len(batch) > 16 {
				write(w, map[string]any{"error": "too many packets"})
				return
			}
			st.mu.Lock()
			qk := key(sess, dir, stream)
			st.queues[qk] = append(st.queues[qk], batch...)
			if len(st.queues[qk]) > 512 {
				st.queues[qk] = st.queues[qk][len(st.queues[qk])-512:]
			}
			st.mu.Unlock()
			write(w, map[string]any{"ok": true})

		case "poll":
			wait, _ := b["wait"].(float64)
			if wait < 1 {
				wait = 1
			}
			if wait > 90 {
				wait = 90
			}
			deadline := time.Now().Add(time.Duration(wait) * time.Second)
			qk := key(sess, dir, stream)
			for {
				st.mu.Lock()
				q := st.queues[qk]
				var packets []string
				if len(q) > 0 {
					n := len(q)
					if n > 32 {
						n = 32
					}
					packets = append([]string(nil), q[:n]...)
					st.queues[qk] = q[n:]
				}
				st.mu.Unlock()
				if len(packets) > 0 || time.Now().After(deadline) {
					if packets == nil {
						packets = []string{}
					}
					write(w, map[string]any{"packets": packets})
					return
				}
				time.Sleep(20 * time.Millisecond)
			}

		default:
			write(w, map[string]any{"error": "unknown op"})
		}
	})
	log.Printf("mockgas on %s", *listen)
	log.Fatal(http.ListenAndServe(*listen, nil))
}

func write(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
