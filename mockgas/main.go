// mockgas: in-memory implementation of the Code.gs protocol, for local
// end-to-end testing of bridge/relay without deploying to Google.
// Usage: mockgas --listen 127.0.0.1:18099 [--token secret]
// Then point bridge/relay --gas http://127.0.0.1:18099/exec at it.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type item struct {
	data string
	fin  bool
}

type store struct {
	mu    sync.Mutex
	tails map[string]int
	items map[string]item
}

func key(sess, dir, stream string) string      { return sess + "|" + dir + "|" + stream }
func dkey(sess, dir, stream string, seq int) string {
	return fmt.Sprintf("%s|%s|%s|%d", sess, dir, stream, seq)
}

func main() {
	listen := flag.String("listen", "127.0.0.1:18099", "listen addr")
	token := flag.String("token", "", "if set, enforce token check like GAS")
	flag.Parse()
	st := &store{tails: map[string]int{}, items: map[string]item{}}

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
			seq := int(b["seq"].(float64))
			st.mu.Lock()
			tk := key(sess, dir, stream)
			tail := st.tails[tk]
			var out map[string]any
			switch {
			case seq <= 0:
				out = map[string]any{"error": "bad seq"}
			case seq <= tail:
				out = map[string]any{"ok": true, "ack": seq, "dup": true}
			case seq != tail+1:
				out = map[string]any{"ok": false, "expect": tail + 1}
			default:
				data, _ := b["data"].(string)
				fin, _ := b["fin"].(bool)
				st.items[dkey(sess, dir, stream, seq)] = item{data, fin}
				st.tails[tk] = seq
				out = map[string]any{"ok": true, "ack": seq}
			}
			st.mu.Unlock()
			write(w, out)
		case "poll":
			after := int(b["after"].(float64))
			wait := b["wait"].(float64)
			if wait < 5 {
				wait = 5
			}
			if wait > 90 {
				wait = 90
			}
			deadline := time.Now().Add(time.Duration(wait) * time.Second)
			tk := key(sess, dir, stream)
			for {
				st.mu.Lock()
				tail := st.tails[tk]
				var chunks []map[string]any
				next := after
				if tail > after {
					to := tail
					if to > after+16 {
						to = after + 16
					}
					for s := after + 1; s <= to; s++ {
						it := st.items[dkey(sess, dir, stream, s)]
						chunks = append(chunks, map[string]any{
							"seq": s, "data": it.data, "fin": it.fin,
						})
					}
					next = to
				}
				st.mu.Unlock()
				if len(chunks) > 0 || time.Now().After(deadline) {
					if chunks == nil {
						chunks = []map[string]any{}
					}
					write(w, map[string]any{"chunks": chunks, "next": next})
					return
				}
				time.Sleep(200 * time.Millisecond)
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
