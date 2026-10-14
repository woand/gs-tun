#!/bin/bash
# End-to-end test of the fused design (no UDP anywhere, runs in-container):
#   curl -> gs_tun_client (socks5 :18080) -> KCP over GAS bus -> gs_tun_server -> http target :19999
# Usage:
#   ./test-kcp.sh                                        # against mockgas
#   GS_URL='https://script.google.com/macros/s/.../exec' TOKEN=... KEY=... ./test-kcp.sh   # against real GAS
set -u
cd "$(dirname "$0")"
export PATH=$HOME/workspace/tools/go/bin:$PATH

GS_URL="${GS_URL:-http://127.0.0.1:18099/exec}"
TOKEN="${TOKEN:-testtoken}"
KEY="${KEY:-testkey}"

go build -o bin/mockgas ./mockgas || exit 1
go build -o bin/gs_tun_client ./gs_tun_client || exit 1
go build -o bin/gs_tun_server ./gs_tun_server || exit 1

cleanup() {
  for p in $(pgrep -f "gs_tun_serve[r]"); do [ "$p" != "$$" ] && kill "$p" 2>/dev/null; done
  for p in $(pgrep -f "gs_tun_clien[t]"); do [ "$p" != "$$" ] && kill "$p" 2>/dev/null; done
  for p in $(pgrep -f "mockga[s]$"); do [ "$p" != "$$" ] && kill "$p" 2>/dev/null; done
  for p in $(pgrep -f "http.server 1999[9]"); do [ "$p" != "$$" ] && kill "$p" 2>/dev/null; done
}
trap cleanup EXIT
cleanup; sleep 1

if [ "$GS_URL" = "http://127.0.0.1:18099/exec" ]; then
  ./bin/mockgas >/tmp/test-mockgas.log 2>&1 &
  sleep 1
fi

# HTTP target
mkdir -p /tmp/tuntest-www
dd if=/dev/urandom of=/tmp/tuntest-www/payload.bin bs=1M count=5 2>/dev/null
(cd /tmp/tuntest-www && python3 -m http.server 19999 >/tmp/test-http.log 2>&1 &)
sleep 1

./bin/gs_tun_server --gs "$GS_URL" --token "$TOKEN" --key "$KEY" >/tmp/test-server.log 2>&1 &
sleep 2
./bin/gs_tun_client --gs "$GS_URL" --token "$TOKEN" --key "$KEY" -l 127.0.0.1:18080 >/tmp/test-client.log 2>&1 &
sleep 12  # announce + KCP/smux handshake through the bus

echo "--- single 5MB download ---"
curl -s -x socks5://127.0.0.1:18080 --max-time 90 -o /tmp/tuntest-got.bin http://127.0.0.1:19999/payload.bin \
  && cmp /tmp/tuntest-www/payload.bin /tmp/tuntest-got.bin && echo "PASS: 5MB byte-exact" || echo "FAIL: single"

echo "--- 4 concurrent 5MB downloads ---"
pids=""
for i in 1 2 3 4; do
  curl -s -x socks5://127.0.0.1:18080 --max-time 90 -o /tmp/tuntest-got$i.bin http://127.0.0.1:19999/payload.bin &
  pids="$pids $!"
done
for p in $pids; do wait "$p"; done
pass=0
for i in 1 2 3 4; do cmp -s /tmp/tuntest-www/payload.bin /tmp/tuntest-got$i.bin && pass=$((pass+1)); done
[ "$pass" = "4" ] && echo "PASS: 4/4 concurrent byte-exact" || echo "FAIL: concurrent ($pass/4)"

echo "--- client restart (new run) ---"
for p in $(pgrep -f "gs_tun_clien[t]"); do [ "$p" != "$$" ] && kill "$p" 2>/dev/null; done
sleep 2
./bin/gs_tun_client --gs "$GS_URL" --token "$TOKEN" --key "$KEY" -l 127.0.0.1:18080 >/tmp/test-client2.log 2>&1 &
sleep 12
curl -s -x socks5://127.0.0.1:18080 --max-time 90 -o /tmp/tuntest-gotR.bin http://127.0.0.1:19999/payload.bin \
  && cmp -s /tmp/tuntest-www/payload.bin /tmp/tuntest-gotR.bin && echo "PASS: restart recovery" || echo "FAIL: restart"
grep -c "starting KCP listener" /tmp/test-server.log | xargs echo "server handler builds:"
