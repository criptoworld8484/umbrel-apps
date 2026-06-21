#!/bin/bash
# Simulates Sparrow's one-RPC-per-line connect sequence (not JSON batch).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
pkill -f fake-electrs.py 2>/dev/null || true
pkill -9 -f 'broadcast-pool' 2>/dev/null || true
sleep 1
TEST_DATA=$(mktemp -d)
trap 'rm -rf "$TEST_DATA"; kill $BP $FE 2>/dev/null || true' EXIT
python3 "$ROOT/scripts/fake-electrs.py" &
FE=$!
sleep 1
if [ -n "${CARGO_TARGET_DIR:-}" ] && [ -x "${CARGO_TARGET_DIR}/release/broadcast-pool" ]; then
  BIN="${CARGO_TARGET_DIR}/release/broadcast-pool"
elif [ -x "$ROOT/target/release/broadcast-pool" ]; then
  BIN="$ROOT/target/release/broadcast-pool"
else
  echo "Build first: cargo build --release"
  exit 1
fi
export BROADCAST_POOL_DATA_DIR="$TEST_DATA"
export BROADCAST_POOL_INDEXER_URL=tcp://127.0.0.1:59999
export BROADCAST_POOL_ELECTRUM_HOST=127.0.0.1 BROADCAST_POOL_ELECTRUM_PORT=50050
export BROADCAST_POOL_WEB_HOST=127.0.0.1 BROADCAST_POOL_WEB_PORT=18080
export BROADCAST_POOL_NETWORK=signet BROADCAST_POOL_UMBREL=1
export BROADCAST_POOL_UMBREL_ELECTRS_TCP=tcp://127.0.0.1:59999
"$BIN" start --config "$ROOT/config/default.toml" > /tmp/bp-sparrow-lines.log 2>&1 &
BP=$!
sleep 2
python3 -u <<'PY'
import socket, json
s = socket.create_connection(("127.0.0.1", 50050), timeout=3)
methods = [
    ("server.version", {"jsonrpc":"2.0","method":"server.version","params":["Sparrow Wallet","1.4"],"id":0}),
    ("server.features", {"jsonrpc":"2.0","method":"server.features","params":[],"id":1}),
    ("blockchain.headers.subscribe", {"jsonrpc":"2.0","method":"blockchain.headers.subscribe","params":[],"id":2}),
]
ok = 0
for name, req in methods:
    s.sendall((json.dumps(req)+"\n").encode())
    s.settimeout(5)
    buf = b""
    while b"\n" not in buf:
        buf += s.recv(4096)
    data = buf.split(b"\n", 1)[0]
    if not data:
        print("FAIL", name, "empty response")
    elif b'"error"' in data and b'"result"' not in data:
        print("ERR ", name, data[:120])
    elif name == "blockchain.headers.subscribe":
        obj = json.loads(data)
        height = obj.get("result", {}).get("height", 0)
        if height <= 0:
            print("FAIL", name, "height", height, "expected >0 from electrs")
        else:
            print("OK  ", name, "height", height)
            ok += 1
    else:
        print("OK  ", name, len(data), "bytes")
        ok += 1
print("passed", ok, "of", len(methods))
s.close()
PY
echo "=== nc one-liner (Sparrow line protocol) ==="
REQ='{"jsonrpc":"2.0","method":"server.version","params":["Sparrow Wallet","1.4"],"id":99}'
OUT=$(printf '%s\n' "$REQ" | nc -N -w 5 127.0.0.1 50050 2>&1 || true)
if echo "$OUT" | grep -q '"result"'; then
  echo "OK  nc server.version via -N"
else
  echo "FAIL nc server.version:" "$OUT"
  exit 1
fi
kill $BP $FE 2>/dev/null || true
wait $BP 2>/dev/null || true
wait $FE 2>/dev/null || true
fuser -k 50050/tcp 59999/tcp 18080/tcp 2>/dev/null || true
sleep 1