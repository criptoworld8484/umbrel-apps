#!/bin/bash
# Simulates Sparrow broadcast RPC (string params + newline-delimited JSON).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
pkill -f fake-electrs.py 2>/dev/null || true
pkill -9 -f 'broadcast-pool' 2>/dev/null || true
sleep 1
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
export BROADCAST_POOL_INDEXER_URL=tcp://127.0.0.1:59999
export BROADCAST_POOL_ELECTRUM_HOST=127.0.0.1 BROADCAST_POOL_ELECTRUM_PORT=50050
export BROADCAST_POOL_WEB_HOST=127.0.0.1 BROADCAST_POOL_WEB_PORT=18080
export BROADCAST_POOL_NETWORK=signet BROADCAST_POOL_UMBREL=1
export BROADCAST_POOL_UMBREL_ELECTRS_TCP=tcp://127.0.0.1:59999
"$BIN" start --config "$ROOT/config/default.toml" > /tmp/bp-sparrow-broadcast.log 2>&1 &
BP=$!
sleep 2

SAMPLE_TX="0100000002f327e86da3e66bd20e1129b1fb36d07056f0b9a117199e759396526b8f3a20780000000000fffffffff0ede03d75050f20801d50358829ae02c058e8677d2cc74df51f738285013c260000000000ffffffff02f028d6dc010000001976a914ffb035781c3c69e076d48b60c3d38592e7ce06a788ac00ca9a3b000000001976a914fa5139067622fd7e1e722a05c17c2bb7d5fd6df088ac00000000"

python3 -u <<PY
import json, socket, sys
sample_tx = """$SAMPLE_TX"""
s = socket.create_connection(("127.0.0.1", 50050), timeout=3)
for name, req in [
    ("server.version", {"jsonrpc":"2.0","method":"server.version","params":["Sparrow Wallet","1.4"],"id":0}),
    ("blockchain.estimatefee", {"jsonrpc":"2.0","method":"blockchain.estimatefee","params":[6],"id":1}),
    ("blockchain.transaction.broadcast", {"jsonrpc":"2.0","method":"blockchain.transaction.broadcast","params":sample_tx,"id":2}),
]:
    s.sendall((json.dumps(req)+"\n").encode())
    s.settimeout(8)
    data = s.recv(65536)
    print(name, "->", data[:140])
s.close()
PY

sleep 1
if grep -q "INTERCEPTED broadcast RPC" /tmp/bp-sparrow-broadcast.log; then
  echo "PASS: broadcast intercepted"
else
  echo "FAIL: no INTERCEPTED in log"
  tail -30 /tmp/bp-sparrow-broadcast.log
  exit 1
fi
kill $BP $FE 2>/dev/null || true
