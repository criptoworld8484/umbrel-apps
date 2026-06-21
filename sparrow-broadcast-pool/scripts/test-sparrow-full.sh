#!/bin/bash
# Full Sparrow invariant test: connect with electrs down (cached tip) + realistic broadcast flow.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
pkill -f fake-electrs.py 2>/dev/null || true
pkill -9 -f 'broadcast-pool' 2>/dev/null || true
fuser -k 50050/tcp 59999/tcp 18080/tcp 2>/dev/null || true
sleep 1
TEST_DATA=$(mktemp -d)
trap 'rm -rf "$TEST_DATA"; kill $BP $FE 2>/dev/null || true; fuser -k 50050/tcp 59999/tcp 2>/dev/null || true' EXIT

if [ -x "$ROOT/target/release/broadcast-pool" ]; then
  BIN="$ROOT/target/release/broadcast-pool"
elif [ -n "${CARGO_TARGET_DIR:-}" ] && [ -x "${CARGO_TARGET_DIR}/release/broadcast-pool" ]; then
  BIN="${CARGO_TARGET_DIR}/release/broadcast-pool"
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

python3 "$ROOT/scripts/fake-electrs.py" &
FE=$!
sleep 1
"$BIN" start --config "$ROOT/config/default.toml" > /tmp/bp-sparrow-full.log 2>&1 &
BP=$!
sleep 3

echo "=== (a) Warm cache via headers.subscribe ==="
python3 -u <<'PY'
import json, socket, time
s = socket.create_connection(("127.0.0.1", 50050), timeout=3)
for req in [
    {"jsonrpc":"2.0","method":"server.version","params":["Sparrow Wallet","1.4"],"id":0},
    {"jsonrpc":"2.0","method":"server.features","params":[],"id":1},
    {"jsonrpc":"2.0","method":"blockchain.headers.subscribe","params":[],"id":2},
]:
    s.sendall((json.dumps(req)+"\n").encode())
    buf = b""
    while b"\n" not in buf:
        buf += s.recv(4096)
    data = json.loads(buf.split(b"\n",1)[0])
    if req["method"] == "blockchain.headers.subscribe":
        h = data.get("result", {}).get("height", 0)
        assert h > 0, f"cache warm failed height={h}"
        print("OK cache warm height", h)
s.close()
PY

echo "=== (a) Kill fake electrs — connect must still complete <2s from cache ==="
kill $FE 2>/dev/null || true
wait $FE 2>/dev/null || true
sleep 1

python3 -u <<'PY'
import json, socket, time
start = time.time()
s = socket.create_connection(("127.0.0.1", 50050), timeout=3)
for req in [
    {"jsonrpc":"2.0","method":"server.version","params":["Sparrow Wallet","1.4"],"id":10},
    {"jsonrpc":"2.0","method":"server.features","params":[],"id":11},
    {"jsonrpc":"2.0","method":"blockchain.headers.subscribe","params":[],"id":12},
]:
    s.sendall((json.dumps(req)+"\n").encode())
    s.settimeout(2)
    buf = b""
    while b"\n" not in buf:
        buf += s.recv(4096)
    data = json.loads(buf.split(b"\n",1)[0])
    if req["method"] == "blockchain.headers.subscribe":
        h = data.get("result", {}).get("height", 0)
        assert h > 0, data
        elapsed = time.time() - start
        assert elapsed < 2.0, f"headers.subscribe too slow: {elapsed:.2f}s"
        print(f"OK connect with electrs down in {elapsed:.2f}s height={h}")
s.close()
PY

echo "=== (b) Realistic Sparrow flow: subscribe → broadcast → get_history on subscribed sh ==="
python3 "$ROOT/scripts/fake-electrs.py" &
FE=$!
sleep 1

SAMPLE_TX="0100000002f327e86da3e66bd20e1129b1fb36d07056f0b9a117199e759396526b8f3a20780000000000fffffffff0ede03d75050f20801d50358829ae02c058e8677d2cc74df51f738285013c260000000000ffffffff02f028d6dc010000001976a914ffb035781c3c69e076d48b60c3d38592e7ce06a788ac00ca9a3b000000001976a914fa5139067622fd7e1e722a05c17c2bb7d5fd6df088ac00000000"
# Wallet input scripthash (simulated — Sparrow polls subscribed addresses, not only outputs)
WALLET_SH="a1a1a1a1b2b2b2b2c3c3c3c3d4d4d4d4e5e5e5e5f6f6f6f6f7f7f7f7"

python3 -u <<PY
import json, socket, sys
SAMPLE_TX = """$SAMPLE_TX"""
WALLET_SH = """$WALLET_SH"""

def read_line(sock):
    buf = b""
    while b"\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError("closed")
        buf += chunk
    return json.loads(buf.split(b"\n",1)[0])

def run_mode(label, tx_hex):
    s = socket.create_connection(("127.0.0.1", 50050), timeout=3)
    s.settimeout(10)
    for req in [
        ("server.version", {"jsonrpc":"2.0","method":"server.version","params":["Sparrow Wallet","1.4"],"id":0}),
        ("scripthash.subscribe", {"jsonrpc":"2.0","method":"blockchain.scripthash.subscribe","params":[WALLET_SH],"id":1}),
        ("broadcast", {"jsonrpc":"2.0","method":"blockchain.transaction.broadcast","params":tx_hex,"id":2}),
    ]:
        s.sendall((json.dumps(req)+"\n").encode())
        resp = read_line(s)
        if req[0] == "broadcast":
            if "error" in resp:
                print("FAIL", label, resp["error"]); sys.exit(1)
            txid = resp["result"]
            print(f"OK {label} txid {txid[:16]}...")
    s.sendall((json.dumps({"jsonrpc":"2.0","method":"blockchain.scripthash.get_history","params":[WALLET_SH],"id":3})+"\n").encode())
    hist = read_line(s)
    entries = hist.get("result", [])
    if not any(e.get("height")==0 and e.get("tx_hash")==txid for e in entries):
        print("FAIL", label, "no height-0 on subscribed get_history", entries)
        sys.exit(1)
    print(f"PASS {label}: subscribed get_history height 0")
    s.close()

run_mode("manual", SAMPLE_TX)
TS = SAMPLE_TX[:-8] + format(1750000000, '08x')
run_mode("timestamp", TS)
BH = SAMPLE_TX[:-8] + format(900000, '08x')
run_mode("by_block", BH)
PY

grep -q "Session fallback" /tmp/bp-sparrow-full.log && echo "PASS: session fallback logged"
grep -q "INTERCEPTED broadcast RPC" /tmp/bp-sparrow-full.log && echo "PASS: broadcasts intercepted"
echo "ALL FULL SPARROW TESTS PASSED"
