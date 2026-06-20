#!/bin/bash
# Simulates Sparrow broadcast + post-broadcast get_history mempool poll.
# Tests manual (nLockTime=0), timestamp scheduled MTP, and by_block locktime.
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
TS_LOCKTIME=$(python3 -c "print((1750000000).to_bytes(4,'little').hex())")
BH_LOCKTIME=$(python3 -c "print((900000).to_bytes(4,'little').hex())")
TS_TX="${SAMPLE_TX::-8}${TS_LOCKTIME}"
BH_TX="${SAMPLE_TX::-8}${BH_LOCKTIME}"

python3 -u <<PY
import json, socket, sys, hashlib

def read_line(sock):
    buf = b""
    while b"\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError("connection closed")
        buf += chunk
    line, _ = buf.split(b"\n", 1)
    return line.decode()

def output_scripthash():
    script = bytes.fromhex("76a914ffb035781c3c69e076d48b60c3d38592e7ce06a788ac")
    h = hashlib.sha256(script).digest()
    return h[::-1].hex()

def test_broadcast(label, tx_hex):
    sh = output_scripthash()
    s = socket.create_connection(("127.0.0.1", 50050), timeout=3)
    s.settimeout(12)
    for name, req in [
        ("server.version", {"jsonrpc":"2.0","method":"server.version","params":["Sparrow Wallet","1.4"],"id":0}),
        ("blockchain.estimatefee", {"jsonrpc":"2.0","method":"blockchain.estimatefee","params":[6],"id":1}),
        ("blockchain.transaction.broadcast", {"jsonrpc":"2.0","method":"blockchain.transaction.broadcast","params":tx_hex,"id":3}),
    ]:
        s.sendall((json.dumps(req)+"\n").encode())
        data = read_line(s)
        if name == "blockchain.transaction.broadcast":
            resp = json.loads(data)
            if "error" in resp:
                print(f"FAIL {label}: broadcast error", resp["error"])
                sys.exit(1)
            txid = resp.get("result")
            print(f"OK {label} txid", txid[:16], "...")
    s.sendall((json.dumps({"jsonrpc":"2.0","method":"blockchain.scripthash.get_history","params":[sh],"id":4})+"\n").encode())
    hist = json.loads(read_line(s))
    entries = hist.get("result", [])
    if not any(e.get("height") == 0 and e.get("tx_hash") == txid for e in entries):
        print(f"FAIL {label}: tx not in get_history at height 0")
        sys.exit(1)
    print(f"PASS {label}: get_history height 0")
    s.close()

test_broadcast("manual", """$SAMPLE_TX""")
test_broadcast("timestamp", """$TS_TX""")
test_broadcast("by_block", """$BH_TX""")
PY

sleep 1
for mode in manual timestamp "block height"; do
  if grep -q "INTERCEPTED broadcast RPC" /tmp/bp-sparrow-broadcast.log; then
    :
  else
    echo "FAIL: no INTERCEPTED in log"
    tail -30 /tmp/bp-sparrow-broadcast.log
    exit 1
  fi
done
grep -c "INTERCEPTED broadcast RPC" /tmp/bp-sparrow-broadcast.log | xargs -I{} echo "PASS: {} broadcasts intercepted"
grep -q "Timestamp nLockTime" /tmp/bp-sparrow-broadcast.log && echo "PASS: timestamp nLockTime ingest logged"
grep -q "Block-height nLockTime" /tmp/bp-sparrow-broadcast.log && echo "PASS: by_block nLockTime ingest logged"
kill $BP $FE 2>/dev/null || true
