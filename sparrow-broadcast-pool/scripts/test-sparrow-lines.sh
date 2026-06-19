#!/bin/bash
# Simulates Sparrow's one-RPC-per-line connect sequence (not JSON batch).
set -euo pipefail
pkill -f fake-electrs.py 2>/dev/null || true
pkill -9 -f 'cargo-target/release/broadcast-pool' 2>/dev/null || true
sleep 1
python3 /home/criptoworld/Documents/OpenCode/Mywalletcompromise/scripts/fake-electrs.py &
FE=$!
sleep 1
BIN=/tmp/cursor-sandbox-cache/a295a663eb36fdead584f0580e3d45a4/cargo-target/release/broadcast-pool
export BROADCAST_POOL_ELECTRUM_HOST=127.0.0.1 BROADCAST_POOL_ELECTRUM_PORT=50050
export BROADCAST_POOL_WEB_HOST=127.0.0.1 BROADCAST_POOL_WEB_PORT=18080
export BROADCAST_POOL_NETWORK=signet BROADCAST_POOL_UMBREL=1
export BROADCAST_POOL_UMBREL_ELECTRS_TCP=tcp://127.0.0.1:59999
"$BIN" start --config /home/criptoworld/Documents/OpenCode/Mywalletcompromise/config/default.toml > /tmp/bp-sparrow-lines.log 2>&1 &
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
kill $BP $FE 2>/dev/null || true
