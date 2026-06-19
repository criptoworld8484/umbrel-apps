#!/bin/bash
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
"$BIN" start --config /home/criptoworld/Documents/OpenCode/Mywalletcompromise/config/default.toml > /tmp/bp-lines-test.log 2>&1 &
BP=$!
sleep 3
python3 -u <<'PY'
import socket,json,time
s=socket.create_connection(('127.0.0.1',50050),3)
time.sleep(0.5)
lines=[
 {'jsonrpc':'2.0','method':'server.version','params':['Sparrow Wallet','1.4'],'id':0},
 {'jsonrpc':'2.0','method':'server.features','params':[],'id':1},
 {'jsonrpc':'2.0','method':'blockchain.headers.subscribe','params':[],'id':2},
]
for line in lines:
    s.sendall((json.dumps(line)+'\n').encode())
    s.settimeout(8)
    print('R:', s.recv(8192).decode()[:200])
PY
kill $BP $FE 2>/dev/null || true
