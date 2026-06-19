#!/bin/bash
set -euo pipefail
BIN=/tmp/cursor-sandbox-cache/a295a663eb36fdead584f0580e3d45a4/cargo-target/release/broadcast-pool
pkill -9 -f 'cargo-target/release/broadcast-pool' 2>/dev/null || true
sleep 1
export BROADCAST_POOL_ELECTRUM_HOST=127.0.0.1
export BROADCAST_POOL_ELECTRUM_PORT=50050
export BROADCAST_POOL_WEB_HOST=127.0.0.1
export BROADCAST_POOL_WEB_PORT=18080
export BROADCAST_POOL_NETWORK=signet
export BROADCAST_POOL_UMBREL=1
export BROADCAST_POOL_UMBREL_ELECTRS_TCP=tcp://127.0.0.1:59999
nohup "$BIN" start --config /home/criptoworld/Documents/OpenCode/Mywalletcompromise/config/default.toml > /tmp/bp-test4.log 2>&1 &
echo "started pid $!"
sleep 3
ss -tln | grep 50050 || echo "NO_PORT"
python3 -c "
import socket,json
s=socket.create_connection(('127.0.0.1',50050),3)
req=[
 {'jsonrpc':'2.0','method':'server.version','params':['Sparrow Wallet','1.4'],'id':0},
 {'jsonrpc':'2.0','method':'server.features','params':[],'id':1},
 {'jsonrpc':'2.0','method':'blockchain.headers.subscribe','params':[],'id':2},
]
s.sendall((json.dumps(req)+'\n').encode())
s.settimeout(8)
data=s.recv(16384)
print('RESP_LEN', len(data))
print(data.decode())
"
grep -E 'Electrum RPC|Connection error|session started' /tmp/bp-test4.log | tail -10
