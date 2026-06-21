#!/bin/bash
# Run on Umbrel node while testing Sparrow connection/send.
set -euo pipefail
APP=sparrow-broadcast-pool_web_1
echo "=== Container & image ==="
sudo docker ps -a | grep -i sparrow-broadcast-pool || echo "WARN: no container"
sudo docker inspect "$APP" --format '{{.Config.Image}}' 2>/dev/null || true
echo ""
echo "=== Port 50050 ==="
sudo ss -tlnp | grep 50050 || echo "WARN: 50050 not listening"
echo ""
echo "=== Recent boot / version ==="
sudo docker logs "$APP" 2>&1 | grep -E 'v0\.|Electrum server|Warmed chain tip|bound on|accept thread|accept dispatcher' | tail -20
echo ""
echo "=== Handshake from Umbrel HOST (isolates Docker LAN vs app) ==="
REQ='{"jsonrpc":"2.0","method":"server.version","params":["Sparrow Wallet","1.4"],"id":0}'
if printf '%s\n' "$REQ" | nc -N -w 3 127.0.0.1 50050 2>/dev/null | head -1; then
  echo "OK: host nc got server.version response"
else
  echo "FAIL: host nc to 127.0.0.1:50050 — no response in 3s (app accept/dispatch broken)"
fi
echo ""
echo "=== Live (run Test Connection or Send in Sparrow now) ==="
echo "Expect: TCP accepted / Electrum client connected / instant cache"
echo "Press Ctrl+C to stop"
sudo docker logs -f "$APP" 2>&1 | grep -E 'TCP accepted|Electrum client connected|accept thread alive|server.version|headers.subscribe|instant cache|INTERCEPTED|Broadcast ingested|Session broadcast poll|Broadcast ack sent'
