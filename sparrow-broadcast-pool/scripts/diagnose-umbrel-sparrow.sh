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
sudo docker logs "$APP" 2>&1 | grep -E 'v0\.|Electrum server|Warmed chain tip|listening on' | tail -15
echo ""
echo "=== Live (run Test Connection or Send in Sparrow now) ==="
echo "Press Ctrl+C to stop"
sudo docker logs -f "$APP" 2>&1 | grep -E 'Electrum client connected|server.version|headers.subscribe|instant cache|INTERCEPTED|Broadcast ingested|Session fallback|without any broadcast RPC|electrs timed out'
