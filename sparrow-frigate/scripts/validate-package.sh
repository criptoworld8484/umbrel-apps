#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "${DIR}"

required=(
  umbrel-app.yml
  docker-compose.yml
  exports.sh
  Dockerfile
  Dockerfile.ui
  entrypoint.sh
  icon.png
  torrc
  web/server.js
  web/public/index.html
  hooks/pre-start
  hooks/post-install
  hooks/post-update
  hooks/sync-web-ui.sh
)

for f in "${required[@]}"; do
  [[ -e "${f}" ]] || { echo "MISSING: ${f}" >&2; exit 1; }
done

grep -q 'sparrow-frigate_web_1' docker-compose.yml
grep -q 'frigate-umbrel-web:ui-' docker-compose.yml
grep -q 'APP_DATA_DIR}/web' docker-compose.yml && { echo "Remove web volume mount (use ui image)" >&2; exit 1; } || true
grep -q 'frigate-umbrel-web:frigate-' docker-compose.yml
grep -q 'APP_SPARROW_FRIGATE_NODE_IP' docker-compose.yml
grep -q 'getumbrel/tor' docker-compose.yml
bash -n entrypoint.sh
bash -n hooks/pre-start
bash -n hooks/post-install
bash -n hooks/post-update
bash -n hooks/sync-web-ui.sh
bash -n exports.sh

echo "Package validation passed."
