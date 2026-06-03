#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "${DIR}"

required=(
  umbrel-app.yml
  docker-compose.yml
  exports.sh
  Dockerfile
  entrypoint.sh
  start-all.sh
  nginx-umbrel.conf
  icon.png
  web/index.html
  web/nginx.conf
  hooks/pre-start
)

for f in "${required[@]}"; do
  [[ -e "${f}" ]] || { echo "MISSING: ${f}" >&2; exit 1; }
done

grep -q 'sparrow-frigate_server_1' docker-compose.yml
grep -q 'ghcr.io/criptoworld8484/frigate-umbrel-web' docker-compose.yml
grep -q 'icon.png' umbrel-app.yml
bash -n entrypoint.sh
bash -n start-all.sh
bash -n hooks/pre-start

echo "Package validation passed."
