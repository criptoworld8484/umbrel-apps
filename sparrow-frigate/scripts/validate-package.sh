#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(cd "${DIR}/.." && pwd)"
cd "${DIR}"

required=(
  umbrel-app.yml
  docker-compose.yml
  exports.sh
  Dockerfile
  Dockerfile.web
  entrypoint.sh
  icon.svg
  web/index.html
  web/nginx.conf
  hooks/pre-start
)

echo "Validating sparrow-frigate..."
for f in "${required[@]}"; do
  [[ -e "${f}" ]] || { echo "MISSING: ${f}" >&2; exit 1; }
  echo "  ok ${f}"
done

grep -q 'sparrow-frigate_web_1' docker-compose.yml
grep -q 'APP_PORT: 3006' docker-compose.yml
grep -q 'ghcr.io/criptoworld8484/frigate-umbrel-web' docker-compose.yml
grep -q '50002:50002' docker-compose.yml
grep -q 'id: sparrow-frigate' umbrel-app.yml
grep -q 'port: 3006' umbrel-app.yml
grep -q 'icon.svg' umbrel-app.yml
! grep -q 'sparrow-frigate_server_1' docker-compose.yml
! grep -q '57001' docker-compose.yml

bash -n entrypoint.sh
bash -n hooks/pre-start

echo "Package validation passed."
