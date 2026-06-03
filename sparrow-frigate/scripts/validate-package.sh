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
  icon.png
  web/index.html
  web/nginx.conf
  hooks/pre-start
)

for f in "${required[@]}"; do
  [[ -e "${f}" ]] || { echo "MISSING: ${f}" >&2; exit 1; }
done

grep -q 'sparrow-frigate_web_1' docker-compose.yml
grep -q 'nginx:1.27-alpine' docker-compose.yml
grep -q 'frigate-umbrel-web:frigate-' docker-compose.yml
grep -q 'icon.png' umbrel-app.yml
bash -n entrypoint.sh
bash -n hooks/pre-start

echo "Package validation passed."
