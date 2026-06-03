#!/bin/sh
set -eu

# UI nginx en segundo plano (puerto 3006)
nginx -g 'daemon off;' &
NGINX_PID=$!

cleanup() {
  kill "${NGINX_PID}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Frigate en primer plano (entrypoint genera config y arranca Electrum)
exec /entrypoint.sh
