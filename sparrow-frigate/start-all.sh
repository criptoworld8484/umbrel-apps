#!/bin/sh
set -eu

mkdir -p /tmp/nginx

nginx -c /etc/nginx/nginx-umbrel.conf -g 'daemon off;' &
NGINX_PID=$!

# Esperar a que nginx escuche en 3006 (app_proxy)
for i in 1 2 3 4 5 6 7 8 9 10; do
  if curl -sf http://127.0.0.1:3006/health >/dev/null 2>&1; then
    echo "nginx listening on :3006"
    break
  fi
  if ! kill -0 "${NGINX_PID}" 2>/dev/null; then
    echo "ERROR: nginx exited during startup" >&2
    cat /tmp/nginx/error.log 2>/dev/null || true
    exit 1
  fi
  sleep 1
done

cleanup() {
  kill "${NGINX_PID}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

exec /entrypoint.sh
