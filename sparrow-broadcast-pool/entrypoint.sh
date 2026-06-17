#!/bin/sh
set -eu

DATA_DIR="${BROADCAST_POOL_DATA_DIR:-/home/app/data}"
mkdir -p "$DATA_DIR"

{
  echo "=== broadcast-pool umbrel boot $(date -Iseconds) ==="
  echo "APP_ELECTRS_NODE_IP=${APP_ELECTRS_NODE_IP:-}"
  echo "APP_ELECTRS_NODE_PORT=${APP_ELECTRS_NODE_PORT:-}"
  echo "APP_ELECTRS_NODE_SSL_PORT=${APP_ELECTRS_NODE_SSL_PORT:-}"
  echo "APP_BITCOIN_NETWORK=${APP_BITCOIN_NETWORK:-}"
  echo "BROADCAST_POOL_LAN_IP=${BROADCAST_POOL_LAN_IP:-}"
} >> "${DATA_DIR}/umbrel-boot.log"

if [ -n "${APP_ELECTRS_NODE_IP:-}" ] && [ -n "${APP_ELECTRS_NODE_PORT:-}" ]; then
  export BROADCAST_POOL_UMBREL_ELECTRS_TCP="tcp://${APP_ELECTRS_NODE_IP}:${APP_ELECTRS_NODE_PORT}"
  export BROADCAST_POOL_UMBREL_ELECTRS_SSL="ssl://${APP_ELECTRS_NODE_IP}:${APP_ELECTRS_NODE_SSL_PORT:-50002}"
fi

exec broadcast-pool start --foreground
