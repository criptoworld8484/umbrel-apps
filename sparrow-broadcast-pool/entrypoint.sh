#!/bin/sh
set -eu

DATA_DIR="${BROADCAST_POOL_DATA_DIR:-/home/app/data}"
mkdir -p "$DATA_DIR"
BOOT_LOG="${DATA_DIR}/umbrel-boot.log"

{
  echo "=== broadcast-pool umbrel boot $(date -Iseconds) ==="
  echo "APP_ELECTRS_NODE_IP=${APP_ELECTRS_NODE_IP:-}"
  echo "APP_ELECTRS_NODE_PORT=${APP_ELECTRS_NODE_PORT:-}"
  echo "APP_ELECTRS_NODE_SSL_PORT=${APP_ELECTRS_NODE_SSL_PORT:-}"
  echo "APP_BITCOIN_NETWORK=${APP_BITCOIN_NETWORK:-}"
  echo "BROADCAST_POOL_LAN_IP=${BROADCAST_POOL_LAN_IP:-}"
} >> "$BOOT_LOG"

if [ -n "${APP_ELECTRS_NODE_IP:-}" ] && ! echo "${APP_ELECTRS_NODE_IP}" | grep -q '\${'; then
  TCP_PORT="${APP_ELECTRS_NODE_PORT:-50001}"
  export BROADCAST_POOL_UMBREL_ELECTRS_TCP="tcp://${APP_ELECTRS_NODE_IP}:${TCP_PORT}"
  {
    echo "UMBREL_ELECTRS_TCP=${BROADCAST_POOL_UMBREL_ELECTRS_TCP}"
  } >> "$BOOT_LOG"
  if [ -n "${APP_ELECTRS_NODE_SSL_PORT:-}" ]; then
    export BROADCAST_POOL_UMBREL_ELECTRS_SSL="ssl://${APP_ELECTRS_NODE_IP}:${APP_ELECTRS_NODE_SSL_PORT}"
    echo "UMBREL_ELECTRS_SSL=${BROADCAST_POOL_UMBREL_ELECTRS_SSL}" >> "$BOOT_LOG"
  fi
else
  echo "WARN: APP_ELECTRS_NODE_IP missing or unresolved — electrs discovery may fail until Electrs is installed" >> "$BOOT_LOG"
fi

exec broadcast-pool start --foreground
