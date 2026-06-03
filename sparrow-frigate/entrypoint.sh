#!/bin/sh
set -eu

NETWORK="${FRIGATE_NETWORK:-signet}"
DATA_ROOT="/data"
NETWORK_DIR="${DATA_ROOT}/${NETWORK}"
CONFIG_FILE="${NETWORK_DIR}/config.toml"
BITCOIN_DATA_DIR="${FRIGATE_BITCOIN_DATA_DIR:-}"
ELECTRUM_PORT="${APP_FRIGATE_ELECTRUM_PORT:-${FRIGATE_PORT:-50002}}"

FRIGATE_CACHE_SIZE="${FRIGATE_CACHE_SIZE:-2M}"
FRIGATE_MEMORY_LIMIT="${FRIGATE_MEMORY_LIMIT:-4GB}"

: "${APP_BITCOIN_NODE_IP:?APP_BITCOIN_NODE_IP is required}"
: "${APP_BITCOIN_RPC_PORT:?APP_BITCOIN_RPC_PORT is required}"

mkdir -p "${NETWORK_DIR}/db"

if [ ! -f "${CONFIG_FILE}" ]; then
  AUTH_BLOCK=""
  if [ -n "${BITCOIN_DATA_DIR}" ] && [ -d "${BITCOIN_DATA_DIR}" ]; then
    COOKIE_FILE=$(find "${BITCOIN_DATA_DIR}" -maxdepth 4 -name '.cookie' -type f 2>/dev/null | head -1)
    if [ -n "${COOKIE_FILE}" ]; then
      AUTH_BLOCK="authType = \"COOKIE\"
dataDir = \"${BITCOIN_DATA_DIR}\""
    fi
  fi
  if [ -z "${AUTH_BLOCK}" ]; then
    : "${APP_BITCOIN_RPC_USER:?APP_BITCOIN_RPC_USER is required without cookie}"
    : "${APP_BITCOIN_RPC_PASS:?APP_BITCOIN_RPC_PASS is required without cookie}"
    AUTH_BLOCK="authType = \"USERPASS\"
auth = \"${APP_BITCOIN_RPC_USER}:${APP_BITCOIN_RPC_PASS}\""
  fi

  ZMQ_LINE=""
  if [ -n "${APP_BITCOIN_ZMQ_SEQUENCE_PORT:-}" ]; then
    ZMQ_LINE="zmqSequenceEndpoint = \"tcp://${APP_BITCOIN_NODE_IP}:${APP_BITCOIN_ZMQ_SEQUENCE_PORT}\""
  fi

  BACKEND_LINE=""
  if [ -n "${APP_ELECTRS_NODE_IP:-}" ] && [ -n "${APP_ELECTRS_NODE_PORT:-}" ]; then
    BACKEND_LINE="backendElectrumServer = \"tcp://${APP_ELECTRS_NODE_IP}:${APP_ELECTRS_NODE_PORT}\""
  fi

  cat > "${CONFIG_FILE}" <<EOF
[core]
connect = true
server = "http://${APP_BITCOIN_NODE_IP}:${APP_BITCOIN_RPC_PORT}"
${AUTH_BLOCK}
${ZMQ_LINE}

[index]
startHeight = 0
cacheSize = "${FRIGATE_CACHE_SIZE}"

[scan]
computeBackend = "CPU"
memoryLimit = "${FRIGATE_MEMORY_LIMIT}"
dbThreads = 4

[server]
tcp = "tcp://0.0.0.0:${ELECTRUM_PORT}"
${BACKEND_LINE}
EOF
  echo "Wrote ${CONFIG_FILE}"
fi

echo "Starting Frigate network=${NETWORK} port=${ELECTRUM_PORT}"
exec /opt/frigate/bin/frigate -d "${DATA_ROOT}" -n "${NETWORK}"
