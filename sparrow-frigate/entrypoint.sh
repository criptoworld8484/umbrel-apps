#!/bin/sh
set -eu

NETWORK="${FRIGATE_NETWORK:-signet}"
DATA_ROOT="/data"
NETWORK_DIR="${DATA_ROOT}/${NETWORK}"
CONFIG_FILE="${NETWORK_DIR}/config.toml"
BITCOIN_DATA_DIR="${FRIGATE_BITCOIN_DATA_DIR:-}"
ELECTRUM_PORT="${APP_FRIGATE_ELECTRUM_PORT:-${FRIGATE_PORT:-50002}}"
BITCOIN_RPC_URL="http://${APP_BITCOIN_NODE_IP}:${APP_BITCOIN_RPC_PORT}"

FRIGATE_CACHE_SIZE="${FRIGATE_CACHE_SIZE:-2M}"
FRIGATE_MEMORY_LIMIT="${FRIGATE_MEMORY_LIMIT:-4GB}"
BITCOIN_WAIT_MAX="${BITCOIN_WAIT_MAX:-120}"
BITCOIN_WAIT_INTERVAL="${BITCOIN_WAIT_INTERVAL:-5}"

: "${APP_BITCOIN_NODE_IP:?APP_BITCOIN_NODE_IP is required}"
: "${APP_BITCOIN_RPC_PORT:?APP_BITCOIN_RPC_PORT is required}"

mkdir -p "${NETWORK_DIR}/db"

# Umbrel always provides RPC user/pass (same as Electrs). Prefer USERPASS over cookie
# to avoid picking a stale .cookie from another network (e.g. mainnet after switching to signet).
build_auth_block() {
  if [ -n "${APP_BITCOIN_RPC_USER:-}" ] && [ -n "${APP_BITCOIN_RPC_PASS:-}" ]; then
    echo "authType = \"USERPASS\"
auth = \"${APP_BITCOIN_RPC_USER}:${APP_BITCOIN_RPC_PASS}\""
    return
  fi
  if [ -n "${BITCOIN_DATA_DIR}" ] && [ -d "${BITCOIN_DATA_DIR}" ]; then
    COOKIE_FILE=""
    case "${NETWORK}" in
      signet)    [ -f "${BITCOIN_DATA_DIR}/signet/.cookie" ] && COOKIE_FILE="${BITCOIN_DATA_DIR}/signet/.cookie" ;;
      testnet)   [ -f "${BITCOIN_DATA_DIR}/testnet3/.cookie" ] && COOKIE_FILE="${BITCOIN_DATA_DIR}/testnet3/.cookie" ;;
      testnet4)  [ -f "${BITCOIN_DATA_DIR}/testnet4/.cookie" ] && COOKIE_FILE="${BITCOIN_DATA_DIR}/testnet4/.cookie" ;;
      regtest)   [ -f "${BITCOIN_DATA_DIR}/regtest/.cookie" ] && COOKIE_FILE="${BITCOIN_DATA_DIR}/regtest/.cookie" ;;
      *)         [ -f "${BITCOIN_DATA_DIR}/.cookie" ] && COOKIE_FILE="${BITCOIN_DATA_DIR}/.cookie" ;;
    esac
    if [ -z "${COOKIE_FILE}" ]; then
      COOKIE_FILE=$(find "${BITCOIN_DATA_DIR}" -maxdepth 4 -name '.cookie' -type f 2>/dev/null | head -1)
    fi
    if [ -n "${COOKIE_FILE}" ]; then
      COOKIE_DIR=$(dirname "${COOKIE_FILE}")
      echo "authType = \"COOKIE\"
dataDir = \"${COOKIE_DIR}\""
      return
    fi
  fi
  echo "ERROR: No Bitcoin RPC credentials (USERPASS or .cookie)." >&2
  exit 1
}

write_config() {
  AUTH_BLOCK=$(build_auth_block)

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
server = "${BITCOIN_RPC_URL}"
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
  echo "Wrote ${CONFIG_FILE} (bitcoin RPC ${BITCOIN_RPC_URL}, auth USERPASS or COOKIE)"
}

rpc_getblockchaininfo() {
  if [ -n "${APP_BITCOIN_RPC_USER:-}" ] && [ -n "${APP_BITCOIN_RPC_PASS:-}" ]; then
    curl -sf -u "${APP_BITCOIN_RPC_USER}:${APP_BITCOIN_RPC_PASS}" \
      -H "content-type: application/json" \
      --data-binary '{"jsonrpc":"1.0","id":"frigate","method":"getblockchaininfo","params":[]}' \
      "${BITCOIN_RPC_URL}/"
  else
    curl -sf \
      -H "content-type: application/json" \
      --data-binary '{"jsonrpc":"1.0","id":"frigate","method":"getblockchaininfo","params":[]}' \
      "${BITCOIN_RPC_URL}/"
  fi
}

wait_for_bitcoin() {
  attempt=0
  while [ "${attempt}" -lt "${BITCOIN_WAIT_MAX}" ]; do
    if resp=$(rpc_getblockchaininfo 2>/dev/null); then
      chain=$(echo "${resp}" | sed -n 's/.*"chain"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)
      echo "Bitcoin Core reachable at ${BITCOIN_RPC_URL} (chain=${chain:-unknown})"
      case "${NETWORK}" in
        signet)
          if [ "${chain}" != "signet" ]; then
            echo "ERROR: Frigate uses -n signet but bitcoind reports chain='${chain}'." >&2
            echo "ERROR: Set Bitcoin Node app to Signet (Advanced), restart Bitcoin, then restart sparrow-frigate." >&2
            exit 1
          fi
          ;;
        testnet)
          if [ "${chain}" != "test" ]; then
            echo "ERROR: Frigate expects testnet but bitcoind chain='${chain}'." >&2
            exit 1
          fi
          ;;
      esac
      return 0
    fi
    attempt=$((attempt + 1))
    echo "Waiting for Bitcoin Core at ${BITCOIN_RPC_URL} (${attempt}/${BITCOIN_WAIT_MAX})..."
    sleep "${BITCOIN_WAIT_INTERVAL}"
  done
  echo "ERROR: Bitcoin Core not reachable at ${BITCOIN_RPC_URL} after ${BITCOIN_WAIT_MAX} attempts." >&2
  echo "ERROR: Ensure the Bitcoin app is running and fully started before Frigate." >&2
  exit 1
}

write_config
wait_for_bitcoin

echo "Starting Frigate network=${NETWORK} client_port=${ELECTRUM_PORT}"
if [ -n "${APP_ELECTRS_NODE_IP:-}" ] && [ -n "${APP_ELECTRS_NODE_PORT:-}" ]; then
  echo "Electrs backend: tcp://${APP_ELECTRS_NODE_IP}:${APP_ELECTRS_NODE_PORT}"
fi
exec /opt/frigate/bin/frigate -d "${DATA_ROOT}" -n "${NETWORK}"
