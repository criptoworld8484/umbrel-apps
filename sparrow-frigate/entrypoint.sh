#!/bin/sh
set -eu

NETWORK="${FRIGATE_NETWORK:-signet}"
DATA_ROOT="/data"
NETWORK_DIR="${DATA_ROOT}/${NETWORK}"
CONFIG_FILE="${NETWORK_DIR}/config.toml"
BITCOIN_DATA_DIR="${FRIGATE_BITCOIN_DATA_DIR:-/bitcoin}"
ELECTRUM_PORT="${APP_FRIGATE_ELECTRUM_PORT:-${FRIGATE_PORT:-57001}}"
BITCOIN_RPC_URL="http://${APP_BITCOIN_NODE_IP}:${APP_BITCOIN_RPC_PORT}"

FRIGATE_CACHE_SIZE="${FRIGATE_CACHE_SIZE:-2M}"
FRIGATE_MEMORY_LIMIT="${FRIGATE_MEMORY_LIMIT:-4GB}"
BITCOIN_WAIT_MAX="${BITCOIN_WAIT_MAX:-180}"
BITCOIN_WAIT_INTERVAL="${BITCOIN_WAIT_INTERVAL:-5}"

: "${APP_BITCOIN_NODE_IP:?APP_BITCOIN_NODE_IP is required}"
: "${APP_BITCOIN_RPC_PORT:?APP_BITCOIN_RPC_PORT is required}"

mkdir -p "${NETWORK_DIR}/db"

resolve_cookie_file() {
  case "${NETWORK}" in
    signet)   [ -f "${BITCOIN_DATA_DIR}/signet/.cookie" ] && echo "${BITCOIN_DATA_DIR}/signet/.cookie" && return 0 ;;
    testnet)  [ -f "${BITCOIN_DATA_DIR}/testnet3/.cookie" ] && echo "${BITCOIN_DATA_DIR}/testnet3/.cookie" && return 0 ;;
    testnet4) [ -f "${BITCOIN_DATA_DIR}/testnet4/.cookie" ] && echo "${BITCOIN_DATA_DIR}/testnet4/.cookie" && return 0 ;;
    regtest)  [ -f "${BITCOIN_DATA_DIR}/regtest/.cookie" ] && echo "${BITCOIN_DATA_DIR}/regtest/.cookie" && return 0 ;;
    *)        [ -f "${BITCOIN_DATA_DIR}/.cookie" ] && echo "${BITCOIN_DATA_DIR}/.cookie" && return 0 ;;
  esac
  find "${BITCOIN_DATA_DIR}" -maxdepth 4 -name '.cookie' -type f 2>/dev/null | head -1
}

has_userpass() {
  [ -n "${APP_BITCOIN_RPC_USER:-}" ] && [ -n "${APP_BITCOIN_RPC_PASS:-}" ]
}

build_auth_block() {
  if has_userpass; then
    echo "authType = \"USERPASS\"
auth = \"${APP_BITCOIN_RPC_USER}:${APP_BITCOIN_RPC_PASS}\""
    return
  fi
  COOKIE_FILE=$(resolve_cookie_file || true)
  if [ -n "${COOKIE_FILE}" ] && [ -f "${COOKIE_FILE}" ]; then
    echo "authType = \"COOKIE\"
dataDir = \"${BITCOIN_DATA_DIR}\""
    return
  fi
  echo "ERROR: No Bitcoin RPC credentials (set APP_BITCOIN_RPC_USER/PASS or ensure .cookie exists under ${BITCOIN_DATA_DIR})." >&2
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
  if has_userpass; then
    echo "Wrote ${CONFIG_FILE} (RPC ${BITCOIN_RPC_URL}, auth USERPASS user=${APP_BITCOIN_RPC_USER})"
  else
    echo "Wrote ${CONFIG_FILE} (RPC ${BITCOIN_RPC_URL}, auth COOKIE $(resolve_cookie_file || echo missing))"
  fi
}

rpc_getblockchaininfo() {
  if has_userpass; then
    curl -sf -u "${APP_BITCOIN_RPC_USER}:${APP_BITCOIN_RPC_PASS}" \
      -H "content-type: application/json" \
      --data-binary '{"jsonrpc":"1.0","id":"frigate","method":"getblockchaininfo","params":[]}' \
      "${BITCOIN_RPC_URL}/"
    return
  fi
  COOKIE_FILE=$(resolve_cookie_file || true)
  if [ -n "${COOKIE_FILE}" ] && [ -f "${COOKIE_FILE}" ]; then
    curl -sf -u "$(tr -d '\n\r' < "${COOKIE_FILE}")" \
      -H "content-type: application/json" \
      --data-binary '{"jsonrpc":"1.0","id":"frigate","method":"getblockchaininfo","params":[]}' \
      "${BITCOIN_RPC_URL}/"
    return
  fi
  return 1
}

log_rpc_diagnostics() {
  echo "--- Bitcoin RPC diagnostics ---" >&2
  echo "URL: ${BITCOIN_RPC_URL}" >&2
  if has_userpass; then
    echo "Auth: USERPASS user=${APP_BITCOIN_RPC_USER} pass_len=${#APP_BITCOIN_RPC_PASS}" >&2
  else
    echo "Auth: USERPASS not set in container env" >&2
  fi
  COOKIE_FILE=$(resolve_cookie_file || true)
  if [ -n "${COOKIE_FILE}" ] && [ -f "${COOKIE_FILE}" ]; then
    echo "Cookie: ${COOKIE_FILE} (present)" >&2
  else
    echo "Cookie: not found under ${BITCOIN_DATA_DIR}" >&2
  fi
  if has_userpass; then
    code=$(curl -s -o /dev/null -w "%{http_code}" -u "${APP_BITCOIN_RPC_USER}:${APP_BITCOIN_RPC_PASS}" \
      -H "content-type: application/json" \
      --data-binary '{"jsonrpc":"1.0","id":"frigate","method":"getblockchaininfo","params":[]}' \
      "${BITCOIN_RPC_URL}/" || echo "000")
    echo "HTTP probe (USERPASS): ${code} (000=unreachable, 401=bad auth)" >&2
  fi
  echo "Is Bitcoin app running? Check: docker ps | grep bitcoin" >&2
  echo "Is Electrs OK? If electrs works, RPC path is fine — compare its env." >&2
  echo "-------------------------------" >&2
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
            echo "ERROR: Set Bitcoin Node to Signet (Advanced), restart Bitcoin, then sparrow-frigate." >&2
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
    if [ "${attempt}" = "1" ] || [ $((attempt % 12)) -eq 0 ]; then
      echo "Waiting for Bitcoin Core at ${BITCOIN_RPC_URL} (${attempt}/${BITCOIN_WAIT_MAX})..."
      if [ "${attempt}" -eq 12 ]; then
        log_rpc_diagnostics
      fi
    fi
    sleep "${BITCOIN_WAIT_INTERVAL}"
  done
  log_rpc_diagnostics
  echo "ERROR: Bitcoin Core not reachable at ${BITCOIN_RPC_URL} after ${BITCOIN_WAIT_MAX} attempts." >&2
  exit 1
}

write_config
wait_for_bitcoin

echo "Starting Frigate network=${NETWORK} client_port=${ELECTRUM_PORT}"
if [ -n "${APP_ELECTRS_NODE_IP:-}" ] && [ -n "${APP_ELECTRS_NODE_PORT:-}" ]; then
  echo "Electrs backend: tcp://${APP_ELECTRS_NODE_IP}:${APP_ELECTRS_NODE_PORT}"
fi
exec /opt/frigate/bin/frigate -d "${DATA_ROOT}" -n "${NETWORK}"
