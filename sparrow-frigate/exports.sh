# Puertos e IPs (misma convención que Electrs/Fulcrum en Umbrel)
export APP_SPARROW_FRIGATE_IP="10.21.22.12"
export APP_SPARROW_FRIGATE_NODE_IP="10.21.21.12"
export APP_SPARROW_FRIGATE_ELECTRUM_PORT="50002"
export APP_SPARROW_FRIGATE_WEB_PORT="3006"

rpc_hidden_service_file="${EXPORTS_TOR_DATA_DIR}/app-${EXPORTS_APP_ID}-rpc/hostname"
export APP_SPARROW_FRIGATE_RPC_HIDDEN_SERVICE="$(cat "${rpc_hidden_service_file}" 2>/dev/null || echo "notyetset.onion")"

# Credenciales RPC de Bitcoin (tiendas community)
BITCOIN_APP_DIR="$(dirname "${EXPORTS_APP_DIR}")/bitcoin"
BITCOIN_ENV_FILE="${BITCOIN_APP_DIR}/.env"
if [ -f "${BITCOIN_ENV_FILE}" ]; then
  # shellcheck disable=SC1090
  . "${BITCOIN_ENV_FILE}"
fi
