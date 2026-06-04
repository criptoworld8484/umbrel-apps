# Puertos expuestos por sparrow-frigate (referencia para otras apps)
export APP_SPARROW_FRIGATE_ELECTRUM_PORT="50002"
export APP_SPARROW_FRIGATE_WEB_PORT="3006"

# Asegurar credenciales RPC de Bitcoin (a veces no se inyectan en tiendas community)
BITCOIN_APP_DIR="$(dirname "${EXPORTS_APP_DIR}")/bitcoin"
BITCOIN_ENV_FILE="${BITCOIN_APP_DIR}/.env"
if [ -f "${BITCOIN_ENV_FILE}" ]; then
  # shellcheck disable=SC1090
  . "${BITCOIN_ENV_FILE}"
fi
