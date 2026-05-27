#!/usr/bin/env bash
set -euo pipefail

CONFIG_DIR="${FRIGATE_HOME:-/data}"
CONFIG_FILE="${CONFIG_DIR}/config.toml"
NETWORK="${FRIGATE_NETWORK:-testnet4}"

mkdir -p "${CONFIG_DIR}/db" "${CONFIG_DIR}/cache"

if [ ! -f "${CONFIG_DIR}/cert.pem" ]; then
    openssl req -x509 -newkey rsa:2048 -keyout "${CONFIG_DIR}/key.pem" -out "${CONFIG_DIR}/cert.pem" -days 3650 -nodes -subj "/CN=localhost" 2>/dev/null
fi

if [ ! -f "${CONFIG_FILE}" ]; then
    cat > "${CONFIG_FILE}" << 'EOF'
[core]
connect = true
server = "http://${BITCOIN_RPC_HOST}:${BITCOIN_RPC_PORT}"
authType = "COOKIE"
dataDir = "${BITCOIN_DATA_DIR}"
zmqSequenceEndpoint = "${ZMQ_SEQUENCE_ENDPOINT}"
rpcRequestTimeoutSeconds = 120
rpcBatchSize = 100

[index]
startHeight = 0

[scan]
batchSize = 300000
computeBackend = "AUTO"
memoryLimit = "4GB"
dbThreads = 4

[server]
tcp = "${FRIGATE_TCP}"
ssl = "${FRIGATE_SSL}"
sslCert = "${FRIGATE_SSL_CERT}"
sslKey = "${FRIGATE_SSL_KEY}"
host = "${FRIGATE_HOST}"
backendElectrumServer = "${BACKEND_ELECTRUM_SERVER}"
EOF
    sed -i "s/\${BITCOIN_RPC_HOST}/${BITCOIN_RPC_HOST}/g" "${CONFIG_FILE}"
    sed -i "s/\${BITCOIN_RPC_PORT}/${BITCOIN_RPC_PORT}/g" "${CONFIG_FILE}"
    sed -i "s/\${BITCOIN_DATA_DIR}/${BITCOIN_DATA_DIR}/g" "${CONFIG_FILE}"
    sed -i "s|\${ZMQ_SEQUENCE_ENDPOINT}|${ZMQ_SEQUENCE_ENDPOINT}|g" "${CONFIG_FILE}"
    sed -i "s/\${FRIGATE_TCP}/${FRIGATE_TCP}/g" "${CONFIG_FILE}"
    sed -i "s/\${FRIGATE_SSL}/${FRIGATE_SSL}/g" "${CONFIG_FILE}"
    sed -i "s|\${FRIGATE_SSL_CERT}|${FRIGATE_SSL_CERT}|g" "${CONFIG_FILE}"
    sed -i "s|\${FRIGATE_SSL_KEY}|${FRIGATE_SSL_KEY}|g" "${CONFIG_FILE}"
    sed -i "s/\${FRIGATE_HOST}/${FRIGATE_HOST}/g" "${CONFIG_FILE}"
    sed -i "s|\${BACKEND_ELECTRUM_SERVER}|${BACKEND_ELECTRUM_SERVER}|g" "${CONFIG_FILE}"
fi

exec /opt/frigate/bin/frigate -d "${CONFIG_DIR}" -n "${NETWORK}"