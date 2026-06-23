# Umbrel exports — LAN IP for wallet Electrum URL (Sparrow/Liana)
export APP_SPARROW_BROADCAST_POOL_LAN_IP="$(ip -o route get to 8.8.8.8 2>/dev/null | sed -n 's/.*src \([0-9.]\+\).*/\1/p')"

# Docker network config (same pattern as Semilla Bitcoin broadcast pool)
export APP_SPARROW_BROADCAST_POOL_IP="10.21.21.60"
export APP_SPARROW_BROADCAST_POOL_PORT="8080"
