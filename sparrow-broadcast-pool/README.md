# Broadcast Pool (Umbrel)

Retiene transacciones firmadas y las difunde según programación (tiempo, bloque, precio).

## Wallet (Sparrow / Liana)

Usa la **IP LAN del nodo** (la misma del navegador), no electrs:

- Sparrow y Liana: `IP-LAN:50050`

## Auto-configuración al arrancar

- Red desde Bitcoin Core (`getblockchaininfo`)
- Indexador Electrs/Fulcrum: puertos TCP **50001** o **50002** (genesis validado)
- IP LAN vía `exports.sh` → `BROADCAST_POOL_LAN_IP`

Repo: https://github.com/criptoworld8484/umbrel-apps/tree/master/sparrow-broadcast-pool
