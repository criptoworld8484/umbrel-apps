# Frigate for Umbrel

Electrum server for Silent Payments (BIP352) adapted for Umbrel with Bitcoin testnet4 support.

## About

**Frigate** from [Sparrow Wallet](https://github.com/sparrowwallet/frigate) is an Electrum server for [Silent Payments](https://github.com/bitcoin/bips/blob/master/bip-0352.mediawiki) (BIP352). It performs Silent Payments scanning server-side using ephemeral client keys.

This Umbrel package is configured for **Bitcoin testnet4** and requires a backend Electrum server (like Electrs).

## Requirements

- Bitcoin Core node running on Umbrel (with `txindex=1`)
- Electrs app installed on Umbrel
- ~10GB storage for indexing
- ~4GB RAM minimum (8GB recommended)

## Installation

### Option 1: For Development/Testing

1. Fork the [umbrel-apps](https://github.com/getumbrel/umbrel-apps) repository

2. Create the Frigate app directory structure:
```bash
mkdir -p frigate/hooks frigate/data/config frigate/data/storage
```

3. Copy the provided files to the new directory

4. Add to your umbrel-apps fork and sync to your Umbrel device

### Option 2: Build Docker Image

```bash
# Build for both architectures
docker buildx build --platform linux/arm64,linux/amd64 \
  -t ghcr.io/sparrowwallet/frigate-umbrel:latest \
  --output "type=registry" .

# Or local build for testing
docker build -t ghcr.io/sparrowwallet/frigate-umbrel:latest .
```

## Configuration

The app is pre-configured for testnet4 with these defaults:

| Setting | Value |
|---------|-------|
| RPC Host | Bitcoin node IP (auto-detected) |
| RPC Port | 48332 (testnet4) |
| Electrum TCP | 50001 |
| Electrum SSL | 50002 |
| Backend | Electrs on port 60001 |

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FRIGATE_NETWORK` | Bitcoin network | `testnet4` |
| `FRIGATE_TCP` | TCP listener | `tcp://0.0.0.0:50001` |
| `FRIGATE_SSL` | SSL listener | `ssl://0.0.0.0:50002` |

## Usage

1. Install the app from the Umbrel App Store
2. Frigate will automatically connect to your Bitcoin node
3. Wait for initial indexing to complete (watch logs)
4. Connect Sparrow Wallet or other Silent Payments wallets to:
   ```
   tcp://your-umbrel.local:50001 (plaintext)
   ssl://your-umbrel.local:50002 (TLS)
   ```

## Silent Payments in Sparrow Wallet

In Sparrow Wallet:
1. Go to Preferences > Server
2. Select "Electrum Server" 
3. Enter your Umbrel's hostname with port 50001
4. For Silent Payments, use the wallet's Silent Payments tab to subscribe

## Networking

The app creates:
- Tor hidden service for RPC access
- SSL/TLS support for secure Electrum connections
- Backend proxy to Electrs for non-Silent-Payments Electrum RPC calls

## Upgrading

1. Stop the app
2. Pull new Docker image
3. Start the app (index will resume automatically)

## Troubleshooting

### Indexing Slow
Reduce `batchSize` in config or set `computeBackend = "CPU"` if no GPU available.

### Connection Refused
Ensure Bitcoin Core is running with `txindex=1` and ZMQ enabled.

### Silent Payments Not Working
Verify Electrs is running and accessible at the backend URL.

## Technical Details

- **DuckDB** for in-database EC computation
- **UltrafastSecp256k1** for GPU-accelerated scanning
- **ZMQ** for low-latency mempool ingestion
- **Backend proxy** for standard Electrum RPC calls

## License

Apache 2.0 - See [original project](https://github.com/sparrowwallet/frigate)