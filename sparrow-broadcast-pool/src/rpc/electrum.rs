use anyhow::{Context, Result};
use electrum_client::{Client, ElectrumApi, ConfigBuilder};
use std::time::Duration;

pub struct ElectrumClient {
    server: String,
}

impl ElectrumClient {
    pub fn new(server: &str) -> Result<Self> {
        Ok(Self {
            server: server.to_string(),
        })
    }

    fn connect(&self) -> Result<Client> {
        let url = if self.server.starts_with("tcp://") || self.server.starts_with("ssl://") {
            self.server.clone()
        } else {
            format!("tcp://{}", self.server)
        };
        tracing::info!("Connecting to indexer at URL: {}", url);

        let config = ConfigBuilder::new()
            .validate_domain(false)
            .timeout(Some(Duration::from_secs(10)))
            .retry(1)
            .build();

        match Client::from_config(&url, config) {
            Ok(client) => Ok(client),
            Err(e) => {
                tracing::error!("Electrum client connect error for '{}': {:?}", url, e);
                Err(anyhow::anyhow!("Failed to connect to indexer at {}: {:?}", url, e))
            }
        }
    }

    pub fn broadcast_transaction(&self, tx_hex: &str) -> Result<String> {
        let mut client = self.connect()?;
        let raw = hex::decode(tx_hex).context("Invalid transaction hex")?;
        match client.transaction_broadcast_raw(&raw) {
            Ok(txid) => Ok(txid.to_string()),
            Err(e) => {
                let msg = format!("{:?}", e);
                let lower = msg.to_lowercase();
                if lower.contains("non-final") || lower.contains("non final") {
                    anyhow::bail!("non-final: transaction locktime not yet satisfied ({msg})");
                }
                Err(anyhow::anyhow!("Failed to broadcast transaction via indexer: {msg}"))
            }
        }
    }

    pub fn get_block_height(&self) -> Result<u64> {
        let mut client = self.connect()?;
        let header = client
            .block_headers_subscribe()
            .context("Failed to subscribe to block headers")?;
        Ok(header.height as u64)
    }

    pub fn get_height(&self) -> Result<u64> {
        self.get_block_height()
    }

    /// Calculate the fee rate of a raw transaction by querying input values from Electrs.
    /// Returns (fee_rate_sat_vb, fee_sat, tx_size_bytes)
    pub fn calculate_tx_fee(&self, tx_hex: &str) -> Result<(f64, u64, usize)> {
        use electrum_client::bitcoin::consensus::Decodable;

        let raw = hex::decode(tx_hex).context("Invalid transaction hex")?;
        let tx_size = raw.len();

        // Parse the transaction to get input txids
        let mut cursor = std::io::Cursor::new(&raw);
        let tx = bitcoin::Transaction::consensus_decode(&mut cursor)
            .context("Failed to decode transaction")?;

        let mut total_input_value: u64 = 0;
        let mut client = self.connect()?;

        // Query each input's value from Indexer
        for input in &tx.input {
            let txid = input.previous_output.txid;
            let vout = input.previous_output.vout as usize;

            // Get the previous transaction
            match client.transaction_get(&txid) {
                Ok(prev_tx) => {
                    if vout < prev_tx.output.len() {
                        total_input_value += prev_tx.output[vout].value.to_sat();
                    }
                }
                Err(e) => {
                    tracing::warn!("Could not fetch input TX {}:{}: {}", txid, vout, e);
                }
            }
        }

        let total_output_value: u64 = tx.output.iter().map(|o| o.value.to_sat()).sum();
        let fee = total_input_value.saturating_sub(total_output_value);

        // Calculate vsize: for segwit, weight / 4 rounded up
        let weight = tx.weight().to_wu();
        let vsize = (weight + 3) / 4;
        let fee_rate = if vsize > 0 { (fee as f64 / vsize as f64) } else { 0.0 };

        tracing::info!(
            "TX fee calculation: inputs={} sat, outputs={} sat, fee={} sat, vsize={} vB, rate={:.2} sat/vB",
            total_input_value, total_output_value, fee, vsize, fee_rate
        );

        Ok((fee_rate, fee, vsize as usize))
    }

    pub fn test_connection(&self) -> Result<bool> {
        let mut client = self.connect()?;
        let header = client
            .block_headers_subscribe()
            .context("Failed to subscribe to block headers")?;
        tracing::info!(
            "Connected to indexer {} (height: {})",
            self.server,
            header.height
        );
        Ok(true)
    }

    /// Median time past from the last 11 block headers (BIP113 approximation).
    pub fn get_median_time_past(&self) -> Result<u64> {
        let mut client = self.connect()?;
        let tip = client
            .block_headers_subscribe()
            .context("Failed to get tip height")?
            .height;
        let start = tip.saturating_sub(10);
        let mut times = Vec::new();
        for height in start..=tip {
            let header = client
                .block_header(height)
                .context("Failed to get block header")?;
            times.push(header.time as u64);
        }
        times.sort_unstable();
        Ok(times[times.len() / 2])
    }
}