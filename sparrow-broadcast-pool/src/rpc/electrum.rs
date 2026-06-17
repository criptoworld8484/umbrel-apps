use anyhow::{Context, Result};
use bitcoin::consensus::Decodable;

pub struct ElectrumClient {
    server: String,
    resolved: std::sync::Mutex<Option<String>>,
}

impl ElectrumClient {
    pub fn new(server: &str) -> Result<Self> {
        Ok(Self {
            server: server.to_string(),
            resolved: std::sync::Mutex::new(None),
        })
    }

    fn working_url(&self) -> Result<String> {
        if let Ok(guard) = self.resolved.lock() {
            if let Some(ref url) = *guard {
                return Ok(url.clone());
            }
        }
        let candidates = crate::discovery::connection_url_candidates(&self.server);
        let url = crate::rpc::indexer_transport::probe_working_url(&candidates)
            .with_context(|| {
                format!(
                    "Failed to connect to indexer at {} (tried TCP and SSL)",
                    crate::discovery::display_indexer_url(&self.server)
                )
            })?;
        if let Ok(mut guard) = self.resolved.lock() {
            *guard = Some(url.clone());
        }
        Ok(url)
    }

    fn rpc(&self, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        let url = self.working_url()?;
        crate::rpc::indexer_transport::json_rpc(&url, method, params)
    }

    pub fn broadcast_transaction(&self, tx_hex: &str) -> Result<String> {
        let raw = hex::decode(tx_hex).context("Invalid transaction hex")?;
        let tx_hex_str = hex::encode(&raw);
        match self.rpc("blockchain.transaction.broadcast", serde_json::json!([tx_hex_str])) {
            Ok(v) => {
                if let Some(s) = v.as_str() {
                    Ok(s.to_string())
                } else {
                    Ok(v.to_string())
                }
            }
            Err(e) => {
                let msg = e.to_string();
                let lower = msg.to_lowercase();
                if lower.contains("non-final") || lower.contains("non final") {
                    anyhow::bail!("non-final: transaction locktime not yet satisfied ({msg})");
                }
                Err(anyhow::anyhow!("Failed to broadcast transaction via indexer: {msg}"))
            }
        }
    }

    pub fn get_block_height(&self) -> Result<u64> {
        let header = self
            .rpc("blockchain.headers.subscribe", serde_json::json!([]))?
            .as_object()
            .and_then(|o| o.get("height"))
            .and_then(|h| h.as_u64())
            .context("Failed to parse block height from indexer")?;
        Ok(header)
    }

    pub fn get_height(&self) -> Result<u64> {
        self.get_block_height()
    }

    /// Calculate the fee rate of a raw transaction by querying input values from Electrs.
    pub fn calculate_tx_fee(&self, tx_hex: &str) -> Result<(f64, u64, usize)> {
        let raw = hex::decode(tx_hex).context("Invalid transaction hex")?;
        let mut cursor = std::io::Cursor::new(&raw);
        let tx = bitcoin::Transaction::consensus_decode(&mut cursor)
            .context("Failed to decode transaction")?;

        let mut total_input_value: u64 = 0;
        for input in &tx.input {
            let txid = input.previous_output.txid;
            let vout = input.previous_output.vout;
            match self.rpc(
                "blockchain.transaction.get",
                serde_json::json!([txid.to_string(), true]),
            ) {
                Ok(v) => {
                    if let Some(hex_str) = v.as_str() {
                        if let Ok(prev_raw) = hex::decode(hex_str) {
                            if let Ok(prev_tx) =
                                bitcoin::Transaction::consensus_decode(&mut &prev_raw[..])
                            {
                                if (vout as usize) < prev_tx.output.len() {
                                    total_input_value +=
                                        prev_tx.output[vout as usize].value.to_sat();
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Could not fetch input TX {}:{}: {}", txid, vout, e);
                }
            }
        }

        let total_output_value: u64 = tx.output.iter().map(|o| o.value.to_sat()).sum();
        let fee = total_input_value.saturating_sub(total_output_value);
        let weight = tx.weight().to_wu();
        let vsize = (weight + 3) / 4;
        let fee_rate = if vsize > 0 {
            fee as f64 / vsize as f64
        } else {
            0.0
        };

        tracing::info!(
            "TX fee calculation: inputs={} sat, outputs={} sat, fee={} sat, vsize={} vB, rate={:.2} sat/vB",
            total_input_value, total_output_value, fee, vsize, fee_rate
        );

        Ok((fee_rate, fee, vsize as usize))
    }

    pub fn test_connection(&self) -> Result<bool> {
        let height = self.get_block_height()?;
        tracing::info!(
            "Connected to indexer {} (height: {})",
            crate::discovery::display_indexer_url(&self.server),
            height
        );
        Ok(true)
    }

    pub fn genesis_block_hash(&self) -> Result<String> {
        let url = self.working_url()?;
        Self::genesis_block_hash_at_url(&url)
    }

    /// Read genesis hash from electrs without resolving transport again.
    pub fn genesis_block_hash_at_url(url: &str) -> Result<String> {
        if let Ok(features) =
            crate::rpc::indexer_transport::json_rpc(url, "server.features", serde_json::json!([]))
        {
            if let Some(gh) = features
                .get("genesis_hash")
                .and_then(|v| v.as_str())
                .map(|s| s.trim().to_lowercase())
            {
                if !gh.is_empty() {
                    return Ok(gh);
                }
            }
        }

        use bitcoin::consensus::Decodable;
        let header_hex = crate::rpc::indexer_transport::json_rpc(
            url,
            "blockchain.block.header",
            serde_json::json!([0]),
        )?
        .as_str()
        .context("Unexpected genesis header format")?
        .to_string();
        let raw = hex::decode(header_hex).context("Invalid genesis header hex")?;
        let header = bitcoin::block::Header::consensus_decode(&mut raw.as_slice())
            .context("Failed to decode genesis header")?;
        Ok(header.block_hash().to_string().to_lowercase())
    }

    pub fn genesis_matches_network(
        &self,
        network: &crate::config::NetworkType,
    ) -> Result<bool> {
        let actual = self.genesis_block_hash()?;
        Ok(actual == crate::discovery::expected_genesis_hash(network))
    }

    pub fn genesis_matches_network_at_url(url: &str, network: &crate::config::NetworkType) -> bool {
        let Ok(actual) = Self::genesis_block_hash_at_url(url) else {
            return false;
        };
        actual == crate::discovery::expected_genesis_hash(network)
    }

    pub fn get_median_time_past(&self) -> Result<u64> {
        let tip = self.get_block_height()? as u32;
        let start = tip.saturating_sub(10);
        let mut times = Vec::new();
        for height in start..=tip {
            let header_hex = self
                .rpc("blockchain.block.header", serde_json::json!([height]))?
                .as_str()
                .map(|s| s.to_string())
                .context("Invalid block header response")?;
            if let Ok(raw) = hex::decode(&header_hex) {
                if raw.len() >= 68 {
                    let time = u32::from_le_bytes(raw[68..72].try_into().unwrap());
                    times.push(time as u64);
                }
            }
        }
        times.sort_unstable();
        if times.is_empty() {
            anyhow::bail!("No block headers available for median time past");
        }
        Ok(times[times.len() / 2])
    }
}
