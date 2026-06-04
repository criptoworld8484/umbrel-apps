use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::db::models::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparrowUtxoExport {
    #[serde(rename = "txid")]
    pub txid: String,
    #[serde(rename = "vout")]
    pub vout: u32,
    #[serde(rename = "value")]
    pub value: Option<f64>,
    #[serde(rename = "valueSat")]
    pub value_sat: Option<u64>,
    #[serde(rename = "address")]
    pub address: Option<String>,
    #[serde(rename = "label")]
    pub label: Option<String>,
    #[serde(rename = "path")]
    pub path: Option<String>,
    #[serde(rename = "confirmations")]
    pub confirmations: Option<u64>,
    #[serde(rename = "output")]
    pub output: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparrowTransactionExport {
    #[serde(rename = "txid")]
    pub txid: String,
    #[serde(rename = "hex")]
    pub hex: Option<String>,
    #[serde(rename = "fee")]
    pub fee: Option<f64>,
    #[serde(rename = "height")]
    pub height: Option<u64>,
    #[serde(rename = "label")]
    pub label: Option<String>,
}

pub struct SparrowImporter;

impl SparrowImporter {
    pub fn import_utxos_from_json(file_path: &Path) -> Result<Vec<NewMigrationUtxo>> {
        let content = std::fs::read_to_string(file_path)
            .with_context(|| format!("Failed to read file: {}", file_path.display()))?;

        let utxos: Vec<SparrowUtxoExport> = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse JSON: {}", file_path.display()))?;

        let mut result = Vec::new();
        for utxo in utxos {
            let value_btc = if let Some(v) = utxo.value {
                v
            } else if let Some(vs) = utxo.value_sat {
                vs as f64 / 100_000_000.0
            } else {
                0.0
            };

            result.push(NewMigrationUtxo {
                txid: utxo.txid,
                vout: utxo.vout as i32,
                value_btc,
                address: utxo.address,
                label: utxo.label.clone(),
                source_label: utxo.label,
            });
        }

        tracing::info!("Imported {} UTXOs from {}", result.len(), file_path.display());
        Ok(result)
    }

    pub fn import_signed_tx_from_json(file_path: &Path) -> Result<Vec<NewBroadcastTx>> {
        let content = std::fs::read_to_string(file_path)
            .with_context(|| format!("Failed to read file: {}", file_path.display()))?;

        // Try array of transactions first
        if let Ok(txs) = serde_json::from_str::<Vec<SparrowTransactionExport>>(&content) {
            let mut result = Vec::new();
            for tx in txs {
                if let Some(hex) = tx.hex {
                    result.push(NewBroadcastTx {
                        tx_hex: hex,
                        network: "signet".to_string(),
                        nlocktime: None,
                        broadcast_mode: None,
                        scheduled_time: None,
                        target_fee_rate: None,
                        source_label: tx.label.clone(),
                        destination_address: None,
                        utxo_count: Some(1),
                        total_value_btc: tx.fee,
                        replacement_of: None,
                    });
                }
            }
            tracing::info!(
                "Imported {} signed transactions from {}",
                result.len(),
                file_path.display()
            );
            return Ok(result);
        }

        // Try single transaction
        let tx: SparrowTransactionExport = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse transaction JSON: {}", file_path.display()))?;

        if let Some(hex) = tx.hex {
            Ok(vec![NewBroadcastTx {
                tx_hex: hex,
                network: "signet".to_string(),
                nlocktime: None,
                broadcast_mode: None,
                scheduled_time: None,
                target_fee_rate: None,
                source_label: tx.label.clone(),
                destination_address: None,
                utxo_count: Some(1),
                total_value_btc: tx.fee,
                replacement_of: None,
            }])
        } else {
            anyhow::bail!("Transaction JSON does not contain hex data")
        }
    }

    pub fn import_raw_tx_hex(hex: &str, network: &str, label: Option<&str>) -> Result<NewBroadcastTx> {
        // Validate hex
        hex::decode(hex).context("Invalid hex data")?;

        Ok(NewBroadcastTx {
            tx_hex: hex.to_string(),
            network: network.to_string(),
            nlocktime: None,
            broadcast_mode: None,
            scheduled_time: None,
            target_fee_rate: None,
            source_label: label.map(|s| s.to_string()),
            destination_address: None,
            utxo_count: Some(1),
            total_value_btc: None,
            replacement_of: None,
        })
    }
}
