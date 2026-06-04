use anyhow::{Context, Result};
use std::sync::Arc;

use crate::config::Config;
use crate::db::models::*;
use crate::db::Database;
use crate::rpc::BitcoinRpc;
use crate::rpc::ElectrumClient;

pub struct Broadcaster {
    rpc: Option<Arc<BitcoinRpc>>,
    indexer: Option<Arc<ElectrumClient>>,
    db: Arc<Database>,
    config: Config,
}

impl Clone for Broadcaster {
    fn clone(&self) -> Self {
        Self {
            rpc: self.rpc.clone(),
            indexer: self.indexer.clone(),
            db: self.db.clone(),
            config: self.config.clone(),
        }
    }
}

impl Broadcaster {
    pub fn new(
        rpc: Option<Arc<BitcoinRpc>>,
        indexer: Option<Arc<ElectrumClient>>,
        db: Arc<Database>,
        config: Config,
    ) -> Self {
        Self { rpc, indexer, db, config }
    }

    fn broadcast_raw(&self, tx_hex: &str) -> Result<String> {
        // Try Indexer first, then RPC
        if let Some(ref indexer) = self.indexer {
            match indexer.broadcast_transaction(tx_hex) {
                Ok(txid) => return Ok(txid),
                Err(e) => tracing::warn!("Indexer broadcast failed: {}, trying RPC...", e),
            }
        }
        if let Some(ref rpc) = self.rpc {
            return rpc.broadcast_transaction(tx_hex);
        }
        anyhow::bail!("No broadcast backend available")
    }

    pub fn broadcast_single(&self, id: &str) -> Result<String> {
        let tx = self.db.get_broadcast_tx_by_id(id)?;

        if tx.status == TxStatus::Confirmed {
            anyhow::bail!("Transaction {} is already confirmed", id);
        }

        let txid = self
            .broadcast_raw(&tx.tx_hex)
            .context(format!("Failed to broadcast transaction {}", id))?;

        let fee_rate = tx.target_fee_rate.unwrap_or(0.0);
        self.db.mark_broadcast(id, &txid, fee_rate)?;

        tracing::info!("Broadcast {} -> txid: {}", id, txid);
        Ok(txid)
    }

    pub fn broadcast_all_pending(&self, network: &str) -> Result<Vec<(String, Result<String>)>> {
        let pending_txs = self.db.list_broadcast_txs(Some("pending"), network, 1000)?;
        let mut results = Vec::new();

        for tx in pending_txs {
            let result = self.broadcast_single(&tx.id);
            results.push((tx.id, result));
        }

        Ok(results)
    }

    pub fn test_connection(&self) -> Result<bool> {
        if let Some(ref indexer) = self.indexer {
            indexer.test_connection()?;
            return Ok(true);
        }
        if let Some(ref rpc) = self.rpc {
            rpc.test_connection()?;
            return Ok(true);
        }
        anyhow::bail!("No backend configured")
    }
}
