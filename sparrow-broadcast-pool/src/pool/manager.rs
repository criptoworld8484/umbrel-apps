use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rand::Rng;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::Config;
use crate::db::models::*;
use crate::db::Database;
use crate::rpc::BitcoinRpc;
use crate::rpc::ElectrumClient;

#[derive(Clone)]
pub struct PendingTxOutput {
    pub output_index: u32,
    pub value: u64, // satoshis
    pub scripthash: String,
}

#[derive(Clone)]
pub struct PendingTxInfo {
    pub tx_hex: String,
    pub scripthashes: Vec<String>,
    pub outputs: Vec<PendingTxOutput>,
}

pub struct PoolManager {
    db: Arc<Database>,
    rpc: Option<Arc<BitcoinRpc>>,
    indexer: Option<Arc<ElectrumClient>>,
    config: Arc<Mutex<Config>>,
    pending_txs: Arc<Mutex<HashMap<String, PendingTxInfo>>>,
    mtp_cache: Arc<Mutex<Option<(Instant, u64)>>>,
}

impl PoolManager {
    pub fn new(
        db: Arc<Database>,
        rpc: Option<Arc<BitcoinRpc>>,
        indexer: Option<Arc<ElectrumClient>>,
        config: Arc<Mutex<Config>>,
    ) -> Self {
        Self {
            db,
            rpc,
            indexer,
            config,
            pending_txs: Arc::new(Mutex::new(HashMap::new())),
            mtp_cache: Arc::new(Mutex::new(None)),
        }
    }

    fn require_rpc(&self) -> Result<&BitcoinRpc> {
        self.rpc.as_ref().map(|r| r.as_ref()).context("RPC not available - this command requires a Bitcoin Core connection")
    }

    pub fn rpc_available(&self) -> bool {
        self.rpc.is_some() || self.indexer.is_some()
    }

    pub fn get_db(&self) -> &Arc<Database> {
        &self.db
    }

    pub fn schedule_at(&self, id: &str, scheduled_time: DateTime<Utc>, fee_rate: f64) -> Result<BroadcastTx> {
        let tx = self.db.get_broadcast_tx_by_id(id)?;

        let now = Utc::now();
        if scheduled_time <= now {
            anyhow::bail!("Scheduled time must be in the future");
        }

        if let Some(n) = tx.nlocktime.filter(|&n| n > 500_000_000) {
            let sched_unix = scheduled_time.timestamp().max(0) as u64;
            if sched_unix < n {
                anyhow::bail!(
                    "Scheduled time cannot be before nLockTime (unix {}). The network will only accept this transaction when chain MTP reaches the signed nLockTime.",
                    n
                );
            }
        }

        let scheduled_str = scheduled_time.to_rfc3339();
        let is_reschedule = tx.broadcast_missed_at.is_some()
            || tx.defer_until.is_some()
            || tx.scheduled_time.is_some()
            || tx.broadcast_mode.as_deref() == Some("scheduled");

        let defer_until = if is_reschedule {
            Some(scheduled_str.as_str())
        } else {
            None
        };

        self.db
            .update_reschedule(id, &scheduled_str, defer_until, fee_rate)?;

        let mut updated = self.db.get_broadcast_tx_by_id(id)?;
        self.enrich_tx_locktime(&mut updated);
        Ok(updated)
    }

    pub fn broadcast_transaction(&self, tx_hex: &str) -> Result<String> {
        let mut indexer_err = None;
        if let Some(ref indexer) = self.indexer {
            match indexer.broadcast_transaction(tx_hex) {
                Ok(txid) => return Ok(txid),
                Err(e) => {
                    tracing::warn!("Indexer broadcast failed: {}, trying RPC...", e);
                    indexer_err = Some(e);
                }
            }
        }
        if let Some(ref rpc) = self.rpc {
            return rpc.broadcast_transaction(tx_hex);
        }
        if let Some(e) = indexer_err {
            return Err(e);
        }
        anyhow::bail!("No broadcast backend available (neither Indexer nor RPC)")
    }

    pub fn import_transaction(&self, new_tx: &NewBroadcastTx) -> Result<BroadcastTx> {
        let tx = self.db.insert_broadcast_tx(new_tx)?;
        tracing::info!(
            "Imported transaction {} into broadcast pool (network: {}, status: {})",
            tx.id,
            tx.network,
            tx.status.as_str()
        );
        Ok(tx)
    }

    pub fn mark_broadcast(&self, id: &str, txid: &str, fee_rate: f64) -> Result<()> {
        self.db.mark_broadcast(id, txid, fee_rate)?;
        self.remove_pending_tx(txid);
        Ok(())
    }

    pub fn has_pending_tx(&self, txid: &str) -> bool {
        self.get_pending_tx_hex(txid).is_some()
    }

    pub fn has_pending_for_scripthash(&self, scripthash: &str) -> bool {
        !self.get_pending_txids_for_scripthash(scripthash).is_empty()
    }

    /// Rehydrate in-memory virtual mempool from DB pending/scheduled txs (survives restarts)
    pub fn load_pending_from_db(&self) -> Result<usize> {
        let network = {
            let config = self
                .config
                .lock()
                .map_err(|e| anyhow::anyhow!("Config lock failed: {}", e))?;
            config.network.network_type.data_dir_name().to_string()
        };

        let indexer_url = self
            .get_indexer_url()
            .context("No indexer configured for pending tx rehydration")?;
        let indexer_addr = super::virtual_mempool::strip_indexer_host(&indexer_url);

        let pending = self.db.list_broadcast_txs(Some("pending"), &network, 10_000)?;
        let scheduled = self.db.list_broadcast_txs(Some("scheduled"), &network, 10_000)?;

        let mut count = 0;
        for tx in pending.into_iter().chain(scheduled) {
            let txid = match tx.txid {
                Some(ref id) => id.clone(),
                None => match super::virtual_mempool::compute_txid(&tx.tx_hex) {
                    Ok(id) => id,
                    Err(e) => {
                        tracing::warn!("Skipping rehydrate for pool tx {}: {}", tx.id, e);
                        continue;
                    }
                },
            };

            match super::virtual_mempool::extract_affected_scripthashes(
                &tx.tx_hex,
                &indexer_addr,
            ) {
                Ok(scripthashes) => {
                    let outputs = super::virtual_mempool::extract_outputs(&tx.tx_hex)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(output_index, value, scripthash)| PendingTxOutput {
                            output_index,
                            value,
                            scripthash,
                        })
                        .collect();
                    self.store_pending_tx(&txid, &tx.tx_hex, scripthashes, outputs);
                    count += 1;
                }
                Err(e) => {
                    tracing::warn!("Failed to rehydrate pending tx {}: {}", txid, e);
                }
            }
        }

        if count > 0 {
            tracing::info!("Rehydrated {} pending/scheduled txs into virtual mempool", count);
        }
        Ok(count)
    }

    pub fn get_tx_hex_by_txid(&self, txid: &str) -> Result<Option<String>> {
        self.db.get_tx_hex_by_txid(txid)
    }

    pub fn schedule_transaction(
        &self,
        id: &str,
        min_delay_hours: Option<u64>,
        max_delay_hours: Option<u64>,
        min_fee_rate: Option<f64>,
        max_fee_rate: Option<f64>,
        fixed_fee_rate: Option<f64>,
    ) -> Result<BroadcastTx> {
        let config = self.config.lock().map_err(|e| anyhow::anyhow!("Config lock failed: {}", e))?;
        let min_delay = min_delay_hours.unwrap_or(config.schedule.min_delay_hours as u64);
        let max_delay_val = max_delay_hours.unwrap_or(config.schedule.max_delay_hours as u64);
        let min_fee = min_fee_rate.unwrap_or(config.schedule.min_fee_rate);
        let max_fee = max_fee_rate.unwrap_or(config.schedule.max_fee_rate);
        drop(config);

        let mut rng = rand::thread_rng();
        let delay_hours = rng.gen_range(min_delay..=max_delay_val.max(min_delay));
        let fee_rate = if let Some(fixed) = fixed_fee_rate {
            fixed
        } else {
            rng.gen_range(min_fee..=max_fee.max(min_fee))
        };

        let scheduled_time = Utc::now()
            .checked_add_signed(chrono::Duration::hours(delay_hours as i64))
            .context("Failed to calculate scheduled time")?;

        let scheduled_time_str = scheduled_time.to_rfc3339();
        let now = Utc::now().to_rfc3339();

        self.db.execute_raw(
            "UPDATE broadcast_pool SET status = 'scheduled', scheduled_time = ?1, target_fee_rate = ?2, updated_at = ?3 WHERE id = ?4",
            &[&scheduled_time_str.as_str(), &fee_rate.to_string().as_str(), &now.as_str(), &id],
        )?;

        tracing::info!(
            "Scheduled transaction {} for {} (fee: {:.2} sat/vB, delay: {}h)",
            id,
            scheduled_time.format("%Y-%m-%d %H:%M UTC"),
            fee_rate,
            delay_hours
        );

        self.db.get_broadcast_tx_by_id(id)
    }

    pub fn schedule_all_pending(&self, network: &str) -> Result<Vec<BroadcastTx>> {
        let pending_txs = self
            .db
            .list_broadcast_txs(Some("pending"), network, 1000)?;

        let mut scheduled = Vec::new();
        for tx in pending_txs {
            let scheduled_tx = self.schedule_transaction(&tx.id, None, None, None, None, None)?;
            scheduled.push(scheduled_tx);
        }

        tracing::info!(
            "Scheduled {} pending transactions on {}",
            scheduled.len(),
            network
        );
        Ok(scheduled)
    }

    pub fn broadcast_due_transactions(&self) -> Result<Vec<(String, Result<String>)>> {
        let network = {
            let config = self.config.lock().map_err(|e| anyhow::anyhow!("Config lock failed: {}", e))?;
            config.network.network_type.data_dir_name().to_string()
        };
        let now = Utc::now();
        let candidate_txs = self.db.get_due_transactions(&network)?;
        let due_txs: Vec<BroadcastTx> = candidate_txs
            .into_iter()
            .filter(|tx| self.is_tx_due_for_broadcast(tx, now).unwrap_or(false))
            .collect();
        let mut results = Vec::new();

        for tx in due_txs {
            if !self.is_locktime_satisfied(tx.nlocktime)? {
                if tx.nlocktime.is_some_and(|n| n > 500_000_000) {
                    if tx.broadcast_missed_at.is_none() {
                        let missed_at = now.to_rfc3339();
                        let original = tx.scheduled_time.map(|t| t.to_rfc3339());
                        if let Err(e) = self.db.record_broadcast_miss(
                            &tx.id,
                            &missed_at,
                            original.as_deref(),
                        ) {
                            tracing::warn!("Failed to record broadcast miss for {}: {}", tx.id, e);
                        }
                    }
                    if let Ok(mtp) = self.get_median_time_past_cached() {
                        let remaining = tx.nlocktime.unwrap_or(0).saturating_sub(mtp);
                        tracing::info!(
                            "Tx {} missed scheduled broadcast — chain MTP not ready (nLockTime={}, chain MTP={}, ~{}s remaining)",
                            tx.id,
                            tx.nlocktime.unwrap_or(0),
                            mtp,
                            remaining
                        );
                    } else {
                        tracing::info!(
                            "Tx {} missed scheduled broadcast — chain MTP not ready (nLockTime={:?})",
                            tx.id,
                            tx.nlocktime
                        );
                    }
                } else if let Some(n) = tx.nlocktime.filter(|&n| n > 0 && n <= 500_000_000) {
                    tracing::info!(
                        "Tx {} waiting for block height locktime (nLockTime={})",
                        tx.id,
                        n
                    );
                } else {
                    tracing::info!(
                        "Tx {} waiting for chain locktime (nLockTime={:?})",
                        tx.id,
                        tx.nlocktime
                    );
                }
                continue;
            }

            tracing::info!("Broadcasting due transaction {}", tx.id);

            match self.broadcast_transaction(&tx.tx_hex) {
                Ok(txid) => {
                    let fee_rate = tx.target_fee_rate.unwrap_or(0.0);
                    self.mark_broadcast(&tx.id, &txid, fee_rate)?;
                    tracing::info!("Successfully broadcast {} (txid: {})", tx.id, txid);
                    results.push((tx.id, Ok(txid)));
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    if is_retriable_broadcast_error(&err_msg) {
                        tracing::info!(
                            "Broadcast deferred for {} (will retry): {}",
                            tx.id,
                            err_msg
                        );
                        results.push((tx.id.clone(), Err(e)));
                        continue;
                    }
                    self.db.update_tx_status(&tx.id, TxStatus::Failed, Some(&err_msg))?;
                    tracing::error!("Failed to broadcast {}: {}", tx.id, err_msg);
                    results.push((tx.id, Err(e)));
                }
            }
        }

        Ok(results)
    }

    fn is_tx_due_for_broadcast(&self, tx: &BroadcastTx, now: DateTime<Utc>) -> Result<bool> {
        let locktime_ok = self.is_locktime_satisfied(tx.nlocktime)?;

        if tx.defer_until.is_some() && locktime_ok {
            return Ok(true);
        }

        if let Some(defer_until) = tx.defer_until {
            if now >= defer_until {
                return Ok(true);
            }
            return Ok(false);
        }

        if tx
            .scheduled_time
            .as_ref()
            .is_some_and(|t| *t <= now)
        {
            return Ok(true);
        }

        Ok(false)
    }

    pub fn requeue_retriable_failures(&self) -> Result<usize> {
        let network = {
            let config = self.config.lock().map_err(|e| anyhow::anyhow!("Config lock failed: {}", e))?;
            config.network.network_type.data_dir_name().to_string()
        };
        let failed = self.db.list_broadcast_txs(Some("failed"), &network, 1000)?;
        let mut count = 0;
        for tx in failed {
            let err = tx.error_message.as_deref().unwrap_or("");
            let retriable = is_retriable_broadcast_error(err)
                || (tx.nlocktime.is_some_and(|n| n > 0) && err.contains("Failed to broadcast"));
            if retriable {
                self.db.reset_failed_to_scheduled(&tx.id)?;
                count += 1;
                tracing::info!("Requeued failed tx {} for retry", tx.id);
            }
        }
        Ok(count)
    }

    pub fn is_locktime_satisfied(&self, nlocktime: Option<u64>) -> Result<bool> {
        let nlocktime = match nlocktime {
            Some(n) if n > 0 => n,
            _ => return Ok(true),
        };

        if nlocktime > 500_000_000 {
            let mtp = self.get_median_time_past_cached()?;
            Ok(mtp >= nlocktime)
        } else {
            match self.check_block_height()? {
                Some(height) => Ok(height >= nlocktime),
                None => Ok(false),
            }
        }
    }

    /// One scheduler tick: mark pending due, requeue failures, broadcast scheduled txs.
    pub fn run_scheduler_tick(&self) -> Result<Vec<(String, Result<String>)>> {
        if !self.indexer_healthy() {
            anyhow::bail!("indexer unavailable");
        }

        let network = {
            let config = self.config.lock().map_err(|e| anyhow::anyhow!("Config lock failed: {}", e))?;
            config.network.network_type.data_dir_name().to_string()
        };

        for tx in self.get_pending_by_scheduled_time(&network)? {
            if tx.broadcast_mode.as_deref() != Some("scheduled") {
                continue;
            }
            tracing::info!("Pending tx {} has scheduled_time reached, marking as due", tx.id);
            if let Err(e) = self.mark_as_due(&tx.id) {
                tracing::error!("Failed to mark {} as due: {}", tx.id, e);
            }
        }

        if let Err(e) = self.requeue_retriable_failures() {
            tracing::warn!("Failed to requeue retriable failures: {}", e);
        }

        self.broadcast_due_transactions()
    }

    fn get_median_time_past(&self) -> Result<u64> {
        if let Some(ref rpc) = self.rpc {
            if rpc.test_connection().unwrap_or(false) {
                return rpc.get_median_time();
            }
        }
        if let Some(ref indexer) = self.indexer {
            return indexer.get_median_time_past();
        }
        anyhow::bail!("No backend available to read median time past")
    }

    pub fn get_chain_mtp(&self) -> Result<u64> {
        self.get_median_time_past_cached()
    }

    fn get_median_time_past_cached(&self) -> Result<u64> {
        const TTL: Duration = Duration::from_secs(10);
        if let Ok(cache) = self.mtp_cache.lock() {
            if let Some((fetched_at, mtp)) = *cache {
                if fetched_at.elapsed() < TTL {
                    return Ok(mtp);
                }
            }
        }
        let mtp = self.get_median_time_past()?;
        if let Ok(mut cache) = self.mtp_cache.lock() {
            *cache = Some((Instant::now(), mtp));
        }
        Ok(mtp)
    }

    pub fn retry_failed_transaction(&self, id: &str) -> Result<BroadcastTx> {
        let tx = self.db.get_broadcast_tx_by_id(id)?;
        if tx.status != TxStatus::Failed {
            anyhow::bail!("Transaction {} is not in failed state", id);
        }
        self.db.reset_failed_to_scheduled(id)?;
        let mut tx = self.db.get_broadcast_tx_by_id(id)?;
        self.enrich_tx_locktime(&mut tx);
        Ok(tx)
    }

    pub fn rebroadcast_pending(&self) -> Result<Vec<(String, Result<String>)>> {
        let (interval, network) = {
            let config = self.config.lock().map_err(|e| anyhow::anyhow!("Config lock failed: {}", e))?;
            (config.pool.rebroadcast_interval_minutes as i32, config.network.network_type.data_dir_name().to_string())
        };
        let pending_txs = self.db.get_pending_rebroadcast(interval, &network)?;
        let mut results = Vec::new();

        for tx in pending_txs {
            tracing::debug!("Rebroadcasting transaction {}", tx.id);

            match self.broadcast_transaction(&tx.tx_hex) {
                Ok(txid) => {
                    let now = Utc::now().to_rfc3339();
                    self.db.execute_raw(
                        "UPDATE broadcast_pool SET broadcast_at = ?1, updated_at = ?1 WHERE id = ?2",
                        &[&now.as_str(), &tx.id.as_str()],
                    )?;
                    results.push((tx.id.clone(), Ok(txid)));
                }
                Err(e) => {
                    tracing::warn!("Rebroadcast failed for {}: {}", tx.id, e);
                    results.push((tx.id.clone(), Err(e)));
                }
            }
        }

        Ok(results)
    }

    pub fn check_confirmations(&self) -> Result<Vec<(String, bool, Option<u64>)>> {
        let network = {
            let config = self.config.lock().map_err(|e| anyhow::anyhow!("Config lock failed: {}", e))?;
            config.network.network_type.data_dir_name().to_string()
        };
        let broadcast_txs = self.db.list_broadcast_txs(Some("broadcast"), &network, 1000)?;
        let mut results = Vec::new();

        // Try RPC first (it can actually check confirmations per tx)
        if let Some(ref rpc) = self.rpc {
            if rpc.test_connection().unwrap_or(false) {
                for tx in &broadcast_txs {
                    if let Some(ref txid) = tx.txid {
                        match rpc.get_transaction(txid) {
                            Ok(raw_tx) => {
                                if let Some(ref blockhash) = raw_tx.blockhash {
                                    // Transaction is in a block
                                    if let Ok(height) = rpc.get_block_count() {
                                        let _ = self.db.mark_confirmed(&tx.id, height);
                                        results.push((tx.id.clone(), true, Some(height)));
                                    } else {
                                        results.push((tx.id.clone(), true, None));
                                    }
                                } else {
                                    // In mempool, not confirmed yet
                                    results.push((tx.id.clone(), false, None));
                                }
                            }
                            Err(_) => {
                                // Can't find tx, might be dropped
                                results.push((tx.id.clone(), false, None));
                            }
                        }
                    } else {
                        results.push((tx.id.clone(), false, None));
                    }
                }
                return Ok(results);
            }
        }

        // Fallback to indexer (limited confirmation checking)
        if let Some(ref indexer) = self.indexer {
            if indexer.test_connection().unwrap_or(false) {
                for tx in broadcast_txs {
                    results.push((tx.id, false, None));
                }
                return Ok(results);
            }
        }

        Ok(results)
    }

    pub fn list_transactions(
        &self,
        status_filter: Option<&str>,
        limit: i32,
    ) -> Result<Vec<BroadcastTx>> {
        let network = {
            let config = self.config.lock().map_err(|e| anyhow::anyhow!("Config lock failed: {}", e))?;
            config.network.network_type.data_dir_name().to_string()
        };
        let mut txs = self.db.list_broadcast_txs(status_filter, &network, limit)?;
        for tx in &mut txs {
            self.enrich_tx_locktime(tx);
        }
        Ok(txs)
    }

    pub fn get_transaction(&self, id: &str) -> Result<BroadcastTx> {
        let mut tx = self.db.get_broadcast_tx_by_id(id)?;
        self.enrich_tx_locktime(&mut tx);
        Ok(tx)
    }

    fn tx_has_broadcast_schedule(tx: &BroadcastTx) -> bool {
        tx.broadcast_mode.as_deref() == Some("scheduled")
            || tx.scheduled_time.is_some()
            || tx.defer_until.is_some()
            || tx.broadcast_missed_at.is_some()
    }

    fn tx_can_reschedule(tx: &BroadcastTx) -> bool {
        matches!(tx.status, TxStatus::Pending | TxStatus::Scheduled)
            && Self::tx_has_broadcast_schedule(tx)
    }

    fn enrich_tx_locktime(&self, tx: &mut BroadcastTx) {
        tx.locktime_waiting = None;
        tx.locktime_deferred = None;
        tx.can_reschedule = None;
        tx.chain_mtp = None;
        tx.locktime_target = None;
        tx.locktime_remaining_secs = None;
        tx.locktime_satisfied = None;

        let emitted = matches!(tx.status, TxStatus::Broadcast | TxStatus::Confirmed);
        let deferred = tx.broadcast_missed_at.is_some() && !emitted;
        tx.locktime_deferred = Some(deferred);
        tx.can_reschedule = Some(!emitted && Self::tx_can_reschedule(tx));

        let nlock = match tx.nlocktime {
            Some(n) if n > 0 && n > 500_000_000 => n,
            _ => {
                tx.locktime_waiting = Some(false);
                return;
            }
        };

        tx.locktime_target = Some(nlock);

        let mtp = match self.get_median_time_past_cached() {
            Ok(mtp) => mtp,
            Err(e) => {
                tracing::debug!("Could not read chain MTP for {}: {}", tx.id, e);
                return;
            }
        };

        tx.chain_mtp = Some(mtp);
        let satisfied = mtp >= nlock;
        tx.locktime_satisfied = Some(satisfied);
        if !satisfied {
            tx.locktime_remaining_secs = Some(nlock as i64 - mtp as i64);
        } else {
            tx.locktime_remaining_secs = Some(0);
        }

        let waiting_for_locktime = !emitted
            && matches!(tx.status, TxStatus::Pending | TxStatus::Scheduled)
            && !satisfied;
        tx.locktime_waiting = Some(waiting_for_locktime);
    }

    pub fn remove_transaction(&self, id: &str) -> Result<()> {
        self.db.remove_broadcast_tx(id)?;
        tracing::info!("Removed transaction {} from broadcast pool", id);
        Ok(())
    }

    pub fn get_stats(&self) -> Result<PoolStats> {
        let network = {
            let config = self.config.lock().map_err(|e| anyhow::anyhow!("Config lock failed: {}", e))?;
            config.network.network_type.data_dir_name().to_string()
        };
        self.db.get_pool_stats(&network)
    }

    pub fn get_pending_by_block_height(&self, network: &str) -> Result<Vec<BroadcastTx>> {
        self.db.get_pending_by_block_height(network)
    }

    pub fn get_pending_by_scheduled_time(&self, network: &str) -> Result<Vec<BroadcastTx>> {
        self.db.get_pending_by_scheduled_time(network)
    }

    pub fn mark_as_due(&self, id: &str) -> Result<()> {
        self.db.mark_due(id)
    }

    pub fn mark_as_due_with_schedule(&self, id: &str, scheduled_time: &chrono::DateTime<chrono::Utc>) -> Result<()> {
        self.db.mark_due_with_schedule(id, scheduled_time)
    }

    pub fn rpc_connected(&self) -> bool {
        if let Some(ref rpc) = self.rpc {
            rpc.test_connection().unwrap_or(false)
        } else {
            false
        }
    }

    pub fn get_rpc(&self) -> Option<&Arc<BitcoinRpc>> {
        self.rpc.as_ref()
    }

    pub fn get_indexer(&self) -> Option<&Arc<ElectrumClient>> {
        self.indexer.as_ref()
    }

    pub fn indexer_healthy(&self) -> bool {
        if let Some(ref indexer) = self.indexer {
            indexer.test_connection().is_ok()
        } else if let Some(ref rpc) = self.rpc {
            rpc.test_connection().is_ok()
        } else {
            false
        }
    }

    pub fn check_block_height(&self) -> Result<Option<u64>> {
        if let Some(ref indexer) = self.indexer {
            match indexer.get_block_height() {
                Ok(height) => return Ok(Some(height)),
                Err(e) => tracing::debug!("Indexer get_height failed: {}", e),
            }
        }
        if let Some(ref rpc) = self.rpc {
            match rpc.get_block_count() {
                Ok(height) => return Ok(Some(height)),
                Err(e) => tracing::debug!("RPC get_block_count failed: {}", e),
            }
        }
        Ok(None)
    }

    pub fn store_pending_tx(&self, txid: &str, tx_hex: &str, scripthashes: Vec<String>, outputs: Vec<PendingTxOutput>) {
        let mut pending = self.pending_txs.lock().unwrap();
        pending.insert(
            txid.to_string(),
            PendingTxInfo {
                tx_hex: tx_hex.to_string(),
                scripthashes,
                outputs,
            },
        );
        tracing::info!("Stored pending tx {} with {} scripthashes, {} outputs", txid, pending.get(txid).unwrap().scripthashes.len(), pending.get(txid).unwrap().outputs.len());
    }

    pub fn lookup_tx_hex(&self, txid: &str) -> Option<String> {
        let normalized = txid.trim().to_lowercase();
        if let Some(hex) = self.get_pending_tx_hex(&normalized) {
            return Some(hex);
        }
        if let Some(alt) = super::virtual_mempool::alternate_txid_format(&normalized) {
            if let Some(hex) = self.get_pending_tx_hex(&alt) {
                return Some(hex);
            }
        }
        if let Ok(Some(hex)) = self.get_tx_hex_by_txid(&normalized) {
            return Some(hex);
        }
        if let Some(alt) = super::virtual_mempool::alternate_txid_format(&normalized) {
            if let Ok(Some(hex)) = self.get_tx_hex_by_txid(&alt) {
                return Some(hex);
            }
        }
        None
    }

    pub fn get_pending_tx_hex(&self, txid: &str) -> Option<String> {
        let pending = self.pending_txs.lock().unwrap();
        pending.get(txid).map(|info| info.tx_hex.clone())
    }

    pub fn get_all_pending_txs(&self) -> HashMap<String, PendingTxInfo> {
        let pending = self.pending_txs.lock().unwrap();
        pending.clone()
    }

    pub fn get_pending_txids_for_scripthash(&self, scripthash: &str) -> Vec<String> {
        let pending = self.pending_txs.lock().unwrap();
        pending.iter()
            .filter(|(_, info)| info.scripthashes.contains(&scripthash.to_string()))
            .map(|(txid, _)| txid.clone())
            .collect()
    }

    pub fn get_pending_utxos_for_scripthash(&self, scripthash: &str) -> Vec<(String, u32, u64)> {
        let pending = self.pending_txs.lock().unwrap();
        let mut utxos = Vec::new();
        for (txid, info) in pending.iter() {
            for output in &info.outputs {
                if output.scripthash == scripthash {
                    utxos.push((txid.clone(), output.output_index, output.value));
                }
            }
        }
        utxos
    }

    pub fn get_pending_unconfirmed_value(&self, scripthash: &str) -> u64 {
        let pending = self.pending_txs.lock().unwrap();
        let mut total = 0u64;
        for (_, info) in pending.iter() {
            for output in &info.outputs {
                if output.scripthash == scripthash {
                    total += output.value;
                }
            }
        }
        total
    }

    pub fn remove_pending_tx(&self, txid: &str) {
        let mut pending = self.pending_txs.lock().unwrap();
        if pending.remove(txid).is_some() {
            tracing::info!("Removed pending tx {}", txid);
        }
    }

    pub fn get_indexer_url(&self) -> Option<String> {
        let config = self.config.lock().ok()?;
        config.indexer.as_ref().map(|i| i.url.clone())
    }
}

fn is_retriable_broadcast_error(msg: &str) -> bool {
    let m = msg.to_lowercase();
    m.contains("non-final")
        || m.contains("non final")
        || m.contains("not final")
        || m.contains("locktime")
        || m.contains("too-long-mempool-chain")
}
