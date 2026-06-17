use anyhow::Result;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{sleep, Instant};

use crate::config::Config;
use crate::pool::manager::PoolManager;

const BROADCAST_CHECK_INTERVAL: Duration = Duration::from_secs(15);

pub struct Scheduler {
    pool_manager: Arc<PoolManager>,
    config: Arc<Mutex<Config>>,
}

impl Scheduler {
    pub fn new(pool_manager: Arc<PoolManager>, config: Arc<Mutex<Config>>) -> Self {
        Self { pool_manager, config }
    }

    pub async fn run_broadcast_loop(&self) -> Result<()> {
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(60);

        tracing::info!(
            "Starting broadcast scheduler loop (interval: {}s)",
            BROADCAST_CHECK_INTERVAL.as_secs()
        );

        loop {
            let start = Instant::now();
            let pool_manager = self.pool_manager.clone();

            let tick = tokio::task::spawn_blocking(move || pool_manager.run_scheduler_tick()).await;

            match tick {
                Ok(Ok(results)) => {
                    backoff = Duration::from_secs(1);
                    for (id, result) in results {
                        match result {
                            Ok(txid) => tracing::info!("Broadcast {} -> txid: {}", id, txid),
                            Err(e) => tracing::debug!("Broadcast deferred for {}: {}", id, e),
                        }
                    }
                }
                Ok(Err(e)) => {
                    if e.to_string().contains("indexer unavailable") {
                        tracing::warn!("Indexer unavailable, backing off for {:?}", backoff);
                        sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);
                    } else {
                        tracing::error!("Error in broadcast loop: {}", e);
                    }
                }
                Err(e) => tracing::error!("Broadcast scheduler task failed: {}", e),
            }

            let elapsed = start.elapsed();
            if elapsed < BROADCAST_CHECK_INTERVAL {
                sleep(BROADCAST_CHECK_INTERVAL - elapsed).await;
            }
        }
    }

    pub async fn run_block_height_monitor(&self) -> Result<()> {
        let check_interval = Duration::from_secs(60);
        tracing::info!("Starting block height monitor loop (interval: 60s)");

        loop {
            let pool_manager = self.pool_manager.clone();
            let config = self.config.clone();

            let result = tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                let network = {
                    let config = config.lock().map_err(|e| anyhow::anyhow!("Config lock: {}", e))?;
                    config.network.network_type.data_dir_name().to_string()
                };

                if !pool_manager.indexer_healthy() {
                    return Ok(());
                }

                match pool_manager.check_block_height()? {
                    Some(current_height) => {
                        let pending_txs = pool_manager.get_pending_by_block_height(&network)?;
                        for tx in pending_txs {
                            if tx.broadcast_mode.as_deref() != Some("by_block") {
                                continue;
                            }
                            if let Some(nlocktime) = tx.nlocktime {
                                if nlocktime > 0
                                    && nlocktime < 500_000_000
                                    && current_height >= nlocktime
                                {
                                    tracing::info!(
                                        "Transaction {} now due (block height {} reached)",
                                        tx.id,
                                        current_height
                                    );
                                    if let Err(e) = pool_manager.mark_as_due(&tx.id) {
                                        tracing::error!("Failed to mark {} as due: {}", tx.id, e);
                                    }
                                }
                            }
                        }
                    }
                    None => tracing::debug!("Could not get block height"),
                }
                Ok(())
            })
            .await;

            if let Err(e) = result {
                tracing::error!("Block height monitor task failed: {}", e);
            } else if let Ok(Err(e)) = result {
                tracing::error!("Block height monitor error: {}", e);
            }

            sleep(check_interval).await;
        }
    }

    pub async fn run_rebroadcast_loop(&self) -> Result<()> {
        let interval = {
            let config = self.config.lock().map_err(|e| anyhow::anyhow!("Config lock: {}", e))?;
            Duration::from_secs(config.pool.rebroadcast_interval_minutes * 60)
        };
        tracing::info!(
            "Starting rebroadcast loop (interval: {}m)",
            interval.as_secs() / 60
        );

        loop {
            let pool_manager = self.pool_manager.clone();
            let tick = tokio::task::spawn_blocking(move || {
                if !pool_manager.indexer_healthy() {
                    return Ok(Vec::new());
                }
                pool_manager.rebroadcast_pending()
            })
            .await;

            match tick {
                Ok(Ok(results)) => {
                    for (id, result) in results {
                        match result {
                            Ok(txid) => tracing::debug!("Rebroadcast {} -> txid: {}", id, txid),
                            Err(e) => tracing::warn!("Rebroadcast failed for {}: {}", id, e),
                        }
                    }
                }
                Ok(Err(e)) => tracing::error!("Error in rebroadcast loop: {}", e),
                Err(e) => tracing::error!("Rebroadcast task failed: {}", e),
            }

            sleep(interval).await;
        }
    }

    pub async fn run_confirmation_checker(&self) -> Result<()> {
        let interval = Duration::from_secs(120);
        tracing::info!("Starting confirmation checker loop (interval: 120s)");

        loop {
            let pool_manager = self.pool_manager.clone();
            let tick = tokio::task::spawn_blocking(move || {
                if !pool_manager.indexer_healthy() {
                    return Ok(Vec::new());
                }
                pool_manager.check_confirmations()
            })
            .await;

            match tick {
                Ok(Ok(results)) => {
                    for (id, confirmed, height) in results {
                        if confirmed {
                            tracing::info!(
                                "Transaction {} confirmed at block {}",
                                id,
                                height.unwrap_or(0)
                            );
                        }
                    }
                }
                Ok(Err(e)) => tracing::error!("Error checking confirmations: {}", e),
                Err(e) => tracing::error!("Confirmation checker task failed: {}", e),
            }

            sleep(interval).await;
        }
    }

    pub async fn run_price_monitor(&self) -> Result<()> {
        let interval = Duration::from_secs(60);
        tracing::info!("Starting BTC/fiat price monitor loop (interval: 60s)");

        loop {
            let pool_manager = self.pool_manager.clone();
            let price_feed = pool_manager.price_feed().clone();

            let fetch_and_check = async move {
                let snapshot = price_feed.fetch_snapshot().await?;
                if snapshot.stale {
                    tracing::warn!(
                        "Price monitor using stale cache from {} — skipping trigger evaluation",
                        snapshot.source
                    );
                    return Ok(0usize);
                }
                tracing::debug!(
                    "BTC prices from {} (EUR={:?}, USD={:?})",
                    snapshot.source,
                    snapshot.prices.get("eur"),
                    snapshot.prices.get("usd")
                );
                let prices = snapshot.prices;
                let triggered = tokio::task::spawn_blocking(move || {
                    pool_manager.check_price_triggers(&prices)
                })
                .await??;
                Ok::<usize, anyhow::Error>(triggered)
            };

            match fetch_and_check.await {
                Ok(n) if n > 0 => tracing::info!("Price monitor marked {} tx(s) as due", n),
                Ok(_) => {}
                Err(e) => tracing::warn!("Price monitor tick failed: {}", e),
            }

            sleep(interval).await;
        }
    }

    pub async fn start_all_loops(&self) -> Result<()> {
        let pool_manager = self.pool_manager.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            let scheduler = Scheduler::new(pool_manager, config);
            if let Err(e) = scheduler.run_broadcast_loop().await {
                tracing::error!("Broadcast loop error: {}", e);
            }
        });

        let pool_manager = self.pool_manager.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            let scheduler = Scheduler::new(pool_manager, config);
            if let Err(e) = scheduler.run_block_height_monitor().await {
                tracing::error!("Block height monitor error: {}", e);
            }
        });

        let pool_manager = self.pool_manager.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            let scheduler = Scheduler::new(pool_manager, config);
            if let Err(e) = scheduler.run_rebroadcast_loop().await {
                tracing::error!("Rebroadcast loop error: {}", e);
            }
        });

        let pool_manager = self.pool_manager.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            let scheduler = Scheduler::new(pool_manager, config);
            if let Err(e) = scheduler.run_confirmation_checker().await {
                tracing::error!("Confirmation checker error: {}", e);
            }
        });

        let pool_manager = self.pool_manager.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            let scheduler = Scheduler::new(pool_manager, config);
            if let Err(e) = scheduler.run_price_monitor().await {
                tracing::error!("Price monitor error: {}", e);
            }
        });

        tracing::info!("All scheduler loops started");
        Ok(())
    }
}
