mod api;
mod config;
mod db;
mod discovery;
mod electrum_server;
mod migration;
mod pool;
mod price;
mod rpc;
mod utils;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use rand::Rng;
use std::path::PathBuf;
use std::sync::Arc;

use config::{Config, NetworkType};
use db::Database;
use migration::SparrowImporter;
use pool::{Broadcaster, PoolManager, Scheduler};
use rpc::{BitcoinRpc, ElectrumClient};

#[derive(Parser)]
#[command(name = "broadcast-pool")]
#[command(about = "Bitcoin Broadcast Pool - Privacy-preserving transaction scheduling")]
#[command(version)]
struct Cli {
    /// Path to config file
    #[arg(short, long, global = true)]
    config: Option<PathBuf>,

    /// Network to use (overrides config) [mainnet, testnet, testnet4, signet]
    #[arg(short, long, global = true)]
    network: Option<String>,

    /// Bitcoin Core RPC URL (overrides config)
    #[arg(long, global = true)]
    rpc_url: Option<String>,

    /// Bitcoin Core RPC user (overrides config)
    #[arg(long, global = true)]
    rpc_user: Option<String>,

    /// Bitcoin Core RPC password (overrides config)
    #[arg(long, global = true)]
    rpc_password: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the broadcast pool daemon
    Start {
        /// Run in foreground (no daemonize)
        #[arg(long)]
        foreground: bool,
    },

    /// Import a transaction into the broadcast pool
    Import {
        /// Raw transaction hex string
        #[arg(short, long)]
        tx: Option<String>,

        /// Path to JSON file with transaction(s)
        #[arg(short, long)]
        file: Option<PathBuf>,

        /// Network for this transaction (overrides global -n)
        #[arg(long)]
        network: Option<String>,

        /// Label for the source of funds
        #[arg(short, long)]
        label: Option<String>,

        /// Schedule broadcast at a specific time (RFC3339)
        #[arg(short, long)]
        schedule: Option<String>,
    },

    /// List transactions in the broadcast pool
    Pool {
        #[command(subcommand)]
        command: PoolCommands,
    },

    /// Manage migration plans
    Migrate {
        #[command(subcommand)]
        command: MigrateCommands,
    },

    /// Show system status
    Status {
        /// Show detailed status
        #[arg(short, long)]
        detailed: bool,
    },

    /// Test connection to Bitcoin Core
    TestRpc,

    /// Broadcast all pending transactions immediately
    BroadcastAll {
        /// Skip confirmation prompt
        #[arg(short, long)]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum PoolCommands {
    /// List all transactions in the pool
    List {
        /// Filter by status (pending, scheduled, broadcast, confirmed, failed)
        #[arg(short, long)]
        status: Option<String>,

        /// Maximum number of results
        #[arg(short, long, default_value = "50")]
        limit: i32,
    },

    /// Show details of a specific transaction
    Show {
        /// Transaction ID in the pool
        id: String,
    },

    /// Manually broadcast a transaction now
    Broadcast {
        /// Transaction ID in the pool
        id: String,
    },

    /// Schedule a pending transaction
    Schedule {
        /// Transaction ID in the pool
        id: String,

        /// Minimum delay in hours
        #[arg(short = 'a', long)]
        min_delay: Option<u64>,

        /// Maximum delay in hours
        #[arg(short = 'z', long)]
        max_delay: Option<u64>,

        /// Minimum fee rate (sat/vB)
        #[arg(long)]
        min_fee: Option<f64>,

        /// Maximum fee rate (sat/vB)
        #[arg(long)]
        max_fee: Option<f64>,
    },

    /// Remove a transaction from the pool
    Remove {
        /// Transaction ID in the pool
        id: String,

        /// Skip confirmation prompt
        #[arg(short, long)]
        yes: bool,
    },

    /// Schedule all pending transactions
    ScheduleAll {
        /// Minimum delay in hours
        #[arg(short = 'a', long)]
        min_delay: Option<u64>,

        /// Maximum delay in hours
        #[arg(short = 'z', long)]
        max_delay: Option<u64>,
    },

    /// Show pool statistics
    Stats,
}

#[derive(Subcommand)]
enum MigrateCommands {
    /// Create a new migration plan
    Create {
        /// Plan name
        #[arg(short, long)]
        name: String,

        /// Source wallet name/description
        #[arg(short, long)]
        source: Option<String>,

        /// Destination wallet name/description
        #[arg(short, long)]
        destination: Option<String>,
    },

    /// List migration plans
    List,

    /// Show details of a specific migration plan
    Show {
        /// Plan ID
        id: String,
    },

    /// Import UTXOs into a migration plan
    ImportUtxos {
        /// Plan ID
        #[arg(short, long)]
        plan: String,

        /// Path to Sparrow UTXO export JSON file
        #[arg(short, long)]
        file: PathBuf,
    },

    /// Generate schedule for all UTXOs in a plan
    Schedule {
        /// Plan ID
        #[arg(short, long)]
        plan: String,

        /// Minimum delay in hours
        #[arg(short = 'a', long)]
        min_delay: Option<u64>,

        /// Maximum delay in hours
        #[arg(short = 'z', long)]
        max_delay: Option<u64>,

        /// Minimum fee rate (sat/vB)
        #[arg(long)]
        min_fee: Option<f64>,

        /// Maximum fee rate (sat/vB)
        #[arg(long)]
        max_fee: Option<f64>,
    },

    /// Activate a migration plan
    Activate {
        /// Plan ID
        id: String,
    },

    /// Pause a migration plan
    Pause {
        /// Plan ID
        id: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Install rustls crypto provider once (avoids race condition in SSL connections)
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Install panic hook to log panics before termination
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown");
        let payload = if let Some(s) = info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "Box<dyn Any>".to_string()
        };
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "unknown".to_string());
        // #region agent log
        crate::utils::debug_log::agent_log(
            "H-PANIC",
            &location,
            "process panic",
            serde_json::json!({
                "thread": thread_name,
                "payload": payload,
            }),
        );
        // #endregion
        eprintln!(
            "PANIC on thread '{}': {}\nLocation: {}",
            thread_name, payload, location
        );
        tracing::error!(
            "PANIC on thread '{}': {}\nLocation: {}",
            thread_name,
            payload,
            location
        );
        default_hook(info);
    }));

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let mut config = Config::load(cli.config.as_deref())?;
    if let Some(network) = cli.network {
        config.network.network_type = match network.to_lowercase().as_str() {
            "mainnet" => NetworkType::Mainnet,
            "testnet" | "testnet3" | "testnet4" => NetworkType::Testnet4,
            "signet" => NetworkType::Signet,
            _ => config.network.network_type.clone(),
        };
    }
    if let Some(url) = cli.rpc_url {
        config.bitcoin_rpc = Some(config::BitcoinRpcConfig {
            url,
            user: config.bitcoin_rpc.as_ref().map(|r| r.user.clone()).unwrap_or_default(),
            password: config.bitcoin_rpc.as_ref().map(|r| r.password.clone()).unwrap_or_default(),
        });
    }
    if let Some(user) = cli.rpc_user {
        if let Some(ref mut rpc) = config.bitcoin_rpc {
            rpc.user = user;
        }
    }
    if let Some(password) = cli.rpc_password {
        if let Some(ref mut rpc) = config.bitcoin_rpc {
            rpc.password = password;
        }
    }

    let data_dir = get_data_dir(&config)?;
    std::fs::create_dir_all(&data_dir)?;

    let db_path = config.db_path(&data_dir);
    let db = Arc::new(Database::open(&db_path)?);

    // RPC is only created when needed (lazy init)
    let rpc_needed = matches!(
        &cli.command,
        Commands::Start { .. } | Commands::TestRpc | Commands::BroadcastAll { .. }
    );
    let rpc = if rpc_needed {
        if let Some(ref rpc_config) = config.bitcoin_rpc {
            match BitcoinRpc::new(rpc_config) {
                Ok(rpc) => Some(Arc::new(rpc)),
                Err(e) => {
                    tracing::warn!("Could not create RPC client: {}", e);
                    None
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    // Auto-detect network (Bitcoin RPC), indexer (50001/50002), and LAN IP for wallet URL.
    discovery::apply_network_from_rpc(&mut config, rpc.as_deref());
    let indexer_before = config.indexer.clone();
    let indexer_found = if discovery::is_umbrel_mode() {
        discovery::heal_umbrel_indexer_config(&mut config);
        config.indexer.is_some()
    } else {
        discovery::apply_indexer_discovery(&mut config)
    };
    discovery::apply_lan_ip(&mut config);
    let config_changed = indexer_before != config.indexer;
    if indexer_found || (discovery::is_umbrel_mode() && config_changed) {
        if let Err(e) = discovery::save_config_to_disk(&config) {
            tracing::warn!("Could not persist indexer config: {}", e);
        }
    }

    if let Some(ref url) = config.indexer.as_ref().map(|i| i.url.as_str()) {
        tracing::info!("Indexer: {}", url);
    }
    if let Some(ref ip) = config.electrum_server.lan_connect_host {
        tracing::info!(
            "Wallet Electrum URL: {}:{} (Liana: {:?})",
            ip,
            config.electrum_server.port,
            config.electrum_server.liana_port
        );
    }

    // Create Indexer client if configured
    let indexer = if let Some(ref indexer_config) = config.indexer {
        match ElectrumClient::new(&indexer_config.url) {
            Ok(client) => Some(Arc::new(client)),
            Err(e) => {
                tracing::warn!("Could not create Indexer client: {}", e);
                None
            }
        }
    } else {
        None
    };

    let shared_config = Arc::new(std::sync::Mutex::new(config.clone()));

    let pool_manager = Arc::new(PoolManager::new(db.clone(), rpc.clone(), indexer.clone(), shared_config.clone()));

    match pool_manager.load_pending_from_db() {
        Ok(count) => {
            if count > 0 {
                tracing::info!("Loaded {} txs into virtual mempool", count);
            }
        }
        Err(e) => tracing::warn!("Failed to rehydrate virtual mempool: {}", e),
    }

    match pool_manager.requeue_retriable_failures() {
        Ok(count) if count > 0 => {
            tracing::info!("Requeued {} failed transaction(s) for retry", count);
        }
        Err(e) => tracing::warn!("Failed to requeue retriable failures on startup: {}", e),
        _ => {}
    }

    match cli.command {
        Commands::Start { foreground: _ } => {
            tracing::info!("Starting broadcast-pool daemon (network: {:?})", config.network.network_type);

            let broadcaster = Broadcaster::new(rpc.clone(), indexer.clone(), db.clone(), config.clone());
            let broadcaster_for_test = broadcaster.clone();
            tokio::spawn(async move {
                match broadcaster_for_test.test_connection() {
                    Ok(true) => tracing::info!("Connected to Indexer server"),
                    Ok(false) => tracing::warn!("Warning: Could not connect to any backend"),
                    Err(e) => tracing::warn!("Warning: Backend connection failed: {}", e),
                }
            });

            let scheduler = Scheduler::new(pool_manager.clone(), shared_config.clone());

            let electrum_server = electrum_server::ElectrumServer::new(
                pool_manager.clone(),
                shared_config.clone(),
            );

            let app_state = api::AppState {
                pool_manager: pool_manager.clone(),
                db,
                config: shared_config.clone(),
            };
            let app = api::router(app_state);
            let bind_addr = format!("{}:{}", config.web.host, config.web.port);
            tracing::info!("Web dashboard at http://{}", bind_addr);

            let electrum_addr = format!(
                "{}:{}",
                config.electrum_server.host, config.electrum_server.port
            );
            tracing::info!("Electrum server (Sparrow) at {}", electrum_addr);
            if let Some(lp) = config.electrum_server.liana_port {
                tracing::info!("Electrum server (Liana) at {}:{}", config.electrum_server.host, lp);
            }

            let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
            tracing::info!("Server listening on {}", bind_addr);

            if discovery::is_umbrel_mode() && config.indexer.is_none() {
                let pm = pool_manager.clone();
                let cfg = shared_config.clone();
                tokio::spawn(async move {
                    tracing::info!(
                        "Umbrel electrs not found at startup — retrying discovery every 15s"
                    );
                    for attempt in 1..=40 {
                        tokio::time::sleep(std::time::Duration::from_secs(15)).await;
                        if pm.get_indexer().is_some() {
                            break;
                        }
                        let reconnect = tokio::task::spawn_blocking({
                            let pm = pm.clone();
                            let cfg = cfg.clone();
                            move || -> bool {
                                let mut c = match cfg.lock() {
                                    Ok(c) => c,
                                    Err(_) => return false,
                                };
                                if c.indexer.is_some() {
                                    return false;
                                }
                                if !discovery::discover_umbrel_if_needed(&mut c, true) {
                                    return false;
                                }
                                if c.indexer.is_none() {
                                    return false;
                                }
                                let _ = discovery::save_config_to_disk(&c);
                                drop(c);
                                match pm.reconnect_indexer_from_config() {
                                    Ok(()) => {
                                        tracing::info!(
                                            "Umbrel electrs connected after background retry #{}",
                                            attempt
                                        );
                                        if let Ok(c) = cfg.lock() {
                                            if let Some(ref idx) = c.indexer {
                                                let url = idx.url.clone();
                                                let pm2 = pm.clone();
                                                std::thread::spawn(move || {
                                                    electrum_server::warm_chain_tip_cache(&url, &pm2);
                                                });
                                            }
                                        }
                                        true
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            "Background electrs reconnect failed (attempt {}): {}",
                                            attempt,
                                            e
                                        );
                                        false
                                    }
                                }
                            }
                        })
                        .await
                        .unwrap_or(false);
                        if reconnect {
                            break;
                        }
                    }
                });
            }

            tokio::spawn(async move {
                if let Err(e) = electrum_server.start().await {
                    tracing::error!("Electrum server error: {}", e);
                }
            });

            tokio::spawn(async move {
                if let Err(e) = scheduler.start_all_loops().await {
                    tracing::error!("Scheduler error: {}", e);
                }
            });

            axum::serve(listener, app).await?;
        }

        Commands::Import {
            tx,
            file,
            network,
            label,
            schedule,
        } => {
            let network_str = network
                .as_deref()
                .unwrap_or(config.network.network_type.data_dir_name());

            let scheduled_time = schedule
                .map(|s| chrono::DateTime::parse_from_rfc3339(&s))
                .transpose()
                .context("Invalid schedule time format")?
                .map(|dt| dt.with_timezone(&chrono::Utc));

            let mut new_txs = Vec::new();

            if let Some(tx_hex) = tx {
                let new_tx = SparrowImporter::import_raw_tx_hex(
                    &tx_hex,
                    network_str,
                    label.as_deref(),
                )?;
                new_txs.push(new_tx);
            } else if let Some(file_path) = file {
                new_txs = SparrowImporter::import_signed_tx_from_json(&file_path)?;
                for tx in &mut new_txs {
                    tx.network = network_str.to_string();
                    if let Some(ref l) = label {
                        tx.source_label = Some(l.clone());
                    }
                }
            } else {
                anyhow::bail!("Either --tx or --file must be provided");
            }

            for mut new_tx in new_txs {
                new_tx.scheduled_time = scheduled_time;
                let tx = pool_manager.import_transaction(&new_tx)?;
                println!("Imported transaction: {}", tx.id);

                if scheduled_time.is_some() {
                    let scheduled = pool_manager.schedule_transaction(
                        &tx.id,
                        None,
                        None,
                        None,
                        None,
                        None,
                    )?;
                    println!(
                        "Scheduled for: {}",
                        scheduled
                            .scheduled_time
                            .map(|t| t.to_rfc3339())
                            .unwrap_or_default()
                    );
                }
            }
        }

        Commands::Pool { command } => match command {
            PoolCommands::List { status, limit } => {
                let txs = pool_manager.list_transactions(status.as_deref(), limit)?;
                if txs.is_empty() {
                    println!("No transactions in the pool.");
                } else {
                    println!("{:<40} {:<12} {:<20} {:<10}", "ID", "STATUS", "SCHEDULED", "FEE");
                    println!("{}", "-".repeat(82));
                    for tx in &txs {
                        println!(
                            "{:<40} {:<12} {:<20} {:<10}",
                            &tx.id[..8.min(tx.id.len())],
                            tx.status.as_str(),
                            tx.scheduled_time
                                .map(|t| t.format("%Y-%m-%d %H:%M").to_string())
                                .unwrap_or_else(|| "immediate".to_string()),
                            tx.target_fee_rate
                                .map(|f| format!("{:.2}", f))
                                .unwrap_or_else(|| "-".to_string())
                        );
                    }
                    println!("\nTotal: {} transactions", txs.len());
                }
            }

            PoolCommands::Show { id } => {
                match pool_manager.get_transaction(&id) {
                    Ok(tx) => {
                        println!("Transaction Details:");
                        println!("  ID:              {}", tx.id);
                        println!("  Status:          {}", tx.status.as_str());
                        println!("  Network:         {}", tx.network);
                        println!("  TXID:            {}", tx.txid.unwrap_or_else(|| "-".to_string()));
                        println!("  Scheduled:       {}", tx.scheduled_time.map(|t| t.to_rfc3339()).unwrap_or_else(|| "-".to_string()));
                        println!("  Broadcast at:    {}", tx.broadcast_at.map(|t| t.to_rfc3339()).unwrap_or_else(|| "-".to_string()));
                        println!("  Confirmed at:    {}", tx.confirmed_at.map(|t| t.to_rfc3339()).unwrap_or_else(|| "-".to_string()));
                        println!("  Block height:    {}", tx.block_height.map(|h| h.to_string()).unwrap_or_else(|| "-".to_string()));
                        println!("  Target fee:      {}", tx.target_fee_rate.map(|f| format!("{:.2} sat/vB", f)).unwrap_or_else(|| "-".to_string()));
                        println!("  Actual fee:      {}", tx.actual_fee_rate.map(|f| format!("{:.2} sat/vB", f)).unwrap_or_else(|| "-".to_string()));
                        println!("  Source:          {}", tx.source_label.unwrap_or_else(|| "-".to_string()));
                        println!("  UTXO count:      {}", tx.utxo_count);
                        println!("  Total value:     {:.8} BTC", tx.total_value_btc);
                        println!("  TX hex (first 80 chars): {}...", &tx.tx_hex[..80.min(tx.tx_hex.len())]);
                        if let Some(err) = &tx.error_message {
                            println!("  Error:           {}", err);
                        }
                    }
                    Err(e) => {
                        eprintln!("Transaction not found: {}", e);
                        std::process::exit(1);
                    }
                }
            }

            PoolCommands::Broadcast { id } => {
                let broadcaster = Broadcaster::new(rpc.clone(), indexer.clone(), db.clone(), config.clone());
                match broadcaster.broadcast_single(&id) {
                    Ok(txid) => {
                        println!("Successfully broadcast transaction!");
                        println!("  Pool ID: {}", id);
                        println!("  TXID:    {}", txid);
                    }
                    Err(e) => {
                        eprintln!("Failed to broadcast: {}", e);
                        std::process::exit(1);
                    }
                }
            }

            PoolCommands::Schedule {
                id,
                min_delay,
                max_delay,
                min_fee,
                max_fee,
            } => {
                match pool_manager.schedule_transaction(&id, min_delay, max_delay, min_fee, max_fee, None) {
                    Ok(tx) => {
                        println!("Transaction scheduled:");
                        println!("  ID:         {}", tx.id);
                        println!("  Scheduled:  {}", tx.scheduled_time.map(|t| t.to_rfc3339()).unwrap_or_default());
                        println!("  Fee rate:   {} sat/vB", tx.target_fee_rate.map(|f| format!("{:.2}", f)).unwrap_or_default());
                    }
                    Err(e) => {
                        eprintln!("Failed to schedule: {}", e);
                        std::process::exit(1);
                    }
                }
            }

            PoolCommands::Remove { id, yes } => {
                if !yes {
                    println!("Remove transaction {} from the pool? [y/N]", id);
                    let mut input = String::new();
                    std::io::stdin().read_line(&mut input)?;
                    if !input.trim().eq_ignore_ascii_case("y") {
                        println!("Aborted.");
                        return Ok(());
                    }
                }
                pool_manager.remove_transaction(&id)?;
                println!("Transaction {} removed from the pool.", id);
            }

            PoolCommands::ScheduleAll { min_delay: _, max_delay: _ } => {
                let scheduled = pool_manager.schedule_all_pending(
                    config.network.network_type.data_dir_name(),
                )?;
                println!("Scheduled {} pending transactions.", scheduled.len());
                for tx in &scheduled {
                    println!(
                        "  {} -> {} (fee: {} sat/vB)",
                        &tx.id[..8.min(tx.id.len())],
                        tx.scheduled_time.map(|t| t.format("%Y-%m-%d %H:%M").to_string()).unwrap_or_default(),
                        tx.target_fee_rate.map(|f| format!("{:.2}", f)).unwrap_or_default()
                    );
                }
            }

            PoolCommands::Stats => {
                let stats = pool_manager.get_stats()?;
                println!("Broadcast Pool Statistics:");
                println!("  Total transactions: {}", stats.total_transactions);
                println!("  Pending:            {}", stats.pending);
                println!("  Scheduled:          {}", stats.scheduled);
                println!("  Broadcast:          {}", stats.broadcast);
                println!("  Confirmed:          {}", stats.confirmed);
                println!("  Failed:             {}", stats.failed);
                println!("  Total value:        {:.8} BTC", stats.total_value_btc);
            }
        },

        Commands::Migrate { command } => match command {
            MigrateCommands::Create { name, source, destination } => {
                let new_plan = db::models::NewMigrationPlan {
                    name,
                    source_wallet: source,
                    destination_wallet: destination,
                    network: config.network.network_type.data_dir_name().to_string(),
                    min_delay_hours: None,
                    max_delay_hours: None,
                    min_fee_rate: None,
                    max_fee_rate: None,
                };
                let plan = db.insert_migration_plan(&new_plan)?;
                println!("Created migration plan:");
                println!("  ID:   {}", plan.id);
                println!("  Name: {}", plan.name);
                println!("  Network: {}", plan.network);
            }

            MigrateCommands::List => {
                let plans = db.list_migration_plans(config.network.network_type.data_dir_name())?;
                if plans.is_empty() {
                    println!("No migration plans found.");
                } else {
                    println!("{:<40} {:<20} {:<12} {:<10}", "ID", "NAME", "STATUS", "UTXOS");
                    println!("{}", "-".repeat(82));
                    for plan in &plans {
                        println!(
                            "{:<40} {:<20} {:<12} {:<10}",
                            &plan.id[..8.min(plan.id.len())],
                            &plan.name[..20.min(plan.name.len())],
                            plan.status.as_str(),
                            plan.total_transactions
                        );
                    }
                }
            }

            MigrateCommands::Show { id } => {
                match db.get_migration_plan_by_id(&id) {
                    Ok(plan) => {
                        let utxos = db.list_migration_utxos(&id)?;
                        println!("Migration Plan Details:");
                        println!("  ID:              {}", plan.id);
                        println!("  Name:            {}", plan.name);
                        println!("  Status:          {}", plan.status.as_str());
                        println!("  Network:         {}", plan.network);
                        println!("  Source:          {}", plan.source_wallet.unwrap_or_else(|| "-".to_string()));
                        println!("  Destination:     {}", plan.destination_wallet.unwrap_or_else(|| "-".to_string()));
                        println!("  Min delay:       {}h", plan.min_delay_hours);
                        println!("  Max delay:       {}h", plan.max_delay_hours);
                        println!("  Min fee:         {} sat/vB", plan.min_fee_rate);
                        println!("  Max fee:         {} sat/vB", plan.max_fee_rate);
                        println!("  Total TXs:       {}", plan.total_transactions);
                        println!("  Completed:       {}", plan.completed_transactions);
                        println!("  Total value:     {:.8} BTC", plan.total_value_migrated_btc);
                        println!("\nUTXOs ({}):", utxos.len());
                        for utxo in &utxos {
                            println!(
                                "  {}:{} ({:.8} BTC) - {}",
                                &utxo.txid[..8.min(utxo.txid.len())],
                                utxo.vout,
                                utxo.value_btc,
                                utxo.label.as_deref().unwrap_or("-")
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("Plan not found: {}", e);
                        std::process::exit(1);
                    }
                }
            }

            MigrateCommands::ImportUtxos { plan, file } => {
                let _plan = db.get_migration_plan_by_id(&plan)?;
                let utxos = SparrowImporter::import_utxos_from_json(&file)?;

                let mut imported = 0;
                for utxo in &utxos {
                    db.insert_migration_utxo(&plan, utxo)?;
                    imported += 1;
                }

                // Update plan total
                let plan_data = db.get_migration_plan_by_id(&plan)?;
                let total = plan_data.total_transactions + imported;
                db.update_plan_total(&plan, total)?;

                println!("Imported {} UTXOs into plan {}", imported, plan);
            }

            MigrateCommands::Schedule {
                plan,
                min_delay,
                max_delay,
                min_fee,
                max_fee,
            } => {
                let _plan = db.get_migration_plan_by_id(&plan)?;
                let utxos = db.list_migration_utxos(&plan)?;

                let min_d = min_delay.unwrap_or(_plan.min_delay_hours as u64);
                let max_d = max_delay.unwrap_or(_plan.max_delay_hours as u64);
                let min_f = min_fee.unwrap_or(_plan.min_fee_rate);
                let max_f = max_fee.unwrap_or(_plan.max_fee_rate);

                println!("Scheduling {} UTXOs for plan '{}'...", utxos.len(), _plan.name);

                let mut rng = rand::thread_rng();
                for utxo in &utxos {
                    let delay_hours: u64 = rng.gen_range(min_d..=max_d.max(min_d));
                    let fee_rate: f64 = rng.gen_range(min_f..=max_f.max(min_f));

                    let scheduled_time = chrono::Utc::now()
                        .checked_add_signed(chrono::Duration::hours(delay_hours as i64));

                    let new_tx = db::models::NewBroadcastTx {
                        tx_hex: String::new(), // Will be filled when user imports signed TX
                        network: _plan.network.clone(),
                        nlocktime: None,
                        broadcast_mode: None,
                        scheduled_time,
                        target_fee_rate: Some(fee_rate),
                        source_label: utxo.source_label.clone(),
                        destination_address: utxo.address.clone(),
                        utxo_count: Some(1),
                        total_value_btc: Some(utxo.value_btc),
                        replacement_of: None,
                    };

                    let tx = pool_manager.import_transaction(&new_tx)?;
                    db.link_utxo_to_pool(&utxo.id, &tx.id)?;

                    println!(
                        "  UTXO {}:{} -> Pool {} (fee: {:.2} sat/vB, delay: {}h)",
                        &utxo.txid[..8.min(utxo.txid.len())],
                        utxo.vout,
                        &tx.id[..8.min(tx.id.len())],
                        fee_rate,
                        delay_hours
                    );
                }

                println!("\nScheduled {} transactions. Import signed TX hex to complete.", utxos.len());
            }

            MigrateCommands::Activate { id } => {
                db.update_plan_status(&id, db::models::PlanStatus::Active)?;
                println!("Plan {} activated.", id);
            }

            MigrateCommands::Pause { id } => {
                db.update_plan_status(&id, db::models::PlanStatus::Paused)?;
                println!("Plan {} paused.", id);
            }
        },

        Commands::Status { detailed } => {
            println!("Broadcast Pool Status:");
            println!("  Network:    {:?}", config.network.network_type);

            match rpc.as_ref().map(|r| r.as_ref()).context("RPC not connected") {
                Ok(rpc_ref) => match rpc_ref.test_connection() {
                    Ok(true) => {
                        println!("  RPC:        Connected");
                        if detailed {
                            let node_status = rpc_ref.get_node_status()?;
                            println!("  Block height: {}", node_status.blockchain_height.unwrap_or(0));
                            println!("  Mempool:    {} txs", node_status.mempool_size.unwrap_or(0));
                            println!("  Sync:       {:.1}%", node_status.sync_percentage.unwrap_or(0.0));
                        }
                    }
                    _ => {
                        println!("  RPC:        Disconnected");
                    }
                },
                Err(_) => {
                    println!("  RPC:        Not configured");
                }
            }

            let stats = pool_manager.get_stats()?;
            println!("\nPool Statistics:");
            println!("  Total:      {}", stats.total_transactions);
            println!("  Pending:    {}", stats.pending);
            println!("  Scheduled:  {}", stats.scheduled);
            println!("  Broadcast:  {}", stats.broadcast);
            println!("  Confirmed:  {}", stats.confirmed);
            println!("  Failed:     {}", stats.failed);
            println!("  Value:      {:.8} BTC", stats.total_value_btc);
        }

        Commands::TestRpc => {
            // Try Indexer first, then RPC
            if let Some(ref indexer) = indexer {
                println!("Testing Indexer connection to {}", config.indexer.as_ref().map(|e| e.url.as_str()).unwrap_or("unknown"));
                match indexer.test_connection() {
                    Ok(true) => {
                        println!("Indexer connection successful!");
                        if let Ok(height) = indexer.get_height() {
                            println!("  Block height: {}", height);
                        }
                    }
                    Ok(false) => {
                        println!("Connection failed.");
                        std::process::exit(1);
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        std::process::exit(1);
                    }
                }
            } else if let Some(ref rpc_client) = rpc {
                let rpc_url = config.bitcoin_rpc.as_ref().map(|r| r.url.as_str()).unwrap_or("unknown");
                println!("Testing RPC connection to {}", rpc_url);
                match rpc_client.test_connection() {
                    Ok(true) => {
                        println!("Connection successful!");
                        let status = rpc_client.get_node_status()?;
                        println!("  Network:      {}", status.network);
                        println!("  Block height: {}", status.blockchain_height.unwrap_or(0));
                        println!("  Mempool:      {} txs", status.mempool_size.unwrap_or(0));
                    }
                    Ok(false) => {
                        println!("Connection failed.");
                        std::process::exit(1);
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        std::process::exit(1);
                    }
                }
            } else {
                eprintln!("No backend configured. Configure indexer or bitcoin_rpc in config.toml");
                std::process::exit(1);
            }
        }

        Commands::BroadcastAll { yes } => {
            if !yes {
                println!("Broadcast ALL pending transactions immediately? [y/N]");
                let mut input = String::new();
                std::io::stdin().read_line(&mut input)?;
                if !input.trim().eq_ignore_ascii_case("y") {
                    println!("Aborted.");
                    return Ok(());
                }
            }

            let broadcaster = Broadcaster::new(rpc, indexer, db.clone(), config.clone());
            let results = broadcaster.broadcast_all_pending(
                config.network.network_type.data_dir_name(),
            )?;
            println!("Broadcast {} transactions.", results.len());
            for (id, result) in results {
                match result {
                    Ok(txid) => println!("  {} -> {}", &id[..8.min(id.len())], txid),
                    Err(e) => eprintln!("  {} -> ERROR: {}", &id[..8.min(id.len())], e),
                }
            }
        }
    }

    Ok(())
}

fn get_data_dir(config: &Config) -> Result<PathBuf> {
    if let Ok(dir) = std::env::var("BROADCAST_POOL_DATA_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let base = dirs::data_dir()
        .or_else(dirs::home_dir)
        .unwrap_or_else(|| PathBuf::from("."));
    Ok(base.join("broadcast-pool").join(config.network.network_type.data_dir_name()))
}
