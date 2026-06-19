use axum::{
    extract::State,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{Html, IntoResponse, Json},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::config::Config;
use crate::db::models::*;
use crate::db::Database;
use crate::pool::manager::PoolManager;

#[derive(Clone)]
pub struct AppState {
    pub pool_manager: Arc<PoolManager>,
    pub db: Arc<Database>,
    pub config: Arc<std::sync::Mutex<Config>>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(dashboard))
        .route("/api/stats", get(get_stats))
        .route("/api/mempool-status", get(get_mempool_status))
        .route("/api/transactions", get(list_transactions))
        .route("/api/transactions/import", post(import_transaction))
        .route("/api/transactions/{id}/schedule", post(schedule_transaction))
        .route("/api/transactions/{id}", get(get_transaction))
        .route("/api/transactions/{id}/remove", post(remove_transaction))
        .route("/api/transactions/{id}/retry", post(retry_transaction))
        .route("/api/status", get(get_status))
        .route("/api/config", get(get_config))
        .route("/api/config", post(save_config))
        .route("/api/restart", post(restart_daemon))
        .route("/api/estimate-fee", post(estimate_fee))
        .route("/api/test-indexer", post(test_indexer))
        .route("/api/discover-indexer", post(discover_indexer))
        .route("/api/indexer-debug", get(get_indexer_debug))
        .route("/api/btc-price", get(get_btc_price))
        .with_state(state)
}

async fn dashboard() -> impl IntoResponse {
    let html = load_dashboard_html();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, no-cache, must-revalidate"),
    );
    (headers, Html(html))
}

fn load_dashboard_html() -> String {
    const EMBEDDED: &str = include_str!("dashboard.html");
    /// Path to dashboard.html in the source tree at compile time. When the repo
    /// is still present (typical local dev), serve this file so HTML edits apply
    /// without rebuilding the binary.
    const SOURCE_TREE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/api/dashboard.html");

    if let Ok(path) = std::env::var("BROADCAST_POOL_DASHBOARD") {
        match std::fs::read_to_string(&path) {
            Ok(content) => {
                tracing::debug!("Serving dashboard from {}", path);
                return content;
            }
            Err(e) => {
                tracing::warn!(
                    "BROADCAST_POOL_DASHBOARD={} unreadable ({}), using embedded HTML",
                    path,
                    e
                );
            }
        }
    } else if let Ok(content) = std::fs::read_to_string(SOURCE_TREE) {
        tracing::debug!("Serving dashboard from {} (source tree)", SOURCE_TREE);
        return content;
    }
    EMBEDDED.to_string()
}

fn supported_networks_vec() -> Vec<String> {
    crate::config::NetworkType::supported_networks()
        .iter()
        .map(|s| s.to_string())
        .collect()
}

/// Best-effort LAN IP for wallet Electrum connections (when server binds 0.0.0.0).
fn wallet_connect_url(config: &Config) -> String {
    crate::discovery::wallet_connect_url(config, config.electrum_server.port)
}

fn config_to_response(config: &Config, network_changed: bool) -> ConfigResponse {
    let umbrel = crate::discovery::is_umbrel_mode();
    let pinned = std::env::var("BROADCAST_POOL_INDEXER_URL").is_ok();
    let indexer = config.indexer.as_ref();
    let active_url = indexer.map(|i| i.url.as_str()).unwrap_or("");
    let is_manual = indexer.map(|i| i.manual_override).unwrap_or(false)
        && crate::discovery::extract_indexer_host(active_url)
            .is_none_or(|h| !crate::discovery::is_mistaken_umbrel_lan_override(&h));
    let node_display = if active_url.is_empty()
        || crate::discovery::extract_indexer_host(active_url)
            .is_some_and(|h| crate::discovery::is_mistaken_umbrel_lan_override(&h))
    {
        String::new()
    } else {
        crate::discovery::display_indexer_url(active_url)
    };

    ConfigResponse {
        indexer_url: if is_manual {
            node_display.clone()
        } else {
            String::new()
        },
        indexer_node_url: node_display,
        indexer_use_ssl: crate::discovery::indexer_url_uses_ssl(active_url),
        indexer_is_manual: is_manual,
        network_editable: !umbrel,
        umbrel_mode: umbrel,
        network: config.network.network_type.data_dir_name().to_string(),
        broadcast_mode: config.schedule.broadcast_mode.to_string(),
        default_delay_hours: config.schedule.default_delay_hours,
        scheduled_datetime: config.schedule.scheduled_datetime.clone(),
        min_delay_hours: config.schedule.min_delay_hours,
        max_delay_hours: config.schedule.max_delay_hours,
        min_fee_rate: config.schedule.min_fee_rate,
        max_fee_rate: config.schedule.max_fee_rate,
        web_port: config.web.port,
        electrum_port: config.electrum_server.port,
        electrum_host: config.electrum_server.host.clone(),
        wallet_connect_url: wallet_connect_url(config),
        indexer_auto_detected: !pinned && !is_manual,
        network_changed,
        supported_networks: supported_networks_vec(),
    }
}

fn live_test_indexer_url(url: &str, network: &crate::config::NetworkType) -> bool {
    crate::discovery::resolve_working_indexer_url(url, network).is_some()
}

async fn get_stats(State(state): State<AppState>) -> Result<Json<PoolStats>, (StatusCode, String)> {
    state
        .pool_manager
        .get_stats()
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn get_mempool_status(
    State(state): State<AppState>,
) -> Json<MempoolStatus> {
    let pool_manager = state.pool_manager.clone();
    let status = tokio::task::spawn_blocking(move || pool_manager.get_mempool_status())
        .await
        .unwrap_or(MempoolStatus {
            available: false,
            mempool_tx_count: None,
            fee_rate_sat_vb: None,
            congestion: None,
        });
    Json(status)
}

#[derive(Serialize)]
struct BtcPriceResponse {
    prices: std::collections::HashMap<String, f64>,
    provider: String,
    source: String,
    stale: bool,
    fetched_at: String,
}

async fn get_btc_price(
    State(state): State<AppState>,
) -> Result<Json<BtcPriceResponse>, (StatusCode, String)> {
    let feed = state.pool_manager.price_feed().clone();
    let snapshot = feed
        .fetch_snapshot()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    Ok(Json(BtcPriceResponse {
        prices: snapshot.prices,
        provider: feed.provider_name().to_string(),
        source: snapshot.source,
        stale: snapshot.stale,
        fetched_at: snapshot.fetched_at.to_rfc3339(),
    }))
}

async fn list_transactions(
    State(state): State<AppState>,
) -> Result<Json<Vec<BroadcastTx>>, (StatusCode, String)> {
    // Warm price cache so table rows with price triggers show current BTC/fiat.
    let feed = state.pool_manager.price_feed().clone();
    if let Err(e) = feed.fetch_snapshot().await {
        tracing::debug!("Could not prefetch BTC prices for list: {}", e);
    }

    let pool_manager = state.pool_manager.clone();
    let started = std::time::Instant::now();
    // #region agent log
    crate::utils::debug_log::agent_log(
        "H4",
        "api/mod.rs:list_transactions",
        "handler start",
        serde_json::json!({ "blocking_on_async": true }),
    );
    // #endregion
    let result = tokio::task::spawn_blocking(move || pool_manager.list_transactions(None, 100))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Task failed: {}", e)))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
    // #region agent log
    crate::utils::debug_log::agent_log(
        "H4",
        "api/mod.rs:list_transactions",
        "handler end",
        serde_json::json!({
            "elapsed_ms": started.elapsed().as_millis(),
            "ok": result.is_ok(),
        }),
    );
    // #endregion
    result.map(Json)
}

#[derive(Deserialize)]
struct ImportRequest {
    tx_hex: String,
    label: Option<String>,
    target_fee_rate: Option<f64>,
    network: Option<String>,
}

#[derive(Deserialize)]
struct ScheduleRequest {
    scheduled_time: Option<String>,
    min_delay_hours: Option<u64>,
    max_delay_hours: Option<u64>,
    min_fee_rate: Option<f64>,
    max_fee_rate: Option<f64>,
    fixed_fee_rate: Option<f64>,
    target_price: Option<f64>,
    price_currency: Option<String>,
    price_condition: Option<String>,
}

async fn import_transaction(
    State(state): State<AppState>,
    Json(req): Json<ImportRequest>,
) -> Result<(StatusCode, Json<BroadcastTx>), (StatusCode, String)> {
    let network = state.config.lock()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .network.network_type.data_dir_name()
        .to_string();

    let new_tx = NewBroadcastTx {
        tx_hex: req.tx_hex,
        network: req.network.unwrap_or(network),
        nlocktime: None,
        broadcast_mode: None,
        scheduled_time: None,
        target_fee_rate: req.target_fee_rate,
        source_label: req.label,
        destination_address: None,
        utxo_count: Some(1),
        total_value_btc: None,
        replacement_of: None,
    };

    state
        .pool_manager
        .import_transaction(&new_tx)
        .map(|tx| (StatusCode::CREATED, Json(tx)))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn schedule_transaction(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(req): Json<ScheduleRequest>,
) -> Result<Json<BroadcastTx>, (StatusCode, String)> {
    if let Some(target_price) = req.target_price {
        let currency = req.price_currency.as_deref().unwrap_or("usd");
        let condition = req.price_condition.as_deref().unwrap_or("above");
        let fee_rate = req.fixed_fee_rate.unwrap_or(5.0);
        return state
            .pool_manager
            .schedule_by_price(&id, target_price, currency, condition, fee_rate)
            .map(Json)
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("must be") || msg.contains("only available") || msg.contains("Cannot set") {
                    (StatusCode::BAD_REQUEST, msg)
                } else {
                    (StatusCode::INTERNAL_SERVER_ERROR, msg)
                }
            });
    }

    // If exact datetime provided, use it directly
    if let Some(ref time_str) = req.scheduled_time {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(time_str) {
            let scheduled = dt.with_timezone(&chrono::Utc);
            let fee_rate = req.fixed_fee_rate.unwrap_or(5.0);
            return state
                .pool_manager
                .schedule_at(&id, scheduled, fee_rate)
                .map(Json)
                .map_err(|e| {
                    let msg = e.to_string();
                    if msg.contains("must be in the future")
                        || msg.contains("cannot be before nLockTime")
                    {
                        (StatusCode::BAD_REQUEST, msg)
                    } else {
                        (StatusCode::INTERNAL_SERVER_ERROR, msg)
                    }
                });
        }
    }

    state
        .pool_manager
        .schedule_transaction(
            &id,
            req.min_delay_hours,
            req.max_delay_hours,
            req.min_fee_rate,
            req.max_fee_rate,
            req.fixed_fee_rate,
        )
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn get_transaction(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Json<BroadcastTx>, (StatusCode, String)> {
    let pool_manager = state.pool_manager.clone();
    tokio::task::spawn_blocking(move || pool_manager.get_transaction(&id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Task failed: {}", e)))?
        .map(Json)
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))
}

async fn remove_transaction(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let pool_manager = state.pool_manager.clone();
    tokio::task::spawn_blocking(move || pool_manager.remove_transaction(&id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Task failed: {}", e)))?
        .map(|_| StatusCode::OK)
        .map_err(|e| {
            let msg = e.to_string();
            if msg.contains("not found") {
                (StatusCode::NOT_FOUND, msg)
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, msg)
            }
        })
}

async fn retry_transaction(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Json<BroadcastTx>, (StatusCode, String)> {
    state
        .pool_manager
        .retry_failed_transaction(&id)
        .map(Json)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))
}

#[derive(serde::Serialize)]
struct StatusResponse {
    network: String,
    network_display: String,
    supported_networks: Vec<String>,
    rpc_connected: bool,
    electrum_connected: bool,
    indexer_height: Option<u64>,
    chain_mtp: Option<u64>,
    pool_stats: PoolStats,
    retain_by_default: bool,
    #[serde(alias = "sparrow_connect_url")]
    wallet_connect_url: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    indexer_status_hint: String,
    /// True when electrs is reachable and wallet URL is configured (Umbrel readiness).
    sparrow_ready: bool,
    sparrow_tor_warning: String,
}

#[derive(serde::Serialize, Deserialize)]
struct ConfigResponse {
    indexer_url: String,
    indexer_node_url: String,
    indexer_use_ssl: bool,
    indexer_is_manual: bool,
    network_editable: bool,
    umbrel_mode: bool,
    network: String,
    broadcast_mode: String,
    default_delay_hours: u64,
    scheduled_datetime: Option<String>,
    min_delay_hours: u64,
    max_delay_hours: u64,
    min_fee_rate: f64,
    max_fee_rate: f64,
    web_port: u16,
    electrum_port: u16,
    electrum_host: String,
    #[serde(alias = "sparrow_connect_url")]
    wallet_connect_url: String,
    indexer_auto_detected: bool,
    network_changed: bool,
    supported_networks: Vec<String>,
}

async fn get_config(State(state): State<AppState>) -> Result<Json<ConfigResponse>, (StatusCode, String)> {
    let mut config = state
        .config
        .lock()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let mut reconnect = false;
    if crate::discovery::is_umbrel_mode() {
        if crate::discovery::sanitize_umbrel_indexer_config(&mut config) {
            reconnect = true;
            let _ = crate::discovery::save_config_to_disk(&config);
        }
    }
    let response = config_to_response(&config, false);
    drop(config);
    if reconnect {
        let pool_manager = state.pool_manager.clone();
        if let Err(e) = pool_manager.reconnect_indexer_from_config() {
            tracing::warn!("Could not reconnect indexer after auto-heal: {}", e);
        }
    }
    Ok(Json(response))
}

#[derive(Deserialize)]
struct SaveConfigRequest {
    indexer_url: Option<String>,
    indexer_use_ssl: Option<bool>,
    network: Option<String>,
    broadcast_mode: Option<String>,
    default_delay_hours: Option<u64>,
    scheduled_datetime: Option<String>,
    min_delay_hours: Option<u64>,
    max_delay_hours: Option<u64>,
    min_fee_rate: Option<f64>,
    max_fee_rate: Option<f64>,
}

async fn save_config(
    State(state): State<AppState>,
    Json(req): Json<SaveConfigRequest>,
) -> Result<Json<ConfigResponse>, (StatusCode, String)> {
    tracing::info!("save_config called");
    let mut config = state.config.lock().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    tracing::info!("Config lock acquired");

    let mut network_changed = false;
    let old_network = config.network.network_type.data_dir_name().to_string();
    let network_editable = !crate::discovery::is_umbrel_mode();

    if network_editable {
        if let Some(ref net) = req.network {
            let new_network = net.to_lowercase();
            if new_network != old_network {
                network_changed = true;
            }
            config.network.network_type = match new_network.as_str() {
                "mainnet" => crate::config::NetworkType::Mainnet,
                "testnet4" => crate::config::NetworkType::Testnet4,
                "signet" => crate::config::NetworkType::Signet,
                _ => config.network.network_type.clone(),
            };
        }
    }

    let network = config.network.network_type.clone();
    let indexer_updated = if network_changed {
        tracing::info!("Network changed — scanning LAN for indexer on new network");
        let found = crate::discovery::apply_indexer_discovery(&mut config);
        found && config.indexer.is_some()
    } else if let Some(url) = req.indexer_url {
        if url.trim().is_empty() {
            if crate::discovery::is_umbrel_mode() {
                tracing::info!("Clearing manual indexer override — reconnecting to node indexer");
                config.indexer = None;
                let from_env = std::env::var("BROADCAST_POOL_UMBREL_ELECTRS_TCP")
                    .ok()
                    .map(|v| v.trim().to_string())
                    .filter(|v| !v.is_empty() && !v.contains("${"));
                if let Some(tcp_url) = from_env {
                    config.indexer = Some(crate::config::IndexerConfig {
                        url: tcp_url,
                        manual_override: false,
                    });
                    true
                } else {
                    crate::discovery::discover_umbrel_if_needed(&mut config, true)
                }
            } else {
                false
            }
        } else {
            if crate::discovery::is_umbrel_mode() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "On Umbrel the node electrs is used automatically. Leave the external indexer field empty."
                        .to_string(),
                ));
            }
            if let Some(host) = crate::discovery::extract_indexer_host(&url) {
                if crate::discovery::is_mistaken_umbrel_lan_override(&host) {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "On Umbrel use the node indexer automatically. Clear the external field and Save — \
                         the wallet LAN IP is for Sparrow, not electrs from this container."
                            .to_string(),
                    ));
                }
            }
            let normalized = crate::discovery::normalize_indexer_url_with_scheme(
                &url,
                req.indexer_use_ssl,
            );
            let working = crate::discovery::resolve_working_indexer_url(&normalized, &network)
                .or_else(|| {
                    crate::discovery::resolve_working_indexer_url(&url, &network)
                })
                .unwrap_or(normalized);
            config.indexer = Some(crate::config::IndexerConfig {
                url: working,
                manual_override: true,
            });
            true
        }
    } else {
        false
    };
    if let Some(mode) = req.broadcast_mode {
        if let Ok(m) = mode.parse::<crate::config::BroadcastMode>() {
            config.schedule.broadcast_mode = m;
        }
    }
    if let Some(v) = req.default_delay_hours {
        config.schedule.default_delay_hours = v;
    }
    if let Some(v) = req.scheduled_datetime {
        config.schedule.scheduled_datetime = if v.is_empty() { None } else { Some(v) };
    }
    if let Some(v) = req.min_delay_hours {
        config.schedule.min_delay_hours = v;
    }
    if let Some(v) = req.max_delay_hours {
        config.schedule.max_delay_hours = v;
    }
    if let Some(v) = req.min_fee_rate {
        config.schedule.min_fee_rate = v;
    }
    if let Some(v) = req.max_fee_rate {
        config.schedule.max_fee_rate = v;
    }
    tracing::info!("Config modified");

    crate::discovery::save_config_to_disk(&config)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("File write error: {}", e)))?;

    let response = config_to_response(&config, network_changed);
    let pool_manager = state.pool_manager.clone();
    if indexer_updated {
        if let Err(e) = pool_manager.reconnect_indexer_from_config() {
            tracing::warn!("Could not reconnect indexer after save: {}", e);
        }
    }

    drop(config);
    tracing::info!("Config lock dropped");

    Ok(Json(response))
}

async fn get_status(State(state): State<AppState>) -> Result<Json<StatusResponse>, (StatusCode, String)> {
    let pool_manager = state.pool_manager.clone();
    let config = state.config.clone();
    let mut reconnect = false;
    if let Ok(mut cfg) = state.config.lock() {
        if crate::discovery::sanitize_umbrel_indexer_config(&mut cfg) {
            let _ = crate::discovery::save_config_to_disk(&cfg);
            reconnect = true;
        }
    }
    if reconnect {
        if let Err(e) = pool_manager.reconnect_indexer_from_config() {
            tracing::warn!("Could not reconnect indexer after status heal: {}", e);
        }
    }

    let result = tokio::task::spawn_blocking(move || {
        let stats = pool_manager.get_stats().map_err(|e| e.to_string())?;
        let rpc_connected = if let Some(rpc) = pool_manager.get_rpc() {
            let (tx, rx) = std::sync::mpsc::channel();
            let rpc_clone = rpc.clone();
            std::thread::spawn(move || {
                let _ = tx.send(rpc_clone.test_connection().unwrap_or(false));
            });
            rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap_or(false)
        } else {
            false
        };
        let electrum_connected = if let Some(indexer) = pool_manager.get_indexer() {
            let (tx, rx) = std::sync::mpsc::channel();
            let idx = indexer.clone();
            std::thread::spawn(move || {
                let _ = tx.send(idx.test_connection().unwrap_or(false));
            });
            rx.recv_timeout(std::time::Duration::from_secs(8)).unwrap_or(false)
        } else {
            false
        };
        let indexer_height = if electrum_connected {
            pool_manager.check_block_height().ok().flatten()
        } else {
            None
        };
        let chain_mtp = pool_manager.get_chain_mtp().ok();
        let network = {
            let cfg = config.lock().map_err(|e| e.to_string())?;
            cfg.network.network_type.data_dir_name().to_string()
        };
        let network_display = {
            let cfg = config.lock().map_err(|e| e.to_string())?;
            cfg.network.network_type.display_name().to_string()
        };
        let wallet_url = {
            let cfg = config.lock().map_err(|e| e.to_string())?;
            wallet_connect_url(&cfg)
        };
        Ok::<_, String>((
            stats,
            rpc_connected,
            electrum_connected,
            indexer_height,
            chain_mtp,
            network,
            network_display,
            wallet_url,
        ))
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Task failed: {}", e)))?;

    let (
        pool_stats,
        rpc_connected,
        electrum_connected,
        indexer_height,
        chain_mtp,
        network,
        network_display,
        wallet_url,
    ) = result.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    let indexer_status_hint = {
        let cfg = state.config.lock().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        crate::discovery::umbrel_indexer_status_hint(&cfg, electrum_connected)
    };

    Ok(Json(StatusResponse {
        network,
        network_display,
        supported_networks: supported_networks_vec(),
        rpc_connected,
        electrum_connected,
        indexer_height,
        chain_mtp,
        pool_stats,
        retain_by_default: true,
        wallet_connect_url: wallet_url.clone(),
        indexer_status_hint,
        sparrow_ready: electrum_connected
            && !wallet_url.is_empty()
            && !wallet_url.contains('<'),
        sparrow_tor_warning: "Disable Sparrow Settings→Network proxy/Tor or broadcasts bypass this pool (mempool.space). Use tcp://LAN:50050 only.".into(),
    }))
}

#[derive(Deserialize)]
struct EstimateFeeRequest {
    tx_hex: String,
}

#[derive(Serialize)]
struct EstimateFeeResponse {
    fee_rate: f64,
    fee_sat: u64,
    vsize: usize,
}

async fn estimate_fee(
    State(state): State<AppState>,
    Json(req): Json<EstimateFeeRequest>,
) -> Result<Json<EstimateFeeResponse>, (StatusCode, String)> {
    // Use spawn_blocking for the synchronous indexer call
    let config_clone = state.config.lock().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?.clone();
    let tx_hex = req.tx_hex.clone();

    let result = tokio::task::spawn_blocking(move || {
        if let Some(ref indexer) = config_clone.indexer {
            if let Ok(electrum) = crate::rpc::ElectrumClient::new(&indexer.url) {
                match electrum.calculate_tx_fee(&tx_hex) {
                    Ok((fee_rate, fee, vsize)) => {
                        return Ok(EstimateFeeResponse { fee_rate, fee_sat: fee, vsize });
                    }
                    Err(e) => {
                        tracing::warn!("Fee estimation failed: {}", e);
                    }
                }
            }
        }

        // Fallback: estimate from TX size
        if let Ok(raw) = hex::decode(&tx_hex) {
            let tx_size = raw.len();
            let vsize = tx_size * 3 / 4;
            return Ok(EstimateFeeResponse {
                fee_rate: 0.0,
                fee_sat: 0,
                vsize,
            });
        }

        Err("Invalid transaction hex".to_string())
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    result.map(Json).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))
}

#[derive(Deserialize)]
struct TestIndexerRequest {
    url: String,
    indexer_use_ssl: Option<bool>,
}

#[derive(Serialize)]
struct TestIndexerResponse {
    success: bool,
    url: String,
    height: Option<u64>,
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    use_ssl: Option<bool>,
}

async fn test_indexer(
    State(state): State<AppState>,
    Json(req): Json<TestIndexerRequest>,
) -> Result<Json<TestIndexerResponse>, (StatusCode, String)> {
    if req.url.trim().is_empty() {
        return Ok(Json(TestIndexerResponse {
            success: false,
            url: String::new(),
            height: None,
            error: Some("Empty indexer URL".to_string()),
            use_ssl: None,
        }));
    }
    if let Some(host) = crate::discovery::extract_indexer_host(&req.url) {
        if crate::discovery::is_mistaken_umbrel_lan_override(&host) {
            return Ok(Json(TestIndexerResponse {
                success: false,
                url: req.url.clone(),
                height: None,
                error: Some(
                    "On Umbrel the node indexer connects automatically. Clear this field and Save — \
                     the wallet LAN IP (for Sparrow) is not reachable as electrs from this app."
                        .to_string(),
                ),
                use_ssl: None,
            }));
        }
    }
    let input = req.url.clone();
    let use_ssl = req.indexer_use_ssl;
    let network = state
        .config
        .lock()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .network
        .network_type
        .clone();
    let normalized =
        crate::discovery::normalize_indexer_url_with_scheme(&input, use_ssl);
    let input_for_timeout = input.clone();

    let response = tokio::task::spawn_blocking(move || {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let result = if let Some(working) =
                crate::discovery::resolve_working_indexer_url(&normalized, &network)
                    .or_else(|| crate::discovery::resolve_working_indexer_url(&input, &network))
            {
                let display = crate::discovery::display_indexer_url(&working);
                let height = crate::rpc::ElectrumClient::new(&working)
                    .ok()
                    .and_then(|c| c.get_height().ok());
                TestIndexerResponse {
                    success: true,
                    url: display,
                    height,
                    error: None,
                    use_ssl: Some(crate::discovery::indexer_url_uses_ssl(&working)),
                }
            } else {
                TestIndexerResponse {
                    success: false,
                    url: input,
                    height: None,
                    error: Some(
                        "Connection failed (tried TCP and SSL on ports 50001/50002)".to_string(),
                    ),
                    use_ssl: None,
                }
            };
            let _ = tx.send(result);
        });
        rx.recv_timeout(std::time::Duration::from_secs(30))
            .unwrap_or_else(|_| TestIndexerResponse {
                success: false,
                url: input_for_timeout,
                height: None,
                error: Some("Connection timeout (30s)".to_string()),
                use_ssl: None,
            })
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", e)))?;

    Ok(Json(response))
}

#[derive(Serialize)]
struct DiscoverIndexerResponse {
    success: bool,
    indexer_url: String,
    connected: bool,
    height: Option<u64>,
    message: String,
}

async fn discover_indexer(
    State(state): State<AppState>,
) -> Result<Json<DiscoverIndexerResponse>, (StatusCode, String)> {
    let pool_manager = state.pool_manager.clone();
    let config = state.config.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<DiscoverIndexerResponse, String> {
        let mut cfg = config.lock().map_err(|e| e.to_string())?;
        let found = if crate::discovery::is_umbrel_mode() {
            crate::discovery::discover_umbrel_if_needed(&mut cfg, true)
        } else {
            if let Some(ref mut idx) = cfg.indexer {
                idx.manual_override = false;
            }
            crate::discovery::apply_indexer_discovery(&mut cfg)
        };
        crate::discovery::save_config_to_disk(&cfg).map_err(|e| e.to_string())?;

        let url = cfg
            .indexer
            .as_ref()
            .map(|i| i.url.clone())
            .unwrap_or_default();
        let network_type = cfg.network.network_type.clone();
        let fail_hint = if !found {
            if crate::discovery::is_umbrel_mode() {
                crate::discovery::umbrel_indexer_status_hint(&cfg, false)
            } else {
                "Could not find Electrs/Fulcrum on the LAN".to_string()
            }
        } else {
            String::new()
        };
        drop(cfg);

        if found && !url.is_empty() {
            if let Err(e) = pool_manager.reconnect_indexer_from_config() {
                tracing::warn!("Indexer discovered but reconnect failed: {}", e);
            }
        }

        let connected = !url.is_empty() && live_test_indexer_url(&url, &network_type);
        let height = if connected {
            crate::discovery::resolve_working_indexer_url(&url, &network_type).and_then(|working| {
                crate::rpc::ElectrumClient::new(&working)
                    .ok()
                    .and_then(|c| c.get_height().ok())
            })
        } else {
            None
        };

        Ok(DiscoverIndexerResponse {
            success: found && connected,
            indexer_url: crate::discovery::display_indexer_url(&url),
            connected,
            height,
            message: if found && connected {
                format!(
                    "Indexer found and connected at {}",
                    crate::discovery::display_indexer_url(&url)
                )
            } else if found {
                format!(
                    "Indexer URL saved ({}) but connection failed",
                    crate::discovery::display_indexer_url(&url)
                )
            } else {
                fail_hint
            },
        })
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Task failed: {}", e)))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(result))
}

async fn get_indexer_debug(
    State(state): State<AppState>,
) -> Result<Json<crate::discovery::UmbrelIndexerDiagnostics>, (StatusCode, String)> {
    let pool_manager = state.pool_manager.clone();
    let config = state.config.clone();
    let diagnostics = tokio::task::spawn_blocking(move || {
        let connected = pool_manager.indexer_healthy();
        let cfg = config.lock().map_err(|e| e.to_string())?;
        Ok(crate::discovery::umbrel_indexer_diagnostics(&cfg, connected))
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Task failed: {}", e)))?
    .map_err(|e: String| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(Json(diagnostics))
}

async fn restart_daemon() -> impl IntoResponse {
    let handle = tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        tracing::info!("Daemon restart triggered");
        std::process::exit(0);
    });
    let _ = handle.await;
    "Restarting"
}
