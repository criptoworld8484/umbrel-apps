mod pending;

use anyhow::{Context, Result};
use bitcoin::consensus::Decodable;
use bitcoin::Transaction;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::config::{BroadcastMode, Config};
use crate::db::models::NewBroadcastTx;
use crate::pool::manager::{PendingTxOutput, PoolManager};

enum IndexerStream {
    Plain(tokio::net::TcpStream),
    Tls(tokio_rustls::client::TlsStream<tokio::net::TcpStream>),
}

impl AsyncRead for IndexerStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            IndexerStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            IndexerStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for IndexerStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut *self {
            IndexerStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            IndexerStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            IndexerStream::Plain(s) => Pin::new(s).poll_flush(cx),
            IndexerStream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            IndexerStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            IndexerStream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

async fn connect_indexer(indexer_url: &str) -> Result<IndexerStream> {
    let use_ssl = indexer_url.starts_with("ssl://");
    let addr = pending::strip_indexer_host(indexer_url);
    let tcp = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio::net::TcpStream::connect(&addr),
    )
    .await
    .map_err(|_| anyhow::anyhow!("TCP connect to indexer timed out ({})", addr))?
    .with_context(|| format!("TCP connect to indexer failed ({})", addr))?;

    if use_ssl {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        let host = addr.split(':').next().unwrap_or(&addr);
        let server_name = if let Ok(ip) = host.parse::<std::net::IpAddr>() {
            rustls_pki_types::ServerName::IpAddress(ip.into())
        } else {
            rustls_pki_types::ServerName::try_from(host.to_string())
                .map_err(|e| anyhow::anyhow!("Invalid TLS server name: {}", e))?
        };
        let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));
        let tls = connector.connect(server_name, tcp).await?;
        Ok(IndexerStream::Tls(tls))
    } else {
        Ok(IndexerStream::Plain(tcp))
    }
}

fn forward_to_indexer_sync(request_str: &str, indexer_addr: &str) -> Option<String> {
    use std::io::{Write, Read};

    let use_ssl = indexer_addr.starts_with("ssl://");
    let addr = indexer_addr
        .strip_prefix("tcp://")
        .or_else(|| indexer_addr.strip_prefix("ssl://"))
        .unwrap_or(indexer_addr)
        .to_string();

    let tcp_stream = match std::net::TcpStream::connect_timeout(
        &addr.parse().ok()?,
        std::time::Duration::from_secs(3),
    ) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("Sync TCP connect to indexer failed: {}", e);
            return None;
        }
    };
    tcp_stream.set_read_timeout(Some(std::time::Duration::from_secs(30))).ok()?;
    tcp_stream.set_write_timeout(Some(std::time::Duration::from_secs(5))).ok()?;

    let mut req_bytes = request_str.as_bytes().to_vec();
    req_bytes.push(b'\n');

    if use_ssl {
        // SSL connection using rustls
        use rustls::ClientConfig;
        use std::sync::Arc;

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let domain = addr.split(':').next().unwrap_or(&addr);

        // Parse as IP address first, then as domain name
        let server_name = if let Ok(ip) = domain.parse::<std::net::IpAddr>() {
            rustls::pki_types::ServerName::IpAddress(ip.into())
        } else {
            rustls::pki_types::ServerName::try_from(domain.to_string()).ok()?
        };
        let connector = rustls::ClientConnection::new(Arc::new(config), server_name).ok()?;
        let mut stream = rustls::StreamOwned::new(connector, tcp_stream);

        if stream.write_all(&req_bytes).is_err() {
            return None;
        }

        let mut response = Vec::new();
        let mut buf = [0u8; 65536];
        loop {
            match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    response.extend_from_slice(&buf[..n]);
                    if response.iter().any(|&b| b == b'\n') {
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!("Sync SSL read from indexer failed: {}", e);
                    break;
                }
            }
        }
        String::from_utf8(response).ok()
    } else {
        // Plain TCP connection
        let mut stream = tcp_stream;
        if stream.write_all(&req_bytes).is_err() {
            return None;
        }

        let mut response = Vec::new();
        let mut buf = [0u8; 65536];
        loop {
            match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    response.extend_from_slice(&buf[..n]);
                    if response.iter().any(|&b| b == b'\n') {
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!("Sync TCP read from indexer failed: {}", e);
                    break;
                }
            }
        }
        String::from_utf8(response).ok()
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct JsonRpcRequest {
    jsonrpc: Option<String>,
    method: String,
    params: Option<serde_json::Value>,
    id: serde_json::Value,
}

const BROADCAST_METHODS: &[&str] = &[
    "blockchain.transaction.broadcast",
    "blockchain.transaction.broadcast_package",
];

fn is_broadcast_method(method: &str) -> bool {
    BROADCAST_METHODS.contains(&method)
}

fn line_mentions_tx_rpc(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    lower.contains("broadcast") || lower.contains("transaction")
}

fn log_incoming_client_line(peer_addr: std::net::SocketAddr, line_str: &str, source_label: &str) {
    if line_mentions_tx_rpc(line_str) {
        tracing::info!(
            "Electrum incoming [{}] from {} (len={}, preview={})",
            source_label,
            peer_addr,
            line_str.len(),
            &line_str[..line_str.len().min(200)]
        );
    }
}

fn peek_line_methods(line: &str) -> Option<Vec<String>> {
    let v = serde_json::from_str::<serde_json::Value>(line.trim()).ok()?;
    if let Some(arr) = v.as_array() {
        return Some(
            arr.iter()
                .filter_map(|item| item.get("method")?.as_str().map(str::to_string))
                .collect(),
        );
    }
    v.get("method")
        .and_then(|m| m.as_str())
        .map(|m| vec![m.to_string()])
}

/// Parse broadcast RPC even when strict struct decode would fail.
/// Supports `[hex]`, `"hex"`, object params, and broadcast_package batches.
fn params_first_string(params: &serde_json::Value) -> Option<String> {
    match params {
        serde_json::Value::Array(arr) => arr.first()?.as_str().map(|s| s.to_string()),
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Object(map) => {
            for key in ["raw_tx", "tx", "transaction", "hex"] {
                if let Some(serde_json::Value::String(s)) = map.get(key) {
                    return Some(s.clone());
                }
            }
            None
        }
        _ => None,
    }
}

fn json_rpc_id(v: &serde_json::Value) -> serde_json::Value {
    v.get("id").cloned().unwrap_or(serde_json::Value::Null)
}

fn extract_broadcast_from_value(v: &serde_json::Value) -> Option<(serde_json::Value, String)> {
    let method = v.get("method")?.as_str()?;
    if !is_broadcast_method(method) {
        return None;
    }
    let id = json_rpc_id(v);
    let params = v.get("params")?;
    if method == "blockchain.transaction.broadcast_package" {
        let tx_arr = params.as_array()?.first()?.as_array()?;
        let hex = tx_arr.first()?.as_str()?;
        return Some((id, hex.to_string()));
    }
    let hex = params_first_string(params)?;
    Some((id, hex))
}

fn extract_broadcast_hex(line: &str) -> Option<(serde_json::Value, String)> {
    let v = serde_json::from_str::<serde_json::Value>(line.trim()).ok()?;
    if let Some(arr) = v.as_array() {
        for item in arr {
            if let Some(found) = extract_broadcast_from_value(item) {
                return Some(found);
            }
        }
        return None;
    }
    extract_broadcast_from_value(&v)
}

fn line_looks_like_broadcast(line: &str) -> bool {
    BROADCAST_METHODS.iter().any(|m| line.contains(m))
}

fn line_json_rpc_id(line: &str) -> serde_json::Value {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(line.trim()) else {
        return serde_json::Value::Null;
    };
    if let Some(arr) = v.as_array() {
        for item in arr {
            if item
                .get("method")
                .and_then(|m| m.as_str())
                .is_some_and(is_broadcast_method)
            {
                return json_rpc_id(item);
            }
        }
    }
    json_rpc_id(&v)
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    result: serde_json::Value,
    id: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

#[derive(Debug, Serialize)]
struct JsonRpcErrorResponse {
    jsonrpc: String,
    error: JsonRpcError,
    id: serde_json::Value,
}

struct BroadcastHandleResult {
    txid: String,
    tx_hex: String,
    retained: bool,
    affected_scripthashes: Vec<String>,
    /// Input scripthash enrichment deferred until after Sparrow ack (electrs can be slow on Umbrel).
    defer_input_enrichment: bool,
}

struct SessionState {
    subscribed_scripthashes: HashSet<String>,
    pending_methods: HashMap<serde_json::Value, (String, String)>,
    /// While handling a client JSON-RPC line, defer upstream notifications to avoid interleaving.
    client_busy: bool,
    deferred_indexer_out: Vec<Vec<u8>>,
    /// Sparrow diagnostics: did this session ever send a broadcast RPC?
    broadcast_intercepted: bool,
    /// Last tx acked this session — Sparrow polls input scripthashes before enrichment finishes.
    recent_broadcast_txid: Option<String>,
    rpc_lines_handled: u32,
}

impl SessionState {
    fn new() -> Self {
        Self {
            subscribed_scripthashes: HashSet::new(),
            pending_methods: HashMap::new(),
            client_busy: false,
            deferred_indexer_out: Vec::new(),
            broadcast_intercepted: false,
            recent_broadcast_txid: None,
            rpc_lines_handled: 0,
        }
    }

    fn log_disconnect_summary(&self, peer_addr: std::net::SocketAddr, source_label: &str) {
        if source_label != "sparrow" {
            return;
        }
        if self.broadcast_intercepted {
            tracing::info!(
                "Sparrow session ended {} (handled {} RPC lines, broadcast received)",
                peer_addr,
                self.rpc_lines_handled
            );
            return;
        }
        if self.rpc_lines_handled > 3 {
            tracing::error!(
                "Sparrow session ended {} without any broadcast RPC ({} lines handled). \
                 If txs stay on 'Broadcasting' or never appear in the pool: disable Tor/proxy in \
                 Sparrow Settings→Network — Sparrow sends broadcasts to mempool.space over Tor \
                 instead of this Electrum server when proxy/Tor is active.",
                peer_addr,
                self.rpc_lines_handled
            );
        }
    }

    fn track_request(&mut self, request: &JsonRpcRequest) {
        let scripthash = request
            .params
            .as_ref()
            .and_then(|p| p.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        match request.method.as_str() {
            "blockchain.scripthash.subscribe" => {
                self.subscribed_scripthashes.insert(scripthash.clone());
                self.pending_methods.insert(
                    request.id.clone(),
                    ("blockchain.scripthash.subscribe".to_string(), scripthash),
                );
            }
            "blockchain.scripthash.get_history" => {
                self.pending_methods.insert(
                    request.id.clone(),
                    ("blockchain.scripthash.get_history".to_string(), scripthash),
                );
            }
            "blockchain.scripthash.get_balance" => {
                self.pending_methods.insert(
                    request.id.clone(),
                    ("blockchain.scripthash.get_balance".to_string(), scripthash),
                );
            }
            "blockchain.scripthash.listunspent" => {
                self.pending_methods.insert(
                    request.id.clone(),
                    ("blockchain.scripthash.listunspent".to_string(), scripthash),
                );
            }
            "blockchain.scripthash.get_mempool" => {
                self.pending_methods.insert(
                    request.id.clone(),
                    ("blockchain.scripthash.get_mempool".to_string(), scripthash),
                );
            }
            _ => {}
        }
    }
}

fn scripthash_from_params(params: &Option<serde_json::Value>) -> Option<String> {
    params
        .as_ref()
        .and_then(|p| p.as_array())
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn modify_upstream_response(
    msg: &mut serde_json::Value,
    method: &str,
    scripthash: &str,
    pool_manager: &PoolManager,
    indexer_url: &str,
    session: Option<&SessionState>,
) {
    match method {
        "blockchain.scripthash.get_history" => {
            if let Some(result) = msg.get_mut("result").and_then(|r| r.as_array_mut()) {
                let history = result.clone();
                let pending = pending_txids_for_scripthash_with_session(
                    pool_manager,
                    scripthash,
                    indexer_url,
                    session,
                );
                *result = pending::inject_in_history(history, scripthash, &pending);
            }
        }
        "blockchain.scripthash.subscribe" => {
            let pending = pending_txids_for_scripthash_with_session(
                pool_manager,
                scripthash,
                indexer_url,
                session,
            );
            if pending.is_empty() {
                return;
            }
            // Never block the async runtime on electrs history fetch — pending-only status hash.
            if let Some(hash) = pending::compute_modified_status_hash(vec![], scripthash, &pending) {
                msg["result"] = serde_json::Value::String(hash);
            }
        }
        "blockchain.scripthash.get_balance" => {
            if let Some(result) = msg.get_mut("result") {
                let extra = pool_manager.get_pending_unconfirmed_value(scripthash);
                *result = pending::inject_balance_unconfirmed(result.clone(), extra);
            }
        }
        "blockchain.scripthash.listunspent" => {
            if let Some(result) = msg.get_mut("result").and_then(|r| r.as_array_mut()) {
                let utxos = result.clone();
                let pending = pool_manager.get_pending_utxos_for_scripthash(scripthash);
                *result = pending::inject_listunspent(utxos, &pending);
            }
        }
        "blockchain.scripthash.get_mempool" => {
            if let Some(result) = msg.get_mut("result") {
                let pending = pending_txids_for_scripthash_with_session(
                    pool_manager,
                    scripthash,
                    indexer_url,
                    session,
                );
                *result = pending::inject_get_mempool(result.clone(), &pending);
            }
        }
        _ => {}
    }
}

fn modify_upstream_notification(
    msg: &mut serde_json::Value,
    pool_manager: &PoolManager,
    _indexer_url: &str,
) {
    let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
    if method != "blockchain.scripthash.subscribe" {
        return;
    }
    let Some(params) = msg.get_mut("params").and_then(|p| p.as_array_mut()) else {
        return;
    };
    if params.is_empty() {
        return;
    }
    let scripthash = params[0].as_str().unwrap_or("").to_string();
    if !pool_manager.has_pending_for_scripthash(&scripthash) {
        return;
    }
    let pending = pool_manager.get_pending_txids_for_scripthash(&scripthash);
    if let Some(hash) = pending::compute_modified_status_hash(vec![], &scripthash, &pending) {
        if params.len() > 1 {
            params[1] = serde_json::Value::String(hash);
        }
    }
}

fn process_indexer_line(
    line_bytes: &[u8],
    session: &mut SessionState,
    pool_manager: &PoolManager,
    indexer_url: &str,
) -> Result<Vec<u8>> {
    let line_str = String::from_utf8_lossy(line_bytes);
    if let Ok(mut msg) = serde_json::from_str::<serde_json::Value>(line_str.trim()) {
        if let Some(id) = msg.get("id") {
            if !id.is_null() {
                if let Some((method, scripthash)) = session.pending_methods.remove(id) {
                    modify_upstream_response(
                        &mut msg,
                        &method,
                        &scripthash,
                        pool_manager,
                        indexer_url,
                        Some(session),
                    );
                }
            }
        }
        if msg.get("method").is_some() {
            modify_upstream_notification(&mut msg, pool_manager, indexer_url);
        }
        let mut out = serde_json::to_vec(&msg)?;
        out.push(b'\n');
        return Ok(out);
    }
    let mut out = line_bytes.to_vec();
    out.push(b'\n');
    Ok(out)
}

/// Answer Electrum handshake locally so Sparrow connects even when upstream electrs is down.
fn local_electrum_response(
    request: &JsonRpcRequest,
    config: &Mutex<Config>,
) -> Option<serde_json::Value> {
    let id = request.id.clone();
    match request.method.as_str() {
        "server.version" => Some(serde_json::json!({
            "jsonrpc": "2.0",
            "result": ["broadcast-pool Electrum", "1.4"],
            "id": id
        })),
        "server.banner" => Some(serde_json::json!({
            "jsonrpc": "2.0",
            "result": format!(
                "broadcast-pool {} - disable Sparrow Tor proxy so broadcasts reach this pool",
                env!("CARGO_PKG_VERSION")
            ),
            "id": id
        })),
        "server.ping" => Some(serde_json::json!({
            "jsonrpc": "2.0",
            "result": true,
            "id": id
        })),
        "server.features" => {
            let genesis = config
                .lock()
                .ok()
                .map(|c| c.network.network_type.genesis_hash().to_string())
                .unwrap_or_else(|| "0".repeat(64));
            Some(serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "genesis_hash": genesis,
                    "protocol_max": "1.4",
                    "protocol_min": "1.0",
                    "protocol_version": "1.4",
                    "server_version": format!("broadcast-pool {}", env!("CARGO_PKG_VERSION")),
                    "hash_function": "sha256",
                    "broadcast_pool": true,
                    "sparrow_tor_warning": "Disable Sparrow Settings→Network proxy/Tor or broadcasts bypass this pool"
                },
                "id": id
            }))
        }
        _ => None,
    }
}

const LOCAL_ELECTRUM_METHODS: &[&str] = &[
    "server.version",
    "server.banner",
    "server.ping",
    "server.features",
];

fn is_local_handshake_method(method: &str) -> bool {
    LOCAL_ELECTRUM_METHODS.contains(&method)
}

fn local_fast_response(request: &JsonRpcRequest) -> Option<serde_json::Value> {
    let id = request.id.clone();
    match request.method.as_str() {
        // Sparrow polls these via electrum before/during broadcast; never block on upstream.
        "blockchain.estimatefee" => Some(serde_json::json!({
            "jsonrpc": "2.0",
            "result": 0.00001,
            "id": id
        })),
        "blockchain.relayfee" => Some(serde_json::json!({
            "jsonrpc": "2.0",
            "result": 0.00001,
            "id": id
        })),
        "mempool.get_fee_histogram" => Some(serde_json::json!({
            "jsonrpc": "2.0",
            "result": [],
            "id": id
        })),
        "blockchain.block.stats" => Some(serde_json::json!({
            "jsonrpc": "2.0",
            "result": {
                "height": request.params.as_ref()
                    .and_then(|p| p.as_array())
                    .and_then(|a| a.first())
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                "time": 0,
                "median_fee": 0.00001
            },
            "id": id
        })),
        _ => None,
    }
}

fn batch_contains_local_handshake(subrequests: &[JsonRpcRequest]) -> bool {
    subrequests
        .iter()
        .any(|r| is_local_handshake_method(&r.method))
}

fn parse_subrequests(line: &str) -> Result<Vec<JsonRpcRequest>> {
    let v = serde_json::from_str::<serde_json::Value>(line.trim()).context("invalid JSON")?;
    if let Some(arr) = v.as_array() {
        arr.iter()
            .map(|item| serde_json::from_value(item.clone()).context("invalid batch item"))
            .collect()
    } else {
        Ok(vec![serde_json::from_value(v).context("invalid request")?])
    }
}

fn line_is_batch(line: &str) -> bool {
    line.trim_start().starts_with('[')
}

/// Prefer live pool indexer URL (healed after Umbrel startup) over session snapshot.
fn resolve_live_indexer_url(pool_manager: &PoolManager, config: &Arc<Mutex<Config>>) -> String {
    if let Some(url) = pool_manager.get_indexer_url() {
        if !url.is_empty() {
            return url;
        }
    }
    config
        .lock()
        .ok()
        .and_then(|c| c.indexer.as_ref().map(|i| i.url.clone()))
        .unwrap_or_default()
}

/// Process broadcast lines before other buffered RPCs so subscribe storms do not delay sends.
fn pop_client_line(client_buf: &mut Vec<u8>) -> Option<(Vec<u8>, String)> {
    let mut line_ranges: Vec<(usize, usize)> = Vec::new();
    let mut start = 0usize;
    for (i, &b) in client_buf.iter().enumerate() {
        if b == b'\n' {
            line_ranges.push((start, i));
            start = i + 1;
        }
    }
    if line_ranges.is_empty() {
        return None;
    }
    let pick = line_ranges
        .iter()
        .position(|&(s, e)| {
            let line_str = String::from_utf8_lossy(&client_buf[s..e]);
            extract_broadcast_hex(&line_str).is_some() || line_looks_like_broadcast(&line_str)
        })
        .unwrap_or(0);
    let (s, e) = line_ranges[pick];
    let line_bytes = client_buf[s..e].to_vec();
    client_buf.drain(..=e);
    let line_str = String::from_utf8_lossy(&line_bytes)
        .trim_end_matches('\r')
        .to_string();
    Some((line_bytes, line_str))
}

async fn forward_subrequest_sync(
    request: &JsonRpcRequest,
    indexer_url: &str,
    session: &mut SessionState,
    pool_manager: &PoolManager,
) -> Result<serde_json::Value> {
    forward_subrequest_sync_timed(
        request,
        indexer_url,
        session,
        pool_manager,
        indexer_forward_timeout(&request.method),
    )
    .await
}

fn indexer_forward_timeout(method: &str) -> std::time::Duration {
    if method.starts_with("blockchain.scripthash.") {
        std::time::Duration::from_secs(8)
    } else if method == "blockchain.transaction.get" {
        std::time::Duration::from_secs(5)
    } else {
        std::time::Duration::from_secs(3)
    }
}

async fn forward_subrequest_sync_timed(
    request: &JsonRpcRequest,
    indexer_url: &str,
    session: &mut SessionState,
    pool_manager: &PoolManager,
    timeout: std::time::Duration,
) -> Result<serde_json::Value> {
    if is_broadcast_method(&request.method) {
        anyhow::bail!("broadcast must not be forwarded to upstream electrs");
    }

    session.track_request(request);
    let raw = serde_json::to_string(request)?;
    let url = indexer_url.to_string();
    let id = request.id.clone();
    let response_str = match tokio::time::timeout(
        timeout,
        tokio::task::spawn_blocking(move || forward_to_indexer_sync(&raw, &url)),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => {
            tracing::warn!("Indexer forward timed out for {}", request.method);
            return Ok(serde_json::json!({
                "jsonrpc": "2.0",
                "error": { "code": -32000, "message": "Indexer temporarily unavailable" },
                "id": id
            }));
        }
    };
    let Some(resp_str) = response_str else {
        return Ok(serde_json::json!({
            "jsonrpc": "2.0",
            "error": { "code": -32000, "message": "Indexer temporarily unavailable" },
            "id": id
        }));
    };
    let mut msg: serde_json::Value = serde_json::from_str(resp_str.trim())?;
    if let Some((method, scripthash)) = session.pending_methods.remove(&id) {
        modify_upstream_response(
            &mut msg,
            &method,
            &scripthash,
            pool_manager,
            indexer_url,
            Some(session),
        );
    }
    Ok(msg)
}

fn parse_headers_subscribe_result(msg: &serde_json::Value) -> Option<(u64, String)> {
    let result = msg.get("result")?;
    let height = result.get("height")?.as_u64()?;
    let hex = result.get("hex")?.as_str()?;
    if height == 0 || hex.is_empty() {
        return None;
    }
    Some((height, hex.to_string()))
}

fn refresh_chain_tip_cache(indexer_url: &str, pool_manager: &PoolManager) {
    if indexer_url.is_empty() {
        return;
    }
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "blockchain.headers.subscribe",
        "params": [],
        "id": 0
    });
    let raw = req.to_string();
    let url = indexer_url.to_string();
    if let Some(resp_str) = forward_to_indexer_sync(&raw, &url) {
        if let Ok(msg) = serde_json::from_str::<serde_json::Value>(resp_str.trim()) {
            if let Some((height, hex)) = parse_headers_subscribe_result(&msg) {
                pool_manager.cache_chain_tip(height, hex);
                tracing::debug!("Chain tip cache refreshed (height={})", height);
            }
        }
    }
}

pub fn warm_chain_tip_cache(indexer_url: &str, pool_manager: &PoolManager) {
    if indexer_url.is_empty() {
        return;
    }
    for attempt in 1..=5 {
        refresh_chain_tip_cache(indexer_url, pool_manager);
        if let Some((height, _)) = pool_manager.get_cached_chain_tip() {
            tracing::info!(
                "Warmed chain tip cache from electrs (height={}, attempt {})",
                height,
                attempt
            );
            return;
        }
        if attempt < 5 {
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
    }
    tracing::warn!(
        "Could not warm chain tip cache after 5 attempts — Sparrow Test Connection waits on electrs until cache fills"
    );
}

fn spawn_chain_tip_refresh(indexer_url: &str, pool_manager: &Arc<PoolManager>) {
    if indexer_url.is_empty() {
        return;
    }
    let url = indexer_url.to_string();
    let pm = pool_manager.clone();
    tokio::spawn(async move {
        let _ = tokio::task::spawn_blocking(move || refresh_chain_tip_cache(&url, &pm)).await;
    });
}

async fn handle_headers_subscribe(
    request: &JsonRpcRequest,
    pool_manager: &Arc<PoolManager>,
    indexer_url: &str,
) -> Result<serde_json::Value> {
    let id = request.id.clone();

    // Cache-first: Sparrow Test Connection must not block on slow Umbrel electrs.
    if let Some((height, hex)) = pool_manager.get_cached_chain_tip() {
        tracing::debug!("headers.subscribe instant cache hit height={}", height);
        spawn_chain_tip_refresh(indexer_url, pool_manager);
        return Ok(serde_json::json!({
            "jsonrpc": "2.0",
            "result": { "height": height, "hex": hex },
            "id": id
        }));
    }

    if !indexer_url.is_empty() {
        let raw = serde_json::to_string(request)?;
        let url = indexer_url.to_string();
        let pm = pool_manager.clone();
        match tokio::time::timeout(
            std::time::Duration::from_secs(3),
            tokio::task::spawn_blocking(move || forward_to_indexer_sync(&raw, &url)),
        )
        .await
        {
            Ok(Ok(Some(resp_str))) => {
                if let Ok(msg) = serde_json::from_str::<serde_json::Value>(resp_str.trim()) {
                    if let Some((height, hex)) = parse_headers_subscribe_result(&msg) {
                        pm.cache_chain_tip(height, hex);
                        tracing::info!("headers.subscribe from electrs height={}", height);
                        return Ok(msg);
                    }
                    if msg.get("error").is_some() {
                        tracing::warn!(
                            "headers.subscribe electrs error: {:?}",
                            msg.get("error")
                        );
                    }
                }
            }
            Ok(Ok(None)) => {
                tracing::warn!("headers.subscribe: electrs connect failed");
            }
            Ok(Err(e)) => {
                tracing::warn!("headers.subscribe task failed: {}", e);
            }
            Err(_) => {
                tracing::warn!("headers.subscribe: electrs timed out (3s)");
            }
        }
    }

    if let Some((height, hex)) = pool_manager.get_cached_chain_tip() {
        tracing::info!("headers.subscribe: serving cached tip height={}", height);
        return Ok(serde_json::json!({
            "jsonrpc": "2.0",
            "result": { "height": height, "hex": hex },
            "id": id
        }));
    }

    Ok(serde_json::json!({
        "jsonrpc": "2.0",
        "error": {
            "code": -32000,
            "message": "Indexer temporarily unavailable — retry in a few seconds"
        },
        "id": id
    }))
}

fn pending_txids_for_scripthash(
    pool_manager: &PoolManager,
    scripthash: &str,
) -> Vec<String> {
    pool_manager.get_pending_txids_for_scripthash(scripthash)
}

fn pending_txids_for_scripthash_with_session(
    pool_manager: &PoolManager,
    scripthash: &str,
    indexer_url: &str,
    session: Option<&SessionState>,
) -> Vec<String> {
    if let Some(session) = session {
        if let Some(ref txid) = session.recent_broadcast_txid {
            // Sparrow polls INPUT scripthashes right after ack — always include pending broadcast.
            let mut pending = vec![txid.clone()];
            for t in pool_manager.get_pending_txids_for_scripthash(scripthash) {
                if !pending.iter().any(|p| p == &t) {
                    pending.push(t);
                }
            }
            tracing::info!(
                "Session broadcast poll {} → tx {} ({} total)",
                &scripthash[..scripthash.len().min(16)],
                &txid[..txid.len().min(16)],
                pending.len()
            );
            return pending;
        }
    }
    pending_txids_for_scripthash(pool_manager, scripthash)
}

fn build_scripthash_mempool_result(
    method: &str,
    pending: &[String],
) -> serde_json::Value {
    if method == "blockchain.scripthash.get_mempool" {
        pending::inject_get_mempool(serde_json::json!([0, []]), pending)
    } else {
        pending
            .iter()
            .map(|txid| {
                serde_json::json!({
                    "tx_hash": txid,
                    "height": 0
                })
            })
            .collect::<Vec<_>>()
            .into()
    }
}

/// Track wallet scripthashes Sparrow cares about (subscribe + post-broadcast poll).
fn track_scripthash_interest(session: &mut SessionState, scripthash: &str) {
    if !scripthash.is_empty() {
        session.subscribed_scripthashes.insert(scripthash.to_string());
    }
}

async fn handle_one_subrequest(
    request: &JsonRpcRequest,
    pool_manager: &Arc<PoolManager>,
    config: &Arc<Mutex<Config>>,
    indexer_url: &str,
    _indexer_stream: &mut Option<IndexerStream>,
    session: &mut SessionState,
    source_label: &str,
    client_stream: &mut tokio::net::TcpStream,
) -> Result<serde_json::Value> {
    if is_local_handshake_method(&request.method) {
        return local_electrum_response(request, config).ok_or_else(|| {
            anyhow::anyhow!("no local response for {}", request.method)
        });
    }

    if let Some(resp) = local_fast_response(request) {
        return Ok(resp);
    }

    if is_broadcast_method(&request.method) {
        return Ok(serde_json::json!({
            "jsonrpc": "2.0",
            "error": {
                "code": -32603,
                "message": format!("{} must use intercept path", request.method)
            },
            "id": request.id
        }));
    }

    // Sparrow polls get_history/get_mempool after broadcast — never block on electrs.
    if request.method == "blockchain.scripthash.get_history"
        || request.method == "blockchain.scripthash.get_mempool"
    {
        if let Some(sh) = scripthash_from_params(&request.params) {
            track_scripthash_interest(session, &sh);
            let pending = pending_txids_for_scripthash_with_session(
                pool_manager,
                &sh,
                indexer_url,
                Some(session),
            );
            if !pending.is_empty() {
                tracing::info!(
                    "Fast {} for {} ({} pool tx(s))",
                    request.method,
                    &sh[..sh.len().min(16)],
                    pending.len()
                );
                let result = build_scripthash_mempool_result(&request.method, &pending);
                return Ok(serde_json::json!({
                    "jsonrpc": "2.0",
                    "result": result,
                    "id": request.id
                }));
            }
        }
    }

    if request.method == "blockchain.scripthash.subscribe" {
        if let Some(sh) = scripthash_from_params(&request.params) {
            track_scripthash_interest(session, &sh);
            let pending = pending_txids_for_scripthash_with_session(
                pool_manager,
                &sh,
                indexer_url,
                Some(session),
            );
            if !pending.is_empty() {
                if let Some(hash) = pending::compute_modified_status_hash(vec![], &sh, &pending) {
                    tracing::info!(
                        "Fast scripthash.subscribe for {} ({} pool tx(s))",
                        &sh[..sh.len().min(16)],
                        pending.len()
                    );
                    return Ok(serde_json::json!({
                        "jsonrpc": "2.0",
                        "result": hash,
                        "id": request.id
                    }));
                }
            }
        }
    }

    if request.method == "blockchain.transaction.get" {
        if let Some(params) = request.params.as_ref().and_then(|p| p.as_array()) {
            if let Some(txid) = params.get(0).and_then(|v| v.as_str()) {
                let verbose = params.get(1).and_then(|v| v.as_bool()).unwrap_or(false);
                if !verbose {
                    if let Some(hex) = pool_manager.lookup_tx_hex(txid) {
                        return Ok(serde_json::json!({
                            "jsonrpc": "2.0",
                            "result": hex,
                            "id": request.id
                        }));
                    }
                }
            }
        }
    }

    if request.method == "blockchain.headers.subscribe" {
        return handle_headers_subscribe(request, pool_manager, indexer_url).await;
    }

    if !indexer_url.is_empty() {
        return forward_subrequest_sync(request, indexer_url, session, pool_manager).await;
    }

    Ok(serde_json::json!({
        "jsonrpc": "2.0",
        "error": { "code": -32000, "message": "Indexer temporarily unavailable" },
        "id": request.id
    }))
}

async fn write_client_responses(
    client_stream: &mut tokio::net::TcpStream,
    responses: &[serde_json::Value],
    batch: bool,
) -> Result<()> {
    let payload = if batch {
        serde_json::to_vec(responses)?
    } else {
        serde_json::to_vec(&responses[0])?
    };
    client_stream.write_all(&payload).await?;
    client_stream.write_all(b"\n").await?;
    client_stream.flush().await?;
    Ok(())
}

async fn write_json_rpc_response(
    client_stream: &mut tokio::net::TcpStream,
    response: &serde_json::Value,
) -> Result<()> {
    client_stream
        .write_all(&serde_json::to_vec(response)?)
        .await?;
    client_stream.write_all(b"\n").await?;
    client_stream.flush().await?;
    Ok(())
}

struct BroadcastPreview {
    txid: String,
}

fn quick_locktime_hint(tx_hex: &str) -> String {
    let tx_hex_clean = tx_hex.trim();
    let Ok(raw) = hex::decode(tx_hex_clean) else {
        return "?".to_string();
    };
    let mut cursor = std::io::Cursor::new(&raw);
    let Ok(tx) = Transaction::consensus_decode(&mut cursor) else {
        return "?".to_string();
    };
    let nlocktime: u32 = match tx.lock_time {
        bitcoin::absolute::LockTime::Blocks(height) => height.to_consensus_u32(),
        bitcoin::absolute::LockTime::Seconds(time) => time.to_consensus_u32(),
        _ => 0,
    };
    if nlocktime == 0 {
        "0 (manual/immediate)".to_string()
    } else if nlocktime > 500_000_000 {
        format!("{} (timestamp)", nlocktime)
    } else {
        format!("{} (block height)", nlocktime)
    }
}

fn quick_broadcast_preview(tx_hex: &str) -> Result<BroadcastPreview> {
    let tx_hex_clean = tx_hex.trim();
    hex::decode(tx_hex_clean).context("Invalid transaction hex")?;
    let txid = pending::compute_txid(tx_hex_clean)?;
    Ok(BroadcastPreview { txid })
}

async fn notify_subscribed_pending_mempool(
    client_stream: &mut tokio::net::TcpStream,
    subscribed: &HashSet<String>,
    pool_manager: &PoolManager,
    txid_hint: &str,
) -> Result<()> {
    for sh in subscribed {
        let mut pending = pool_manager.get_pending_txids_for_scripthash(sh);
        if !pending.iter().any(|t| t == txid_hint) {
            pending.push(txid_hint.to_string());
        }
        let Some(hash) = pending::compute_modified_status_hash(vec![], sh, &pending) else {
            continue;
        };
        tracing::info!(
            "Mempool notify for scripthash {} (txid={})",
            &sh[..sh.len().min(16)],
            &txid_hint[..txid_hint.len().min(16)]
        );
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "blockchain.scripthash.subscribe",
            "params": [sh, hash]
        });
        client_stream
            .write_all(&serde_json::to_vec(&notification)?)
            .await?;
        client_stream.write_all(b"\n").await?;
        client_stream.flush().await?;
    }
    Ok(())
}

async fn intercept_and_handle_broadcast(
    client_stream: &mut tokio::net::TcpStream,
    id: serde_json::Value,
    hex_param: String,
    pool_manager: &Arc<PoolManager>,
    config: &Arc<Mutex<Config>>,
    indexer_url: &str,
    source_label: &str,
    session: &mut SessionState,
) -> Result<()> {
    session.broadcast_intercepted = true;
    let lock_hint = quick_locktime_hint(&hex_param);
    tracing::info!(
        "INTERCEPTED broadcast RPC (hex len={}, nLockTime={})",
        hex_param.len(),
        lock_hint
    );

    let hex_quick = hex_param.clone();
    let preview = match tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio::task::spawn_blocking(move || quick_broadcast_preview(&hex_quick)),
    )
    .await
    {
        Ok(Ok(Ok(p))) => p,
        Ok(Ok(Err(e))) => {
            tracing::error!("Broadcast preview failed: {}", e);
            write_json_rpc_response(
                client_stream,
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": { "code": -25, "message": e.to_string() },
                    "id": id
                }),
            )
            .await?;
            return Ok(());
        }
        Ok(Err(e)) => {
            tracing::error!("Broadcast preview task failed: {}", e);
            write_json_rpc_response(
                client_stream,
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": { "code": -25, "message": e.to_string() },
                    "id": id
                }),
            )
            .await?;
            return Ok(());
        }
        Err(_) => {
            tracing::error!("Broadcast preview timed out");
            write_json_rpc_response(
                client_stream,
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": { "code": -25, "message": "Invalid transaction hex" },
                    "id": id
                }),
            )
            .await?;
            return Ok(());
        }
    };

    // Invariant 5: Sparrow polls immediately after ack — set before ack, never wait on ingest.
    session.recent_broadcast_txid = Some(preview.txid.clone());

    write_json_rpc_response(
        client_stream,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "result": preview.txid,
            "id": id
        }),
    )
    .await?;
    tracing::info!("Broadcast ack sent to wallet (pre-ingest), txid={}", preview.txid);

    // Ingest in background — never block read loop or tokio workers (avoids accept starvation).
    let pm = pool_manager.clone();
    let cfg = config.clone();
    let src = source_label.to_string();
    let hex_owned = hex_param;
    let preview_txid = preview.txid.clone();

    tokio::spawn(async move {
        let pm_enrich = pm.clone();
        let ingest = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::task::spawn_blocking(move || {
                handle_broadcast(&hex_owned, &pm, &cfg, &src)
            }),
        )
        .await;

        match ingest {
            Ok(Ok(Ok(result))) => {
                tracing::info!("Broadcast ingested into pool, txid={}", result.txid);
                if result.defer_input_enrichment {
                    let url = pm_enrich.get_indexer_url().unwrap_or_default();
                    let txid = result.txid.clone();
                    let hex = result.tx_hex.clone();
                    tokio::task::spawn_blocking(move || {
                        enrich_pending_input_scripthashes(&pm_enrich, &txid, &hex, &url);
                    });
                }
            }
            Ok(Ok(Err(e))) => {
                tracing::error!("Broadcast ingest failed for {}: {}", preview_txid, e);
            }
            Err(_) => {
                tracing::error!("Broadcast ingest timed out for {}", preview_txid);
            }
            Ok(Err(e)) => {
                tracing::error!("Broadcast ingest task failed for {}: {}", preview_txid, e);
            }
        }
    });

    Ok(())
}

fn enrich_pending_input_scripthashes(
    pool_manager: &PoolManager,
    txid: &str,
    tx_hex: &str,
    indexer_url: &str,
) {
    let indexer_addr = pending::strip_indexer_host(indexer_url);
    let lookup = |id: &str| pool_manager.lookup_tx_hex(id);
    let output_sh = pending::extract_affected_scripthashes_opts(tx_hex, "", true).unwrap_or_default();
    match pending::enrich_input_scripthashes(
        tx_hex,
        &indexer_addr,
        std::time::Duration::from_secs(8),
        Some(&lookup),
    ) {
        Ok(extra) if !extra.is_empty() => {
            pool_manager.merge_pending_scripthashes(txid, &extra);
            tracing::info!(
                "Background input scripthash enrichment for {}: {} output + {} input",
                txid,
                output_sh.len(),
                extra.len()
            );
        }
        Ok(_) => {
            tracing::debug!("Background input enrichment for {} found no extra scripthashes", txid);
        }
        Err(e) => {
            tracing::warn!(
                "Background input scripthash enrichment failed for {}: {}",
                txid,
                e
            );
        }
    }
}

pub struct ElectrumServer {
    pool_manager: Arc<PoolManager>,
    config: Arc<Mutex<Config>>,
}

impl ElectrumServer {
    pub fn new(pool_manager: Arc<PoolManager>, config: Arc<Mutex<Config>>) -> Self {
        Self {
            pool_manager,
            config,
        }
    }

    pub async fn start(&self) -> Result<()> {
        let (host, port, liana_port) = {
            let config = self.config.lock().map_err(|e| anyhow::anyhow!("Config lock: {}", e))?;
            (
                config.electrum_server.host.clone(),
                config.electrum_server.port,
                config.electrum_server.liana_port,
            )
        };

        let pool_sparrow = self.pool_manager.clone();
        let config_sparrow = self.config.clone();
        let host_sparrow = host.clone();
        let pool_warm = self.pool_manager.clone();
        let config_warm = self.config.clone();
        tokio::spawn(async move {
            for attempt in 1..=30 {
                let delay = if attempt == 1 { 1 } else { 10 };
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                let url = resolve_live_indexer_url(&pool_warm, &config_warm);
                if url.is_empty() {
                    continue;
                }
                let pm = pool_warm.clone();
                let u = url.clone();
                let warmed = tokio::task::spawn_blocking(move || {
                    warm_chain_tip_cache(&u, &pm);
                    pm.get_cached_chain_tip().is_some()
                })
                .await
                .unwrap_or(false);
                if warmed {
                    break;
                }
            }
        });
        tokio::spawn(async move {
            if let Err(e) =
                run_electrum_listener(host_sparrow, port, "sparrow", pool_sparrow, config_sparrow)
                    .await
            {
                tracing::error!("Sparrow Electrum listener error: {}", e);
            }
        });
        tracing::info!("Electrum server (Sparrow) listening on {}:{}", host, port);

        if let Some(liana_port) = liana_port {
            let pool_liana = self.pool_manager.clone();
            let config_liana = self.config.clone();
            let host_liana = host.clone();
            tokio::spawn(async move {
                if let Err(e) = run_electrum_listener(
                    host_liana,
                    liana_port,
                    "liana",
                    pool_liana,
                    config_liana,
                )
                .await
                {
                    tracing::error!("Liana Electrum listener error: {}", e);
                }
            });
            tracing::info!("Electrum server (Liana) listening on {}:{}", host, liana_port);
        } else {
            tracing::info!(
                "No Liana Electrum port configured (set BROADCAST_POOL_LIANA_ELECTRUM_PORT or electrum_server.liana_port)"
            );
        }

        // Keep the task alive (listeners run in spawned tasks).
        std::future::pending::<()>().await;
        Ok(())
    }
}

async fn run_electrum_listener(
    host: String,
    port: u16,
    source_label: &'static str,
    pool_manager: Arc<PoolManager>,
    config: Arc<Mutex<Config>>,
) -> Result<()> {
    let addr = format!("{}:{}", host, port);
    let (conn_tx, mut conn_rx) =
        tokio::sync::mpsc::channel::<(std::net::TcpStream, std::net::SocketAddr)>(64);

    let accept_addr = addr.clone();
    let accept_label = source_label;
    let version = env!("CARGO_PKG_VERSION").to_string();

    std::thread::Builder::new()
        .name(format!("electrum-accept-{source_label}"))
        .spawn(move || run_electrum_accept_thread(accept_addr, accept_label, version, conn_tx))
        .map_err(|e| anyhow::anyhow!("spawn electrum accept thread: {}", e))?;

    tracing::info!(
        "Electrum accept dispatcher [{}] ready on {} (v{})",
        source_label,
        addr,
        env!("CARGO_PKG_VERSION")
    );

    while let Some((std_stream, peer_addr)) = conn_rx.recv().await {
        let client_stream = match tokio::net::TcpStream::from_std(std_stream) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("TcpStream::from_std failed for {}: {}", peer_addr, e);
                continue;
            }
        };
        tracing::info!(
            "Electrum client connected from {} [{}] (broadcast-pool v{})",
            peer_addr,
            source_label,
            env!("CARGO_PKG_VERSION")
        );
        if source_label == "sparrow" {
            tracing::warn!(
                "Sparrow [{}]: disable Tor proxy in Settings→Network or broadcasts bypass this pool (mempool.space) and txs never appear here",
                peer_addr
            );
        }
        let pool_manager = pool_manager.clone();
        let config = config.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(
                pool_manager,
                config,
                client_stream,
                peer_addr,
                source_label,
            )
            .await
            {
                tracing::error!("Connection error ({}): {}", source_label, e);
            }
        });
    }

    Ok(())
}

fn set_tcp_keepalive(stream: &std::net::TcpStream) {
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(60))
        .with_interval(std::time::Duration::from_secs(10));
    let _ = socket2::SockRef::from(stream).set_tcp_keepalive(&keepalive);
}

fn run_electrum_accept_thread(
    addr: String,
    source_label: &'static str,
    version: String,
    conn_tx: tokio::sync::mpsc::Sender<(std::net::TcpStream, std::net::SocketAddr)>,
) {
    let listener = match std::net::TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(
                "Electrum bind failed [{}] {}: {}",
                source_label,
                addr,
                e
            );
            return;
        }
    };
    tracing::info!(
        "Electrum listener [{}] bound on {} (dedicated accept thread v{})",
        source_label,
        addr,
        version
    );
    listener.set_nonblocking(true).ok();
    let mut last_heartbeat = std::time::Instant::now();
    loop {
        if last_heartbeat.elapsed() >= std::time::Duration::from_secs(60) {
            tracing::info!("Electrum accept thread alive [{}]", source_label);
            last_heartbeat = std::time::Instant::now();
        }
        match listener.accept() {
            Ok((stream, peer)) => {
                tracing::info!(
                    "TCP accepted from {} [{}] (v{}, pre-dispatch)",
                    peer,
                    source_label,
                    version
                );
                set_tcp_keepalive(&stream);
                if conn_tx.blocking_send((stream, peer)).is_err() {
                    tracing::warn!("Electrum accept channel closed [{}]", source_label);
                    break;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Err(e) => {
                tracing::error!("Failed to accept connection ({}): {}", source_label, e);
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    }
}

async fn flush_deferred_indexer_out(
    session: &mut SessionState,
    client_stream: &mut tokio::net::TcpStream,
) -> Result<()> {
    session.client_busy = false;
    for out in session.deferred_indexer_out.drain(..) {
        client_stream.write_all(&out).await?;
    }
    client_stream.flush().await?;
    Ok(())
}

async fn process_client_line(
    line_bytes: &[u8],
    line_str: &str,
    peer_addr: std::net::SocketAddr,
    pool_manager: &Arc<PoolManager>,
    config: &Arc<Mutex<Config>>,
    indexer_url: &str,
    indexer_stream: &mut Option<IndexerStream>,
    session: &mut SessionState,
    source_label: &str,
    client_stream: &mut tokio::net::TcpStream,
) -> Result<()> {
    if line_str.trim().is_empty() {
        return Ok(());
    }

    session.rpc_lines_handled += 1;

    // Handshake RPCs: always answer locally (never block on upstream electrs).
    if let Ok(handshake) = parse_subrequests(line_str) {
        if !handshake.is_empty()
            && handshake
                .iter()
                .all(|r| is_local_handshake_method(&r.method))
        {
            let mut responses = Vec::with_capacity(handshake.len());
            for req in &handshake {
                if let Some(resp) = local_electrum_response(req, config) {
                    responses.push(resp);
                } else {
                    responses.clear();
                    break;
                }
            }
            if responses.len() == handshake.len() {
                let batch = line_is_batch(line_str);
                write_client_responses(client_stream, &responses, batch).await?;
                tracing::info!(
                    "Electrum RPC from {}: [{}] (local handshake)",
                    peer_addr,
                    handshake
                        .iter()
                        .map(|r| r.method.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                return Ok(());
            }
        }
    }

    // Instant headers.subscribe when cache is warm (Sparrow Test Connection).
    if let Ok(reqs) = parse_subrequests(line_str) {
        if reqs.len() == 1 && reqs[0].method == "blockchain.headers.subscribe" {
            if let Some((height, hex)) = pool_manager.get_cached_chain_tip() {
                spawn_chain_tip_refresh(indexer_url, pool_manager);
                write_json_rpc_response(
                    client_stream,
                    &serde_json::json!({
                        "jsonrpc": "2.0",
                        "result": { "height": height, "hex": hex },
                        "id": reqs[0].id
                    }),
                )
                .await?;
                tracing::info!(
                    "Electrum RPC from {}: [blockchain.headers.subscribe] (instant cache height={})",
                    peer_addr,
                    height
                );
                return Ok(());
            }
        }
    }

    log_incoming_client_line(peer_addr, line_str, source_label);

    if line_str.len() > 280 {
        tracing::info!(
            "Electrum large RPC from {} [{}] len={} methods={:?}",
            peer_addr,
            source_label,
            line_str.len(),
            peek_line_methods(line_str)
        );
    }

    if line_mentions_tx_rpc(line_str) {
        if let Some(methods) = peek_line_methods(line_str) {
            tracing::info!(
                "Electrum tx/broadcast methods from {}: [{}]",
                peer_addr,
                methods.join(", ")
            );
        }
    }

    if let Some((id, hex_param)) = extract_broadcast_hex(line_str) {
        if let Err(e) = intercept_and_handle_broadcast(
            client_stream,
            id,
            hex_param,
            pool_manager,
            config,
            indexer_url,
            source_label,
            session,
        )
        .await
        {
            tracing::error!("Broadcast handler error ({}): {}", source_label, e);
        }
        return Ok(());
    }

    if line_looks_like_broadcast(line_str) {
        let id = line_json_rpc_id(line_str);
        tracing::error!(
            "Unparsed broadcast request (not forwarding to indexer, id={}): {}",
            id,
            &line_str[..line_str.len().min(240)]
        );
        write_json_rpc_response(
            client_stream,
            &serde_json::json!({
                "jsonrpc": "2.0",
                "error": { "code": -32602, "message": "Invalid broadcast params" },
                "id": id
            }),
        )
        .await?;
        return Ok(());
    }

    let batch = line_is_batch(line_str);
    let subrequests = match parse_subrequests(line_str) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                "Invalid client JSON from {} (len={}): {}",
                peer_addr,
                line_str.len(),
                e
            );
            if line_mentions_tx_rpc(line_str) {
                tracing::error!(
                    "Invalid JSON on tx/broadcast line from {}: {}",
                    peer_addr,
                    &line_str[..line_str.len().min(200)]
                );
            }
            let id = line_json_rpc_id(line_str);
            write_json_rpc_response(
                client_stream,
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": { "code": -32700, "message": "Parse error" },
                    "id": id
                }),
            )
            .await?;
            return Ok(());
        }
    };

    // Broadcast: always intercept locally (never proxy to electrs — non-final txs hang/reject).
    if subrequests.iter().any(|r| is_broadcast_method(&r.method)) {
        let broadcast_req = subrequests
            .iter()
            .find(|r| is_broadcast_method(&r.method))
            .expect("checked any");
        if let Some(hex_param) = broadcast_req
            .params
            .as_ref()
            .and_then(params_first_string)
        {
            if let Err(e) = intercept_and_handle_broadcast(
                client_stream,
                broadcast_req.id.clone(),
                hex_param,
                pool_manager,
                config,
                indexer_url,
                source_label,
                session,
            )
            .await
            {
                tracing::error!("Broadcast handler error ({}): {}", source_label, e);
            }
        } else {
            write_json_rpc_response(
                client_stream,
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": { "code": -32602, "message": "Invalid broadcast params" },
                    "id": broadcast_req.id.clone()
                }),
            )
            .await?;
        }
        return Ok(());
    }

    tracing::info!(
        "Electrum RPC from {}: [{}]",
        peer_addr,
        subrequests
            .iter()
            .map(|r| r.method.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    session.client_busy = true;

    // Always answer each RPC immediately (Sparrow sends one method per line, not batches).
    let mut responses = Vec::with_capacity(subrequests.len());
    for req in &subrequests {
        match handle_one_subrequest(
            req,
            pool_manager,
            config,
            indexer_url,
            indexer_stream,
            session,
            source_label,
            client_stream,
        )
        .await
        {
            Ok(resp) => responses.push(resp),
            Err(e) => {
                tracing::error!("RPC handler error for {}: {}", req.method, e);
                responses.push(serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": { "code": -32603, "message": e.to_string() },
                    "id": req.id
                }));
            }
        }
    }
    write_client_responses(client_stream, &responses, batch).await?;
    flush_deferred_indexer_out(session, client_stream).await?;
    Ok(())
}

async fn handle_connection(
    pool_manager: Arc<PoolManager>,
    config: Arc<Mutex<Config>>,
    mut client_stream: tokio::net::TcpStream,
    peer_addr: std::net::SocketAddr,
    source_label: &'static str,
) -> Result<()> {
    let mut indexer_stream: Option<IndexerStream> = None;

    tracing::info!(
        "Electrum session started for {} [{}] (broadcast-pool v{}, sync RPC responses)",
        peer_addr,
        source_label,
        env!("CARGO_PKG_VERSION")
    );

    let mut client_buf = Vec::new();
    let mut session = SessionState::new();

    loop {
        let n = client_stream.read_buf(&mut client_buf).await?;
        if n == 0 {
            session.log_disconnect_summary(peer_addr, source_label);
            break;
        }
        while let Some((line_bytes, line_str)) = pop_client_line(&mut client_buf) {
            let indexer_url = resolve_live_indexer_url(&pool_manager, &config);

            process_client_line(
                &line_bytes,
                &line_str,
                peer_addr,
                &pool_manager,
                &config,
                &indexer_url,
                &mut indexer_stream,
                &mut session,
                source_label,
                &mut client_stream,
            )
            .await?;
        }

        if !client_buf.is_empty() && client_buf.len() >= 4096 {
            let preview = String::from_utf8_lossy(&client_buf[..client_buf.len().min(120)]);
            if line_mentions_tx_rpc(&preview) || preview.contains("method") {
                tracing::warn!(
                    "Electrum client {} [{}] buffered {} bytes without newline (preview={})",
                    peer_addr,
                    source_label,
                    client_buf.len(),
                    preview
                );
            }
        }
    }

    Ok(())
}

fn find_message_end(data: &[u8]) -> usize {
    data.iter().position(|&b| b == b'\n' || b == b'\0').map(|p| p + 1).unwrap_or(0)
}

fn trim_end(data: &[u8]) -> &[u8] {
    let mut end = data.len();
    while end > 0 {
        let b = data[end - 1];
        if b == b'\n' || b == b'\r' || b == b'\0' {
            end -= 1;
        } else {
            break;
        }
    }
    &data[..end]
}

fn parse_json_rpc(data: &[u8]) -> Result<Option<(JsonRpcRequest, usize)>> {
    if data.is_empty() {
        return Ok(None);
    }

    let trimmed = trim_end(data);
    if trimmed.is_empty() {
        return Ok(None);
    }

    match std::str::from_utf8(trimmed) {
        Ok(json_str) => {
            match serde_json::from_str::<JsonRpcRequest>(json_str) {
                Ok(request) => Ok(Some((request, data.len()))),
                Err(e) => {
                    tracing::debug!("Failed to parse JSON: {} - data: {:?}", e, &trimmed[..trimmed.len().min(100)]);
                    Ok(None)
                }
            }
        }
        Err(_) => {
            if let Some(pos) = trimmed.iter().position(|&b| b == b'{') {
                let remaining = &trimmed[pos..];
                if let Ok(json_str) = std::str::from_utf8(remaining) {
                    match serde_json::from_str::<JsonRpcRequest>(json_str) {
                        Ok(request) => {
                            tracing::debug!("Skipped {} garbage bytes before JSON", pos);
                            return Ok(Some((request, data.len())));
                        }
                        Err(_) => {}
                    }
                }
            }
            tracing::debug!("Invalid UTF-8 bytes in Electrum request: first bytes: {:?}", &trimmed[..trimmed.len().min(20)]);
            Ok(None)
        }
    }
}

fn resolve_ingest_plan(
    source_label: &str,
    nlocktime: u32,
    config: &Config,
) -> (BroadcastMode, Option<chrono::DateTime<chrono::Utc>>) {
    if source_label == "liana" {
        tracing::info!("Liana ingest → manual scheduling (pending until user sets date/price)");
        return (BroadcastMode::Manual, None);
    }
    if source_label == "sparrow" && nlocktime == 0 {
        tracing::info!("Sparrow ingest with nLockTime disabled → manual scheduling");
        return (BroadcastMode::Manual, None);
    }
    // Timestamp nLockTime (Sparrow MTP-by-date or block MTP converted to unix time).
    if nlocktime > 500_000_000 {
        let scheduled = chrono::DateTime::from_timestamp(nlocktime as i64, 0);
        tracing::info!(
            "Timestamp nLockTime {} → scheduled (MTP enforced when broadcasting)",
            nlocktime
        );
        return (BroadcastMode::Scheduled, scheduled);
    }
    // Block-height nLockTime — no indexer RPC at ingest; scheduler waits for chain height.
    if nlocktime > 0 && nlocktime < 500_000_000 {
        tracing::info!(
            "Block-height nLockTime {} → by_block mode",
            nlocktime
        );
        return (BroadcastMode::ByBlock, None);
    }
    resolve_broadcast_plan(nlocktime, config)
}

fn resolve_broadcast_plan(
    nlocktime: u32,
    config: &Config,
) -> (BroadcastMode, Option<chrono::DateTime<chrono::Utc>>) {
    use chrono::Utc;
    use rand::Rng;

    match config.schedule.broadcast_mode {
        BroadcastMode::Immediate => (BroadcastMode::Immediate, Some(Utc::now())),
        BroadcastMode::Scheduled => {
            if let Some(ref dt_str) = config.schedule.scheduled_datetime {
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(dt_str) {
                    return (BroadcastMode::Scheduled, Some(dt.with_timezone(&Utc)));
                }
                if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%dT%H:%M") {
                    return (BroadcastMode::Scheduled, Some(dt.and_utc()));
                }
            }
            let min = config.schedule.min_delay_hours;
            let max = config.schedule.max_delay_hours.max(min);
            let delay = rand::thread_rng().gen_range(min..=max);
            (
                BroadcastMode::Scheduled,
                Utc::now().checked_add_signed(chrono::Duration::hours(delay as i64)),
            )
        }
        BroadcastMode::ByBlock => {
            if nlocktime > 0 && nlocktime < 500_000_000 {
                (BroadcastMode::ByBlock, None)
            } else {
                tracing::warn!(
                    "by_block mode but TX has no block-height nLockTime (nlocktime={}); staying pending",
                    nlocktime
                );
                (BroadcastMode::ByBlock, None)
            }
        }
        BroadcastMode::Manual => (BroadcastMode::Manual, None),
    }
}

fn handle_broadcast(
    tx_hex: &str,
    pool_manager: &Arc<PoolManager>,
    config: &Arc<Mutex<Config>>,
    source_label: &str,
) -> Result<BroadcastHandleResult> {
    tracing::info!("handle_broadcast called with tx_hex length: {}", tx_hex.len());

    let tx_hex_clean = tx_hex.trim();
    if tx_hex.len() != tx_hex_clean.len() {
        tracing::warn!("Tx hex had {} extra chars (whitespace?), original length: {}, cleaned length: {}",
            tx_hex.len() - tx_hex_clean.len(), tx_hex.len(), tx_hex_clean.len());
    }
    tracing::debug!("First 10 chars of tx_hex: {:?}", &tx_hex_clean[..tx_hex_clean.len().min(10)]);
    tracing::debug!("Last 10 chars of tx_hex: {:?}", &tx_hex_clean[tx_hex_clean.len().saturating_sub(10)..]);

    let raw = hex::decode(tx_hex_clean).context("Invalid transaction hex")?;
    let mut cursor = std::io::Cursor::new(&raw);
    let tx = Transaction::consensus_decode(&mut cursor).context("Failed to decode transaction")?;

    let txid = pending::compute_txid(tx_hex_clean)?;
    tracing::info!("Decoded transaction txid (electrum): {}", txid);

    let nlocktime: u32 = match tx.lock_time {
        bitcoin::absolute::LockTime::Blocks(height) => height.to_consensus_u32(),
        bitcoin::absolute::LockTime::Seconds(time) => time.to_consensus_u32(),
        _ => 0,
    };
    tracing::info!("Transaction locktime: {} ({})", nlocktime, if nlocktime == 0 { "no lock" } else if nlocktime > 500_000_000 { "timestamp" } else { "block height" });

    let network = {
        let cfg = config.lock().map_err(|e| anyhow::anyhow!("Config lock: {}", e))?;
        cfg.network.network_type.data_dir_name().to_string()
    };

    let (broadcast_mode, scheduled_time) = {
        let cfg = config.lock().map_err(|e| anyhow::anyhow!("Config lock: {}", e))?;
        resolve_ingest_plan(source_label, nlocktime, &cfg)
    };

    tracing::info!(
        "Broadcast plan: source={}, mode={}, scheduled={:?}, nlocktime={}",
        source_label,
        broadcast_mode,
        scheduled_time,
        nlocktime
    );

    let new_tx = NewBroadcastTx {
        tx_hex: tx_hex_clean.to_string(),
        network,
        nlocktime: if nlocktime > 0 {
            Some(nlocktime as u64)
        } else {
            None
        },
        broadcast_mode: Some(broadcast_mode.to_string()),
        scheduled_time,
        target_fee_rate: None,
        source_label: Some(source_label.to_string()),
        destination_address: None,
        utxo_count: Some(tx.input.len() as i32),
        total_value_btc: None,
        replacement_of: None,
    };

    tracing::info!("Calling pool_manager.import_transaction...");
    let imported_tx = pool_manager.import_transaction(&new_tx)?;

    tracing::info!(
        "Imported transaction from {}: txid={} (mode: {}, pool_id: {})",
        source_label,
        txid,
        broadcast_mode,
        imported_tx.id
    );

    fn store_pending_outputs(
        pool_manager: &PoolManager,
        txid: &str,
        tx_hex: &str,
    ) -> Vec<String> {
        let output_sh = pending::extract_affected_scripthashes_opts(tx_hex, "", true)
            .unwrap_or_else(|e| {
                tracing::warn!("Could not derive output scripthashes for {}: {}", txid, e);
                Vec::new()
            });
        let outputs: Vec<PendingTxOutput> = pending::extract_outputs(tx_hex)
            .unwrap_or_default()
            .into_iter()
            .map(|(output_index, value, scripthash)| PendingTxOutput {
                output_index,
                value,
                scripthash,
            })
            .collect();
        pool_manager.store_pending_tx(txid, tx_hex, output_sh.clone(), outputs);
        if output_sh.is_empty() {
            tracing::warn!(
                "Stored {} without output scripthashes — Sparrow mempool poll may fail until enrichment",
                txid
            );
        }
        output_sh
    }

    // Invariant 4: outputs-only before Sparrow ack; input enrichment runs in background after ack.
    let scripthashes = store_pending_outputs(pool_manager, &txid, tx_hex_clean);

    if broadcast_mode == BroadcastMode::Immediate {
        if let Err(e) = pool_manager.mark_as_due(&imported_tx.id) {
            tracing::warn!("Failed to mark immediate tx as due: {}", e);
        }
    }

    tracing::info!(
        "Retained tx {} in virtual mempool (mode: {}, pool_id: {}, {} output sh)",
        txid,
        broadcast_mode,
        imported_tx.id,
        scripthashes.len()
    );

    Ok(BroadcastHandleResult {
        txid,
        tx_hex: tx_hex_clean.to_string(),
        retained: true,
        affected_scripthashes: scripthashes,
        defer_input_enrichment: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_TX: &str = "0100000002f327e86da3e66bd20e1129b1fb36d07056f0b9a117199e759396526b8f3a20780000000000fffffffff0ede03d75050f20801d50358829ae02c058e8677d2cc74df51f738285013c260000000000ffffffff02f028d6dc010000001976a914ffb035781c3c69e076d48b60c3d38592e7ce06a788ac00ca9a3b000000001976a914fa5139067622fd7e1e722a05c17c2bb7d5fd6df088ac00000000";

    #[test]
    fn extract_broadcast_hex_array_params() {
        let line = format!(
            r#"{{"jsonrpc":"2.0","method":"blockchain.transaction.broadcast","params":["{}"],"id":42}}"#,
            SAMPLE_TX
        );
        let (id, hex) = extract_broadcast_hex(&line).expect("array params");
        assert_eq!(id, serde_json::json!(42));
        assert_eq!(hex, SAMPLE_TX);
    }

    #[test]
    fn extract_broadcast_hex_string_params() {
        let line = format!(
            r#"{{"jsonrpc":"2.0","method":"blockchain.transaction.broadcast","params":"{}","id":7}}"#,
            SAMPLE_TX
        );
        let (id, hex) = extract_broadcast_hex(&line).expect("string params");
        assert_eq!(id, serde_json::json!(7));
        assert_eq!(hex, SAMPLE_TX);
    }

    #[test]
    fn extract_broadcast_hex_sparrow_single_param() {
        // Sparrow SimpleElectrumServerRpc uses .params(txHex) — often serializes as bare string.
        let line = format!(
            r#"{{"jsonrpc":"2.0","method":"blockchain.transaction.broadcast","params":"{}","id":99}}"#,
            SAMPLE_TX
        );
        assert!(line_looks_like_broadcast(&line));
        assert!(extract_broadcast_hex(&line).is_some());
    }

    #[test]
    fn extract_broadcast_package_first_tx() {
        let line = format!(
            r#"{{"jsonrpc":"2.0","method":"blockchain.transaction.broadcast_package","params":[["{}","02000000ffffffff0100"]],"id":3}}"#,
            SAMPLE_TX
        );
        let (_, hex) = extract_broadcast_hex(&line).expect("package");
        assert_eq!(hex, SAMPLE_TX);
    }

    #[test]
    fn resolve_ingest_timestamp_locktime_is_scheduled() {
        let cfg: Config = toml::from_str(
            r#"
            [network]
            type = "signet"
            [pool]
            max_size_kb = 300
            rebroadcast_interval_minutes = 30
            expiry_days = 14
            [privacy]
            use_tor = false
            tor_socks_port = 9050
            rotate_identity_per_tx = false
            "#,
        )
        .expect("test config");
        let (mode, sched) = resolve_ingest_plan("sparrow", 1_750_000_000, &cfg);
        assert_eq!(mode, BroadcastMode::Scheduled);
        assert!(sched.is_some());
    }

    #[test]
    fn resolve_ingest_block_height_locktime_is_by_block() {
        let cfg: Config = toml::from_str(
            r#"
            [network]
            type = "signet"
            [pool]
            max_size_kb = 300
            rebroadcast_interval_minutes = 30
            expiry_days = 14
            [privacy]
            use_tor = false
            tor_socks_port = 9050
            rotate_identity_per_tx = false
            "#,
        )
        .expect("test config");
        let (mode, _) = resolve_ingest_plan("sparrow", 900_000, &cfg);
        assert_eq!(mode, BroadcastMode::ByBlock);
    }

    #[test]
    fn local_fast_response_estimatefee() {
        let req = JsonRpcRequest {
            jsonrpc: Some("2.0".into()),
            method: "blockchain.estimatefee".into(),
            params: Some(serde_json::json!([6])),
            id: serde_json::json!(1),
        };
        let resp = local_fast_response(&req).expect("estimatefee");
        assert!(resp.get("result").is_some());
    }

    #[test]
    fn parse_headers_subscribe_rejects_height_zero() {
        let zero = serde_json::json!({"result":{"height":0,"hex":"abc"}});
        assert!(parse_headers_subscribe_result(&zero).is_none());
        let ok = serde_json::json!({"result":{"height":100,"hex":"deadbeef"}});
        assert_eq!(
            parse_headers_subscribe_result(&ok),
            Some((100, "deadbeef".to_string()))
        );
    }

    #[test]
    fn fast_scripthash_extract_is_outputs_only() {
        use crate::pool::virtual_mempool as pending;
        let fast = pending::extract_affected_scripthashes_opts(SAMPLE_TX, "", true).expect("fast");
        assert_eq!(fast.len(), 2, "sample tx has two outputs");
    }
}