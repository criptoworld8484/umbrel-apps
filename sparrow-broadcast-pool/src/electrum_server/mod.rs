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
use tokio::net::TcpListener;

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

/// Parse `blockchain.transaction.broadcast` even when strict struct decode would fail.
/// Supports single requests, batch arrays, and params as `[hex]` or `"hex"`.
fn params_first_string(params: &serde_json::Value) -> Option<String> {
    match params {
        serde_json::Value::Array(arr) => arr.first()?.as_str().map(|s| s.to_string()),
        serde_json::Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

fn json_rpc_id(v: &serde_json::Value) -> serde_json::Value {
    v.get("id").cloned().unwrap_or(serde_json::Value::Null)
}

fn extract_broadcast_from_value(v: &serde_json::Value) -> Option<(serde_json::Value, String)> {
    if v.get("method")?.as_str()? != "blockchain.transaction.broadcast" {
        return None;
    }
    let id = json_rpc_id(v);
    let hex = params_first_string(v.get("params")?)?;
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
    line.contains("blockchain.transaction.broadcast")
}

fn line_json_rpc_id(line: &str) -> serde_json::Value {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(line.trim()) else {
        return serde_json::Value::Null;
    };
    if let Some(arr) = v.as_array() {
        for item in arr {
            if item.get("method").and_then(|m| m.as_str()) == Some("blockchain.transaction.broadcast")
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
    retained: bool,
    affected_scripthashes: Vec<String>,
}

struct SessionState {
    subscribed_scripthashes: HashSet<String>,
    pending_methods: HashMap<serde_json::Value, (String, String)>,
    /// While handling a client JSON-RPC line, defer upstream notifications to avoid interleaving.
    client_busy: bool,
    deferred_indexer_out: Vec<Vec<u8>>,
}

impl SessionState {
    fn new() -> Self {
        Self {
            subscribed_scripthashes: HashSet::new(),
            pending_methods: HashMap::new(),
            client_busy: false,
            deferred_indexer_out: Vec::new(),
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

fn fetch_scripthash_history_sync(scripthash: &str, indexer_url: &str) -> Vec<serde_json::Value> {
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "blockchain.scripthash.get_history",
        "params": [scripthash],
        "id": 999
    });
    forward_to_indexer_sync(&request.to_string(), indexer_url)
        .and_then(|resp| {
            serde_json::from_str::<serde_json::Value>(&resp)
                .ok()
                .and_then(|v| v.get("result").and_then(|r| r.as_array()).cloned())
        })
        .unwrap_or_default()
}

fn modify_upstream_response(
    msg: &mut serde_json::Value,
    method: &str,
    scripthash: &str,
    pool_manager: &PoolManager,
    indexer_url: &str,
) {
    match method {
        "blockchain.scripthash.get_history" => {
            if let Some(result) = msg.get_mut("result").and_then(|r| r.as_array_mut()) {
                let history = result.clone();
                let pending = pool_manager.get_pending_txids_for_scripthash(scripthash);
                *result = pending::inject_in_history(history, scripthash, &pending);
            }
        }
        "blockchain.scripthash.subscribe" => {
            let pending = pool_manager.get_pending_txids_for_scripthash(scripthash);
            if pending.is_empty() {
                return;
            }
            let real_history = fetch_scripthash_history_sync(scripthash, indexer_url);
            if let Some(hash) =
                pending::compute_modified_status_hash(real_history, scripthash, &pending)
            {
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
                let pending = pool_manager.get_pending_txids_for_scripthash(scripthash);
                *result = pending::inject_get_mempool(result.clone(), &pending);
            }
        }
        _ => {}
    }
}

fn modify_upstream_notification(
    msg: &mut serde_json::Value,
    pool_manager: &PoolManager,
    indexer_url: &str,
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
    let real_history = fetch_scripthash_history_sync(&scripthash, indexer_url);
    let pending = pool_manager.get_pending_txids_for_scripthash(&scripthash);
    if let Some(hash) = pending::compute_modified_status_hash(real_history, &scripthash, &pending) {
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
                    modify_upstream_response(&mut msg, &method, &scripthash, pool_manager, indexer_url);
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
            "result": format!("broadcast-pool {} — Bitcoin transaction pool for Sparrow", env!("CARGO_PKG_VERSION")),
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
                    "hash_function": "sha256"
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

async fn forward_subrequest_sync(
    request: &JsonRpcRequest,
    indexer_url: &str,
    session: &mut SessionState,
    pool_manager: &PoolManager,
) -> Result<serde_json::Value> {
    session.track_request(request);
    let raw = serde_json::to_string(request)?;
    let url = indexer_url.to_string();
    let id = request.id.clone();
    let response_str = tokio::task::spawn_blocking(move || forward_to_indexer_sync(&raw, &url))
        .await
        .context("sync forward task failed")?;
    let Some(resp_str) = response_str else {
        return Ok(serde_json::json!({
            "jsonrpc": "2.0",
            "error": { "code": -32000, "message": "Indexer temporarily unavailable" },
            "id": id
        }));
    };
    let mut msg: serde_json::Value = serde_json::from_str(resp_str.trim())?;
    if let Some((method, scripthash)) = session.pending_methods.remove(&id) {
        modify_upstream_response(&mut msg, &method, &scripthash, pool_manager, indexer_url);
    }
    Ok(msg)
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

    if request.method == "blockchain.transaction.broadcast" {
        if let Some(ref params) = request.params {
            if let Some(hex_param) = params_first_string(params) {
                if let Err(e) = intercept_and_handle_broadcast(
                    client_stream,
                    request.id.clone(),
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
                return Ok(serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": { "code": -32603, "message": "Broadcast handled out-of-band" },
                    "id": request.id
                }));
            }
        }
        return Ok(serde_json::json!({
            "jsonrpc": "2.0",
            "error": { "code": -32602, "message": "Invalid params" },
            "id": request.id
        }));
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

fn quick_broadcast_preview(tx_hex: &str) -> Result<BroadcastPreview> {
    let tx_hex_clean = tx_hex.trim();
    hex::decode(tx_hex_clean).context("Invalid transaction hex")?;
    let txid = pending::compute_txid(tx_hex_clean)?;
    Ok(BroadcastPreview { txid })
}

async fn notify_subscriptions(
    client_stream: &mut tokio::net::TcpStream,
    subscribed: &HashSet<String>,
    scripthashes: &[String],
    pool_manager: Arc<PoolManager>,
    indexer_url: String,
) -> Result<()> {
    for sh in scripthashes {
        if !subscribed.contains(sh) {
            continue;
        }
        let sh_clone = sh.clone();
        let pm = pool_manager.clone();
        let url = indexer_url.clone();
        let new_hash = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::task::spawn_blocking(move || {
                let real_history = fetch_scripthash_history_sync(&sh_clone, &url);
                let pending = pm.get_pending_txids_for_scripthash(&sh_clone);
                pending::compute_modified_status_hash(real_history, &sh_clone, &pending)
            }),
        )
        .await;

        let new_hash = match new_hash {
            Ok(Ok(h)) => h,
            Ok(Err(e)) => {
                tracing::warn!("Subscription notify task failed for {}: {}", &sh[..sh.len().min(16)], e);
                continue;
            }
            Err(_) => {
                tracing::warn!("Subscription notify timed out for {}", &sh[..sh.len().min(16)]);
                continue;
            }
        };

        if let Some(hash) = new_hash {
            tracing::info!(
                "Sending subscription notification for scripthash {}",
                &sh[..sh.len().min(16)]
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
        }
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
    session: &SessionState,
) -> Result<()> {
    tracing::info!(
        "INTERCEPTED blockchain.transaction.broadcast (hex len={})",
        hex_param.len()
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

    write_json_rpc_response(
        client_stream,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "result": preview.txid,
            "id": id
        }),
    )
    .await?;
    tracing::info!("Broadcast ack sent to wallet, txid={}", preview.txid);

    let pm = pool_manager.clone();
    let cfg = config.clone();
    let url = indexer_url.to_string();
    let src = source_label.to_string();
    let hex_owned = hex_param;

    let ingest = tokio::task::spawn_blocking(move || {
        handle_broadcast(&hex_owned, &pm, &cfg, &url, &src)
    })
    .await;

    match ingest {
        Ok(Ok(result)) => {
            tracing::info!("Broadcast ingested into pool, txid={}", result.txid);
            if result.retained {
                if let Err(e) = notify_subscriptions(
                    client_stream,
                    &session.subscribed_scripthashes,
                    &result.affected_scripthashes,
                    pool_manager.clone(),
                    indexer_url.to_string(),
                )
                .await
                {
                    tracing::warn!("Post-broadcast subscription notify failed: {}", e);
                }
            }
        }
        Ok(Err(e)) => tracing::error!(
            "Broadcast ack sent but pool ingest failed for {}: {}",
            preview.txid,
            e
        ),
        Err(e) => tracing::error!(
            "Broadcast ack sent but pool ingest task failed for {}: {}",
            preview.txid,
            e
        ),
    }
    Ok(())
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
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("Electrum listener [{}] bound on {}", source_label, addr);

    loop {
        match listener.accept().await {
            Ok((client_stream, peer_addr)) => {
                tracing::info!(
                    "Electrum client connected from {} [{}]",
                    peer_addr,
                    source_label
                );
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
            Err(e) => {
                tracing::error!("Failed to accept connection ({}): {}", source_label, e);
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
            tracing::warn!("Invalid client JSON from {}: {}", peer_addr, e);
            return Ok(());
        }
    };
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
        responses.push(
            handle_one_subrequest(
                req,
                pool_manager,
                config,
                indexer_url,
                indexer_stream,
                session,
                source_label,
                client_stream,
            )
            .await?,
        );
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
    let indexer_url = {
        let cfg = config.lock().map_err(|e| anyhow::anyhow!("Config lock: {}", e))?;
        cfg.indexer.as_ref().map(|idx| idx.url.clone()).unwrap_or_default()
    };
    let mut indexer_stream: Option<IndexerStream> = None;

    tracing::info!(
        "Electrum session started for {} [{}] (sync RPC responses)",
        peer_addr,
        source_label
    );

    let mut client_buf = Vec::new();
    let mut session = SessionState::new();

    loop {
        let n = client_stream.read_buf(&mut client_buf).await?;
        if n == 0 {
            break;
        }
        while let Some(newline_pos) = client_buf.iter().position(|&b| b == b'\n') {
            let line_bytes = client_buf[..newline_pos].to_vec();
            client_buf.drain(..=newline_pos);
            let line_str = String::from_utf8_lossy(&line_bytes).to_string();

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

async fn process_request(
    request: JsonRpcRequest,
    pool_manager: &Arc<PoolManager>,
    config: &Arc<Mutex<Config>>,
) -> serde_json::Value {
    let id = request.id.clone();
    tracing::debug!("Received Electrum request: method={}", request.method);

    let should_forward = matches!(
        request.method.as_str(),
        "blockchain.scripthash.get_balance"
            | "blockchain.scripthash.get_history"
            | "blockchain.scripthash.listunspent"
            | "blockchain.scripthash.subscribe"
            | "blockchain.scripthash.get_mempool"
            | "blockchain.transaction.get"
            | "blockchain.transaction.get_merkle"
            | "blockchain.block.header"
            | "blockchain.block.headers"
            | "blockchain.block.get_block"
            | "blockchain.transaction.id_from_pos"
            | "blockchain.headers.subscribe"
            | "blockchain.numblocks.subscribe"
    );

    if should_forward {
        let indexer_url = {
            let cfg = config.lock().ok();
            cfg.and_then(|c| c.indexer.as_ref().map(|i| i.url.clone()))
        };

        if let Some(indexer_url) = indexer_url {
            let raw_request = serde_json::to_string(&request).unwrap_or_default();
            let indexer_url_clone = indexer_url.clone();
            let method = request.method.clone();
            tracing::debug!("Forwarding {} to indexer at {}", method, indexer_url);

            let response_str = tokio::task::spawn_blocking(move || {
                forward_to_indexer_sync(&raw_request, &indexer_url_clone)
            }).await.unwrap_or(None);

            if let Some(response_str) = response_str {
                tracing::debug!("Indexer response for {} (first 200 chars): {}", method, &response_str[..response_str.len().min(200)]);
                match serde_json::from_str::<serde_json::Value>(&response_str) {
                    Ok(val) => return val,
                    Err(e) => {
                        tracing::warn!("Failed to parse indexer response for {}: {}", method, e);
                    }
                }
            } else {
                tracing::warn!("Failed to connect to indexer for {}", method);
            }
        }
    }

    match request.method.as_str() {
        "server.version" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": ["broadcast-pool v1.0", "1.4"],
                "id": id
            })
        }
        "server.banner" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": "broadcast-pool v1.0 - Bitcoin Transaction Pool",
                "id": id
            })
        }
        "server.ping" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": true,
                "id": id
            })
        }
        "server.features" => {
            let genesis = {
                config
                    .lock()
                    .ok()
                    .map(|c| c.network.network_type.genesis_hash().to_string())
                    .unwrap_or_else(|| "0000000000000000000000000000000000000000000000000000000000000000".to_string())
            };
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "protocol_version": "1.4",
                    "server_version": "broadcast-pool v1.0",
                    "genesis_hash": genesis,
                    "hosts": {},
                    "protocol_max": "1.4",
                    "protocol_min": "1.0",
                    "settings": {},
                    "hash_function": "sha256"
                },
                "id": id
            })
        }
        "blockchain.transaction.broadcast" => {
            if let Some(params) = request.params.as_ref().and_then(|p| p.as_array()) {
                if let Some(hex_param) = params.get(0).and_then(|v| v.as_str()) {
                    tracing::info!("broadcast request received, tx_hex length: {}", hex_param.len());
                    match handle_broadcast(hex_param, pool_manager, config, "", "sparrow") {
                        Ok(result) => {
                            tracing::info!("Broadcast success, returning txid: {}", result.txid);
                            return serde_json::json!({
                                "jsonrpc": "2.0",
                                "result": result.txid,
                                "id": id
                            });
                        }
                        Err(e) => {
                            tracing::error!("Broadcast failed: {}", e);
                            return serde_json::json!({
                                "jsonrpc": "2.0",
                                "error": {
                                    "code": -25,
                                    "message": e.to_string()
                                },
                                "id": id
                            });
                        }
                    }
                }
            }

            serde_json::json!({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32602,
                    "message": "Invalid params"
                },
                "id": id
            })
        }
        "blockchain.headers.subscribe" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "hex": "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
                    "height": 0
                },
                "id": id
            })
        }
        "blockchain.scripthash.get_balance" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "confirmed": 0,
                    "unconfirmed": 0
                },
                "id": id
            })
        }
        "blockchain.scripthash.get_history" | "blockchain.scripthash.get_mempool" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": [],
                "id": id
            })
        }
        "blockchain.scripthash.listunspent" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": [],
                "id": id
            })
        }
        "blockchain.scripthash.subscribe" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "confirmed": 0,
                    "unconfirmed": 0
                },
                "id": id
            })
        }
        "mempool.get_fee_histogram" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": [],
                "id": id
            })
        }
        "blockchain.relayfee" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": 1.0,
                "id": id
            })
        }
        "blockchain.estimatefee" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": 10.0,
                "id": id
            })
        }
        "blockchain.util.links" | "blockchain.scripthash.get_mempool" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": [],
                "id": id
            })
        }
        "blockchain.numblocks.subscribe" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": 0,
                "id": id
            })
        }
        "blockchain.transaction.get" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": "",
                "id": id
            })
        }
        "blockchain.block.get_block" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": "",
                "id": id
            })
        }
        "blockchain.block.header" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
                "id": id
            })
        }
        "blockchain.transaction.id_from_pos" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": null,
                "id": id
            })
        }
        "server.add_peer" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": true,
                "id": id
            })
        }
        "server.peers" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": [],
                "id": id
            })
        }
        "server.history" => {
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": [],
                "id": id
            })
        }
        _ => {
            tracing::warn!("Unhandled method: {}", request.method);
            serde_json::json!({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32601,
                    "message": format!("Method '{}' not found", request.method)
                },
                "id": id
            })
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
        tracing::info!(
            "Timestamp nLockTime {} → scheduled (MTP enforced when broadcasting)",
            nlocktime
        );
        return (BroadcastMode::Scheduled, None);
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
    indexer_url: &str,
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

    fn store_retained(
        pool_manager: &PoolManager,
        txid: &str,
        tx_hex: &str,
        indexer_url: &str,
    ) -> Result<Vec<String>> {
        let url = if indexer_url.is_empty() {
            pool_manager
                .get_indexer_url()
                .context("No indexer configured")?
        } else {
            indexer_url.to_string()
        };
        let indexer_addr = pending::strip_indexer_host(&url);
        let scripthashes = pending::extract_affected_scripthashes_opts(tx_hex, &indexer_addr, true)?;
        let outputs: Vec<PendingTxOutput> = pending::extract_outputs(tx_hex)?
            .into_iter()
            .map(|(output_index, value, scripthash)| PendingTxOutput {
                output_index,
                value,
                scripthash,
            })
            .collect();
        pool_manager.store_pending_tx(txid, tx_hex, scripthashes.clone(), outputs);
        Ok(scripthashes)
    }

    // Protocol: always retain in virtual mempool; scheduler emits to the network
    let scripthashes = store_retained(pool_manager, &txid, tx_hex_clean, indexer_url)?;

    if broadcast_mode == BroadcastMode::Immediate {
        if let Err(e) = pool_manager.mark_as_due(&imported_tx.id) {
            tracing::warn!("Failed to mark immediate tx as due: {}", e);
        }
    }

    tracing::info!(
        "Retained tx {} in virtual mempool (mode: {}, pool_id: {})",
        txid,
        broadcast_mode,
        imported_tx.id
    );

    Ok(BroadcastHandleResult {
        txid,
        retained: true,
        affected_scripthashes: scripthashes,
    })
}