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
    let tcp = tokio::net::TcpStream::connect(&addr)
        .await
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
}

impl SessionState {
    fn new() -> Self {
        Self {
            subscribed_scripthashes: HashSet::new(),
            pending_methods: HashMap::new(),
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
            let real_history = fetch_scripthash_history_sync(scripthash, indexer_url);
            let pending = pool_manager.get_pending_txids_for_scripthash(scripthash);
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

async fn notify_subscriptions(
    client_stream: &mut tokio::net::TcpStream,
    _subscribed: &HashSet<String>,
    scripthashes: &[String],
    pool_manager: Arc<PoolManager>,
    indexer_url: String,
) -> Result<()> {
    for sh in scripthashes {
        let sh_clone = sh.clone();
        let pm = pool_manager.clone();
        let url = indexer_url.clone();
        let new_hash = tokio::task::spawn_blocking(move || {
            let real_history = fetch_scripthash_history_sync(&sh_clone, &url);
            let pending = pm.get_pending_txids_for_scripthash(&sh_clone);
            pending::compute_modified_status_hash(real_history, &sh_clone, &pending)
        })
        .await?;

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

async fn handle_connection(
    pool_manager: Arc<PoolManager>,
    config: Arc<Mutex<Config>>,
    mut client_stream: tokio::net::TcpStream,
    peer_addr: std::net::SocketAddr,
    source_label: &'static str,
) -> Result<()> {
    let indexer_url = {
        let cfg = config.lock().map_err(|e| anyhow::anyhow!("Config lock: {}", e))?;
        match &cfg.indexer {
            Some(idx) => idx.url.clone(),
            None => anyhow::bail!("No indexer configured"),
        }
    };

    let mut indexer_stream = match connect_indexer(&indexer_url).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("Indexer connect failed ({}): {}", indexer_url, e);
            return Ok(());
        }
    };
    tracing::debug!("Proxy connection: {} <-> indexer {}", peer_addr, indexer_url);

    let mut client_buf = Vec::new();
    let mut indexer_buf = Vec::new();
    let mut session = SessionState::new();

    loop {
        tokio::select! {
            result = client_stream.read_buf(&mut client_buf) => {
                match result {
                    Ok(0) => break,
                    Ok(_) => {
                        while let Some(newline_pos) = client_buf.iter().position(|&b| b == b'\n') {
                            let line_bytes = client_buf[..newline_pos].to_vec();
                            client_buf.drain(..=newline_pos);

                            let line_str = String::from_utf8_lossy(&line_bytes).to_string();

                            if line_str.trim().is_empty() {
                                continue;
                            }

                            if let Ok(request) = serde_json::from_str::<JsonRpcRequest>(&line_str) {

                                // === Intercept blockchain.transaction.get for pending txs ===
                                if request.method == "blockchain.transaction.get" {
                                    if let Some(params) = request.params.as_ref().and_then(|p| p.as_array()) {
                                        if let Some(txid) = params.get(0).and_then(|v| v.as_str()) {
                                            let verbose = params.get(1).and_then(|v| v.as_bool()).unwrap_or(false);
                                            if !verbose {
                                                if let Some(hex) = pool_manager.lookup_tx_hex(txid) {
                                                    tracing::info!("Serving retained tx {} from pool", &txid[..txid.len().min(16)]);
                                                    let response = serde_json::json!({
                                                        "jsonrpc": "2.0",
                                                        "result": hex,
                                                        "id": request.id
                                                    });
                                                    client_stream.write_all(&serde_json::to_vec(&response)?).await?;
                                                    client_stream.write_all(b"\n").await?;
                                                    continue;
                                                }
                                            }
                                        }
                                    }
                                }

                                // === Intercept blockchain.transaction.broadcast ===
                                if request.method == "blockchain.transaction.broadcast" {
                                    tracing::info!("INTERCEPTED blockchain.transaction.broadcast");
                                    let id = request.id.clone();

                                    if let Some(params) = request.params.as_ref().and_then(|p| p.as_array()) {
                                        if let Some(hex_param) = params.get(0).and_then(|v| v.as_str()) {
                                            tracing::info!("broadcast intercepted, tx_hex length: {}", hex_param.len());
                                            let hex_owned = hex_param.to_string();
                                            let pm = pool_manager.clone();
                                            let cfg = config.clone();
                                            let url = indexer_url.clone();
                                            let src = source_label.to_string();
                                            let broadcast_result = tokio::task::spawn_blocking(move || {
                                                handle_broadcast(&hex_owned, &pm, &cfg, &url, &src)
                                            }).await.unwrap_or_else(|e| Err(anyhow::anyhow!("Task join error: {}", e)));

                                            match broadcast_result {
                                                Ok(result) => {
                                                    tracing::info!("Broadcast successful, txid: {}", result.txid);
                                                    let response = serde_json::json!({
                                                        "jsonrpc": "2.0",
                                                        "result": result.txid,
                                                        "id": id
                                                    });
                                                    client_stream.write_all(&serde_json::to_vec(&response)?).await?;
                                                    client_stream.write_all(b"\n").await?;

                                                    if result.retained {
                                                        notify_subscriptions(
                                                            &mut client_stream,
                                                            &session.subscribed_scripthashes,
                                                            &result.affected_scripthashes,
                                                            pool_manager.clone(),
                                                            indexer_url.clone(),
                                                        ).await?;
                                                    }
                                                    continue;
                                                }
                                                Err(e) => {
                                                    tracing::error!("Broadcast intercepted but failed: {}", e);
                                                    let response = serde_json::json!({
                                                        "jsonrpc": "2.0",
                                                        "error": { "code": -25, "message": e.to_string() },
                                                        "id": id
                                                    });
                                                    client_stream.write_all(&serde_json::to_vec(&response)?).await?;
                                                    client_stream.write_all(b"\n").await?;
                                                    continue;
                                                }
                                            }
                                        }
                                    }

                                    let response = serde_json::json!({
                                        "jsonrpc": "2.0",
                                        "error": { "code": -32602, "message": "Invalid params" },
                                        "id": id
                                    });
                                    client_stream.write_all(&serde_json::to_vec(&response)?).await?;
                                    client_stream.write_all(b"\n").await?;
                                    continue;
                                }

                                // Track requests that need response modification
                                session.track_request(&request);

                                // === Everything else: forward to indexer ===
                                indexer_stream.write_all(&line_bytes).await?;
                                indexer_stream.write_all(b"\n").await?;
                            } else {
                                indexer_stream.write_all(&line_bytes).await?;
                                indexer_stream.write_all(b"\n").await?;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!("Client read error: {}", e);
                        break;
                    }
                }
            }
            result = indexer_stream.read_buf(&mut indexer_buf) => {
                match result {
                    Ok(0) => break,
                    Ok(_) => {
                        while let Some(newline_pos) = indexer_buf.iter().position(|&b| b == b'\n') {
                            let line_bytes = indexer_buf[..newline_pos].to_vec();
                            indexer_buf.drain(..=newline_pos);
                            let out = process_indexer_line(
                                &line_bytes,
                                &mut session,
                                &pool_manager,
                                &indexer_url,
                            )?;
                            client_stream.write_all(&out).await?;
                        }
                    }
                    Err(e) => {
                        tracing::debug!("Indexer read error: {}", e);
                        break;
                    }
                }
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
    current_block_height: Option<u64>,
) -> (BroadcastMode, Option<chrono::DateTime<chrono::Utc>>) {
    if source_label == "liana" {
        tracing::info!("Liana ingest → manual scheduling (pending until user sets date/price)");
        return (BroadcastMode::Manual, None);
    }
    if source_label == "sparrow" && nlocktime == 0 {
        tracing::info!("Sparrow ingest with nLockTime disabled → manual scheduling");
        return (BroadcastMode::Manual, None);
    }
    // Block-height nLockTime already satisfied at ingest → manual (date/price scheduling).
    // Future block-height or MTP locktimes keep by_block / scheduled behaviour.
    if nlocktime > 0 && nlocktime < 500_000_000 {
        if let Some(height) = current_block_height {
            if height >= nlocktime as u64 {
                tracing::info!(
                    "Ingest with block-height nLockTime {} already satisfied (chain at {}) → manual scheduling",
                    nlocktime,
                    height
                );
                return (BroadcastMode::Manual, None);
            }
        }
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

    let current_block_height = pool_manager.check_block_height().ok().flatten();
    let (broadcast_mode, scheduled_time) = {
        let cfg = config.lock().map_err(|e| anyhow::anyhow!("Config lock: {}", e))?;
        resolve_ingest_plan(source_label, nlocktime, &cfg, current_block_height)
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
        let scripthashes = pending::extract_affected_scripthashes(tx_hex, &indexer_addr)?;
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