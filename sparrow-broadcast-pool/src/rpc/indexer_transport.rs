//! Sync Electrum JSON-RPC transport with correct TLS for LAN IP addresses.

use anyhow::{Context, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as RustlsError, SignatureScheme};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
struct AcceptAnyCert;

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA256,
        ]
    }
}

fn strip_scheme(url: &str) -> &str {
    url.strip_prefix("tcp://")
        .or_else(|| url.strip_prefix("ssl://"))
        .unwrap_or(url)
}

fn tls_config_for_host(host: &str) -> ClientConfig {
    if host.parse::<std::net::IpAddr>().is_ok() {
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCert))
            .with_no_client_auth()
    } else {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    }
}

fn server_name(host: &str) -> Result<ServerName<'static>> {
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        Ok(ServerName::IpAddress(ip.into()))
    } else {
        ServerName::try_from(host.to_string())
            .map_err(|e| anyhow::anyhow!("Invalid TLS server name: {}", e))
    }
}

enum TransportStream {
    Plain(TcpStream),
    Tls(rustls::StreamOwned<rustls::ClientConnection, TcpStream>),
}

impl Read for TransportStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            TransportStream::Plain(s) => s.read(buf),
            TransportStream::Tls(s) => s.read(buf),
        }
    }
}

impl Write for TransportStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            TransportStream::Plain(s) => s.write(buf),
            TransportStream::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            TransportStream::Plain(s) => s.flush(),
            TransportStream::Tls(s) => s.flush(),
        }
    }
}

fn connect_timeout_secs() -> u64 {
    if std::env::var("BROADCAST_POOL_UMBREL")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
        || std::env::var("APP_ELECTRS_NODE_IP")
            .map(|v| !v.trim().is_empty() && !v.contains("${"))
            .unwrap_or(false)
    {
        2
    } else {
        5
    }
}

fn connect_stream(url: &str) -> Result<TransportStream> {
    let use_ssl = url.starts_with("ssl://");
    let addr = strip_scheme(url);
    let tcp = TcpStream::connect_timeout(
        &addr.parse().context("Invalid indexer address")?,
        Duration::from_secs(connect_timeout_secs()),
    )
    .with_context(|| format!("TCP connect failed ({})", addr))?;
    tcp.set_read_timeout(Some(Duration::from_secs(15)))?;
    tcp.set_write_timeout(Some(Duration::from_secs(10)))?;

    if use_ssl {
        let host = addr.split(':').next().unwrap_or(addr);
        let config = tls_config_for_host(host);
        let sn = server_name(host)?;
        let conn = rustls::ClientConnection::new(Arc::new(config), sn)
            .context("TLS client setup failed")?;
        Ok(TransportStream::Tls(rustls::StreamOwned::new(conn, tcp)))
    } else {
        Ok(TransportStream::Plain(tcp))
    }
}

fn read_json_line(stream: &mut TransportStream) -> Result<String> {
    let mut response = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = stream
            .read(&mut buf)
            .context("Read from indexer failed")?;
        if n == 0 {
            break;
        }
        response.extend_from_slice(&buf[..n]);
        if response.iter().any(|&b| b == b'\n') {
            break;
        }
    }
    String::from_utf8(response).context("Invalid UTF-8 from indexer")
}

/// Send one JSON-RPC request and return the raw JSON response line.
pub fn send_request(url: &str, request_json: &str) -> Result<String> {
    let mut stream = connect_stream(url)?;
    let mut payload = request_json.as_bytes().to_vec();
    payload.push(b'\n');
    stream
        .write_all(&payload)
        .context("Write to indexer failed")?;
    read_json_line(&mut stream)
}

/// JSON-RPC call; returns the `result` field on success.
pub fn json_rpc(
    url: &str,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value> {
    static ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let id = ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": id,
    });
    let line = send_request(url, &req.to_string())?;
    let parsed: serde_json::Value =
        serde_json::from_str(line.trim()).context("Invalid JSON from indexer")?;
    if let Some(err) = parsed.get("error") {
        anyhow::bail!("Indexer RPC error: {}", err);
    }
    parsed
        .get("result")
        .cloned()
        .context("Missing result in indexer response")
}

/// Electrum `server.version` requires `[client_name, protocol_version]`.
fn server_version_probe_params() -> serde_json::Value {
    serde_json::json!(["broadcast-pool", "1.4"])
}

/// First URL from candidates that responds to `server.version`.
pub fn probe_working_url(candidates: &[String]) -> Option<String> {
    let params = server_version_probe_params();
    for url in candidates {
        if json_rpc(url, "server.version", params.clone()).is_ok() {
            return Some(url.clone());
        }
    }
    None
}
